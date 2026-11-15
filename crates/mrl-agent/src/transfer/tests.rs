//! Transfer client tests against a small local HTTP server (no object store needed).

use std::sync::{Arc, Mutex};

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use time::OffsetDateTime;

use super::*;

#[derive(Default)]
struct Server {
    data: Vec<u8>,
    drop_first_get: bool,
    fail_first_put: bool,
    ranges: Vec<Option<String>>,
    uploaded: Option<(Vec<u8>, Option<String>, Option<String>)>,
}

type Shared = Arc<Mutex<Server>>;

async fn get_obj(State(s): State<Shared>, headers: HeaderMap) -> Response {
    let mut s = s.lock().unwrap();
    let range = headers.get("range").map(|v| v.to_str().unwrap().to_owned());
    s.ranges.push(range.clone());
    let start: usize = range
        .as_deref()
        .and_then(|r| r.strip_prefix("bytes="))
        .and_then(|r| r.strip_suffix('-'))
        .map_or(0, |n| n.parse().unwrap());
    let len = s.data.len();
    if start >= len && start > 0 {
        return Response::builder().status(416).body(Body::empty()).unwrap();
    }
    let body = s.data[start..].to_vec();
    let mut resp = Response::builder()
        .header("content-length", body.len())
        .status(if start > 0 { 206 } else { 200 });
    if start > 0 {
        resp = resp.header("content-range", format!("bytes {start}-{}/{len}", len - 1));
    }
    if s.drop_first_get {
        s.drop_first_get = false;
        let half = Bytes::from(body[..body.len() / 2].to_vec());
        let chunks: Vec<std::io::Result<Bytes>> =
            vec![Ok(half), Err(std::io::Error::other("connection dropped"))];
        return resp
            .body(Body::from_stream(futures::stream::iter(chunks)))
            .unwrap();
    }
    resp.body(Body::from(body)).unwrap()
}

async fn put_obj(State(s): State<Shared>, headers: HeaderMap, body: Bytes) -> StatusCode {
    let mut s = s.lock().unwrap();
    if s.fail_first_put {
        s.fail_first_put = false;
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    let h = |n: &str| headers.get(n).map(|v| v.to_str().unwrap().to_owned());
    s.uploaded = Some((body.to_vec(), h(SHA256_HEADER), h("x-ticket-header")));
    StatusCode::OK
}

async fn serve(server: Server) -> (String, Shared) {
    let shared = Arc::new(Mutex::new(server));
    let app = Router::new()
        .route("/obj", get(get_obj).put(put_obj))
        .route("/forbidden", get(|| async { StatusCode::FORBIDDEN }))
        .route("/broken", get(|| async { StatusCode::BAD_GATEWAY }))
        .with_state(Arc::clone(&shared));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), shared)
}

fn ticket(url: &str, method: TicketMethod, max_size: u64) -> TransferTicket {
    TransferTicket {
        method,
        key: "orgs/o1/runs/r/obj".into(),
        max_size,
        expires_at: OffsetDateTime::now_utc() + Duration::from_secs(600),
        url: url.to_owned(),
        presigned: true,
        headers: [("x-ticket-header".to_owned(), "kept".to_owned())].into(),
    }
}

fn client(retries: u32, server_url: Option<String>) -> TransferClient {
    TransferClient::new(&TransferConfig {
        server_url,
        retries,
        ..TransferConfig::default()
    })
    .unwrap()
}

fn data() -> Vec<u8> {
    (0..200_000u32).map(|i| (i % 251) as u8).collect()
}

#[tokio::test]
async fn download_resumes_and_checks_the_hash() {
    let (base, shared) = serve(Server {
        data: data(),
        drop_first_get: true,
        ..Server::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("obj.bin");
    // A partial file from an earlier run.
    std::fs::write(dir.path().join("obj.bin.part"), &data()[..1000]).unwrap();
    let sha = mrl_storage::sha256_hex(&data());
    let t = ticket(&format!("{base}/obj"), TicketMethod::Get, 1 << 20);
    let r = client(3, None)
        .download(&t, &dest, Some(&sha))
        .await
        .unwrap();
    assert_eq!(r.bytes, 200_000);
    assert_eq!(r.sha256, sha);
    assert_eq!(r.resumed_from, 1000);
    assert_eq!(r.attempts, 2, "the dropped connection is resumed once");
    assert_eq!(std::fs::read(&dest).unwrap(), data());
    assert!(!dir.path().join("obj.bin.part").exists());
    let ranges = shared.lock().unwrap().ranges.clone();
    assert_eq!(ranges[0].as_deref(), Some("bytes=1000-"));
    // The second request resumes after what arrived before the drop (at most half was sent).
    let resumed: usize = ranges[1]
        .as_deref()
        .and_then(|r| r.strip_prefix("bytes="))
        .and_then(|r| r.strip_suffix('-'))
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        (1000..=1000 + (200_000 - 1000) / 2).contains(&resumed),
        "{resumed}"
    );
}

#[tokio::test]
async fn bad_content_and_refusals_fail_with_their_codes() {
    let (base, _) = serve(Server {
        data: data(),
        ..Server::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("obj.bin");
    let c = client(0, None);
    let url = format!("{base}/obj");

    let wrong = mrl_storage::sha256_hex(b"other");
    let t = ticket(&url, TicketMethod::Get, 1 << 20);
    let e = c.download(&t, &dest, Some(&wrong)).await.unwrap_err();
    assert_eq!(e.code(), "MRL-STORE-010");
    assert!(!dest.exists() && !dir.path().join("obj.bin.part").exists());

    let e = c
        .download(&ticket(&url, TicketMethod::Get, 10), &dest, None)
        .await
        .unwrap_err();
    assert_eq!(e.code(), "MRL-STORE-009", "bigger than the ticket allows");
    let e = c
        .download(&ticket(&url, TicketMethod::Put, 1 << 20), &dest, None)
        .await
        .unwrap_err();
    assert_eq!(e.code(), "MRL-STORE-009", "wrong method");

    let mut expired = ticket(&url, TicketMethod::Get, 1 << 20);
    expired.expires_at = OffsetDateTime::now_utc() - Duration::from_secs(1);
    assert_eq!(
        c.download(&expired, &dest, None).await.unwrap_err().code(),
        "MRL-STORE-008"
    );

    let t = ticket(&format!("{base}/forbidden"), TicketMethod::Get, 1 << 20);
    assert_eq!(
        c.download(&t, &dest, None).await.unwrap_err().code(),
        "MRL-STORE-007"
    );
    let t = ticket(&format!("{base}/broken"), TicketMethod::Get, 1 << 20);
    let e = c.download(&t, &dest, None).await.unwrap_err();
    assert_eq!(e.code(), "MRL-STORE-004");
    assert!(e.to_string().contains("HTTP 502"), "{e}");
    let t = ticket(&format!("{base}/missing"), TicketMethod::Get, 1 << 20);
    assert_eq!(
        c.download(&t, &dest, None).await.unwrap_err().code(),
        "MRL-STORE-003"
    );
}

#[tokio::test]
async fn upload_retries_and_sends_headers() {
    let (base, shared) = serve(Server {
        fail_first_put: true,
        ..Server::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("up.bin");
    std::fs::write(&src, data()).unwrap();

    // Relative (proxied) URL needs server_url.
    let t = ticket("/obj", TicketMethod::Put, 1 << 20);
    let e = client(1, None).upload(&t, &src).await.unwrap_err();
    assert_eq!(e.code(), "MRL-STORE-004");

    let r = client(1, Some(format!("{base}/")))
        .upload(&t, &src)
        .await
        .unwrap();
    assert_eq!(r.attempts, 2);
    let (body, sha, kept) = shared.lock().unwrap().uploaded.clone().unwrap();
    assert_eq!(body, data());
    assert_eq!(sha.as_deref(), Some(r.sha256.as_str()));
    assert_eq!(kept.as_deref(), Some("kept"), "ticket headers are sent");

    let t = ticket("/obj", TicketMethod::Put, 100);
    let e = client(0, Some(base)).upload(&t, &src).await.unwrap_err();
    assert_eq!(e.code(), "MRL-STORE-009");
}

#[tokio::test]
async fn bandwidth_limit_is_shared() {
    let c = TransferClient::new(&TransferConfig {
        bandwidth_bytes_per_sec: Some(10_000),
        ..TransferConfig::default()
    })
    .unwrap();
    let other = c.clone();
    let start = std::time::Instant::now();
    c.throttle(1000).await;
    other.throttle(1000).await;
    c.throttle(1000).await;
    // Three 1000-byte chunks at 10 kB/s: the third may start only after 200 ms.
    assert!(
        start.elapsed() >= Duration::from_millis(195),
        "{:?}",
        start.elapsed()
    );
}

//! Transfer client against SeaweedFS (`just dev`), through real mrl-storage tickets.
//!
//! GET tickets are presigned S3 URLs. PUT tickets always go through the server's proxy
//! endpoint (P19), so this test runs a minimal stand-in: it verifies the ticket and checks the
//! size and sha256 header, then writes with the store.
//!
//! `#[ignore]`d; `MRL_TEST_S3_ENDPOINT` overrides the endpoint. When it is down the test prints
//! SKIPPED and passes, unless `MRL_TEST_S3_REQUIRED=1`.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::put;
use mrl_agent::config::TransferConfig;
use mrl_agent::transfer::{TransferClient, SHA256_HEADER};
use mrl_storage::{verify_ticket, Backend, BackendConfig, Store, TicketKey, TicketMethod};

const BUCKET: &str = "mrl-agent-transfer";

async fn s3() -> Option<(Backend, TicketKey)> {
    let endpoint =
        std::env::var("MRL_TEST_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:8333".into());
    let hostport = endpoint
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_owned();
    let up = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect(&hostport),
    )
    .await;
    if !matches!(up, Ok(Ok(_))) {
        assert!(
            std::env::var("MRL_TEST_S3_REQUIRED").as_deref() != Ok("1"),
            "S3 endpoint {endpoint} is not reachable; start it with `just dev`"
        );
        eprintln!("SKIPPED: S3 endpoint {endpoint} is not reachable; start it with `just dev`");
        return None;
    }
    create_bucket(&endpoint).await;
    let config: BackendConfig = serde_json::from_value(serde_json::json!({
        "kind": "s3",
        "bucket": BUCKET,
        "endpoint": endpoint,
        "access_key_id": "mrl-dev",
        "secret_access_key": "mrl-dev-only",
        "allow_http": true,
    }))
    .unwrap();
    let key = TicketKey::new([9u8; 32]).unwrap();
    Some((
        Backend::connect(&config)
            .unwrap()
            .with_ticket_key(key.clone()),
        key,
    ))
}

/// `PUT /bucket` through a presigned URL (dev-only credentials from
/// deploy/local/seaweedfs/s3.json); "already exists" is fine.
async fn create_bucket(endpoint: &str) {
    use object_store::signer::{Method, Signer};
    let s3 = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(BUCKET)
        .with_region("us-east-1")
        .with_endpoint(endpoint)
        .with_access_key_id("mrl-dev")
        .with_secret_access_key("mrl-dev-only")
        .with_allow_http(true)
        .build()
        .unwrap();
    let url = s3
        .signed_url(
            Method::PUT,
            &object_store::path::Path::from(""),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    let status = reqwest::Client::new()
        .put(url.as_str())
        .send()
        .await
        .unwrap()
        .status()
        .as_u16();
    assert!(
        status == 200 || status == 409,
        "create bucket: HTTP {status}"
    );
}

#[derive(Clone)]
struct Proxy {
    backend: Arc<Backend>,
    key: TicketKey,
}

/// Stand-in for the server's `PUT /api/v1/transfer/{token}` (P19).
async fn proxy_put(
    State(p): State<Proxy>,
    Path(token): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let now = time::OffsetDateTime::now_utc();
    let Ok(claims) = verify_ticket(&p.key, &token, TicketMethod::Put, now) else {
        return StatusCode::FORBIDDEN;
    };
    if claims.check_size(body.len() as u64).is_err() {
        return StatusCode::PAYLOAD_TOO_LARGE;
    }
    let sha = headers.get(SHA256_HEADER).and_then(|v| v.to_str().ok());
    if sha != Some(mrl_storage::sha256_hex(&body).as_str()) {
        return StatusCode::BAD_REQUEST;
    }
    let store = p.backend.org(claims.org_id()).unwrap();
    match store.put(&claims.key, body).await {
        Ok(()) => StatusCode::OK,
        Err(_) => StatusCode::BAD_GATEWAY,
    }
}

async fn proxy(backend: Backend, key: TicketKey) -> String {
    let app = axum::Router::new()
        .route("/api/v1/transfer/{token}", put(proxy_put))
        .layer(axum::extract::DefaultBodyLimit::disable())
        .with_state(Proxy {
            backend: Arc::new(backend),
            key,
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn org(backend: &Backend) -> Store {
    backend.org("o1").unwrap()
}

#[tokio::test]
#[ignore = "needs SeaweedFS (just dev)"]
async fn download_and_upload_through_tickets() {
    let Some((backend, key)) = s3().await else {
        return;
    };
    let store = org(&backend);
    let data: Vec<u8> = (0..3_000_000u32).map(|i| (i * 7 % 256) as u8).collect();
    let sha = mrl_storage::sha256_hex(&data);
    let obj = format!("orgs/o1/runs/r-{}/blob.bin", std::process::id());
    store.put(&obj, Bytes::from(data.clone())).await.unwrap();

    let server = proxy(backend.clone(), key).await;
    let client = TransferClient::new(&TransferConfig {
        server_url: Some(server),
        bandwidth_bytes_per_sec: Some(200_000_000),
        ..TransferConfig::default()
    })
    .unwrap();
    let dir = tempfile::tempdir().unwrap();

    // Presigned GET, resumed from a partial file left by an earlier try.
    let ticket = store
        .transfer_ticket(&obj, TicketMethod::Get, 10 << 20, Duration::from_secs(300))
        .await
        .unwrap();
    assert!(ticket.presigned, "S3 GET tickets are presigned");
    let dest = dir.path().join("blob.bin");
    std::fs::write(dir.path().join("blob.bin.part"), &data[..1_000_000]).unwrap();
    let r = client.download(&ticket, &dest, Some(&sha)).await.unwrap();
    assert_eq!(r.resumed_from, 1_000_000);
    assert_eq!(r.sha256, sha);
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    // A wrong expected hash fails and leaves nothing behind.
    let dest2 = dir.path().join("again.bin");
    let wrong = mrl_storage::sha256_hex(b"no");
    let e = client
        .download(&ticket, &dest2, Some(&wrong))
        .await
        .unwrap_err();
    assert_eq!(e.code(), "MRL-STORE-010");
    assert!(!dest2.exists());

    // PUT through the proxied endpoint, then read it back.
    let up = format!("orgs/o1/runs/r-{}/up.bin", std::process::id());
    let put_ticket = store
        .transfer_ticket(&up, TicketMethod::Put, 10 << 20, Duration::from_secs(300))
        .await
        .unwrap();
    assert!(!put_ticket.presigned);
    let r = client.upload(&put_ticket, &dest).await.unwrap();
    assert_eq!(r.sha256, sha);
    assert_eq!(store.sha256(&up).await.unwrap(), sha);

    // A body bigger than the ticket allows is refused.
    let small = store
        .transfer_ticket(&up, TicketMethod::Put, 1, Duration::from_secs(300))
        .await
        .unwrap();
    let e = client.upload(&small, &dest).await.unwrap_err();
    assert_eq!(e.code(), "MRL-STORE-009");

    store.delete(&obj).await.unwrap();
    store.delete(&up).await.unwrap();
}

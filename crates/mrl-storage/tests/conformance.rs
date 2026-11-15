//! One conformance suite, run against every backend.
//!
//! `fs` runs with the unit tests. `s3` is `#[ignore]`d and needs SeaweedFS (`just dev`); run it
//! with `cargo nextest run -p mrl-storage --run-ignored all`. Set `MRL_TEST_S3_ENDPOINT` to use
//! another endpoint. When the endpoint is down the S3 cases print SKIPPED and pass, unless
//! `MRL_TEST_S3_REQUIRED=1` (set in CI), which makes them fail instead.

use std::time::Duration;

use bytes::Bytes;
use mrl_storage::*;
use object_store::signer::Url;
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MIB: u64 = 1 << 20;
const BUCKET: &str = "mrl-conformance";

struct Harness {
    backend: Backend,
    _dir: Option<tempfile::TempDir>,
}

mod harness {
    use super::*;

    pub async fn fs() -> Option<Harness> {
        let dir = tempfile::tempdir().unwrap();
        let backend = Backend::connect(&BackendConfig::Local {
            root: dir.path().join("objects"),
        })
        .unwrap();
        Some(Harness {
            backend,
            _dir: Some(dir),
        })
    }

    pub async fn s3() -> Option<Harness> {
        let endpoint = std::env::var("MRL_TEST_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:8333".into());
        let url = Url::parse(&endpoint).unwrap();
        let addr = (
            url.host_str().unwrap().to_owned(),
            url.port_or_known_default().unwrap(),
        );
        let up = tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(addr))
            .await;
        if !matches!(up, Ok(Ok(_))) {
            assert!(
                std::env::var("MRL_TEST_S3_REQUIRED").as_deref() != Ok("1"),
                "S3 endpoint {endpoint} is not reachable; start it with `just dev`"
            );
            eprintln!("SKIPPED: S3 endpoint {endpoint} is not reachable; start it with `just dev`");
            return None;
        }
        // Dev-only credentials from deploy/local/seaweedfs/s3.json.
        let config: BackendConfig = serde_json::from_value(serde_json::json!({
            "kind": "s3",
            "bucket": BUCKET,
            "endpoint": endpoint,
            "access_key_id": "mrl-dev",
            "secret_access_key": "mrl-dev-only",
            "allow_http": true,
        }))
        .unwrap();
        create_bucket(&endpoint).await;
        let backend = Backend::connect(&config).unwrap();
        Some(Harness {
            backend,
            _dir: None,
        })
    }

    /// `PUT /bucket` through a presigned URL; "already exists" is fine.
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
        let (status, body) = http("PUT", url.as_str(), b"").await;
        assert!(
            status == 200 || status == 409,
            "create bucket: HTTP {status} {}",
            String::from_utf8_lossy(&body)
        );
    }
}

/// Minimal HTTP/1.1 client for presigned URLs (keeps test deps small).
async fn http(method: &str, url: &str, body: &[u8]) -> (u16, Vec<u8>) {
    http_with(method, url, &[], body).await
}

/// Like [`http`], plus extra request headers.
async fn http_with(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, Vec<u8>) {
    let extra: String = headers
        .iter()
        .map(|(k, v)| format!("{k}: {v}\r\n"))
        .collect();
    let u = Url::parse(url).unwrap();
    let host = u.host_str().unwrap().to_owned();
    let port = u.port_or_known_default().unwrap();
    let target = &url[u.origin().ascii_serialization().len()..];
    let mut s = tokio::net::TcpStream::connect((host.as_str(), port))
        .await
        .unwrap();
    let head = format!(
        "{method} {target} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n",
        body.len()
    );
    s.write_all(head.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).await.unwrap();
    let status = std::str::from_utf8(&resp[9..12]).unwrap().parse().unwrap();
    let split = resp.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    (status, resp[split..].to_vec())
}

fn org() -> String {
    format!("t{}", uuid::Uuid::now_v7().simple())
}

/// Deterministic, non-repeating bytes so a misplaced part changes the hash.
fn pattern(len: u64, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i * 31 + i / 4099) as u8) ^ seed)
        .collect()
}

fn opts() -> TransferOptions {
    TransferOptions {
        part_size: 5 * MIB,
        concurrency: 3,
        ..TransferOptions::default()
    }
}

macro_rules! suite {
    ($backend:ident $(, #[$attr:meta])?) => {
        mod $backend {
            use super::*;

            macro_rules! case {
                ($name:ident) => {
                    #[tokio::test(flavor = "multi_thread")]
                    $(#[$attr])?
                    async fn $name() {
                        if let Some(h) = harness::$backend().await {
                            cases::$name(&h).await;
                        }
                    }
                };
            }

            case!(capabilities_are_honest);
            case!(put_get_list_delete);
            case!(multipart_round_trip);
            case!(resume_after_interrupted_upload);
            case!(resume_after_interrupted_download);
            case!(prefix_isolation);
            case!(ticket_scope_and_expiry);
            case!(checksum_verification);
        }
    };
}

suite!(fs);
suite!(s3, #[ignore = "needs SeaweedFS: run `just dev`"]);

mod cases {
    use super::*;

    pub async fn capabilities_are_honest(h: &Harness) {
        let c = h.backend.capabilities();
        let s = h.backend.org(&org()).unwrap();
        // presign <=> GET tickets are presigned URLs; PUT tickets always use the proxy.
        let key = s.keys().run_config("r").unwrap();
        s.put(&key, Bytes::from_static(b"x")).await.unwrap();
        let s = h
            .backend
            .clone()
            .with_ticket_key(TicketKey::new([3u8; 32]).unwrap())
            .org(s.keys().org_id())
            .unwrap();
        let ttl = Duration::from_secs(5);
        let get = s
            .transfer_ticket(&key, TicketMethod::Get, 1, ttl)
            .await
            .unwrap();
        assert_eq!(get.presigned, c.presign);
        let put = s
            .transfer_ticket(&key, TicketMethod::Put, 1, ttl)
            .await
            .unwrap();
        assert!(!put.presigned);
    }

    pub async fn put_get_list_delete(h: &Harness) {
        let s = h.backend.org(&org()).unwrap();
        let a = s.keys().run_samples("r1", 1).unwrap();
        let b = s.keys().run_samples("r1", 2).unwrap();
        let other = s.keys().run_config("r2").unwrap();
        s.put(&a, Bytes::from_static(b"hello world")).await.unwrap();
        s.put(&b, Bytes::from_static(b"two")).await.unwrap();
        s.put(&other, Bytes::from_static(b"cfg")).await.unwrap();

        assert_eq!(s.get(&a).await.unwrap(), Bytes::from_static(b"hello world"));
        assert_eq!(
            s.get_range(&a, 6..11).await.unwrap(),
            Bytes::from_static(b"world")
        );
        assert_eq!(s.head(&a).await.unwrap().size, 11);
        assert_eq!(s.sha256(&b).await.unwrap(), sha256_hex(b"two"));

        let prefix = format!("{}runs/r1/", s.keys().prefix());
        let listed: Vec<String> = s
            .list(&prefix)
            .await
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect();
        assert_eq!(listed, vec![a.clone(), b.clone()]);
        assert_eq!(s.list(s.keys().prefix()).await.unwrap().len(), 3);

        s.put(&a, Bytes::from_static(b"replaced")).await.unwrap();
        assert_eq!(s.get(&a).await.unwrap(), Bytes::from_static(b"replaced"));

        s.delete(&a).await.unwrap();
        s.delete(&a).await.unwrap();
        assert_eq!(s.get(&a).await.unwrap_err().code(), "MRL-STORE-003");
        assert_eq!(s.head(&a).await.unwrap_err().code(), "MRL-STORE-003");
        assert_eq!(s.list(&prefix).await.unwrap().len(), 1);
        assert!(s
            .list(&format!("{}runs/none/", s.keys().prefix()))
            .await
            .unwrap()
            .is_empty());
    }

    pub async fn multipart_round_trip(h: &Harness) {
        let s = h.backend.org(&org()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("weights.bin");
        let data = pattern(11 * MIB + 123, 1);
        std::fs::write(&src, &data).unwrap();
        let key = s
            .keys()
            .join(
                &s.keys().checkpoint_staging("r", "c", "a1").unwrap(),
                "weights.bin",
            )
            .unwrap();

        let up = s.upload_file(&src, &key, opts()).await.unwrap();
        assert_eq!(
            (up.size, up.parts, up.resumed_parts),
            (data.len() as u64, 3, 0)
        );
        assert_eq!(up.sha256, sha256_hex(&data));
        assert_eq!(s.head(&key).await.unwrap().size, data.len() as u64);
        assert_eq!(s.sha256(&key).await.unwrap(), up.sha256);
        assert!(!dir.path().join("weights.bin.mrl-upload.json").exists());
        // Staged parts (backends without native multipart) are cleaned up.
        let uploads = format!("{}uploads/", s.keys().prefix());
        assert!(s.list(&uploads).await.unwrap().is_empty());

        let dst = dir.path().join("copy.bin");
        let down = s
            .download_file(&key, &dst, Some(&up.sha256), opts())
            .await
            .unwrap();
        assert_eq!(
            (down.size, down.parts, down.resumed_parts),
            (data.len() as u64, 3, 0)
        );
        assert_eq!(std::fs::read(&dst).unwrap(), data);
        assert!(!dir.path().join("copy.bin.mrl-download.json").exists());
        assert!(!dir.path().join("copy.bin.mrl-part").exists());

        // Small and empty files take the single-request path.
        for len in [0, 10] {
            let small = dir.path().join(format!("small{len}"));
            std::fs::write(&small, pattern(len, 2)).unwrap();
            let k = s.keys().run_samples("r", len).unwrap();
            let out = s.upload_file(&small, &k, opts()).await.unwrap();
            assert_eq!((out.size, out.parts), (len, 1));
            let back = dir.path().join(format!("back{len}"));
            s.download_file(&k, &back, Some(&out.sha256), opts())
                .await
                .unwrap();
            assert_eq!(std::fs::read(&back).unwrap(), pattern(len, 2));
        }
    }

    pub async fn resume_after_interrupted_upload(h: &Harness) {
        let s = h.backend.org(&org()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("ckpt.bin");
        let data = pattern(12 * MIB, 3);
        std::fs::write(&src, &data).unwrap();
        let key = s
            .keys()
            .join(&s.keys().export("e1").unwrap(), "ckpt.bin")
            .unwrap();

        // First run: part 1 finishes, part 0 is cut off mid-flight, then the process "crashes".
        {
            let up = s.begin_upload(&src, &key, opts()).await.unwrap();
            assert_eq!(up.part_count(), 3);
            up.put_part(1).await.unwrap();
            let cut = tokio::time::timeout(Duration::from_micros(1), up.put_part(0)).await;
            assert!(cut.is_err() || cut.unwrap().is_ok());
        }
        assert!(
            dir.path().join("ckpt.bin.mrl-upload.json").exists(),
            "ledger survives the crash"
        );
        assert_eq!(
            s.head(&key).await.unwrap_err().code(),
            "MRL-STORE-003",
            "nothing is visible yet"
        );

        let out = s.upload_file(&src, &key, opts()).await.unwrap();
        assert!(
            out.resumed_parts >= 1,
            "the finished part is not sent again"
        );
        assert_eq!(s.sha256(&key).await.unwrap(), sha256_hex(&data));

        // A ledger for a file that changed since is ignored, and the upload starts over.
        {
            let up = s.begin_upload(&src, &key, opts()).await.unwrap();
            up.put_part(0).await.unwrap();
        }
        let changed = pattern(12 * MIB, 4);
        tokio::time::sleep(Duration::from_millis(20)).await;
        std::fs::write(&src, &changed).unwrap();
        let out = s.upload_file(&src, &key, opts()).await.unwrap();
        assert_eq!(out.resumed_parts, 0);
        assert_eq!(s.sha256(&key).await.unwrap(), sha256_hex(&changed));

        // A file that changes during the upload is refused.
        let up = s.begin_upload(&src, &key, opts()).await.unwrap();
        up.put_part(0).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        std::fs::write(&src, pattern(12 * MIB, 5)).unwrap();
        assert_eq!(up.finish().await.unwrap_err().code(), "MRL-STORE-013");
        assert_eq!(
            s.sha256(&key).await.unwrap(),
            sha256_hex(&changed),
            "old object untouched"
        );
    }

    pub async fn resume_after_interrupted_download(h: &Harness) {
        let s = h.backend.org(&org()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let data = pattern(11 * MIB, 6);
        let key = s.keys().eval_per_sample("ev").unwrap();
        s.put(&key, Bytes::from(data.clone())).await.unwrap();
        let dst = dir.path().join("per_sample.parquet");
        {
            let d = s.begin_download(&key, &dst, opts()).await.unwrap();
            d.fetch_part(2).await.unwrap();
            let _ = tokio::time::timeout(Duration::from_micros(1), d.fetch_part(0)).await;
        }
        assert!(dir
            .path()
            .join("per_sample.parquet.mrl-download.json")
            .exists());
        let out = s
            .download_file(&key, &dst, Some(&sha256_hex(&data)), opts())
            .await
            .unwrap();
        assert!(out.resumed_parts >= 1);
        assert_eq!(std::fs::read(&dst).unwrap(), data);

        // The object changed since: the stale ledger is ignored.
        {
            let d = s.begin_download(&key, &dst, opts()).await.unwrap();
            d.fetch_part(0).await.unwrap();
        }
        let newer = pattern(11 * MIB, 7);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        s.put(&key, Bytes::from(newer.clone())).await.unwrap();
        let out = s
            .download_file(&key, &dst, Some(&sha256_hex(&newer)), opts())
            .await
            .unwrap();
        assert_eq!(out.resumed_parts, 0);
        assert_eq!(std::fs::read(&dst).unwrap(), newer);
    }

    pub async fn prefix_isolation(h: &Harness) {
        let (org_a, org_b) = (org(), org());
        let a = h.backend.org(&org_a).unwrap();
        let b = h.backend.org(&org_b).unwrap();
        let key_a = a.keys().run_config("r").unwrap();
        a.put(&key_a, Bytes::from_static(b"secret")).await.unwrap();

        assert!(b.list(b.keys().prefix()).await.unwrap().is_empty());
        assert_eq!(b.get(&key_a).await.unwrap_err().code(), "MRL-STORE-001");
        assert_eq!(b.head(&key_a).await.unwrap_err().code(), "MRL-STORE-001");
        assert_eq!(
            b.put(&key_a, Bytes::new()).await.unwrap_err().code(),
            "MRL-STORE-001"
        );
        assert_eq!(b.delete(&key_a).await.unwrap_err().code(), "MRL-STORE-001");
        assert_eq!(
            b.list(a.keys().prefix()).await.unwrap_err().code(),
            "MRL-STORE-001"
        );
        assert_eq!(b.list("orgs/").await.unwrap_err().code(), "MRL-STORE-001");
        let sneaky = format!(
            "{}../{org_a}/runs/r/config/resolved.yaml",
            b.keys().prefix()
        );
        assert_eq!(b.get(&sneaky).await.unwrap_err().code(), "MRL-STORE-001");
        assert_eq!(
            b.get(&format!("/{key_a}")).await.unwrap_err().code(),
            "MRL-STORE-001"
        );
        let ticket = b
            .transfer_ticket(&key_a, TicketMethod::Get, 6, Duration::from_secs(60))
            .await;
        assert_eq!(ticket.unwrap_err().code(), "MRL-STORE-001");
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            b.download_file(&key_a, dir.path().join("x"), None, opts())
                .await
                .unwrap_err()
                .code(),
            "MRL-STORE-001"
        );
        assert_eq!(a.get(&key_a).await.unwrap(), Bytes::from_static(b"secret"));
    }

    pub async fn ticket_scope_and_expiry(h: &Harness) {
        let tk = TicketKey::new(*b"0123456789abcdef0123456789abcdef").unwrap();
        let s = h
            .backend
            .clone()
            .with_ticket_key(tk.clone())
            .org(&org())
            .unwrap();
        let key = s.keys().model_manifest("m", "1").unwrap();
        let ttl = Duration::from_secs(300);
        let too_long = s
            .transfer_ticket(&key, TicketMethod::Put, 5, Duration::from_secs(16 * 60))
            .await;
        assert_eq!(too_long.unwrap_err().code(), "MRL-STORE-009");

        // PUT: always the validating proxy, on every backend.
        let put = s
            .transfer_ticket(&key, TicketMethod::Put, 5, ttl)
            .await
            .unwrap();
        assert_eq!(
            (put.key.as_str(), put.method, put.max_size),
            (key.as_str(), TicketMethod::Put, 5)
        );
        assert!(!put.presigned && put.headers.is_empty());
        let left = put.expires_at - OffsetDateTime::now_utc();
        assert!(left <= ttl && left > ttl - Duration::from_secs(30));
        let token = put.url.strip_prefix(PROXY_PATH).unwrap();
        let now = OffsetDateTime::now_utc();
        let claims = verify_ticket(&tk, token, TicketMethod::Put, now).unwrap();
        assert_eq!(
            (claims.key.as_str(), claims.org_id()),
            (key.as_str(), s.keys().org_id())
        );
        assert!(claims.check_size(5).is_ok());
        assert_eq!(claims.check_size(6).unwrap_err().code(), "MRL-STORE-009");
        let wrong_method = verify_ticket(&tk, token, TicketMethod::Get, now);
        assert_eq!(wrong_method.unwrap_err().code(), "MRL-STORE-009");
        assert!(matches!(
            verify_ticket(&tk, token, TicketMethod::Put, now + ttl),
            Err(StorageError::TicketExpired)
        ));
        let mut forged = token.to_owned();
        forged.insert(5, 'x');
        let forged = verify_ticket(&tk, &forged, TicketMethod::Put, now);
        assert_eq!(forged.unwrap_err().code(), "MRL-STORE-007");
        let other = TicketKey::new([9u8; 32]).unwrap();
        let other = verify_ticket(&other, token, TicketMethod::Put, now);
        assert_eq!(other.unwrap_err().code(), "MRL-STORE-007");
        // Without a ticket key, no PUT ticket can be issued.
        let plain = h.backend.org(s.keys().org_id()).unwrap();
        let err = plain
            .transfer_ticket(&key, TicketMethod::Put, 5, ttl)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "MRL-STORE-006");

        // GET: the size is checked when the ticket is issued.
        s.put(&key, Bytes::from_static(b"hello")).await.unwrap();
        let small = s.transfer_ticket(&key, TicketMethod::Get, 4, ttl).await;
        assert_eq!(small.unwrap_err().code(), "MRL-STORE-009");
        let get = s
            .transfer_ticket(&key, TicketMethod::Get, 5, ttl)
            .await
            .unwrap();
        assert_eq!(get.presigned, s.capabilities().presign);

        if get.presigned {
            // Presigned: the URL carries scope and expiry, and if-match pins this version.
            let url = Url::parse(&get.url).unwrap();
            assert!(url.path().ends_with(&key), "{url}");
            let q: Vec<(String, String)> = url.query_pairs().into_owned().collect();
            assert!(q.contains(&("X-Amz-Expires".into(), "300".into())), "{q:?}");
            let etag = get
                .headers
                .get("if-match")
                .expect("GET tickets pin the version");
            let (status, body) = http_with("GET", &get.url, &[("If-Match", etag)], b"").await;
            assert_eq!((status, body.as_slice()), (200, b"hello".as_slice()));
            assert_eq!(
                http("GET", &get.url, b"").await.0,
                403,
                "if-match is required"
            );
            // The GET URL does not allow a PUT, and a tampered signature fails.
            let put_try = http_with("PUT", &get.url, &[("If-Match", etag)], b"evil").await;
            assert_eq!(put_try.0, 403);
            let forged = get.url.replace("X-Amz-Expires=300", "X-Amz-Expires=900");
            assert_eq!(
                http_with("GET", &forged, &[("If-Match", etag)], b"")
                    .await
                    .0,
                403
            );
            // A bigger object written later cannot be read with the old ticket.
            s.put(&key, Bytes::from(vec![b'x'; 1000])).await.unwrap();
            let (status, _) = http_with("GET", &get.url, &[("If-Match", etag)], b"").await;
            assert_eq!(status, 412);
        } else {
            let token = get.url.strip_prefix(PROXY_PATH).unwrap();
            let claims = verify_ticket(&tk, token, TicketMethod::Get, now).unwrap();
            assert_eq!(claims.max_size, 5);
            // The proxy checks the object size it is about to send.
            s.put(&key, Bytes::from(vec![b'x'; 1000])).await.unwrap();
            let size = s.head(&key).await.unwrap().size;
            assert_eq!(claims.check_size(size).unwrap_err().code(), "MRL-STORE-009");
        }
    }

    pub async fn checksum_verification(h: &Harness) {
        let s = h.backend.org(&org()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("model");
        std::fs::create_dir_all(root.join("w")).unwrap();
        std::fs::write(root.join("w/model.safetensors"), pattern(6 * MIB, 8)).unwrap();
        std::fs::write(root.join("config.json"), b"{\"layers\":2}").unwrap();
        let specs = vec![
            FileSpec {
                path: "w/model.safetensors".into(),
                role: "weights".into(),
                format: Some("safetensors".into()),
            },
            FileSpec {
                path: "config.json".into(),
                role: "config".into(),
                format: None,
            },
        ];
        let m = ArtifactManifest::build(ArtifactKind::Model, &root, specs, None)
            .await
            .unwrap();
        assert_eq!(m.files[0].size, 6 * MIB);
        assert_eq!(m.files[1].sha256, sha256_hex(b"{\"layers\":2}"));

        let manifest_key = s.keys().model_manifest("m", "1").unwrap();
        let base = manifest_key.trim_end_matches("manifest.json").to_owned();
        for f in &m.files {
            s.upload_file(
                root.join(&f.path),
                &s.keys().join(&base, &f.path).unwrap(),
                opts(),
            )
            .await
            .unwrap();
        }
        s.put_manifest(&manifest_key, &m).await.unwrap();
        let back = s.get_manifest(&manifest_key).await.unwrap();
        assert_eq!(back, m);
        let at = |f: &ManifestFile| s.keys().join(&base, &f.path);
        back.verify(&s, true, at).await.unwrap();

        // Same size, different content: only a rehash notices.
        let cfg = s.keys().join(&base, "config.json").unwrap();
        s.put(&cfg, Bytes::from_static(b"{\"layers\":3}"))
            .await
            .unwrap();
        back.verify(&s, false, at).await.unwrap();
        match back.verify(&s, true, at).await.unwrap_err() {
            StorageError::ManifestMismatch(p) => {
                assert!(
                    matches!(&p[..], [FileProblem::HashMismatch { path, .. }] if path == "config.json"),
                    "{p:?}"
                )
            }
            e => panic!("unexpected {e}"),
        }
        s.put(&cfg, Bytes::from_static(b"{}")).await.unwrap();
        s.delete(&s.keys().join(&base, "w/model.safetensors").unwrap())
            .await
            .unwrap();
        match back.verify(&s, false, at).await.unwrap_err() {
            StorageError::ManifestMismatch(p) => assert!(
                matches!(
                    &p[..],
                    [
                        FileProblem::Missing { .. },
                        FileProblem::SizeMismatch {
                            expected: 12,
                            actual: 2,
                            ..
                        }
                    ]
                ),
                "{p:?}"
            ),
            e => panic!("unexpected {e}"),
        }

        // A download that does not match its expected hash is refused and removed.
        let dst = dir.path().join("cfg.json");
        let err = s
            .download_file(&cfg, &dst, Some(&m.files[1].sha256), opts())
            .await
            .unwrap_err();
        assert_eq!(err.code(), "MRL-STORE-010");
        assert!(!dst.exists());
        assert!(!dir.path().join("cfg.json.mrl-part").exists());
        assert!(!dir.path().join("cfg.json.mrl-download.json").exists());

        // A failed download never replaces or deletes a file already at the destination.
        std::fs::write(&dst, b"keep me").unwrap();
        let err = s
            .download_file(&cfg, &dst, Some(&m.files[1].sha256), opts())
            .await
            .unwrap_err();
        assert_eq!(err.code(), "MRL-STORE-010");
        assert_eq!(std::fs::read(&dst).unwrap(), b"keep me");

        // A manifest that is not artifact-manifest/v1 is refused on read.
        s.put(
            &manifest_key,
            Bytes::from_static(b"{\"schema_version\":\"v0\"}"),
        )
        .await
        .unwrap();
        assert_eq!(
            s.get_manifest(&manifest_key).await.unwrap_err().code(),
            "MRL-STORE-012"
        );
    }
}

//! P19 integration tests: uploads, tickets, artifacts, lineage and secrets over real HTTP.
//!
//! Compiled by `mrl-testkit` (see its `lib.rs`, P19 marker), not by this crate: they need
//! `TestServer` and `TestDb`, which depend on `mrl-server`. `#[ignore]`d; run them with
//! `just dev && just test-integration`. The SeaweedFS cases print SKIPPED and pass when it is
//! down, unless `MRL_TEST_S3_REQUIRED=1`.

use crate::pg::TestDb;
use crate::seams::{self as ts, user, InMemorySecretStore, RecordingAuditSink};
use crate::server::{TestResponse, TestServer};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::Method;
use mrl_server::config::{Config, Mode};
use mrl_server::modules::artifacts::{ArtifactStore, ArtifactsModule, TICKET_KEY_NAME};
use mrl_server::modules::secrets::SecretResolver;
use mrl_server::seams::{
    Authorizer, Decision, RequestContext, Resource, SeamError, Secret, SecretStore,
};
use mrl_storage::{
    sha256_hex, sign_ticket, Backend, BackendConfig, TicketClaims, TicketKey, TicketMethod,
};
use serde_json::{json, Value};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const A: &str = "Bearer tok-a";
const B: &str = "Bearer tok-b";
const VIEWER: &str = "Bearer tok-viewer";
const BUCKET: &str = "mrl-p19-test";

#[derive(Clone, Copy, Debug)]
enum Kind {
    Fs,
    S3,
}

/// Shares one in-memory store between the server and the test.
struct Shared(Arc<InMemorySecretStore>);

#[async_trait]
impl SecretStore for Shared {
    async fn get(&self, name: &str) -> Result<Option<Secret>, SeamError> {
        self.0.get(name).await
    }
    async fn set(&self, name: &str, value: Secret) -> Result<(), SeamError> {
        self.0.set(name, value).await
    }
    async fn delete(&self, name: &str) -> Result<(), SeamError> {
        self.0.delete(name).await
    }
}

/// `usr-viewer` may not read runs (Hide); everyone else may do everything.
struct ViewerCantSeeRuns;

#[async_trait]
impl Authorizer for ViewerCantSeeRuns {
    async fn authorize(
        &self,
        ctx: &RequestContext,
        action: &str,
        _: &Resource,
    ) -> Result<Decision, SeamError> {
        Ok(
            if ctx.principal.id == "usr-viewer" && action == "runs.read" {
                Decision::Hide
            } else {
                Decision::Allow
            },
        )
    }
}

struct Env {
    srv: TestServer,
    db: TestDb,
    store: Arc<ArtifactStore>,
    resolver: SecretResolver,
    audit: Arc<RecordingAuditSink>,
    secrets: Arc<InMemorySecretStore>,
    backend: Backend,
    _dir: Option<tempfile::TempDir>,
}

impl Env {
    async fn start(kind: Kind) -> Option<Env> {
        let (config, dir) = match kind {
            Kind::Fs => {
                let dir = tempfile::tempdir().unwrap();
                (
                    BackendConfig::Local {
                        root: dir.path().to_owned(),
                    },
                    Some(dir),
                )
            }
            Kind::S3 => (s3_config().await?, None),
        };
        let db = TestDb::new().await;
        let module = ArtifactsModule::with_backend(db.db().clone(), config.clone());
        let (store, resolver) = (module.store(), module.secrets());
        let audit = Arc::new(RecordingAuditSink::default());
        let secrets = Arc::new(InMemorySecretStore::default());
        let builder = ts::team(&[
            ("tok-a", user("usr-a", "org-a")),
            ("tok-b", user("usr-b", "org-b")),
            ("tok-viewer", user("usr-viewer", "org-a")),
        ])
        .authorizer(ViewerCantSeeRuns)
        .audit_arc(audit.clone())
        .ids(mrl_server::seams::UuidV7Ids)
        .secrets(Shared(secrets.clone()));
        let mut c = Config::default();
        c.server.mode = Mode::Team;
        // Tests call `sweep` themselves; the hourly job would race them.
        c.server.roles = vec![mrl_server::config::Role::Api];
        let any = "127.0.0.1:0".parse().unwrap();
        (
            c.server.listeners.api,
            c.server.listeners.grpc,
            c.server.listeners.internal,
        ) = (any, any, any);
        for b in [
            &mut c.server.rate_limit.principal,
            &mut c.server.rate_limit.org,
        ] {
            (b.per_second, b.burst) = (100_000, 100_000);
        }
        let srv = TestServer::start(c, ts::build(builder, Mode::Team), vec![Box::new(module)])
            .await
            .unwrap_or_else(|e| panic!("{}", e.explain()));
        Some(Env {
            srv,
            db,
            store,
            resolver,
            audit,
            secrets,
            backend: Backend::connect(&config).unwrap(),
            _dir: dir,
        })
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        auth: &str,
        body: Option<Value>,
    ) -> TestResponse {
        let (headers, body): (Vec<(&str, &str)>, Body) = match &body {
            Some(v) => (
                vec![
                    ("authorization", auth),
                    ("content-type", "application/json"),
                ],
                Body::from(v.to_string()),
            ),
            None => (vec![("authorization", auth)], Body::empty()),
        };
        self.srv.request(method, path, &headers, body).await
    }

    async fn put_bytes(
        &self,
        path: &str,
        auth: Option<&str>,
        data: Vec<u8>,
        extra: &[(&str, &str)],
    ) -> TestResponse {
        let mut headers: Vec<(&str, &str)> = extra.to_vec();
        if let Some(a) = auth {
            headers.push(("authorization", a));
        }
        self.srv
            .request(Method::PUT, path, &headers, Body::from(data))
            .await
    }
}

async fn s3_config() -> Option<BackendConfig> {
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
    create_bucket(&endpoint, &hostport).await;
    Some(
        serde_json::from_value(json!({
            "kind": "s3", "bucket": BUCKET, "endpoint": endpoint,
            "access_key_id": "mrl-dev", "secret_access_key": "mrl-dev-only", "allow_http": true,
        }))
        .unwrap(),
    )
}

/// `PUT /bucket` through a presigned URL (dev-only credentials); "already exists" is fine.
async fn create_bucket(endpoint: &str, hostport: &str) {
    use object_store::signer::{Method as M, Signer};
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
            M::PUT,
            &object_store::path::Path::from(""),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    let path = &url.as_str()[url.as_str().find(hostport).unwrap() + hostport.len()..];
    let addr = hostport.parse().unwrap();
    let status = TestServer::request_to(addr, Method::PUT, path, &[], Body::empty())
        .await
        .status;
    assert!(
        status == 200 || status == 409,
        "create bucket: HTTP {status}"
    );
}

fn data(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i * 31 % 251) as u8).collect()
}

fn code(r: &TestResponse) -> String {
    r.json()["code"].as_str().unwrap_or_default().to_owned()
}

fn token_of(ticket: &Value) -> String {
    ticket["url"].as_str().unwrap().to_owned()
}

async fn interrupted_upload_resumes(kind: Kind) {
    let Some(e) = Env::start(kind).await else {
        return;
    };
    let part = 2 << 20;
    let file = data(4 * part + 12_345); // 5 parts, stored in one put
    let big = data(8 * part + 77); // 9 parts, over 16 MiB: stored by `upload_file` (multipart on S3)
    for content in [file, big] {
        let sha = sha256_hex(&content);
        let r = e
            .call(
                Method::POST,
                "/api/v1/uploads",
                A,
                Some(json!({"filename": "d.jsonl", "size": content.len(), "sha256": sha})),
            )
            .await;
        assert_eq!(r.status, 201, "{}", r.text());
        let up = r.json();
        let id = up["id"].as_str().unwrap().to_owned();
        let count = up["part_count"].as_u64().unwrap() as usize;
        assert_eq!(up["part_size"], part);
        let chunk = |n: usize| content[n * part..((n + 1) * part).min(content.len())].to_vec();
        // First try: part 0 signed in, the last part through a ticket; then the "crash".
        let r = e
            .put_bytes(
                &format!("/api/v1/uploads/{id}/parts/0"),
                Some(A),
                chunk(0),
                &[],
            )
            .await;
        assert_eq!(r.status, 200, "{}", r.text());
        let r = e
            .call(
                Method::POST,
                &format!("/api/v1/uploads/{id}:presign"),
                A,
                Some(json!({"parts": [count - 1]})),
            )
            .await;
        assert_eq!(r.status, 200, "{}", r.text());
        let t = r.json()["tickets"][0]["ticket"].clone();
        assert_eq!(t["method"], "PUT");
        let r = e
            .put_bytes(&token_of(&t), None, chunk(count - 1), &[])
            .await;
        assert_eq!(r.status, 200, "{}", r.text());
        // Finishing too early names the missing parts.
        let r = e
            .call(
                Method::POST,
                &format!("/api/v1/uploads/{id}:complete"),
                A,
                None,
            )
            .await;
        assert_eq!(
            (r.status, code(&r).as_str()),
            (409, "MRL-STORE-190"),
            "{}",
            r.text()
        );
        assert!(r.json()["detail"].as_str().unwrap().contains("1 (missing)"));
        // Resume: ask what arrived, send the rest.
        let r = e
            .call(Method::GET, &format!("/api/v1/uploads/{id}"), A, None)
            .await;
        let got: Vec<u64> = serde_json::from_value(r.json()["parts_received"].clone()).unwrap();
        assert_eq!(got, vec![0, count as u64 - 1]);
        for n in 1..count - 1 {
            let r = e
                .put_bytes(
                    &format!("/api/v1/uploads/{id}/parts/{n}"),
                    Some(A),
                    chunk(n),
                    &[],
                )
                .await;
            assert_eq!(r.status, 200, "{}", r.text());
        }
        let r = e
            .call(
                Method::POST,
                &format!("/api/v1/uploads/{id}:complete"),
                A,
                None,
            )
            .await;
        assert_eq!(r.status, 200, "{}", r.text());
        let done = r.json();
        assert_eq!(done["state"], "COMPLETED");
        let blob = done["blob_key"].as_str().unwrap();
        let st = e.backend.org("org-a").unwrap();
        assert_eq!(st.sha256(blob).await.unwrap(), sha);
        let parts = st.keys().upload_parts(&id).unwrap();
        assert!(
            st.list(&parts).await.unwrap().is_empty(),
            "parts are deleted"
        );
        let r = e
            .put_bytes(
                &format!("/api/v1/uploads/{id}/parts/0"),
                Some(A),
                chunk(0),
                &[],
            )
            .await;
        assert_eq!(code(&r), "MRL-CONFLICT-191");
    }
    // A file whose parts do not hash to the declared sha256 is refused and stays open.
    let content = data(1000);
    let r = e
        .call(
            Method::POST,
            "/api/v1/uploads",
            A,
            Some(json!({"filename": "x", "size": 1000, "sha256": sha256_hex(b"other")})),
        )
        .await;
    let id = r.json()["id"].as_str().unwrap().to_owned();
    e.put_bytes(
        &format!("/api/v1/uploads/{id}/parts/0"),
        Some(A),
        content.clone(),
        &[],
    )
    .await;
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/uploads/{id}:complete"),
            A,
            None,
        )
        .await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (422, "MRL-STORE-010"),
        "{}",
        r.text()
    );
    let r = e
        .call(Method::GET, &format!("/api/v1/uploads/{id}"), A, None)
        .await;
    assert_eq!(r.json()["state"], "OPEN");
    // A part with a wrong `x-mrl-content-sha256` is refused.
    let r = e
        .put_bytes(
            &format!("/api/v1/uploads/{id}/parts/0"),
            Some(A),
            content,
            &[("x-mrl-content-sha256", &sha256_hex(b"no"))],
        )
        .await;
    assert_eq!(code(&r), "MRL-STORE-010");
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/uploads/{id}:abort"),
            A,
            None,
        )
        .await;
    assert_eq!(r.json()["state"], "ABORTED");
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn interrupted_upload_resumes_and_verifies_the_hash_fs() {
    interrupted_upload_resumes(Kind::Fs).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL and SeaweedFS (just dev)"]
async fn interrupted_upload_resumes_and_verifies_the_hash_s3() {
    interrupted_upload_resumes(Kind::S3).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn ticket_scope_is_enforced() {
    let e = Env::start(Kind::Fs).await.unwrap();
    let r = e
        .call(
            Method::POST,
            "/api/v1/uploads",
            A,
            Some(json!({"filename": "t", "size": 10, "sha256": sha256_hex(b"0123456789")})),
        )
        .await;
    let id = r.json()["id"].as_str().unwrap().to_owned();
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/uploads/{id}:presign"),
            A,
            None,
        )
        .await;
    let ticket = r.json()["tickets"][0]["ticket"].clone();
    let url = token_of(&ticket);
    let token = url.trim_start_matches("/api/v1/transfer/");
    // Wrong method.
    let r = e.srv.request(Method::GET, &url, &[], Body::empty()).await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (403, "MRL-STORE-009"),
        "{}",
        r.text()
    );
    // Over the size limit.
    let r = e.put_bytes(&url, None, data(11), &[]).await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (413, "MRL-STORE-009"),
        "{}",
        r.text()
    );
    // Signed with another key, or with a changed key in the payload.
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let claims = TicketClaims {
        key: ticket["key"].as_str().unwrap().to_owned(),
        method: TicketMethod::Put,
        max_size: 10,
        issued_at: now,
        expires_at: now + 300,
    };
    let other = sign_ticket(&TicketKey::new([3u8; 32]).unwrap(), &claims).unwrap();
    let (_, sig) = token.rsplit_once('.').unwrap();
    let forged_claims = TicketClaims {
        key: claims.key.replace("org-a", "org-b"),
        ..claims.clone()
    };
    let forged = sign_ticket(&TicketKey::new([3u8; 32]).unwrap(), &forged_claims).unwrap();
    let (forged_payload, _) = forged.rsplit_once('.').unwrap();
    for bad in [other, format!("{forged_payload}.{sig}")] {
        let r = e
            .put_bytes(&format!("/api/v1/transfer/{bad}"), None, data(10), &[])
            .await;
        assert_eq!(
            (r.status, code(&r).as_str()),
            (403, "MRL-STORE-007"),
            "{}",
            r.text()
        );
    }
    // Expired, signed with the server's own key.
    let key = e.secrets.get(TICKET_KEY_NAME).await.unwrap().unwrap();
    let key = TicketKey::new(key.expose().as_bytes().to_vec()).unwrap();
    let expired = sign_ticket(
        &key,
        &TicketClaims {
            issued_at: now - 900,
            expires_at: now - 1,
            ..claims.clone()
        },
    )
    .unwrap();
    let r = e
        .put_bytes(&format!("/api/v1/transfer/{expired}"), None, data(10), &[])
        .await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (403, "MRL-STORE-008"),
        "{}",
        r.text()
    );
    // The real ticket works.
    let r = e.put_bytes(&url, None, b"0123456789".to_vec(), &[]).await;
    assert_eq!(r.status, 200, "{}", r.text());
    // PUT tickets are not audited; nothing was downloaded.
    assert!(e
        .audit
        .events()
        .iter()
        .all(|a| a.action != "transfer.ticket_issued"));
}

/// Writes `files` under `prefix` of org-a and returns the manifest for them.
async fn put_files(e: &Env, prefix: &str, files: &[(&str, &[u8])]) -> Value {
    let st = e.backend.org("org-a").unwrap();
    let mut list = Vec::new();
    for (path, bytes) in files {
        st.put(&format!("orgs/org-a/{prefix}{path}"), bytes.to_vec().into())
            .await
            .unwrap();
        list.push(json!({"path": path, "size": bytes.len(), "sha256": sha256_hex(bytes), "role": "report"}));
    }
    json!({"schema_version": "artifact-manifest/v1", "kind": "eval", "created_at": "2026-11-16T00:00:00Z", "files": list})
}

async fn artifacts_register_and_download(kind: Kind) {
    let Some(e) = Env::start(kind).await else {
        return;
    };
    let manifest = put_files(
        &e,
        "evals/e1/",
        &[
            ("results.json", b"{\"score\": 1}"),
            ("notes.txt", b"hello world"),
        ],
    )
    .await;
    let ckpt = mrl_domain::ids::CheckpointId::new().to_string();
    let body = json!({"name": "eval-1", "prefix": "evals/e1/", "manifest": manifest, "links": [{"relation": "derived_from", "to": ckpt}]});
    let r = e
        .call(Method::POST, "/api/v1/artifacts", A, Some(body.clone()))
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    let art = r.json();
    assert_eq!(art["state"], "COMMITTED");
    let id = art["id"].as_str().unwrap().to_owned();
    // Same name again.
    let r = e
        .call(Method::POST, "/api/v1/artifacts", A, Some(body))
        .await;
    assert_eq!((r.status, code(&r).as_str()), (409, "MRL-CONFLICT-190"));
    // A manifest that does not match storage fails and frees its name.
    let mut wrong = manifest.clone();
    wrong["files"][1]["sha256"] = json!(sha256_hex(b"something else"));
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(json!({"name": "eval-2", "prefix": "evals/e1/", "manifest": wrong})),
        )
        .await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (422, "MRL-STORE-011"),
        "{}",
        r.text()
    );
    assert!(r.json()["detail"].as_str().unwrap().contains("notes.txt"));
    let missing = json!({"schema_version": "artifact-manifest/v1", "kind": "export", "created_at": "2026-11-16T00:00:00Z",
        "files": [{"path": "gone.bin", "size": 1, "sha256": sha256_hex(b"x"), "role": "weights"}]});
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(json!({"name": "eval-2", "manifest": missing})),
        )
        .await;
    assert_eq!(code(&r), "MRL-STORE-011");
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(json!({"name": "eval-2", "prefix": "evals/e1/", "manifest": manifest})),
        )
        .await;
    assert_eq!(
        r.status,
        201,
        "a failed registration frees the name: {}",
        r.text()
    );
    let r = e
        .call(Method::GET, "/api/v1/artifacts?kind=eval&limit=1", A, None)
        .await;
    assert_eq!(r.json()["items"].as_array().unwrap().len(), 1);
    assert!(r.json()["next_cursor"].is_string());
    // Download ticket: audited; proxied on FS (with Range), presigned on S3.
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/artifacts/{id}:download"),
            A,
            Some(json!({"path": "notes.txt"})),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let t = r.json();
    let issued: Vec<_> = e
        .audit
        .events()
        .into_iter()
        .filter(|a| a.action == "transfer.ticket_issued")
        .collect();
    assert_eq!(issued.len(), 1);
    assert_eq!(issued[0].actor, "usr-a");
    assert!(issued[0].target.ends_with("evals/e1/notes.txt"));
    match kind {
        Kind::Fs => {
            assert_eq!(t["presigned"], false);
            let r = e.srv.get(&token_of(&t)).await;
            assert_eq!((r.status, r.text().as_str()), (200, "hello world"));
            let r = e
                .srv
                .request(
                    Method::GET,
                    &token_of(&t),
                    &[("range", "bytes=6-")],
                    Body::empty(),
                )
                .await;
            assert_eq!((r.status, r.text().as_str()), (206, "world"));
            assert_eq!(r.headers["content-range"], "bytes 6-10/11");
            let r = e
                .srv
                .request(
                    Method::GET,
                    &token_of(&t),
                    &[("range", "bytes=11-")],
                    Body::empty(),
                )
                .await;
            assert_eq!(r.status, 416);
            let r = e
                .put_bytes(&token_of(&t), None, b"overwrite".to_vec(), &[])
                .await;
            assert_eq!((r.status, code(&r).as_str()), (403, "MRL-STORE-009"));
        }
        Kind::S3 => assert_eq!(t["presigned"], true),
    }
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/artifacts/{id}:download"),
            A,
            Some(json!({"path": "nope"})),
        )
        .await;
    assert_eq!(r.status, 422);
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn artifacts_register_verify_and_download_fs() {
    artifacts_register_and_download(Kind::Fs).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL and SeaweedFS (just dev)"]
async fn artifacts_register_verify_and_download_s3() {
    artifacts_register_and_download(Kind::S3).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn lineage_redacts_what_the_caller_cannot_see() {
    let e = Env::start(Kind::Fs).await.unwrap();
    let (run, dsv, ckpt) = (
        mrl_domain::ids::RunId::new().to_string(),
        mrl_domain::ids::DatasetVersionId::new().to_string(),
        mrl_domain::ids::CheckpointId::new().to_string(),
    );
    // run trained_on dsv; ckpt derived_from run; art derived_from ckpt (written on register).
    let mut tx = e.db.db().begin().await.unwrap();
    use mrl_server::modules::lineage::{LineageWriter, NodeRef, Relation};
    let n = |s: &str| NodeRef::parse(s).unwrap();
    LineageWriter::link(&mut tx, "org-a", n(&run), Relation::TrainedOn, n(&dsv))
        .await
        .unwrap();
    LineageWriter::link(&mut tx, "org-a", n(&ckpt), Relation::DerivedFrom, n(&run))
        .await
        .unwrap();
    // Writing an edge twice keeps one.
    LineageWriter::link(&mut tx, "org-a", n(&ckpt), Relation::DerivedFrom, n(&run))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let manifest = put_files(&e, "evals/l/", &[("r.json", b"{}")]).await;
    let r = e
        .call(Method::POST, "/api/v1/artifacts", A, Some(json!({"name": "l", "prefix": "evals/l/", "manifest": manifest, "links": [{"relation": "derived_from", "to": ckpt}]})))
        .await;
    let art = r.json()["id"].as_str().unwrap().to_owned();

    let path = format!("/api/v1/lineage/{art}?direction=upstream&depth=5");
    let full = e.call(Method::GET, &path, A, None).await.json();
    let ids: Vec<&str> = full["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        vec![art.as_str(), ckpt.as_str(), run.as_str(), dsv.as_str()]
    );
    assert_eq!(full["edges"].as_array().unwrap().len(), 3);

    let seen = e.call(Method::GET, &path, VIEWER, None).await;
    assert_eq!(seen.status, 200, "{}", seen.text());
    let text = seen.text();
    assert!(
        !text.contains(&run) && !text.contains(&dsv),
        "hidden ids never appear: {text}"
    );
    let g = seen.json();
    let nodes = g["nodes"].as_array().unwrap();
    assert_eq!(
        nodes.len(),
        3,
        "the run is redacted and not followed to the dataset"
    );
    assert_eq!(
        nodes[2],
        json!({"id": "hidden-2", "kind": null, "redacted": true})
    );
    assert_eq!(
        g["edges"][1],
        json!({"from": ckpt, "relation": "derived_from", "to": "hidden-2"})
    );
    let r = e
        .call(Method::GET, &format!("/api/v1/lineage/{run}"), VIEWER, None)
        .await;
    assert_eq!(r.status, 404, "a hidden root is not found");
    // Downstream from the dataset: the root plus the hidden run.
    let r = e
        .call(
            Method::GET,
            &format!("/api/v1/lineage/{dsv}?direction=downstream"),
            VIEWER,
            None,
        )
        .await;
    assert_eq!(r.json()["nodes"].as_array().unwrap().len(), 2);
    // Bad parameters.
    for q in ["direction=sideways", "depth=0", "depth=11"] {
        let r = e
            .call(Method::GET, &format!("/api/v1/lineage/{art}?{q}"), A, None)
            .await;
        assert_eq!(r.status, 422, "{q}");
    }
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn other_orgs_see_nothing() {
    let e = Env::start(Kind::Fs).await.unwrap();
    let r = e
        .call(
            Method::POST,
            "/api/v1/uploads",
            A,
            Some(json!({"filename": "a", "size": 3, "sha256": sha256_hex(b"abc")})),
        )
        .await;
    let up = r.json()["id"].as_str().unwrap().to_owned();
    let manifest = put_files(&e, "evals/x/", &[("a.json", b"{}")]).await;
    let ckpt = mrl_domain::ids::CheckpointId::new().to_string();
    let r = e
        .call(Method::POST, "/api/v1/artifacts", A, Some(json!({"name": "x", "prefix": "evals/x/", "manifest": manifest, "links": [{"relation": "derived_from", "to": ckpt}]})))
        .await;
    let art = r.json()["id"].as_str().unwrap().to_owned();
    let r = e
        .call(
            Method::POST,
            "/api/v1/secrets",
            A,
            Some(json!({"name": "hf", "kind": "hf_token", "allowed_uses": ["run"]})),
        )
        .await;
    let sec = r.json()["id"].as_str().unwrap().to_owned();
    for (m, p) in [
        (Method::GET, format!("/api/v1/uploads/{up}")),
        (Method::POST, format!("/api/v1/uploads/{up}:complete")),
        (Method::POST, format!("/api/v1/uploads/{up}:abort")),
        (Method::GET, format!("/api/v1/artifacts/{art}")),
        (Method::GET, format!("/api/v1/secrets/{sec}")),
        (Method::DELETE, format!("/api/v1/secrets/{sec}")),
    ] {
        let r = e.call(m.clone(), &p, B, None).await;
        assert_eq!(r.status, 404, "{m} {p}: {}", r.text());
    }
    let r = e
        .put_bytes(
            &format!("/api/v1/uploads/{up}/parts/0"),
            Some(B),
            b"abc".to_vec(),
            &[],
        )
        .await;
    assert_eq!(r.status, 404);
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/artifacts/{art}:download"),
            B,
            Some(json!({"path": "a.json"})),
        )
        .await;
    assert_eq!(r.status, 404);
    let r = e
        .call(
            Method::PUT,
            &format!("/api/v1/secrets/{sec}/value"),
            B,
            Some(json!({"value": "x"})),
        )
        .await;
    assert_eq!(r.status, 404);
    for p in ["/api/v1/artifacts", "/api/v1/secrets"] {
        let r = e.call(Method::GET, p, B, None).await;
        assert_eq!(r.json()["items"], json!([]), "{p}");
    }
    let r = e
        .call(Method::GET, &format!("/api/v1/lineage/{art}"), B, None)
        .await;
    assert_eq!(r.json()["edges"], json!([]), "org-a's edges stay in org-a");
    // org-b's own storage is separate from org-a's.
    assert!(e
        .backend
        .org("org-b")
        .unwrap()
        .list("orgs/org-b/")
        .await
        .unwrap()
        .is_empty());
}

/// Collects every log line written on this thread.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn canary_secret_never_leaks() {
    // Every log line of this test process (all levels) goes into `logs`. A thread-local
    // subscriber would miss log sites other tests already cached as unused.
    let logs = Logs::default();
    let sink = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("only this test sets a global logger");
    let canary = format!("canary-{}", uuid::Uuid::new_v4().simple());
    let e = Env::start(Kind::Fs).await.unwrap();
    let mut bodies = Vec::new();
    let r = e
        .call(Method::POST, "/api/v1/secrets", A, Some(json!({"name": "hf", "kind": "hf_token", "allowed_uses": ["run", "model_download"]})))
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    let sec = r.json()["id"].as_str().unwrap().to_owned();
    let r = e
        .call(
            Method::PUT,
            &format!("/api/v1/secrets/{sec}/value"),
            A,
            Some(json!({"value": canary})),
        )
        .await;
    assert_eq!((r.status, r.body.len()), (204, 0));
    // Bad bodies must not echo the value either.
    for bad in [
        format!("{{\"value\": \"{canary}\", \"extra\": 1}}"),
        format!("{{\"value\": {canary}}}"),
    ] {
        let r = e
            .srv
            .request(
                Method::PUT,
                &format!("/api/v1/secrets/{sec}/value"),
                &[("authorization", A), ("content-type", "application/json")],
                Body::from(bad),
            )
            .await;
        assert_eq!(r.status, 400);
        bodies.push(r.text());
    }
    let run = mrl_domain::ids::RunId::new();
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/secrets/{sec}/grants"),
            A,
            Some(json!({"run_id": run.to_string(), "mount": "env"})),
        )
        .await;
    assert_eq!(r.status, 204, "{}", r.text());
    for p in [
        format!("/api/v1/secrets/{sec}"),
        "/api/v1/secrets".to_owned(),
    ] {
        let r = e.call(Method::GET, &p, A, None).await;
        assert_eq!(r.status, 200);
        assert!(r.json().to_string().contains("rotated_at"));
        bodies.push(r.text());
    }
    // The resolver returns it, for allowed uses only.
    let store: Arc<dyn SecretStore> = Arc::new(Shared(e.secrets.clone()));
    let id: mrl_domain::ids::SecretId = sec.parse().unwrap();
    let got = e
        .resolver
        .resolve(Some(&store), "org-a", id, "model_download")
        .await
        .unwrap();
    assert_eq!(got.expose(), canary);
    let err = e
        .resolver
        .resolve(Some(&store), "org-a", id, "export")
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-PERM-190");
    assert!(
        e.resolver
            .resolve(Some(&store), "org-b", id, "run")
            .await
            .is_err(),
        "other orgs can't resolve it"
    );
    let granted = e
        .resolver
        .resolve_for_run(Some(&store), "org-a", run)
        .await
        .unwrap();
    assert_eq!((granted.len(), granted[0].mount.as_str()), (1, "env"));
    assert_eq!(granted[0].value.expose(), canary);
    assert!(!format!("{granted:?}").contains(&canary));
    assert_eq!(
        e.resolver
            .resolve(None, "org-a", id, "run")
            .await
            .unwrap_err()
            .code(),
        "MRL-ENV-190"
    );
    // A secret without the run use can't be granted.
    let r = e
        .call(
            Method::POST,
            "/api/v1/secrets",
            A,
            Some(json!({"name": "cloud", "kind": "generic", "allowed_uses": ["export"]})),
        )
        .await;
    let other = r.json()["id"].as_str().unwrap().to_owned();
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/secrets/{other}/grants"),
            A,
            Some(json!({"run_id": run.to_string(), "mount": "file"})),
        )
        .await;
    assert_eq!(code(&r), "MRL-PERM-190");
    // `env` secrets read the server's environment when resolved.
    let var = format!("MRL_P19_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let r = e
        .call(
            Method::POST,
            "/api/v1/secrets",
            A,
            Some(
                json!({"name": "env1", "kind": "generic", "allowed_uses": ["run"], "env_var": var}),
            ),
        )
        .await;
    let env_id: mrl_domain::ids::SecretId = r.json()["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(
        e.resolver
            .resolve(None, "org-a", env_id, "run")
            .await
            .unwrap_err()
            .code(),
        "MRL-RES-190"
    );
    // Everything a person or a scraper can read: responses, metrics, the audit log, logs.
    let internal = e.srv.running().internal_addr().unwrap();
    let metrics =
        TestServer::request_to(internal, Method::GET, "/metrics", &[], Body::empty()).await;
    assert_eq!(metrics.status, 200);
    bodies.push(metrics.text());
    bodies.push(format!("{:?}", e.audit.events()));
    let r = e
        .call(Method::DELETE, &format!("/api/v1/secrets/{sec}"), A, None)
        .await;
    assert_eq!(r.status, 204);
    assert!(
        e.secrets
            .get(&format!("mrl.secret.org-a.{sec}"))
            .await
            .unwrap()
            .is_none(),
        "the value is gone"
    );
    let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("listening"), "the capture works: {logs}");
    for text in bodies.iter().chain([&logs]) {
        assert!(!text.contains(&canary), "the canary leaked: {text}");
    }
    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM secrets_metadata s WHERE s::text LIKE '%' || $1 || '%'",
    )
    .bind(&canary)
    .fetch_one(e.db.pool())
    .await
    .unwrap();
    assert_eq!(rows, 0, "the value is never in PostgreSQL");
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn cleanup_expires_uploads_and_removes_old_parts() {
    let e = Env::start(Kind::Fs).await.unwrap();
    let r = e
        .call(
            Method::POST,
            "/api/v1/uploads",
            A,
            Some(json!({"filename": "a", "size": 3, "sha256": sha256_hex(b"abc")})),
        )
        .await;
    let old = r.json()["id"].as_str().unwrap().to_owned();
    e.put_bytes(
        &format!("/api/v1/uploads/{old}/parts/0"),
        Some(A),
        b"abc".to_vec(),
        &[],
    )
    .await;
    let r = e
        .call(
            Method::POST,
            "/api/v1/uploads",
            A,
            Some(json!({"filename": "b", "size": 3, "sha256": sha256_hex(b"xyz")})),
        )
        .await;
    let fresh = r.json()["id"].as_str().unwrap().to_owned();
    e.put_bytes(
        &format!("/api/v1/uploads/{fresh}/parts/0"),
        Some(A),
        b"xyz".to_vec(),
        &[],
    )
    .await;
    // `old` passed its 24 hours; its part (and a stray object) are 25 hours old.
    e.db.seed(&format!(
        "UPDATE uploads SET expires_at = now() - interval '1 hour' WHERE id = '{}'",
        old.parse::<mrl_domain::ids::UploadId>().unwrap().as_uuid()
    ))
    .await;
    let st = e.backend.org("org-a").unwrap();
    st.put(
        "orgs/org-a/uploads/stray/parts/000000",
        b"zz".to_vec().into(),
    )
    .await
    .unwrap();
    let root = match e._dir.as_ref() {
        Some(d) => d.path().to_owned(),
        None => unreachable!("fs backend"),
    };
    let aged = std::time::SystemTime::now() - Duration::from_secs(25 * 3600);
    for key in [
        format!("orgs/org-a/uploads/{old}/parts/000000"),
        "orgs/org-a/uploads/stray/parts/000000".to_owned(),
    ] {
        std::fs::File::options()
            .write(true)
            .open(root.join(&key))
            .unwrap()
            .set_modified(aged)
            .unwrap();
    }
    let swept = mrl_server::modules::uploads::sweep(&e.store).await.unwrap();
    assert_eq!((swept.expired, swept.objects), (1, 2));
    let r = e
        .call(Method::GET, &format!("/api/v1/uploads/{old}"), A, None)
        .await;
    assert_eq!(r.json()["state"], "EXPIRED");
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/uploads/{old}:complete"),
            A,
            None,
        )
        .await;
    assert_eq!(code(&r), "MRL-CONFLICT-191");
    let r = e
        .call(Method::GET, &format!("/api/v1/uploads/{fresh}"), A, None)
        .await;
    assert_eq!(
        r.json()["parts_received"],
        json!([0]),
        "open uploads keep their parts"
    );
    assert_eq!(e.db.count("uploads").await, 2);
}

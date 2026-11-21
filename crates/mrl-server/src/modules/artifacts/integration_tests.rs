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
use mrl_server::modules::artifacts::{
    ArtifactStore, ArtifactsModule, NewArtifact, NodeLookup, TICKET_KEY_NAME,
};
use mrl_server::modules::secrets::SecretResolver;
use mrl_server::seams::{
    Authorizer, Decision, RequestContext, RequestMeta, Resource, SeamError, Seams, Secret,
    SecretStore,
};
use mrl_storage::{
    sha256_hex, sign_ticket, Backend, BackendConfig, TicketClaims, TicketKey, TicketMethod,
};
use serde_json::{json, Value};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

const A: &str = "Bearer tok-a";
const B: &str = "Bearer tok-b";
const VIEWER: &str = "Bearer tok-viewer";
const A_IN_B: &str = "Bearer tok-a-b";
const BUCKET: &str = "mrl-p19-test";
/// Only the cleanup test uses it.
const SWEEP_BUCKET: &str = "mrl-p19-sweep";

#[derive(Clone, Copy, Debug)]
enum Kind {
    Fs,
    S3,
}

/// Switches the test flips on the shared secret store and the audit sink.
#[derive(Default)]
struct Ctl {
    /// Every call fails, like a store that does not answer.
    down: AtomicBool,
    /// Audit records fail.
    audit_down: AtomicBool,
    /// The next `set` reports through the sender, then waits for the receiver.
    hold: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

impl Ctl {
    fn check(&self) -> Result<(), SeamError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(SeamError::new("secrets", "the test store is down"));
        }
        Ok(())
    }
}

/// Shares one in-memory store between the server and the test.
#[derive(Clone)]
struct Shared(Arc<InMemorySecretStore>, Arc<Ctl>);

#[async_trait]
impl SecretStore for Shared {
    async fn get(&self, name: &str) -> Result<Option<Secret>, SeamError> {
        self.1.check()?;
        self.0.get(name).await
    }
    async fn set(&self, name: &str, value: Secret) -> Result<(), SeamError> {
        self.1.check()?;
        let hold = self.1.hold.lock().unwrap().take();
        if let Some((entered, release)) = hold {
            let _ = entered.send(());
            let _ = release.await;
        }
        self.0.set(name, value).await
    }
    async fn delete(&self, name: &str) -> Result<(), SeamError> {
        self.1.check()?;
        self.0.delete(name).await
    }
}

/// Records audit events unless the test turned it off.
struct Audit(Arc<RecordingAuditSink>, Arc<Ctl>);

#[async_trait]
impl mrl_server::seams::AuditSink for Audit {
    async fn record(&self, event: mrl_server::seams::AuditEvent) -> Result<(), SeamError> {
        if self.1.audit_down.load(Ordering::SeqCst) {
            return Err(SeamError::new("audit", "the test audit sink is down"));
        }
        self.0.record(event).await
    }
}

/// At most this many P19 test servers at once: each holds PostgreSQL connections, and the
/// whole workspace's integration tests share one server's connection limit.
fn permits() -> Arc<tokio::sync::Semaphore> {
    static P: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    P.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(4)))
        .clone()
}

/// The project whose items `usr-viewer` may not see.
const PRIVATE: &str = "private";

/// The project for which the authorizer fails when `usr-viewer` asks.
const BROKEN: &str = "broken";

/// `usr-viewer` may not read runs, nor anything recorded in project [`PRIVATE`] (Hide), and
/// its checks on project [`BROKEN`] fail; everyone else may do everything.
struct ViewerRules;

#[async_trait]
impl Authorizer for ViewerRules {
    async fn authorize(
        &self,
        ctx: &RequestContext,
        action: &str,
        res: &Resource,
    ) -> Result<Decision, SeamError> {
        let viewer = ctx.principal.id == "usr-viewer";
        if viewer && res.project.as_deref() == Some(BROKEN) {
            return Err(SeamError::new("authorizer", "the test authorizer is down"));
        }
        Ok(
            if viewer && (action == "runs.read" || res.project.as_deref() == Some(PRIVATE)) {
                Decision::Hide
            } else {
                Decision::Allow
            },
        )
    }
}

/// Stands in for the modules that own runs, datasets and checkpoints: it finds the ids the
/// test said exist, checked with the kind's read action like their GETs would be.
#[derive(Clone, Default)]
struct Known(Arc<Mutex<std::collections::HashSet<(String, uuid::Uuid)>>>);

struct KindLookup(&'static str, Known);

#[async_trait]
impl NodeLookup for KindLookup {
    async fn find(
        &self,
        org: &str,
        id: uuid::Uuid,
    ) -> Result<Option<(&'static str, Resource)>, mrl_server::modules::artifacts::Error> {
        let known = self.1 .0.lock().unwrap().contains(&(org.to_owned(), id));
        Ok(known.then(|| {
            (
                mrl_server::modules::lineage::read_action(self.0),
                Resource {
                    kind: self.0.to_owned(),
                    id: Some(id.to_string()),
                    org: Some(org.to_owned()),
                    project: None,
                },
            )
        }))
    }
}

/// The kinds the test lookups cover.
const LOOKED_UP: [&str; 3] = ["run", "dsv", "ckpt"];

struct Env {
    srv: TestServer,
    seams: Seams,
    known: Known,
    db: TestDb,
    store: Arc<ArtifactStore>,
    resolver: SecretResolver,
    audit: Arc<RecordingAuditSink>,
    secrets: Arc<InMemorySecretStore>,
    ctl: Arc<Ctl>,
    backend: Backend,
    scratch: tempfile::TempDir,
    _dir: Option<tempfile::TempDir>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Env {
    async fn start(kind: Kind) -> Option<Env> {
        Self::start_in(kind, true, BUCKET).await
    }

    /// A team server; `secret_store: false` starts it without a `SecretStore`.
    async fn start_with(kind: Kind, secret_store: bool) -> Option<Env> {
        Self::start_in(kind, secret_store, BUCKET).await
    }

    /// [`Env::start_with`] using S3 bucket `bucket` (one of its own for tests that sweep).
    async fn start_in(kind: Kind, secret_store: bool, bucket: &str) -> Option<Env> {
        let permit = permits().acquire_owned().await.unwrap();
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
            Kind::S3 => (s3_config(bucket).await?, None),
        };
        let db = TestDb::new().await;
        let scratch = tempfile::tempdir().unwrap();
        let module = ArtifactsModule::with_backend(
            db.db().clone(),
            config.clone(),
            Mode::Team,
            scratch.path().to_owned(),
        );
        let (store, resolver) = (module.store(), module.secrets());
        let audit = Arc::new(RecordingAuditSink::default());
        let secrets = Arc::new(InMemorySecretStore::default());
        let ctl = Arc::new(Ctl::default());
        let builder = ts::team(&[
            ("tok-a", user("usr-a", "org-a")),
            ("tok-b", user("usr-b", "org-b")),
            ("tok-viewer", user("usr-viewer", "org-a")),
            // The same person signed in to another org.
            ("tok-a-b", user("usr-a", "org-b")),
        ])
        .authorizer(ViewerRules)
        .audit(Audit(audit.clone(), ctl.clone()))
        .ids(mrl_server::seams::UuidV7Ids)
        .secrets(Shared(secrets.clone(), ctl.clone()));
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
        let known = Known::default();
        for k in LOOKED_UP {
            store.add_lineage_lookup(k, Arc::new(KindLookup(k, known.clone())));
        }
        let mut seams = ts::build(builder, Mode::Team);
        if !secret_store {
            seams.secrets = None;
        }
        let srv = TestServer::start(c, seams.clone(), vec![Box::new(module)])
            .await
            .unwrap_or_else(|e| panic!("{}", e.explain()));
        Some(Env {
            srv,
            seams,
            known,
            db,
            store,
            resolver,
            audit,
            secrets,
            ctl,
            backend: Backend::connect(&config).unwrap(),
            scratch,
            _dir: dir,
            _permit: permit,
        })
    }

    /// Tells the stand-in lookups that these ids exist in `org`.
    fn exists(&self, org: &str, ids: &[&str]) {
        let mut k = self.known.0.lock().unwrap();
        for id in ids {
            let n = mrl_server::modules::lineage::NodeRef::parse(id).unwrap();
            k.insert((org.to_owned(), n.id));
        }
    }

    /// `usr-a`'s context, for calls the server's own modules make.
    fn ctx_a(&self) -> RequestContext {
        RequestContext::new(
            user("usr-a", "org-a"),
            &RequestMeta {
                trace_id: "0".repeat(32),
                request_id: "test".to_owned(),
            },
        )
    }

    /// Registers like a module of the server does: from a storage folder.
    async fn register_from(
        &self,
        name: &str,
        prefix: &str,
        manifest: Value,
    ) -> Result<Value, mrl_server::modules::artifacts::Error> {
        let new: NewArtifact =
            serde_json::from_value(json!({"name": name, "prefix": prefix, "manifest": manifest}))
                .unwrap();
        let art = self
            .store
            .register(&self.seams, &self.ctx_a(), new, None)
            .await?;
        Ok(serde_json::to_value(art).unwrap())
    }

    /// Uploads `bytes` as `auth` (one part) and finishes it; returns the upload id.
    async fn upload(&self, auth: &str, bytes: &[u8], project: Option<&str>) -> String {
        let r = self
            .call(
                Method::POST,
                "/api/v1/uploads",
                auth,
                Some(json!({"filename": "f", "size": bytes.len(), "sha256": sha256_hex(bytes), "project_id": project})),
            )
            .await;
        assert_eq!(r.status, 201, "{}", r.text());
        let up = r.json()["id"].as_str().unwrap().to_owned();
        if !bytes.is_empty() {
            let r = self
                .put_bytes(
                    &format!("/api/v1/uploads/{up}/parts/0"),
                    Some(auth),
                    bytes.to_vec(),
                    &[],
                )
                .await;
            assert_eq!(r.status, 200, "{}", r.text());
        }
        let r = self
            .call(
                Method::POST,
                &format!("/api/v1/uploads/{up}:complete"),
                auth,
                None,
            )
            .await;
        assert_eq!(r.status, 200, "{}", r.text());
        up
    }

    /// Uploads `files` as `usr-a`; returns the manifest and the `uploads` map for them.
    async fn upload_files(&self, files: &[(&str, &[u8])]) -> (Value, Value) {
        let mut list = Vec::new();
        let mut uploads = serde_json::Map::new();
        for (path, bytes) in files {
            let up = self.upload(A, bytes, None).await;
            uploads.insert((*path).to_owned(), json!(up));
            list.push(json!({"path": path, "size": bytes.len(), "sha256": sha256_hex(bytes), "role": "report"}));
        }
        (
            json!({"schema_version": "artifact-manifest/v1", "kind": "eval", "created_at": "2026-11-16T00:00:00Z", "files": list}),
            Value::Object(uploads),
        )
    }

    /// The server's secret store, as the test sees it.
    fn store_seam(&self) -> Arc<dyn SecretStore> {
        Arc::new(Shared(self.secrets.clone(), self.ctl.clone()))
    }

    /// Scratch folders now present.
    fn scratch_dirs(&self) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(self.scratch.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect()
    }

    /// Waits until every scratch folder is gone (cleanup can finish just after a response).
    async fn scratch_cleaned(&self, what: &str) {
        for _ in 0..100 {
            if self.scratch_dirs().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("{what}: scratch left behind: {:?}", self.scratch_dirs());
    }

    /// Bytes behind a download ticket: through the server, or straight from the bucket
    /// when presigned.
    async fn fetch(&self, ticket: &Value) -> Vec<u8> {
        let url = token_of(ticket);
        let r = if ticket["presigned"] == true {
            let rest = url.split_once("://").unwrap().1;
            let (hostport, path) = rest.split_at(rest.find('/').unwrap());
            let addr = tokio::net::lookup_host(hostport)
                .await
                .unwrap()
                .next()
                .unwrap();
            let headers: Vec<(String, String)> = ticket["headers"]
                .as_object()
                .map(|h| {
                    h.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_owned()))
                        .collect()
                })
                .unwrap_or_default();
            let headers: Vec<(&str, &str)> = headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            TestServer::request_to(addr, Method::GET, path, &headers, Body::empty()).await
        } else {
            self.srv.get(&url).await
        };
        assert_eq!(r.status, 200, "{}", r.text());
        r.body.to_vec()
    }

    /// The ticket signing key the server keeps in the secret store.
    async fn ticket_key(&self) -> TicketKey {
        let key = self.secrets.get(TICKET_KEY_NAME).await.unwrap().unwrap();
        TicketKey::new(key.expose().as_bytes().to_vec()).unwrap()
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

    /// [`Env::call`] with extra headers.
    async fn call_h(
        &self,
        method: Method,
        path: &str,
        auth: &str,
        headers: &[(&str, &str)],
        body: Value,
    ) -> TestResponse {
        let mut h = vec![
            ("authorization", auth),
            ("content-type", "application/json"),
        ];
        h.extend_from_slice(headers);
        self.srv
            .request(method, path, &h, Body::from(body.to_string()))
            .await
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

async fn s3_config(bucket: &str) -> Option<BackendConfig> {
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
    create_bucket(&endpoint, &hostport, bucket).await;
    Some(
        serde_json::from_value(json!({
            "kind": "s3", "bucket": bucket, "endpoint": endpoint,
            "access_key_id": "mrl-dev", "secret_access_key": "mrl-dev-only", "allow_http": true,
        }))
        .unwrap(),
    )
}

/// `PUT /bucket` through a presigned URL (dev-only credentials); "already exists" is fine.
async fn create_bucket(endpoint: &str, hostport: &str, bucket: &str) {
    use object_store::signer::{Method as M, Signer};
    let s3 = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(bucket)
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
async fn ticket_scope_is_enforced_fs() {
    ticket_scope(Kind::Fs).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL and SeaweedFS (just dev)"]
async fn ticket_scope_is_enforced_s3() {
    ticket_scope(Kind::S3).await;
}

async fn ticket_scope(kind: Kind) {
    let Some(e) = Env::start(kind).await else {
        return;
    };
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
    let key = e.ticket_key().await;
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
    // A ticket PUT above the API's 2 MiB limit streams through (multipart above 16 MiB).
    let big = data(17 << 20);
    let big_claims = TicketClaims {
        key: format!(
            "orgs/org-a/runs/r-{}/big.bin",
            uuid::Uuid::new_v4().simple()
        ),
        max_size: 20 << 20,
        ..claims.clone()
    };
    let big_ticket = sign_ticket(&key, &big_claims).unwrap();
    let sha = sha256_hex(&big);
    let r = e
        .put_bytes(
            &format!("/api/v1/transfer/{big_ticket}"),
            None,
            big.clone(),
            &[("x-mrl-content-sha256", &sha)],
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["size"], big.len());
    e.scratch_cleaned("multipart upload").await;
    assert_eq!(
        e.backend
            .org("org-a")
            .unwrap()
            .sha256(&big_claims.key)
            .await
            .unwrap(),
        sha
    );
    // Over the ticket's own limit, and a wrong hash, are still refused.
    let small = sign_ticket(
        &key,
        &TicketClaims {
            max_size: 3 << 20,
            ..big_claims.clone()
        },
    )
    .unwrap();
    // A declared length over the limit is refused from the head alone (no body is sent,
    // so nothing races the early answer); without a declared length the stream is counted.
    let (status, text) = raw_put(
        &e,
        &format!("/api/v1/transfer/{small}"),
        RawBody::Declared(17 << 20),
    )
    .await;
    assert_eq!(status, 413, "{text}");
    assert!(text.contains("MRL-STORE-009"), "{text}");
    let tiny = sign_ticket(
        &key,
        &TicketClaims {
            max_size: 100,
            ..big_claims.clone()
        },
    )
    .unwrap();
    let (status, text) = raw_put(
        &e,
        &format!("/api/v1/transfer/{tiny}"),
        RawBody::Chunked(data(200)),
    )
    .await;
    assert_eq!(status, 413, "{text}");
    assert!(text.contains("MRL-STORE-009"), "{text}");
    let r = e
        .put_bytes(
            &format!("/api/v1/transfer/{big_ticket}"),
            None,
            data(3 << 20),
            &[("x-mrl-content-sha256", &sha)],
        )
        .await;
    assert_eq!(code(&r), "MRL-STORE-010");
    e.scratch_cleaned("hash failure").await;
    // A client that stops halfway: its scratch folder is private while it lasts, and goes.
    let stalled = async {
        use tokio::io::AsyncWriteExt;
        let mut tcp = tokio::net::TcpStream::connect(e.srv.api()).await.unwrap();
        let head = format!(
            "PUT /api/v1/transfer/{big_ticket} HTTP/1.1\r\nhost: test\r\ncontent-length: {}\r\n\r\n",
            4 << 20
        );
        tcp.write_all(head.as_bytes()).await.unwrap();
        tcp.write_all(&data(1 << 20)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;
        drop(tcp);
    };
    let look = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let dirs = e.scratch_dirs();
        assert_eq!(dirs.len(), 1, "one folder while receiving: {dirs:?}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&dirs[0]).unwrap().permissions().mode();
            assert_eq!(mode & 0o077, 0, "group and others can't open it: {mode:o}");
        }
    };
    tokio::join!(stalled, look);
    e.scratch_cleaned("cancelled upload").await;
    // Committed content is never a PUT target.
    let blob = TicketClaims {
        key: format!("orgs/org-a/blobs/sha256/{}", sha256_hex(b"x")),
        ..claims.clone()
    };
    let r = e
        .put_bytes(
            &format!("/api/v1/transfer/{}", sign_ticket(&key, &blob).unwrap()),
            None,
            b"x".to_vec(),
            &[],
        )
        .await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (403, "MRL-PERM-001"),
        "{}",
        r.text()
    );
    // Every other route keeps the 2 MiB limit.
    let (status, text) = raw_put(
        &e,
        &format!("/api/v1/uploads/{id}/parts/0"),
        RawBody::Declared((2 << 20) + 1),
    )
    .await;
    assert_eq!(status, 413, "{text}");
    // PUT tickets are not audited; nothing was downloaded.
    assert!(e
        .audit
        .events()
        .iter()
        .all(|a| a.action != "transfer.ticket_issued"));
}

/// How [`raw_put`] sends its body.
enum RawBody {
    /// Only a `content-length` header; no bytes follow.
    Declared(usize),
    /// These bytes, chunked, with no declared length.
    Chunked(Vec<u8>),
}

/// A PUT written by hand as `usr-a`, so the test controls exactly what is sent; returns the
/// status and the whole answer.
async fn raw_put(e: &Env, path: &str, body: RawBody) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut tcp = tokio::net::TcpStream::connect(e.srv.api()).await.unwrap();
    let head = |extra: String| {
        format!("PUT {path} HTTP/1.1\r\nhost: test\r\nauthorization: {A}\r\nconnection: close\r\n{extra}\r\n")
    };
    match body {
        RawBody::Declared(n) => {
            tcp.write_all(head(format!("content-length: {n}\r\n")).as_bytes())
                .await
                .unwrap();
        }
        RawBody::Chunked(bytes) => {
            let mut req = head("transfer-encoding: chunked\r\n".to_owned()).into_bytes();
            req.extend(format!("{:x}\r\n", bytes.len()).into_bytes());
            req.extend(bytes);
            req.extend(b"\r\n0\r\n\r\n");
            tcp.write_all(&req).await.unwrap();
        }
    }
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), tcp.read_to_end(&mut out)).await;
    let text = String::from_utf8_lossy(&out).into_owned();
    let status = text
        .split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    (status, text)
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
    let (manifest, uploads) = e
        .upload_files(&[
            ("results.json", b"{\"score\": 1}"),
            ("notes.txt", b"hello world"),
        ])
        .await;
    let body = json!({"name": "eval-1", "manifest": manifest, "uploads": uploads});
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
    // A manifest that does not match its upload fails and frees its name.
    let mut wrong = manifest.clone();
    wrong["files"][1]["sha256"] = json!(sha256_hex(b"something else"));
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(json!({"name": "eval-2", "manifest": wrong, "uploads": uploads})),
        )
        .await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (422, "MRL-STORE-011"),
        "{}",
        r.text()
    );
    assert!(r.json()["detail"].as_str().unwrap().contains("notes.txt"));
    // Every file needs an upload; storage folders are for the server's own modules.
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(json!({"name": "eval-2", "manifest": manifest})),
        )
        .await;
    assert_eq!(r.status, 422, "{}", r.text());
    assert!(r.text().contains("/uploads/"), "{}", r.text());
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(json!({"name": "eval-2", "prefix": "evals/e1/", "manifest": manifest})),
        )
        .await;
    assert_eq!(r.status, 422, "{}", r.text());
    assert!(r.text().contains("/prefix"), "{}", r.text());
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(json!({"name": "eval-2", "manifest": manifest, "uploads": uploads})),
        )
        .await;
    assert_eq!(
        r.status,
        201,
        "a failed registration frees the name; content is shared: {}",
        r.text()
    );
    let id2 = r.json()["id"].as_str().unwrap().to_owned();
    // The server's own modules register from a folder: the files are copied and checked.
    let folder = put_files(
        &e,
        "evals/e3/",
        &[("results.json", b"{\"score\": 3}"), ("notes.txt", b"third")],
    )
    .await;
    let id3 = e
        .register_from("eval-3", "evals/e3/", folder.clone())
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut bad = folder.clone();
    bad["files"][1]["sha256"] = json!(sha256_hex(b"other"));
    let err = e
        .register_from("eval-4", "evals/e3/", bad)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-STORE-011");
    let missing = json!({"schema_version": "artifact-manifest/v1", "kind": "export", "created_at": "2026-11-16T00:00:00Z",
        "files": [{"path": "gone.bin", "size": 1, "sha256": sha256_hex(b"x"), "role": "weights"}]});
    let err = e
        .register_from("eval-4", "evals/e3/", missing)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-STORE-011");
    // Staging areas are refused: their files are deleted or rewritten later.
    let up = e.upload(A, b"{\"score\": 1}", None).await;
    let part = json!({"schema_version": "artifact-manifest/v1", "kind": "eval", "created_at": "2026-11-16T00:00:00Z",
        "files": [{"path": "000000", "size": 12, "sha256": sha256_hex(b"{\"score\": 1}"), "role": "report"}]});
    let staged = put_files(
        &e,
        "runs/r/checkpoints/c/_staging/attempt-1/",
        &[("w.bin", b"w")],
    )
    .await;
    for (prefix, m) in [
        (format!("uploads/{up}/parts/"), part.clone()),
        (format!("UPLOADS/{up}/parts/"), part),
        (
            "runs/r/checkpoints/c/_staging/attempt-1/".to_owned(),
            staged,
        ),
    ] {
        let err = e.register_from("staged", &prefix, m).await.unwrap_err();
        assert!(format!("{err:?}").contains("staging"), "{prefix}: {err:?}");
    }
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
    let blob = issued[0].target.clone();
    assert!(
        blob.ends_with(&sha256_hex(b"hello world")) && blob.contains("/blobs/"),
        "downloads read the checked copy: {blob}"
    );
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
    // The reserved blob area can't be written, however it is spelled (on a case-insensitive
    // disk `BLOBS` is the same folder): neither issued nor redeemed.
    let key = e.ticket_key().await;
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    for spelling in ["/BLOBS/", "/Blobs/", "/blobs./"] {
        let target = blob.replacen("/blobs/", spelling, 1);
        let err = e
            .store
            .ticket(&e.seams, &e.ctx_a(), &target, TicketMethod::Put, 11)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "MRL-PERM-001", "{spelling}");
        let forged = sign_ticket(
            &key,
            &TicketClaims {
                key: target,
                method: TicketMethod::Put,
                max_size: 11,
                issued_at: now,
                expires_at: now + 300,
            },
        )
        .unwrap();
        let r = e
            .put_bytes(
                &format!("/api/v1/transfer/{forged}"),
                None,
                b"HELLO WORLD".to_vec(),
                &[],
            )
            .await;
        assert_eq!(r.status, 403, "{spelling}: {}", r.text());
    }
    // Rewriting a source folder (same length, other bytes) changes no committed artifact,
    // nor the ones sharing its content.
    put_files(
        &e,
        "evals/e3/",
        &[("results.json", b"{\"score\": 0}"), ("notes.txt", b"THIRD")],
    )
    .await;
    for (art, path, want) in [
        (&id, "notes.txt", &b"hello world"[..]),
        (&id, "results.json", b"{\"score\": 1}"),
        (&id2, "notes.txt", b"hello world"),
        (&id3, "notes.txt", b"third"),
        (&id3, "results.json", b"{\"score\": 3}"),
    ] {
        let r = e
            .call(
                Method::POST,
                &format!("/api/v1/artifacts/{art}:download"),
                A,
                Some(json!({"path": path})),
            )
            .await;
        assert_eq!(r.status, 200, "{}", r.text());
        assert_eq!(e.fetch(&r.json()).await, want, "{art} {path}");
    }
    // An artifact committed before files were copied (only its folder has them) gets its
    // checked copy on the first download.
    let legacy = uuid::Uuid::now_v7();
    // Unique bytes: the S3 test bucket outlives runs, so fixed content may already be stored.
    let legacy_owned = format!("legacy {}", uuid::Uuid::new_v4());
    let legacy_bytes = legacy_owned.as_bytes();
    let st = e.backend.org("org-a").unwrap();
    let legacy_blob = st.keys().blob(&sha256_hex(legacy_bytes)).unwrap();
    assert!(st.head(&legacy_blob).await.is_err(), "no copy yet");
    let legacy_manifest = put_files(&e, "old/l1/", &[("l.bin", legacy_bytes)]).await;
    e.db.seed(&format!(
        "INSERT INTO artifacts (id, org_id, name, kind, prefix, manifest, size_bytes, state, created_by, committed_at)
         VALUES ('{legacy}', 'org-a', 'legacy', 'eval', 'orgs/org-a/old/l1/', '{}', {}, 'COMMITTED', 'usr-a', now())",
        legacy_manifest, legacy_bytes.len()
    ))
    .await;
    let legacy_id = mrl_domain::ids::ArtifactId::from_uuid(legacy).to_string();
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/artifacts/{legacy_id}:download"),
            A,
            Some(json!({"path": "l.bin"})),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(e.fetch(&r.json()).await, legacy_bytes);
    assert!(st.head(&legacy_blob).await.is_ok(), "the copy was made");
    // A finished upload's stored file that is gone, or changed with the same size, is
    // caught when registering; the name stays free.
    for case in ["missing", "corrupted"] {
        let bytes = format!("{case} {}", uuid::Uuid::new_v4());
        let up = e.upload(A, bytes.as_bytes(), None).await;
        let blob = e
            .call(Method::GET, &format!("/api/v1/uploads/{up}"), A, None)
            .await
            .json()["blob_key"]
            .as_str()
            .unwrap()
            .to_owned();
        if case == "missing" {
            st.delete(&blob).await.unwrap();
        } else {
            st.put(&blob, bytes.to_uppercase().into_bytes().into())
                .await
                .unwrap();
        }
        let manifest = json!({"schema_version": "artifact-manifest/v1", "kind": "eval", "created_at": "2026-11-16T00:00:00Z",
            "files": [{"path": "b.txt", "size": bytes.len(), "sha256": sha256_hex(bytes.as_bytes()), "role": "report"}]});
        let r = e
            .call(
                Method::POST,
                "/api/v1/artifacts",
                A,
                Some(json!({"name": "from-blob", "manifest": manifest, "uploads": {"b.txt": up}})),
            )
            .await;
        assert_eq!(
            (r.status, code(&r).as_str()),
            (422, "MRL-STORE-011"),
            "{case}: {}",
            r.text()
        );
        let detail = r.json()["detail"].as_str().unwrap().to_owned();
        assert!(
            detail.contains(if case == "missing" {
                "missing"
            } else {
                "changed"
            }),
            "{detail}"
        );
    }
    let (fine, fine_up) = e.upload_files(&[("b.txt", b"fine")]).await;
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(json!({"name": "from-blob", "manifest": fine, "uploads": fine_up})),
        )
        .await;
    assert_eq!(r.status, 201, "the name was freed: {}", r.text());
    // A same-size corrupted object already at a content address is rewritten from the
    // checked bytes, never reused: when an upload finishes, and when a module registers
    // from a folder. Downloads return the right bytes.
    let download = |art: String, path: &'static str| {
        let e = &e;
        async move {
            let r = e
                .call(
                    Method::POST,
                    &format!("/api/v1/artifacts/{art}:download"),
                    A,
                    Some(json!({ "path": path })),
                )
                .await;
            assert_eq!(r.status, 200, "{}", r.text());
            e.fetch(&r.json()).await
        }
    };
    let good = format!("repair upload {}", uuid::Uuid::new_v4());
    let blob = st.keys().blob(&sha256_hex(good.as_bytes())).unwrap();
    st.put(&blob, good.to_uppercase().into_bytes().into())
        .await
        .unwrap();
    let up = e.upload(A, good.as_bytes(), None).await;
    assert_eq!(
        st.get(&blob).await.unwrap(),
        good.as_bytes(),
        "repaired on complete"
    );
    let manifest = json!({"schema_version": "artifact-manifest/v1", "kind": "eval", "created_at": "2026-11-16T00:00:00Z",
        "files": [{"path": "u.txt", "size": good.len(), "sha256": sha256_hex(good.as_bytes()), "role": "report"}]});
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(
                json!({"name": "repaired-upload", "manifest": manifest, "uploads": {"u.txt": up}}),
            ),
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    let art = r.json()["id"].as_str().unwrap().to_owned();
    assert_eq!(download(art, "u.txt").await, good.as_bytes());
    let good = format!("repair folder {}", uuid::Uuid::new_v4());
    let blob = st.keys().blob(&sha256_hex(good.as_bytes())).unwrap();
    st.put(&blob, good.to_uppercase().into_bytes().into())
        .await
        .unwrap();
    let folder = put_files(&e, "evals/repair/", &[("f.txt", good.as_bytes())]).await;
    let art = e
        .register_from("repaired-folder", "evals/repair/", folder)
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        st.get(&blob).await.unwrap(),
        good.as_bytes(),
        "repaired on register"
    );
    assert_eq!(download(art, "f.txt").await, good.as_bytes());
    e.scratch_cleaned("registration").await;
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
async fn lineage_redacts_what_the_caller_cannot_see_fs() {
    lineage_redacts_what_the_caller_cannot_see(Kind::Fs).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL and SeaweedFS (just dev)"]
async fn lineage_redacts_what_the_caller_cannot_see_s3() {
    lineage_redacts_what_the_caller_cannot_see(Kind::S3).await;
}

async fn lineage_redacts_what_the_caller_cannot_see(kind: Kind) {
    let Some(e) = Env::start(kind).await else {
        return;
    };
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
    e.exists("org-a", &[&run, &dsv, &ckpt]);
    let (manifest, uploads) = e.upload_files(&[("r.json", b"{}")]).await;
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(json!({"name": "l", "manifest": manifest, "uploads": uploads})),
        )
        .await;
    let art = r.json()["id"].as_str().unwrap().to_owned();
    // Callers can't link to checkpoints (only their module knows real ones); it does.
    let mut tx = e.db.db().begin().await.unwrap();
    LineageWriter::link(&mut tx, "org-a", n(&art), Relation::DerivedFrom, n(&ckpt))
        .await
        .unwrap();
    tx.commit().await.unwrap();

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
async fn lineage_follows_what_depends_on_what() {
    let e = Env::start(Kind::Fs).await.unwrap();
    let (run, dsv, ckpt) = (
        mrl_domain::ids::RunId::new().to_string(),
        mrl_domain::ids::DatasetVersionId::new().to_string(),
        mrl_domain::ids::CheckpointId::new().to_string(),
    );
    use mrl_server::modules::lineage::{LineageWriter, NodeRef, Relation};
    let n = |s: &str| NodeRef::parse(s).unwrap();
    e.exists("org-a", &[&run, &dsv, &ckpt]);
    let mut tx = e.db.db().begin().await.unwrap();
    LineageWriter::link(&mut tx, "org-a", n(&run), Relation::TrainedOn, n(&dsv))
        .await
        .unwrap();
    LineageWriter::link(&mut tx, "org-a", n(&run), Relation::Produced, n(&ckpt))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let ids = |g: &Value| -> Vec<String> {
        g["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["id"].as_str().unwrap().to_owned())
            .collect()
    };
    for dir in ["up", "upstream"] {
        let g = e
            .call(
                Method::GET,
                &format!("/api/v1/lineage/{ckpt}?direction={dir}"),
                A,
                None,
            )
            .await
            .json();
        assert_eq!(
            ids(&g),
            vec![ckpt.clone(), run.clone(), dsv.clone()],
            "{dir}"
        );
        // Edges keep their stored direction.
        assert_eq!(
            g["edges"][0],
            json!({"from": run, "relation": "produced", "to": ckpt})
        );
    }
    for dir in ["down", "downstream"] {
        let g = e
            .call(
                Method::GET,
                &format!("/api/v1/lineage/{dsv}?direction={dir}"),
                A,
                None,
            )
            .await
            .json();
        assert_eq!(
            ids(&g),
            vec![dsv.clone(), run.clone(), ckpt.clone()],
            "{dir}"
        );
    }
    // Nothing is upstream of the dataset, nothing downstream of the checkpoint.
    for (root, dir) in [(&dsv, "up"), (&ckpt, "down")] {
        let g = e
            .call(
                Method::GET,
                &format!("/api/v1/lineage/{root}?direction={dir}"),
                A,
                None,
            )
            .await
            .json();
        assert_eq!(ids(&g), vec![root.clone()], "{root} {dir}");
    }
    // `both` from the run reaches both ends.
    let g = e
        .call(Method::GET, &format!("/api/v1/lineage/{run}"), A, None)
        .await
        .json();
    assert_eq!(g["nodes"].as_array().unwrap().len(), 3);
    // The viewer can't see the run: the walk stops there.
    let g = e
        .call(
            Method::GET,
            &format!("/api/v1/lineage/{ckpt}?direction=up"),
            VIEWER,
            None,
        )
        .await;
    assert!(
        !g.text().contains(&run) && !g.text().contains(&dsv),
        "{}",
        g.text()
    );
    assert_eq!(
        g.json()["nodes"],
        json!([{"id": ckpt, "kind": "ckpt", "redacted": false}, {"id": "hidden-1", "kind": null, "redacted": true}])
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn links_need_a_visible_item_of_the_org() {
    let e = Env::start(Kind::Fs).await.unwrap();
    let register = |name: &str, project: Option<&str>, links: Value| {
        json!({"name": name, "project_id": project, "manifest": {"schema_version": "artifact-manifest/v1", "kind": "eval",
            "created_at": "2026-11-16T00:00:00Z", "files": []}, "links": links})
    };
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(register("open", None, json!([]))),
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    let open = r.json()["id"].as_str().unwrap().to_owned();
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(register("secret", Some(PRIVATE), json!([]))),
        )
        .await;
    let hidden = r.json()["id"].as_str().unwrap().to_owned();
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            B,
            Some(register("theirs", None, json!([]))),
        )
        .await;
    let foreign = r.json()["id"].as_str().unwrap().to_owned();
    let missing = mrl_domain::ids::ArtifactId::new().to_string();
    let run = mrl_domain::ids::RunId::new().to_string();
    // Hidden from the caller, another org's, made up, or a kind only its module links.
    for (i, to) in [&hidden, &foreign, &missing, &run].into_iter().enumerate() {
        let r = e
            .call(
                Method::POST,
                "/api/v1/artifacts",
                VIEWER,
                Some(register(
                    &format!("bad-{i}"),
                    None,
                    json!([{"relation": "derived_from", "to": to}]),
                )),
            )
            .await;
        let want = if to == &run { 422 } else { 404 };
        assert_eq!(r.status, want, "{to}: {}", r.text());
        assert!(!r.text().contains("theirs"), "{}", r.text());
    }
    assert_eq!(e.db.count("lineage_edges").await, 0, "no edge was written");
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            VIEWER,
            Some(register(
                "good",
                None,
                json!([{"relation": "derived_from", "to": open}]),
            )),
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    assert_eq!(e.db.count("lineage_edges").await, 1);
    // The foreign id is still nothing in org-a.
    let r = e
        .call(Method::GET, &format!("/api/v1/lineage/{foreign}"), A, None)
        .await;
    assert_eq!(r.status, 404, "{}", r.text());
    // A row made FAILED between the check and the commit links nothing.
    e.db.seed(
        "CREATE FUNCTION p19_skip() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
         CREATE TRIGGER p19_skip BEFORE UPDATE ON artifacts FOR EACH ROW
           WHEN (NEW.state = 'COMMITTED') EXECUTE FUNCTION p19_skip();",
    )
    .await;
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(register(
                "raced",
                None,
                json!([{"relation": "derived_from", "to": open}]),
            )),
        )
        .await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (409, "MRL-CONFLICT-190"),
        "{}",
        r.text()
    );
    // A link that fails to insert leaves no committed artifact.
    e.db.seed(
        "DROP TRIGGER p19_skip ON artifacts;
         CREATE FUNCTION p19_boom() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'no'; END $$;
         CREATE TRIGGER p19_boom BEFORE INSERT ON lineage_edges FOR EACH ROW EXECUTE FUNCTION p19_boom();",
    )
    .await;
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(register(
                "broken",
                None,
                json!([{"relation": "derived_from", "to": open}]),
            )),
        )
        .await;
    assert!(r.status >= 500, "{}", r.text());
    let states: Vec<(String, String)> = sqlx::query_as(
        "SELECT name, state FROM artifacts WHERE name IN ('raced', 'broken') ORDER BY name",
    )
    .fetch_all(e.db.pool())
    .await
    .unwrap();
    assert_eq!(
        states,
        vec![
            ("broken".into(), "FAILED".into()),
            ("raced".into(), "FAILED".into())
        ]
    );
    assert_eq!(
        e.db.count("lineage_edges").await,
        1,
        "still only the good link"
    );
    // Sizes that don't fit PostgreSQL are refused before anything is reserved.
    let max = i64::MAX as u64;
    for sizes in [vec![max + 1], vec![max, 1], vec![u64::MAX, 1]] {
        let files: Vec<Value> = sizes
            .iter()
            .enumerate()
            .map(|(i, s)| json!({"path": format!("f{i}"), "size": s, "sha256": sha256_hex(b"x"), "role": "other"}))
            .collect();
        let mut body = register("huge", None, json!([]));
        body["manifest"]["files"] = json!(files);
        let r = e
            .call(Method::POST, "/api/v1/artifacts", A, Some(body))
            .await;
        assert_eq!(r.status, 422, "{sizes:?}: {}", r.text());
        assert!(r.text().contains("/manifest/files"), "{}", r.text());
    }
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(register("huge", None, json!([]))),
        )
        .await;
    assert_eq!(r.status, 201, "the name stayed free: {}", r.text());
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn lists_show_only_what_a_get_would() {
    let e = Env::start(Kind::Fs).await.unwrap();
    let mut seen = Vec::new();
    let mut hidden = Vec::new();
    // Hidden rows first and between visible ones (ids grow in creation order).
    for (name, project) in [
        ("h1", Some(PRIVATE)),
        ("v1", None),
        ("h2", Some(PRIVATE)),
        ("h3", Some(PRIVATE)),
        ("v2", Some("shared")),
        ("v3", None),
        ("h4", Some(PRIVATE)),
    ] {
        let r = e
            .call(Method::POST, "/api/v1/artifacts", A, Some(json!({"name": name, "project_id": project,
                "manifest": {"schema_version": "artifact-manifest/v1", "kind": "eval", "created_at": "2026-11-16T00:00:00Z", "files": []}})))
            .await;
        assert_eq!(r.status, 201, "{}", r.text());
        let id = r.json()["id"].as_str().unwrap().to_owned();
        if project == Some(PRIVATE) {
            let r = e
                .call(
                    Method::GET,
                    &format!("/api/v1/artifacts/{id}"),
                    VIEWER,
                    None,
                )
                .await;
            assert_eq!(r.status, 404, "a GET hides it");
            hidden.push((name, id.parse::<mrl_domain::ids::ArtifactId>().unwrap()));
        }
    }
    let mut cursor: Option<String> = None;
    loop {
        let path = match &cursor {
            Some(c) => format!("/api/v1/artifacts?limit=1&cursor={c}"),
            None => "/api/v1/artifacts?limit=1".to_owned(),
        };
        let r = e.call(Method::GET, &path, VIEWER, None).await;
        assert_eq!(r.status, 200, "{}", r.text());
        let text = r.text();
        for (name, id) in &hidden {
            assert!(
                !text.contains(&id.to_string()) && !text.contains(&format!("\"{name}\"")),
                "{text}"
            );
        }
        let page = r.json();
        for item in page["items"].as_array().unwrap() {
            seen.push(item["name"].as_str().unwrap().to_owned());
        }
        match page["next_cursor"].as_str() {
            Some(c) => {
                let decoded = String::from_utf8(base64_decode(c)).unwrap();
                for (_, id) in &hidden {
                    assert!(
                        !decoded.contains(&id.as_uuid().to_string()),
                        "cursor {decoded}"
                    );
                }
                cursor = Some(c.to_owned());
            }
            None => break,
        }
    }
    assert_eq!(seen, vec!["v1", "v2", "v3"]);
    let all = e
        .call(Method::GET, "/api/v1/artifacts", A, None)
        .await
        .json();
    assert_eq!(
        all["items"].as_array().unwrap().len(),
        7,
        "the owner sees everything"
    );
    // Secret metadata: the same rule.
    for (name, project) in [("s-hidden", Some(PRIVATE)), ("s-open", None)] {
        let r = e
            .call(Method::POST, "/api/v1/secrets", A, Some(json!({"name": name, "kind": "generic", "allowed_uses": ["run"], "project_id": project})))
            .await;
        assert_eq!(r.status, 201, "{}", r.text());
        if project.is_some() {
            let id = r.json()["id"].as_str().unwrap().to_owned();
            let r = e
                .call(Method::GET, &format!("/api/v1/secrets/{id}"), VIEWER, None)
                .await;
            assert_eq!(r.status, 404);
        }
    }
    let r = e.call(Method::GET, "/api/v1/secrets", VIEWER, None).await;
    assert_eq!(r.json()["items"].as_array().unwrap().len(), 1);
    assert!(!r.text().contains("s-hidden"), "{}", r.text());
}

/// Unwraps a URL-safe, unpadded base64 cursor.
fn base64_decode(s: &str) -> Vec<u8> {
    let val = |c: u8| match c {
        b'A'..=b'Z' => c - b'A',
        b'a'..=b'z' => c - b'a' + 26,
        b'0'..=b'9' => c - b'0' + 52,
        b'-' => 62,
        b'_' => 63,
        _ => panic!("not base64url: {s}"),
    };
    let mut out = Vec::new();
    let (mut acc, mut bits) = (0u32, 0);
    for c in s.bytes() {
        acc = (acc << 6) | u32::from(val(c));
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    out
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn other_orgs_see_nothing_fs() {
    other_orgs_see_nothing(Kind::Fs).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL and SeaweedFS (just dev)"]
async fn other_orgs_see_nothing_s3() {
    other_orgs_see_nothing(Kind::S3).await;
}

async fn other_orgs_see_nothing(kind: Kind) {
    let Some(e) = Env::start(kind).await else {
        return;
    };
    let r = e
        .call(
            Method::POST,
            "/api/v1/uploads",
            A,
            Some(json!({"filename": "a", "size": 3, "sha256": sha256_hex(b"abc")})),
        )
        .await;
    let up = r.json()["id"].as_str().unwrap().to_owned();
    let (manifest, uploads) = e.upload_files(&[("a.json", b"{}")]).await;
    let ckpt = mrl_domain::ids::CheckpointId::new().to_string();
    e.exists("org-a", &[&ckpt]);
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(json!({"name": "x", "manifest": manifest, "uploads": uploads})),
        )
        .await;
    let art = r.json()["id"].as_str().unwrap().to_owned();
    {
        use mrl_server::modules::lineage::{LineageWriter, NodeRef, Relation};
        let mut tx = e.db.db().begin().await.unwrap();
        LineageWriter::link(
            &mut tx,
            "org-a",
            NodeRef::parse(&art).unwrap(),
            Relation::DerivedFrom,
            NodeRef::parse(&ckpt).unwrap(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
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
    assert_eq!(
        r.status,
        404,
        "another org's artifact is not a lineage root: {}",
        r.text()
    );
    let r = e
        .call(Method::GET, &format!("/api/v1/lineage/{ckpt}"), B, None)
        .await;
    assert_eq!(
        r.status, 404,
        "a node only org-a's edges know is not found in org-b"
    );
    // org-b can't link its artifacts to org-a's.
    let manifest_b = json!({"schema_version": "artifact-manifest/v1", "kind": "eval", "created_at": "2026-11-16T00:00:00Z", "files": []});
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            B,
            Some(json!({"name": "b", "manifest": manifest_b, "links": [{"relation": "derived_from", "to": art}]})),
        )
        .await;
    assert_eq!(r.status, 404, "{}", r.text());
    // Nor build an artifact from org-a's upload.
    let first = uploads
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            B,
            Some(json!({"name": "b2", "manifest": manifest, "uploads": {"a.json": first}})),
        )
        .await;
    assert_eq!(r.status, 404, "{}", r.text());
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

/// Collects every log line of the test process.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl Logs {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

/// The process-wide log capture: every level, plus each span's fields when it opens and
/// closes (what an OTLP exporter would send). Set once, shared by the tests that read it;
/// a thread-local subscriber would miss log sites other tests already cached as unused.
fn logs() -> &'static Logs {
    static LOGS: OnceLock<Logs> = OnceLock::new();
    LOGS.get_or_init(|| {
        let logs = Logs::default();
        let sink = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .with_span_events(
                tracing_subscriber::fmt::format::FmtSpan::NEW
                    | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
            )
            .with_writer(move || sink.clone())
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("only `logs()` sets a global logger");
        logs
    })
}

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
async fn canary_secret_never_leaks_fs() {
    canary_secret_never_leaks(Kind::Fs).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL and SeaweedFS (just dev)"]
async fn canary_secret_never_leaks_s3() {
    canary_secret_never_leaks(Kind::S3).await;
}

async fn canary_secret_never_leaks(kind: Kind) {
    let logs = logs();
    let canary = format!("canary-{}", uuid::Uuid::new_v4().simple());
    let Some(e) = Env::start(kind).await else {
        return;
    };
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
    // Right after the write, while the row exists: the value is in no P19 row and in no
    // stored idempotent response.
    let rows: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM secrets_metadata s WHERE s::text LIKE '%' || $1 || '%')
              + (SELECT count(*) FROM idempotency_keys k WHERE k::text LIKE '%' || $1 || '%')
              + (SELECT count(*) FROM secrets_metadata WHERE rotated_at IS NULL)",
    )
    .bind(&canary)
    .fetch_one(e.db.pool())
    .await
    .unwrap();
    assert_eq!(
        rows, 0,
        "the value is never in PostgreSQL, and the row says it was saved"
    );
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
    // Grants: only the runs module adds them, in its own transaction, for runs it checked.
    let run = mrl_domain::ids::RunId::new();
    let id: mrl_domain::ids::SecretId = sec.parse().unwrap();
    let mut tx = e.db.db().begin().await.unwrap();
    for (org, secret) in [("org-b", id), ("org-a", mrl_domain::ids::SecretId::new())] {
        let err = e
            .resolver
            .grant(&e.seams, &e.ctx_a(), &mut tx, org, secret, run, "env")
            .await
            .unwrap_err();
        assert_eq!(err.code(), "MRL-RES-001", "{org}");
    }
    e.resolver
        .grant(&e.seams, &e.ctx_a(), &mut tx, "org-a", id, run, "env")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(e.db.count("run_secret_grants").await, 1);
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/secrets/{sec}/grants"),
            A,
            Some(json!({"run_id": run.to_string(), "mount": "env"})),
        )
        .await;
    assert_eq!(r.status, 404, "no public grant endpoint: {}", r.text());
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
    let store = e.store_seam();
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
    assert!(
        e.resolver
            .resolve_for_run(Some(&store), "org-b", run)
            .await
            .unwrap()
            .is_empty(),
        "another org resolves no grants of this run"
    );
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
    let other: mrl_domain::ids::SecretId = r.json()["id"].as_str().unwrap().parse().unwrap();
    let mut tx = e.db.db().begin().await.unwrap();
    let err = e
        .resolver
        .grant(&e.seams, &e.ctx_a(), &mut tx, "org-a", other, run, "file")
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-PERM-190");
    drop(tx);
    // A team server's environment holds its own credentials (like AWS_SECRET_ACCESS_KEY):
    // tenants can't point a secret at it, and an `env` row is never resolved there.
    let var = format!("MRL_P19_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let server_canary = format!("server-{canary}");
    std::env::set_var(&var, &server_canary);
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
    assert_eq!(
        (r.status, code(&r).as_str()),
        (403, "MRL-PERM-001"),
        "{}",
        r.text()
    );
    bodies.push(r.text());
    let env_uuid = uuid::Uuid::new_v4();
    let run2 = mrl_domain::ids::RunId::new();
    e.db.seed(&format!(
        "INSERT INTO secrets_metadata (id, org_id, name, backend, ref, kind, allowed_uses, created_by)
         VALUES ('{env_uuid}', 'org-a', 'env1', 'env', '{var}', 'generic', '{{run}}', 'usr-a');
         INSERT INTO run_secret_grants (run_id, secret_id, mount) VALUES ('{}', '{env_uuid}', 'env');",
        run2.as_uuid()
    ))
    .await;
    let env_id = mrl_domain::ids::SecretId::from_uuid(env_uuid);
    let err = e
        .resolver
        .resolve(None, "org-a", env_id, "run")
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-PERM-001");
    let err = e
        .resolver
        .resolve_for_run(None, "org-a", run2)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-PERM-001");
    assert!(!format!("{err:?}").contains(&canary));
    // A solo (CLI) server reads it.
    let solo = SecretResolver::new(e.db.db().clone(), Mode::Solo);
    let got = solo.resolve(None, "org-a", env_id, "run").await.unwrap();
    assert_eq!(got.expose(), server_canary);
    std::env::remove_var(&var);
    let err = solo
        .resolve(None, "org-a", env_id, "run")
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-RES-190");
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
    let logs = logs.text();
    assert!(logs.contains("listening"), "the capture works: {logs}");
    for text in bodies.iter().chain([&logs]) {
        assert!(!text.contains(&canary), "the canary leaked: {text}");
    }
}

async fn cleanup(kind: Kind) {
    // Its own bucket: a pass 25 hours ahead would reclaim other tests' objects in org-a.
    let Some(e) = Env::start_in(kind, true, SWEEP_BUCKET).await else {
        return;
    };
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
    // The pass runs 25 hours from now (no backend lets a test age objects): `old` is past
    // its deadline, `fresh` was given two days.
    let uuid = |up: &str| up.parse::<mrl_domain::ids::UploadId>().unwrap().as_uuid();
    e.db.seed(&format!(
        "UPDATE uploads SET expires_at = now() + interval '1 hour' WHERE id = '{}';
         UPDATE uploads SET expires_at = now() + interval '48 hours' WHERE id = '{}';",
        uuid(&old),
        uuid(&fresh)
    ))
    .await;
    let st = e.backend.org("org-a").unwrap();
    st.put(
        "orgs/org-a/uploads/stray/parts/000000",
        b"zz".to_vec().into(),
    )
    .await
    .unwrap();
    // Multipart staging of an org with no upload rows, known only by a write intent.
    let lone = format!("org-z{}", uuid::Uuid::new_v4().simple());
    let z = e.backend.org(&lone).unwrap();
    let staged = format!("orgs/{lone}/uploads/0193aa/parts/000001");
    z.put(&staged, b"part".to_vec().into()).await.unwrap();
    e.db.seed(&format!(
        "INSERT INTO storage_intents (id, org_id, key) VALUES ('{}', '{lone}', 'orgs/{lone}/runs/r/big.bin')",
        uuid::Uuid::new_v4()
    ))
    .await;
    // Blobs written for commits that never happened, and one an upload still uses.
    let orphan = format!("orphan {}", uuid::Uuid::new_v4());
    let orphan_blob = st.keys().blob(&sha256_hex(orphan.as_bytes())).unwrap();
    st.put(&orphan_blob, orphan.clone().into_bytes().into())
        .await
        .unwrap();
    let kept = format!("kept {}", uuid::Uuid::new_v4());
    let kept_up = e.upload(A, kept.as_bytes(), None).await;
    let kept_blob = st.keys().blob(&sha256_hex(kept.as_bytes())).unwrap();
    for (key, sha) in [
        (&orphan_blob, sha256_hex(orphan.as_bytes())),
        (&kept_blob, sha256_hex(kept.as_bytes())),
    ] {
        e.db.seed(&format!(
            "INSERT INTO storage_intents (id, org_id, key, sha256) VALUES ('{}', 'org-a', '{key}', '{sha}')",
            uuid::Uuid::new_v4()
        ))
        .await;
    }
    let later = time::OffsetDateTime::now_utc() + time::Duration::hours(25);
    let swept = mrl_server::modules::uploads::sweep_at(&e.store, later)
        .await
        .unwrap();
    assert_eq!(swept.expired, 1, "{swept:?}");
    assert_eq!(swept.blobs, 1, "{swept:?}");
    assert!(
        swept.objects >= 3,
        "old part, stray, other org's staging: {swept:?}"
    );
    assert!(
        z.head(&staged).await.is_err(),
        "staging of an org without uploads is found"
    );
    assert!(
        st.head(&orphan_blob).await.is_err(),
        "the orphan blob is gone"
    );
    assert_eq!(
        st.get(&kept_blob).await.unwrap(),
        kept.as_bytes(),
        "a used blob stays"
    );
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
    let r = e
        .call(Method::GET, &format!("/api/v1/uploads/{kept_up}"), A, None)
        .await;
    assert_eq!(r.json()["state"], "COMPLETED");
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn cleanup_expires_uploads_and_removes_old_parts_fs() {
    cleanup(Kind::Fs).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL and SeaweedFS (just dev)"]
async fn cleanup_expires_uploads_and_removes_old_parts_s3() {
    cleanup(Kind::S3).await;
}

async fn new_secret(e: &Env, name: &str) -> String {
    let r = e
        .call(
            Method::POST,
            "/api/v1/secrets",
            A,
            Some(json!({"name": name, "kind": "generic", "allowed_uses": ["run"]})),
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    r.json()["id"].as_str().unwrap().to_owned()
}

async fn set_secret(e: &Env, sec: &str, v: &str) -> TestResponse {
    e.call(
        Method::PUT,
        &format!("/api/v1/secrets/{sec}/value"),
        A,
        Some(json!({ "value": v })),
    )
    .await
}

async fn delete_secret(e: &Env, sec: &str) -> TestResponse {
    e.call(Method::DELETE, &format!("/api/v1/secrets/{sec}"), A, None)
        .await
}

async fn stored(e: &Env, sec: &str) -> Option<Secret> {
    e.secrets
        .get(&format!("mrl.secret.org-a.{sec}"))
        .await
        .unwrap()
}

async fn secret_rows(e: &Env, sec: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM secrets_metadata WHERE id = $1")
        .bind(sec.parse::<mrl_domain::ids::SecretId>().unwrap().as_uuid())
        .fetch_one(e.db.pool())
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn secret_delete_and_rotation_never_interleave() {
    let e = Env::start(Kind::Fs).await.unwrap();
    // A rotation in progress: the delete waits for it, then removes what it wrote.
    let sec = new_secret(&e, "one").await;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    *e.ctl.hold.lock().unwrap() = Some((entered_tx, release_rx));
    let rotate = set_secret(&e, &sec, "rotated");
    let driver = async {
        entered_rx.await.unwrap();
        let delete = delete_secret(&e, &sec);
        tokio::pin!(delete);
        assert!(
            tokio::time::timeout(Duration::from_millis(400), &mut delete)
                .await
                .is_err(),
            "the delete waits while the rotation writes"
        );
        release_tx.send(()).unwrap();
        delete.await
    };
    let (rotated, deleted) = tokio::join!(rotate, driver);
    assert_eq!(
        (rotated.status, deleted.status),
        (204, 204),
        "{}",
        deleted.text()
    );
    assert!(
        stored(&e, &sec).await.is_none(),
        "the rotated value is gone too"
    );
    assert_eq!(secret_rows(&e, &sec).await, 0);

    // Deleted first: a later rotation finds nothing.
    let sec = new_secret(&e, "two").await;
    assert_eq!(delete_secret(&e, &sec).await.status, 204);
    assert_eq!(set_secret(&e, &sec, "late").await.status, 404);
    assert!(stored(&e, &sec).await.is_none());

    // The store fails mid-delete: the reference stays, marked, and refuses use; deleting
    // again finishes.
    let sec = new_secret(&e, "three").await;
    assert_eq!(set_secret(&e, &sec, "v").await.status, 204);
    e.ctl.down.store(true, Ordering::SeqCst);
    let r = delete_secret(&e, &sec).await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (503, "MRL-INTERNAL-002"),
        "{}",
        r.text()
    );
    e.ctl.down.store(false, Ordering::SeqCst);
    assert_eq!(secret_rows(&e, &sec).await, 1, "the reference is kept");
    let r = set_secret(&e, &sec, "again").await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (409, "MRL-CONFLICT-192"),
        "{}",
        r.text()
    );
    let r = e
        .call(Method::GET, &format!("/api/v1/secrets/{sec}"), A, None)
        .await;
    assert_eq!(code(&r), "MRL-CONFLICT-192");
    let store = e.store_seam();
    let id: mrl_domain::ids::SecretId = sec.parse().unwrap();
    let err = e
        .resolver
        .resolve(Some(&store), "org-a", id, "run")
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-CONFLICT-192");
    let mut tx = e.db.db().begin().await.unwrap();
    let err = e
        .resolver
        .grant(
            &e.seams,
            &e.ctx_a(),
            &mut tx,
            "org-a",
            id,
            mrl_domain::ids::RunId::new(),
            "env",
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-CONFLICT-192");
    drop(tx);
    assert_eq!(delete_secret(&e, &sec).await.status, 204);
    assert!(stored(&e, &sec).await.is_none());
    assert_eq!(secret_rows(&e, &sec).await, 0);

    // A server restarted without its secret store: the reference is kept (503), and a
    // retry once the store is back removes the value.
    let sec = new_secret(&e, "five").await;
    assert_eq!(set_secret(&e, &sec, "v").await.status, 204);
    let id: mrl_domain::ids::SecretId = sec.parse().unwrap();
    let err = e
        .resolver
        .delete(None, "org-a", id.as_uuid())
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-ENV-190");
    assert_eq!(secret_rows(&e, &sec).await, 1, "the reference is kept");
    assert!(stored(&e, &sec).await.is_some());
    e.resolver
        .delete(Some(&e.store_seam()), "org-a", id.as_uuid())
        .await
        .unwrap();
    assert!(stored(&e, &sec).await.is_none());
    assert_eq!(secret_rows(&e, &sec).await, 0);

    // A server stopped after removing the value but before the metadata: deleting again
    // finishes, grants included.
    let sec = new_secret(&e, "four").await;
    assert_eq!(set_secret(&e, &sec, "v").await.status, 204);
    let id: mrl_domain::ids::SecretId = sec.parse().unwrap();
    let mut tx = e.db.db().begin().await.unwrap();
    e.resolver
        .grant(
            &e.seams,
            &e.ctx_a(),
            &mut tx,
            "org-a",
            id,
            mrl_domain::ids::RunId::new(),
            "file",
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    e.db.seed(&format!(
        "UPDATE secrets_metadata SET deleting = true WHERE id = '{}'",
        id.as_uuid()
    ))
    .await;
    e.secrets
        .delete(&format!("mrl.secret.org-a.{sec}"))
        .await
        .unwrap();
    assert_eq!(delete_secret(&e, &sec).await.status, 204);
    assert_eq!(secret_rows(&e, &sec).await, 0);
    assert_eq!(e.db.count("run_secret_grants").await, 0);
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn team_replicas_share_one_ticket_key() {
    let db = TestDb::new().await;
    let dir = tempfile::tempdir().unwrap();
    let config = BackendConfig::Local {
        root: dir.path().to_owned(),
    };
    let replica = |db: mrl_server::db::Db, mode: Mode| {
        ArtifactsModule::with_backend(db, config.clone(), mode, dir.path().join("scratch")).store()
    };
    // Seams of a server whose secret store is `store`.
    let seams_with = |store: Option<Arc<dyn SecretStore>>, mode: Mode| {
        let b = if mode == Mode::Solo {
            ts::solo("s")
        } else {
            ts::team(&[])
        };
        let mut seams = ts::build(b, mode);
        seams.secrets = store;
        seams
    };
    let ctx = |org: &str| {
        RequestContext::new(
            user("usr-a", org),
            &RequestMeta {
                trace_id: "0".repeat(32),
                request_id: "test".to_owned(),
            },
        )
    };
    let shared: Arc<dyn SecretStore> = Arc::new(Shared(
        Arc::new(InMemorySecretStore::default()),
        Arc::new(Ctl::default()),
    ));
    let team = seams_with(Some(shared.clone()), Mode::Team);
    let key = "orgs/org-a/runs/r/w.bin";
    let token =
        |t: mrl_storage::TransferTicket| t.url.trim_start_matches("/api/v1/transfer/").to_owned();
    // Two replicas start together on an empty store.
    let (one, two) = (
        replica(db.db().clone(), Mode::Team),
        replica(db.replica(), Mode::Team),
    );
    let (c1, c2) = (ctx("org-a"), ctx("org-a"));
    let (a, b) = tokio::join!(
        one.ticket(&team, &c1, key, TicketMethod::Put, 10),
        two.ticket(&team, &c2, key, TicketMethod::Put, 10)
    );
    let (from_one, from_two) = (token(a.unwrap()), token(b.unwrap()));
    one.verify(Some(&shared), &from_two, TicketMethod::Put)
        .await
        .unwrap();
    two.verify(Some(&shared), &from_one, TicketMethod::Put)
        .await
        .unwrap();
    // A restarted replica still accepts unexpired tickets.
    let three = replica(db.replica(), Mode::Team);
    three
        .verify(Some(&shared), &from_one, TicketMethod::Put)
        .await
        .unwrap();
    // No usable store: a team server signs nothing rather than use a key of its own, while
    // plain storage keeps working.
    let ctl = Arc::new(Ctl::default());
    ctl.down.store(true, Ordering::SeqCst);
    let down: Arc<dyn SecretStore> = Arc::new(Shared(
        Arc::new(InMemorySecretStore::default()),
        ctl.clone(),
    ));
    let four = replica(db.replica(), Mode::Team);
    let err = four
        .ticket(
            &seams_with(Some(down.clone()), Mode::Team),
            &c1,
            key,
            TicketMethod::Put,
            10,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-INTERNAL-002");
    let err = four
        .ticket(
            &seams_with(None, Mode::Team),
            &c1,
            key,
            TicketMethod::Put,
            10,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-ENV-190");
    let err = four
        .verify(None, &from_one, TicketMethod::Put)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-ENV-190");
    four.org_store("org-a")
        .await
        .unwrap()
        .put(key, b"x".to_vec().into())
        .await
        .unwrap();
    ctl.down.store(false, Ordering::SeqCst);
    four.ticket(
        &seams_with(Some(down), Mode::Team),
        &c1,
        key,
        TicketMethod::Put,
        10,
    )
    .await
    .expect("a store that answers again is used on the next call");
    // A solo server has one process: a key of its own is fine.
    let solo = replica(db.replica(), Mode::Solo);
    let local = RequestContext::new(
        mrl_server::seams::Principal::local_owner(),
        &RequestMeta {
            trace_id: "0".repeat(32),
            request_id: "test".to_owned(),
        },
    );
    solo.ticket(
        &seams_with(None, Mode::Solo),
        &local,
        "orgs/local/runs/r/w.bin",
        TicketMethod::Put,
        10,
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn transfer_tickets_stay_out_of_telemetry() {
    let logs = logs();
    let e = Env::start(Kind::Fs).await.unwrap();
    let (manifest, uploads) = e.upload_files(&[("t.txt", b"telemetry")]).await;
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            A,
            Some(json!({"name": "t", "manifest": manifest, "uploads": uploads})),
        )
        .await;
    let art = r.json()["id"].as_str().unwrap().to_owned();
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/artifacts/{art}:download"),
            A,
            Some(json!({"path": "t.txt"})),
        )
        .await;
    let url = token_of(&r.json());
    let token = url.trim_start_matches("/api/v1/transfer/").to_owned();
    let (payload, sig) = token.rsplit_once('.').unwrap();
    let tampered = format!("{payload}.{}", sig.chars().rev().collect::<String>());
    let trace = "0af7651916cd43dd8448eb211c80319c";
    let traceparent = format!("00-{trace}-b7ad6b7169203331-01");
    let get = |path: String| {
        let tp = traceparent.clone();
        let e = &e;
        async move {
            e.srv
                .request(Method::GET, &path, &[("traceparent", &tp)], Body::empty())
                .await
        }
    };
    let ok = get(url.clone()).await;
    assert_eq!((ok.status, ok.text().as_str()), (200, "telemetry"));
    let mut bodies = Vec::new();
    for path in [
        format!("/api/v1/transfer/{tampered}"),
        format!("/api/v1/transfer/{token}/more"),
        format!("/api/v1//transfer/{token}"),
    ] {
        let r = get(path.clone()).await;
        assert!(r.status >= 400, "{path}: {}", r.text());
        let p = r.json();
        assert_eq!(p["instance"], "/api/v1/transfer/{ticket}", "{path}: {p}");
        assert_eq!(p["trace_id"], trace, "the trace id survives: {p}");
        bodies.push(r.text());
    }
    let internal = e.srv.running().internal_addr().unwrap();
    let metrics =
        TestServer::request_to(internal, Method::GET, "/metrics", &[], Body::empty()).await;
    bodies.push(metrics.text());
    bodies.push(format!("{:?}", e.audit.events()));
    let logs = logs.text();
    assert!(
        logs.contains("path=/api/v1/transfer/{ticket}") && logs.contains(trace),
        "spans are captured with their fields"
    );
    for text in bodies.iter().chain([&logs]) {
        for secret in [&token, &tampered] {
            assert!(!text.contains(secret.as_str()), "a ticket leaked: {text}");
        }
        assert!(!text.contains(sig), "a signature leaked");
    }
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn solo_ticket_urls_still_need_the_launch_secret() {
    // Owner decision (review round 2): a solo server gates every path with the launch
    // secret, ticket URLs included; the desktop app, CLI and local agents send it.
    let db = TestDb::new().await;
    let dir = tempfile::tempdir().unwrap();
    let config = BackendConfig::Local {
        root: dir.path().join("objects"),
    };
    let module = ArtifactsModule::with_backend(
        db.db().clone(),
        config.clone(),
        Mode::Solo,
        dir.path().join("scratch"),
    );
    let srv = TestServer::solo(vec![Box::new(module)]).await;
    let gate = [("x-mrl-launch-secret", srv.secret.as_str())];
    let with_json = [
        ("x-mrl-launch-secret", srv.secret.as_str()),
        ("content-type", "application/json"),
    ];
    let post = |path: String, body: Value| {
        let srv = &srv;
        async move {
            srv.request(
                Method::POST,
                &path,
                &with_json,
                Body::from(body.to_string()),
            )
            .await
        }
    };
    let r = post(
        "/api/v1/uploads".into(),
        json!({"filename": "s", "size": 4, "sha256": sha256_hex(b"solo")}),
    )
    .await;
    let up = r.json()["id"].as_str().unwrap().to_owned();
    let r = srv
        .request(
            Method::PUT,
            &format!("/api/v1/uploads/{up}/parts/0"),
            &gate,
            Body::from(b"solo".to_vec()),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let r = post(format!("/api/v1/uploads/{up}:complete"), json!({})).await;
    assert_eq!(r.status, 200, "{}", r.text());
    let manifest = json!({"schema_version": "artifact-manifest/v1", "kind": "eval", "created_at": "2026-11-16T00:00:00Z",
        "files": [{"path": "s.txt", "size": 4, "sha256": sha256_hex(b"solo"), "role": "report"}]});
    let r = post(
        "/api/v1/artifacts".into(),
        json!({"name": "s", "manifest": manifest, "uploads": {"s.txt": up}}),
    )
    .await;
    assert_eq!(r.status, 201, "{}", r.text());
    let art = r.json()["id"].as_str().unwrap().to_owned();
    let r = srv
        .request(
            Method::POST,
            &format!("/api/v1/artifacts/{art}:download"),
            &with_json,
            Body::from(json!({"path": "s.txt"}).to_string()),
        )
        .await;
    let url = token_of(&r.json());
    assert_eq!(
        srv.get(&url).await.status,
        401,
        "the ticket alone is not enough"
    );
    let r = srv.request(Method::GET, &url, &gate, Body::empty()).await;
    assert_eq!((r.status, r.text().as_str()), (200, "solo"));
    let r = srv
        .request(Method::GET, &format!("{url}x"), &gate, Body::empty())
        .await;
    assert_eq!(r.status, 403, "a bad ticket fails even with the secret");
    for path in [
        "/api/v1/transfer",
        "/healthz",
        "/metrics",
        "/api/v1/artifacts",
    ] {
        assert_eq!(srv.get(path).await.status, 401, "{path}");
    }
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn creating_needs_the_destination_and_a_visible_source() {
    let e = Env::start(Kind::Fs).await.unwrap();
    let empty = json!({"schema_version": "artifact-manifest/v1", "kind": "eval", "created_at": "2026-11-16T00:00:00Z", "files": []});
    // The viewer can't create anything in the project it can't see.
    for (path, body) in [
        (
            "/api/v1/artifacts",
            json!({"name": "p-art", "project_id": PRIVATE, "manifest": empty}),
        ),
        (
            "/api/v1/uploads",
            json!({"filename": "p", "size": 1, "sha256": sha256_hex(b"p"), "project_id": PRIVATE}),
        ),
        (
            "/api/v1/secrets",
            json!({"name": "p-sec", "kind": "generic", "allowed_uses": ["run"], "project_id": PRIVATE}),
        ),
    ] {
        let r = e.call(Method::POST, path, VIEWER, Some(body)).await;
        assert_eq!(r.status, 404, "{path}: {}", r.text());
    }
    for table in ["artifacts", "uploads", "secrets_metadata"] {
        assert_eq!(e.db.count(table).await, 0, "{table}");
    }
    // Nor build an artifact from an upload it can't see, even knowing its size and hash.
    let bytes = b"private weights";
    let up = e.upload(A, bytes, Some(PRIVATE)).await;
    let manifest = json!({"schema_version": "artifact-manifest/v1", "kind": "eval", "created_at": "2026-11-16T00:00:00Z",
        "files": [{"path": "w.bin", "size": bytes.len(), "sha256": sha256_hex(bytes), "role": "weights"}]});
    let body = json!({"name": "stolen", "manifest": manifest, "uploads": {"w.bin": up}});
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            VIEWER,
            Some(body.clone()),
        )
        .await;
    assert_eq!(r.status, 404, "{}", r.text());
    assert_eq!(e.db.count("artifacts").await, 0, "no name was reserved");
    let r = e
        .call(Method::POST, "/api/v1/artifacts", A, Some(body))
        .await;
    assert_eq!(r.status, 201, "its owner can: {}", r.text());
    // An authorizer that fails is an outage (503), not a missing upload.
    let up = e.upload(A, bytes, Some(BROKEN)).await;
    let r = e
        .call(
            Method::POST,
            "/api/v1/artifacts",
            VIEWER,
            Some(json!({"name": "outage", "manifest": manifest, "uploads": {"w.bin": up}})),
        )
        .await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (503, "MRL-INTERNAL-002"),
        "{}",
        r.text()
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn lineage_shows_only_nodes_their_owner_resolves() {
    let e = Env::start(Kind::Fs).await.unwrap();
    use mrl_server::modules::lineage::{LineageWriter, NodeRef, Relation};
    let r = e
        .call(Method::POST, "/api/v1/artifacts", A, Some(json!({"name": "open", "manifest":
            {"schema_version": "artifact-manifest/v1", "kind": "eval", "created_at": "2026-11-16T00:00:00Z", "files": []}})))
        .await;
    let art = r.json()["id"].as_str().unwrap().to_owned();
    let r = e
        .call(Method::POST, "/api/v1/secrets", A, Some(json!({"name": "hidden", "kind": "generic", "allowed_uses": ["run"], "project_id": PRIVATE})))
        .await;
    let sec = r.json()["id"].as_str().unwrap().to_owned();
    // A model kind no module resolves yet.
    let mdl = mrl_domain::ids::ModelId::new().to_string();
    let n = |s: &str| NodeRef::parse(s).unwrap();
    let mut tx = e.db.db().begin().await.unwrap();
    LineageWriter::link(&mut tx, "org-a", n(&art), Relation::GeneratedBy, n(&sec))
        .await
        .unwrap();
    LineageWriter::link(&mut tx, "org-a", n(&art), Relation::DerivedFrom, n(&mdl))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let r = e
        .call(Method::GET, &format!("/api/v1/secrets/{sec}"), VIEWER, None)
        .await;
    assert_eq!(r.status, 404, "the viewer can't read the secret");
    let g = e
        .call(Method::GET, &format!("/api/v1/lineage/{art}"), VIEWER, None)
        .await;
    assert_eq!(g.status, 200, "{}", g.text());
    assert!(
        !g.text().contains(&sec) && !g.text().contains(&mdl),
        "{}",
        g.text()
    );
    let g = e
        .call(Method::GET, &format!("/api/v1/lineage/{art}"), A, None)
        .await;
    assert!(
        g.text().contains(&sec),
        "its owner sees the secret: {}",
        g.text()
    );
    assert!(
        !g.text().contains(&mdl),
        "nobody sees an unresolved node: {}",
        g.text()
    );
    let redacted = g.json()["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|n| n["redacted"] == true)
        .count();
    assert_eq!(redacted, 1);
    for root in [&mdl, &sec] {
        let r = e
            .call(
                Method::GET,
                &format!("/api/v1/lineage/{root}"),
                VIEWER,
                None,
            )
            .await;
        assert_eq!(r.status, 404, "{root}");
    }
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn storage_works_without_a_signing_key() {
    let e = Env::start_with(Kind::Fs, false).await.unwrap();
    // Uploading through the API needs no ticket, so no signing key.
    let up = e.upload(A, b"no store", None).await;
    let r = e
        .call(Method::GET, &format!("/api/v1/uploads/{up}"), A, None)
        .await;
    assert_eq!(r.json()["state"], "COMPLETED");
    // Tickets do, and fail closed.
    let r = e
        .call(
            Method::POST,
            "/api/v1/uploads",
            A,
            Some(json!({"filename": "t", "size": 3, "sha256": sha256_hex(b"abc")})),
        )
        .await;
    let open = r.json()["id"].as_str().unwrap().to_owned();
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/uploads/{open}:presign"),
            A,
            None,
        )
        .await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (503, "MRL-ENV-190"),
        "{}",
        r.text()
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn retries_with_a_key_are_deduped_per_org_and_never_replayed() {
    let e = Env::start(Kind::Fs).await.unwrap();
    let key = [("idempotency-key", "k-1")];
    let body = json!({"filename": "i", "size": 1, "sha256": sha256_hex(b"i")});
    let first = e
        .call_h(Method::POST, "/api/v1/uploads", A, &key, body.clone())
        .await;
    let again = e
        .call_h(Method::POST, "/api/v1/uploads", A, &key, body.clone())
        .await;
    assert_eq!((first.status, again.status), (201, 201), "{}", again.text());
    assert_eq!(
        first.json()["id"],
        again.json()["id"],
        "the retry finds the same upload"
    );
    assert!(
        !again.headers.contains_key("idempotent-replayed"),
        "answered afresh, not replayed"
    );
    // The same person in another org gets an upload there, not the first org's answer.
    let other = e
        .call_h(Method::POST, "/api/v1/uploads", A_IN_B, &key, body.clone())
        .await;
    assert_eq!(other.status, 201, "{}", other.text());
    assert_ne!(other.json()["id"], first.json()["id"]);
    let id = first.json()["id"].as_str().unwrap().to_owned();
    let r = e
        .call(Method::GET, &format!("/api/v1/uploads/{id}"), A_IN_B, None)
        .await;
    assert_eq!(r.status, 404);
    // Another body with the same key is refused.
    let r = e
        .call_h(
            Method::POST,
            "/api/v1/uploads",
            A,
            &key,
            json!({"filename": "j", "size": 1, "sha256": sha256_hex(b"j")}),
        )
        .await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (422, "MRL-IDEM-002"),
        "{}",
        r.text()
    );
    // Secrets and artifacts too.
    let sbody = json!({"name": "s-idem", "kind": "generic", "allowed_uses": ["run"]});
    let a = e
        .call_h(Method::POST, "/api/v1/secrets", A, &key, sbody.clone())
        .await;
    let b = e
        .call_h(Method::POST, "/api/v1/secrets", A, &key, sbody)
        .await;
    assert_eq!((a.status, b.status), (201, 201), "{}", b.text());
    assert_eq!(a.json()["id"], b.json()["id"]);
    let (manifest, uploads) = e.upload_files(&[("x.txt", b"idem")]).await;
    let abody = json!({"name": "a-idem", "manifest": manifest, "uploads": uploads});
    let a = e
        .call_h(Method::POST, "/api/v1/artifacts", A, &key, abody.clone())
        .await;
    let b = e
        .call_h(Method::POST, "/api/v1/artifacts", A, &key, abody)
        .await;
    assert_eq!(
        (a.status, b.status),
        (201, 201),
        "the retry is not a name clash: {}",
        b.text()
    );
    assert_eq!(a.json()["id"], b.json()["id"]);
    // Tickets are issued afresh on every try (each download ticket is audited).
    let art = a.json()["id"].as_str().unwrap().to_owned();
    for _ in 0..2 {
        let r = e
            .call_h(
                Method::POST,
                &format!("/api/v1/artifacts/{art}:download"),
                A,
                &[("idempotency-key", "dl")],
                json!({"path": "x.txt"}),
            )
            .await;
        assert_eq!(r.status, 200, "{}", r.text());
        assert!(!r.headers.contains_key("idempotent-replayed"));
    }
    let issued = e
        .audit
        .events()
        .iter()
        .filter(|a| a.action == "transfer.ticket_issued")
        .count();
    assert_eq!(issued, 2);
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn closed_or_expiring_uploads_take_no_parts() {
    let e = Env::start(Kind::Fs).await.unwrap();
    async fn new(e: &Env) -> String {
        let r = e
            .call(
                Method::POST,
                "/api/v1/uploads",
                A,
                Some(json!({"filename": "f", "size": 10, "sha256": sha256_hex(&data(10))})),
            )
            .await;
        r.json()["id"].as_str().unwrap().to_owned()
    }
    let presign = |up: String| {
        let e = &e;
        async move {
            let r = e
                .call(
                    Method::POST,
                    &format!("/api/v1/uploads/{up}:presign"),
                    A,
                    Some(json!({"parts": [0]})),
                )
                .await;
            (r.status, r.json())
        }
    };
    // A short body through a part ticket can't replace a good part.
    let up = new(&e).await;
    let r = e
        .put_bytes(
            &format!("/api/v1/uploads/{up}/parts/0"),
            Some(A),
            data(10),
            &[],
        )
        .await;
    assert_eq!(r.status, 200);
    let (_, t) = presign(up.clone()).await;
    let url = token_of(&t["tickets"][0]["ticket"]);
    let r = e.put_bytes(&url, None, data(5), &[]).await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (422, "MRL-STORE-190"),
        "{}",
        r.text()
    );
    let r = e
        .call(Method::GET, &format!("/api/v1/uploads/{up}"), A, None)
        .await;
    assert_eq!(
        r.json()["parts_received"],
        json!([0]),
        "the good part is still there"
    );
    // After abort the ticket writes nothing.
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/uploads/{up}:abort"),
            A,
            None,
        )
        .await;
    assert_eq!(r.status, 200);
    let r = e.put_bytes(&url, None, data(10), &[]).await;
    assert_eq!(
        (r.status, code(&r).as_str()),
        (409, "MRL-CONFLICT-191"),
        "{}",
        r.text()
    );
    // Part tickets end with the upload; past its deadline (not yet swept) nothing works.
    let up = new(&e).await;
    let id = up.parse::<mrl_domain::ids::UploadId>().unwrap().as_uuid();
    e.db.seed(&format!(
        "UPDATE uploads SET expires_at = now() + interval '2 minutes' WHERE id = '{id}'"
    ))
    .await;
    let (status, t) = presign(up.clone()).await;
    assert_eq!(status, 200);
    let ticket_end = time::OffsetDateTime::parse(
        t["tickets"][0]["ticket"]["expires_at"].as_str().unwrap(),
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    assert!(
        ticket_end
            <= time::OffsetDateTime::now_utc()
                + time::Duration::minutes(2)
                + time::Duration::seconds(1)
    );
    let url = token_of(&t["tickets"][0]["ticket"]);
    e.put_bytes(
        &format!("/api/v1/uploads/{up}/parts/0"),
        Some(A),
        data(10),
        &[],
    )
    .await;
    e.db.seed(&format!(
        "UPDATE uploads SET expires_at = now() - interval '1 minute' WHERE id = '{id}'"
    ))
    .await;
    for (what, r) in [
        (
            "complete",
            e.call(
                Method::POST,
                &format!("/api/v1/uploads/{up}:complete"),
                A,
                None,
            )
            .await,
        ),
        (
            "abort",
            e.call(
                Method::POST,
                &format!("/api/v1/uploads/{up}:abort"),
                A,
                None,
            )
            .await,
        ),
        (
            "part",
            e.put_bytes(
                &format!("/api/v1/uploads/{up}/parts/0"),
                Some(A),
                data(10),
                &[],
            )
            .await,
        ),
        ("ticket", e.put_bytes(&url, None, data(10), &[]).await),
    ] {
        assert_eq!(
            (r.status, code(&r).as_str()),
            (409, "MRL-CONFLICT-191"),
            "{what}: {}",
            r.text()
        );
    }
    assert_eq!(presign(up).await.0, 409);
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn a_proxied_download_never_mixes_versions() {
    // FS: proxied through the server. (S3 download tickets are presigned and pinned with
    // `if-match` by mrl-storage.)
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let e = Env::start(Kind::Fs).await.unwrap();
    let st = e.backend.org("org-a").unwrap();
    let key = "orgs/org-a/runs/r/mutable.bin";
    let v1 = data(40 << 20);
    st.put(key, v1.clone().into()).await.unwrap();
    let t = e
        .store
        .ticket(
            &e.seams,
            &e.ctx_a(),
            key,
            TicketMethod::Get,
            v1.len() as u64,
        )
        .await
        .unwrap();
    let mut tcp = tokio::net::TcpStream::connect(e.srv.api()).await.unwrap();
    tcp.write_all(
        format!(
            "GET {} HTTP/1.1\r\nhost: test\r\nconnection: close\r\n\r\n",
            t.url
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut got = vec![0u8; 1 << 20];
    tcp.read_exact(&mut got).await.unwrap();
    // Replaced (same size, other bytes) while the response is in flight.
    let v2: Vec<u8> = v1.iter().map(|b| b ^ 0xff).collect();
    st.put(key, v2.into()).await.unwrap();
    let mut rest = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(20), tcp.read_to_end(&mut rest)).await;
    got.extend(rest);
    let split = got.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let body = &got[split..];
    assert!(
        body.len() < v1.len(),
        "the response is cut short ({} bytes)",
        body.len()
    );
    assert_eq!(
        body,
        &v1[..body.len()],
        "everything sent is from the first version"
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn a_legacy_download_repairs_a_bad_copy() {
    let e = Env::start(Kind::Fs).await.unwrap();
    let good = format!("legacy {}", uuid::Uuid::new_v4());
    let manifest = put_files(&e, "old/l2/", &[("l.bin", good.as_bytes())]).await;
    let st = e.backend.org("org-a").unwrap();
    let blob = st.keys().blob(&sha256_hex(good.as_bytes())).unwrap();
    st.put(&blob, good.to_uppercase().into_bytes().into())
        .await
        .unwrap();
    let id = uuid::Uuid::now_v7();
    e.db.seed(&format!(
        "INSERT INTO artifacts (id, org_id, name, kind, prefix, manifest, size_bytes, state, created_by, committed_at)
         VALUES ('{id}', 'org-a', 'legacy2', 'eval', 'orgs/org-a/old/l2/', '{manifest}', {}, 'COMMITTED', 'usr-a', now())",
        good.len()
    ))
    .await;
    let art = mrl_domain::ids::ArtifactId::from_uuid(id).to_string();
    let r = e
        .call(
            Method::POST,
            &format!("/api/v1/artifacts/{art}:download"),
            A,
            Some(json!({"path": "l.bin"})),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(e.fetch(&r.json()).await, good.as_bytes());
    assert_eq!(
        st.get(&blob).await.unwrap(),
        good.as_bytes(),
        "the copy was repaired"
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn lineage_walks_each_direction_through_cycles() {
    use mrl_server::modules::lineage::{LineageWriter, NodeRef, Relation};
    let e = Env::start(Kind::Fs).await.unwrap();
    let ids: Vec<String> = (0..5)
        .map(|_| mrl_domain::ids::CheckpointId::new().to_string())
        .collect();
    let (r, a, b, x, c) = (&ids[0], &ids[1], &ids[2], &ids[3], &ids[4]);
    e.exists("org-a", &ids.iter().map(String::as_str).collect::<Vec<_>>());
    let n = |s: &str| NodeRef::parse(s).unwrap();
    let mut tx = e.db.db().begin().await.unwrap();
    for (from, to) in [(r, a), (a, b), (b, x), (x, r), (c, a)] {
        LineageWriter::link(&mut tx, "org-a", n(from), Relation::DerivedFrom, n(to))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    for dir in ["down", "both"] {
        let g = e
            .call(
                Method::GET,
                &format!("/api/v1/lineage/{r}?direction={dir}&depth=4"),
                A,
                None,
            )
            .await;
        assert_eq!(g.status, 200, "{}", g.text());
        assert!(
            g.text().contains(c.as_str()),
            "{dir} reaches C: {}",
            g.text()
        );
        let edges = g.json()["edges"].as_array().unwrap().len();
        assert!(edges <= 5, "each edge once: {edges}");
    }
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn the_module_reads_its_storage_settings() {
    let db = TestDb::new().await;
    let dir = tempfile::tempdir().unwrap();
    let mut c = Config::default();
    // `file:///var/…` on Unix, `file:///C:/…` on Windows.
    let path = dir.path().to_string_lossy().replace('\\', "/");
    c.object_store.url = Some(format!("file:///{}", path.trim_start_matches('/')));
    let module = ArtifactsModule::new(db.db().clone(), &c);
    let st = module.store().org_store("org-a").await.unwrap();
    st.put("orgs/org-a/x/y.txt", b"via config".to_vec().into())
        .await
        .unwrap();
    assert!(dir
        .path()
        .join("orgs")
        .join("org-a")
        .join("x")
        .join("y.txt")
        .exists());
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn secret_changes_audit_first_and_grants_check_the_secret() {
    let e = Env::start(Kind::Fs).await.unwrap();
    let sec = new_secret(&e, "audited").await;
    assert_eq!(set_secret(&e, &sec, "v1").await.status, 204);
    let rotated = |e: &Env, sec: &str| {
        let id = sec.parse::<mrl_domain::ids::SecretId>().unwrap().as_uuid();
        let pool = e.db.pool().clone();
        async move {
            sqlx::query_scalar::<_, Option<time::OffsetDateTime>>(
                "SELECT rotated_at FROM secrets_metadata WHERE id = $1",
            )
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let before = rotated(&e, &sec).await;
    // An audit that fails changes nothing.
    e.ctl.audit_down.store(true, Ordering::SeqCst);
    let r = e
        .call(
            Method::POST,
            "/api/v1/secrets",
            A,
            Some(json!({"name": "unaudited", "kind": "generic", "allowed_uses": ["run"]})),
        )
        .await;
    assert_eq!(r.status, 503, "{}", r.text());
    assert_eq!(set_secret(&e, &sec, "v2").await.status, 503);
    assert_eq!(delete_secret(&e, &sec).await.status, 503);
    e.ctl.audit_down.store(false, Ordering::SeqCst);
    assert_eq!(
        e.db.count("secrets_metadata").await,
        1,
        "nothing was created"
    );
    assert_eq!(
        stored(&e, &sec).await.unwrap().expose(),
        "v1",
        "the value is unchanged"
    );
    assert_eq!(rotated(&e, &sec).await, before);
    let r = e
        .call(Method::GET, &format!("/api/v1/secrets/{sec}"), A, None)
        .await;
    assert_eq!(r.status, 200, "not marked as being deleted: {}", r.text());
    // The rotation time is saved before the value: a failed write leaves it early, never late.
    e.ctl.down.store(true, Ordering::SeqCst);
    assert_eq!(set_secret(&e, &sec, "v3").await.status, 503);
    e.ctl.down.store(false, Ordering::SeqCst);
    assert!(rotated(&e, &sec).await > before);
    assert_eq!(stored(&e, &sec).await.unwrap().expose(), "v1");
    // A grant needs permission on the secret itself (with its project), not only the run.
    let r = e
        .call(Method::POST, "/api/v1/secrets", A, Some(json!({"name": "p", "kind": "generic", "allowed_uses": ["run"], "project_id": PRIVATE})))
        .await;
    let private: mrl_domain::ids::SecretId = r.json()["id"].as_str().unwrap().parse().unwrap();
    let viewer = RequestContext::new(
        user("usr-viewer", "org-a"),
        &RequestMeta {
            trace_id: "0".repeat(32),
            request_id: "test".to_owned(),
        },
    );
    let mut tx = e.db.db().begin().await.unwrap();
    let err = e
        .resolver
        .grant(
            &e.seams,
            &viewer,
            &mut tx,
            "org-a",
            private,
            mrl_domain::ids::RunId::new(),
            "env",
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-RES-001");
    drop(tx);
    assert_eq!(e.db.count("run_secret_grants").await, 0);
}

/// A cleanup pass that runs while `:complete` reuses a stored blob (left by a commit that
/// never happened, its intent two days old) keeps it: the completion recorded a fresh
/// intent under the blob's lock before it checked the blob.
#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn cleanup_keeps_a_blob_a_completion_is_reusing() {
    let Some(e) = Env::start(Kind::Fs).await else {
        return;
    };
    let bytes = format!("reused {}", uuid::Uuid::new_v4());
    let sha = sha256_hex(bytes.as_bytes());
    let st = e.backend.org("org-a").unwrap();
    let blob = st.keys().blob(&sha).unwrap();
    st.put(&blob, bytes.clone().into_bytes().into())
        .await
        .unwrap();
    e.db.seed(&format!(
        "INSERT INTO storage_intents (id, org_id, key, sha256, created_at)
         VALUES ('{}', 'org-a', '{blob}', '{sha}', now() - interval '48 hours')",
        uuid::Uuid::new_v4()
    ))
    .await;
    let r = e
        .call(
            Method::POST,
            "/api/v1/uploads",
            A,
            Some(json!({"filename": "f", "size": bytes.len(), "sha256": sha})),
        )
        .await;
    let up = r.json()["id"].as_str().unwrap().to_owned();
    let r = e
        .put_bytes(
            &format!("/api/v1/uploads/{up}/parts/0"),
            Some(A),
            bytes.clone().into_bytes(),
            &[],
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    // The completion waits 3 s right before it records COMPLETED; the pass runs meanwhile.
    e.db.seed(
        "CREATE FUNCTION p19_wait() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(3); RETURN NEW; END $$;
         CREATE TRIGGER p19_wait BEFORE UPDATE ON uploads FOR EACH ROW
           WHEN (NEW.state = 'COMPLETED') EXECUTE FUNCTION p19_wait();",
    )
    .await;
    let complete = format!("/api/v1/uploads/{up}:complete");
    let (r, swept) = tokio::join!(
        e.call(Method::POST, &complete, A, None),
        async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            mrl_server::modules::uploads::sweep(&e.store).await.unwrap()
        }
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["blob_key"], json!(blob));
    assert_eq!(
        st.get(&blob).await.map(|b| b.to_vec()).ok(),
        Some(bytes.into_bytes()),
        "the completed upload's file is still stored ({swept:?})"
    );
}

/// A registration cancelled after it reserved its name (the client went away) frees the
/// name at once: the row becomes FAILED and the name can be registered again.
#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn a_cancelled_registration_frees_its_name() {
    let Some(e) = Env::start(Kind::Fs).await else {
        return;
    };
    let manifest = put_files(&e, "exports/cancel/", &[("r.json", b"{}")]).await;
    e.db.seed(
        "CREATE FUNCTION p19_wait() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2); RETURN NEW; END $$;
         CREATE TRIGGER p19_wait BEFORE UPDATE ON artifacts FOR EACH ROW
           WHEN (NEW.state = 'COMMITTED') EXECUTE FUNCTION p19_wait();",
    )
    .await;
    let cancelled = tokio::time::timeout(
        Duration::from_millis(700),
        e.register_from("cancelled", "exports/cancel/", manifest.clone()),
    )
    .await;
    assert!(cancelled.is_err(), "the registration was still running");
    let mut state = String::new();
    for _ in 0..100 {
        state = sqlx::query_scalar("SELECT state FROM artifacts WHERE name = 'cancelled'")
            .fetch_one(e.db.pool())
            .await
            .unwrap();
        if state != "PENDING" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(state, "FAILED");
    e.db.seed("DROP TRIGGER p19_wait ON artifacts;").await;
    let art = e
        .register_from("cancelled", "exports/cancel/", manifest)
        .await
        .unwrap();
    assert_eq!(art["state"], "COMMITTED");
}

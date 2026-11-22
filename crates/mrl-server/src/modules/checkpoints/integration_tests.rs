//! P23 integration tests: the checkpoint commit through the real gateway and a real agent link.
//!
//! Compiled by `mrl-testkit` (see its `lib.rs`, P23 marker), not by this crate: they need
//! `TestServer`, `TestDb` and `TestAgent`. `#[ignore]`d; run them with
//! `just dev && just test-integration`.
//!
//! The "fake engine" plays the worker and the agent's upload side: it stages files, relays
//! `CheckpointStaged` / `CheckpointCommitRequest` on the agent's CONTROL link (exactly what the
//! agent relays for a worker), and answers `UploadArtifacts` by PUTting each file with its
//! ticket. Faults (skip a file, cut one short) are switched per test.

use crate::agent::gateway::{fast_config, server_config, Running, TestAgent};
use crate::pg::TestDb;
use crate::seams::{self as ts, InMemorySecretStore};
use crate::server::{TestResponse, TestServer, TEST_SECRET};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::Method;
use mrl_agent::session::{CommandHandler, Link};
use mrl_domain::ids::{AttemptId, CheckpointId, JobId, NodeId, ProjectId, RunId};
use mrl_domain::state::{JobAttemptEvent, JobEvent, Owner};
use mrl_proto::agent::v1::{agent_message, command, Command, WorkerRelay};
use mrl_proto::common::v1::{AttemptRef, Failure};
use mrl_proto::worker::v1::{
    worker_message, CheckpointCommitRequest, CheckpointKind, CheckpointStaged, FileEntry,
    WorkerMessage,
};
use mrl_server::app::ServerModule;
use mrl_server::config::Mode;
use mrl_server::modules::artifacts::{ArtifactStore, ArtifactsModule};
use mrl_server::modules::checkpoints::{
    cleanup, CheckpointService, CheckpointsConfig, CheckpointsModule, EmergencyOutcome,
    FinalCheckpointVerifier,
};
use mrl_server::modules::jobs::{
    AttemptChange, Chains, JobChange, JobRow, JobSpec, JobTransitions, JobValidator, Verdict,
};
use mrl_server::modules::nodes::commands::{CommandKind, CommandSender, NewCommand};
use mrl_server::modules::nodes::lanes::LaneRouter;
use mrl_server::modules::nodes::{Gateway, NodesModule};
use mrl_server::modules::runs::RunsModule;
use mrl_server::seams::{SeamError, SecretStore, LAUNCH_SECRET_HEADER};
use mrl_storage::{sha256_hex, BackendConfig};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

struct AcceptAll;
#[async_trait]
impl JobValidator for AcceptAll {
    fn name(&self) -> &'static str {
        "accept-all"
    }
    async fn validate(&self, _: &JobSpec) -> Result<Verdict, SeamError> {
        Ok(Verdict::Accept)
    }
}

/// How the fake engine uploads.
#[derive(Clone, Default)]
enum Fault {
    #[default]
    None,
    /// Never uploads files whose path ends with this.
    Skip(&'static str),
    /// Uploads only the first byte of files whose path ends with this.
    Cut(&'static str),
    /// Never answers uploads (the agent is still uploading).
    Hold,
}

/// The worker and the agent's upload side, in Rust.
struct FakeEngine {
    staging: PathBuf,
    api: std::net::SocketAddr,
    fault: Mutex<Fault>,
    attempt: Mutex<Option<AttemptRef>>,
    run: Mutex<Option<Uuid>>,
    link: Mutex<Option<Arc<Link>>>,
    next_step: Mutex<u64>,
}

impl FakeEngine {
    fn set(&self, f: Fault) {
        *self.fault.lock().unwrap() = f;
    }

    fn relay(&self, body: worker_message::Body) {
        let link = self.link.lock().unwrap().clone().unwrap();
        let attempt = self.attempt.lock().unwrap().clone();
        link.send_control(agent_message::Body::Worker(WorkerRelay {
            attempt,
            role_id: "trainer".into(),
            message: Some(WorkerMessage {
                body: Some(body),
                ..Default::default()
            }),
        }))
        .unwrap();
    }

    /// Stages a checkpoint (weights + optimizer + manifest) and sends CheckpointStaged.
    fn stage(&self, step: u64, is_final: bool, metrics: &[(&str, f64)]) -> (Uuid, String) {
        let id = CheckpointId::new();
        let dir = format!("checkpoints/{id}");
        std::fs::create_dir_all(self.staging.join(&dir)).unwrap();
        let mut files = Vec::new();
        for (name, role, bytes) in [
            (
                "weights.bin",
                "weights",
                format!("weights-{step}").into_bytes(),
            ),
            (
                "optimizer.pt",
                "optimizer",
                format!("adam-{step}").into_bytes(),
            ),
        ] {
            let path = format!("{dir}/{name}");
            std::fs::write(self.staging.join(&path), &bytes).unwrap();
            files.push(FileEntry {
                path,
                size: bytes.len() as u64,
                sha256: sha256_hex(&bytes),
                role: role.into(),
            });
        }
        let run = RunId::from_uuid(self.run.lock().unwrap().unwrap()).to_string();
        let manifest = json!({
            "schema_version": "checkpoint-manifest/v1", "checkpoint_id": id.to_string(),
            "run_id": run, "attempt_id": "att", "step": step, "kind": "full_resume",
            "format": "fake-v1", "engine": {"name": "fake", "adapter_version": "1.0.0"},
            "base_model_sha256": sha256_hex(b"model"), "created_at": "2026-11-22T00:00:00Z",
            "is_final": is_final,
            "files": files.iter().map(|f| json!({"path": f.path, "size": f.size, "sha256": f.sha256, "role": f.role})).collect::<Vec<_>>(),
        })
        .to_string()
        .into_bytes();
        let mpath = format!("{dir}/checkpoint-manifest.json");
        std::fs::write(self.staging.join(&mpath), &manifest).unwrap();
        let msha = sha256_hex(&manifest);
        files.push(FileEntry {
            path: mpath,
            size: manifest.len() as u64,
            sha256: msha.clone(),
            role: "manifest".into(),
        });
        self.relay(worker_message::Body::CheckpointStaged(CheckpointStaged {
            checkpoint_id: id.to_string(),
            step,
            format: "fake-v1".into(),
            kind: CheckpointKind::FullResume as i32,
            is_final,
            files,
            manifest_sha256: msha.clone(),
            metrics_at_step: metrics.iter().map(|(k, v)| ((*k).to_owned(), *v)).collect(),
            policy_version: 0,
        }));
        (id.as_uuid(), msha)
    }

    fn commit(&self, id: Uuid, manifest_sha256: &str) {
        self.relay(worker_message::Body::CheckpointCommit(
            CheckpointCommitRequest {
                checkpoint_id: CheckpointId::from_uuid(id).to_string(),
                manifest_sha256: manifest_sha256.to_owned(),
            },
        ));
    }

    fn checkpoint(&self, step: u64, is_final: bool, metrics: &[(&str, f64)]) -> Uuid {
        let (id, sha) = self.stage(step, is_final, metrics);
        self.commit(id, &sha);
        id
    }

    async fn upload(&self, u: &mrl_proto::agent::v1::UploadArtifacts) -> Result<(), Failure> {
        let fault = self.fault.lock().unwrap().clone();
        if matches!(fault, Fault::Hold) {
            return Ok(());
        }
        for item in &u.items {
            let mut bytes = std::fs::read(self.staging.join(&item.local_path)).unwrap();
            match fault {
                Fault::Skip(end) if item.local_path.ends_with(end) => continue,
                Fault::Cut(end) if item.local_path.ends_with(end) => bytes.truncate(1),
                _ => {}
            }
            let r = TestServer::request_to(
                self.api,
                Method::PUT,
                &item.url,
                &[
                    (LAUNCH_SECRET_HEADER, TEST_SECRET),
                    ("content-type", "application/octet-stream"),
                ],
                bytes,
            )
            .await;
            assert!(r.status < 300, "ticket PUT {}: {}", r.status, r.text());
        }
        Ok(())
    }
}

#[async_trait]
impl CommandHandler for FakeEngine {
    async fn handle(&self, c: Command) -> Result<(), Failure> {
        match c.kind {
            Some(command::Kind::UploadArtifacts(u)) => self.upload(&u).await,
            Some(command::Kind::CheckpointAttempt(_)) => {
                let step = {
                    let mut n = self.next_step.lock().unwrap();
                    *n += 1;
                    *n
                };
                self.checkpoint(step, false, &[]);
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

struct Env {
    server: TestServer,
    db: TestDb,
    gw: Arc<Gateway>,
    service: Arc<CheckpointService>,
    store: Arc<ArtifactStore>,
    chains: Chains,
    engine: Arc<FakeEngine>,
    node: Uuid,
    running: Option<Running>,
    agent: TestAgent,
    _dirs: tempfile::TempDir,
}

fn fast_checkpoints() -> CheckpointsConfig {
    CheckpointsConfig {
        tick_seconds: 1,
        // Background passes are driven by hand (`tick`, `cleanup::*`), so tests decide when.
        cleanup_seconds: 3600,
        staging_ttl_seconds: 3,
        keep_last_n: 2,
        ..CheckpointsConfig::default()
    }
}

impl Env {
    async fn start() -> Self {
        let db = TestDb::new().await;
        let dirs = tempfile::tempdir().unwrap();
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let lanes = LaneRouter::new();
        let cfg = server_config(fast_config(), Mode::Solo, 0);
        let nodes = NodesModule::new(db.db().clone(), &cfg, secrets.clone(), lanes.clone());
        let gw = nodes.gateway();
        let artifacts = ArtifactsModule::with_backend(
            db.db().clone(),
            BackendConfig::Local {
                root: dirs.path().join("objects"),
            },
            Mode::Solo,
            dirs.path().join("scratch"),
        );
        let store = artifacts.store();
        let chains = Chains::new(
            vec![Arc::new(AcceptAll)],
            vec![Arc::new(FinalCheckpointVerifier::new(db.db().clone()))],
            vec![],
        );
        let module = CheckpointsModule::with_parts(
            db.db().clone(),
            store.clone(),
            secrets.clone(),
            &gw,
            &lanes,
            fast_checkpoints(),
            chains.clone(),
        );
        let service = module.service();
        let runs = RunsModule::with_chains(db.db().clone(), chains.clone());
        let seams = ts::build(
            ts::solo(TEST_SECRET).secrets_arc(secrets.clone()),
            Mode::Solo,
        );
        let server = TestServer::start(
            cfg,
            seams,
            vec![
                Box::new(nodes),
                Box::new(artifacts),
                Box::new(runs),
                // The background passes are driven by the tests (see `fast_checkpoints`);
                // the module still serves its routes and its sink.
                Box::new(RoutesOnly(module)),
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("server: {}", e.explain()));
        let engine = Arc::new(FakeEngine {
            staging: dirs.path().join("staging"),
            api: server.api(),
            fault: Mutex::default(),
            attempt: Mutex::default(),
            run: Mutex::default(),
            link: Mutex::default(),
            next_step: Mutex::new(100),
        });
        let agent = TestAgent::new(1, |_| {});
        let mut env = Self {
            server,
            db,
            gw,
            service,
            store,
            chains,
            engine,
            node: Uuid::nil(),
            running: None,
            agent,
            _dirs: dirs,
        };
        let pool = env.post("/api/v1/pools", json!({"name": "test"})).await;
        let pool = pool.json()["id"].as_str().unwrap().to_owned();
        let token = env
            .post(
                &format!("/api/v1/pools/{pool}/enrollment-tokens"),
                json!({}),
            )
            .await;
        assert_eq!(token.status, 201, "token: {}", token.text());
        let token = token.json()["token"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let node = mrl_agent::enroll::enroll_dial(
            &env.agent.ctx,
            &env.gw.addr().unwrap().to_string(),
            &token,
            mrl_agent::transports::TransportKind::Direct,
            false,
        )
        .await
        .unwrap()
        .link
        .node_id;
        env.node = NodeId::from_str(&node).unwrap().as_uuid();
        let link = Link::of(&env.agent.ctx).unwrap();
        for kind in ["LaunchAttempt", "UploadArtifacts", "CheckpointAttempt"] {
            link.register(kind, env.engine.clone());
        }
        *env.engine.link.lock().unwrap() = Some(link);
        env.running = Some(env.agent.run());
        for _ in 0..200 {
            let n = env.get(&format!("/api/v1/nodes/{node}")).await.json();
            if n["status"] == "ONLINE" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        env
    }

    async fn post(&self, path: &str, body: Value) -> TestResponse {
        self.send(Method::POST, path, Some(body)).await
    }

    async fn get(&self, path: &str) -> TestResponse {
        self.send(Method::GET, path, None).await
    }

    async fn send(&self, m: Method, path: &str, body: Option<Value>) -> TestResponse {
        let key = Uuid::new_v4().to_string();
        let body = body.map_or_else(Body::empty, |b| Body::from(b.to_string()));
        let mut headers = vec![
            (LAUNCH_SECRET_HEADER, TEST_SECRET),
            ("content-type", "application/json"),
        ];
        if path.ends_with("/runs") {
            headers.push(("idempotency-key", &key));
        }
        self.server.request(m, path, &headers, body).await
    }

    /// A TRAIN run whose job is RUNNING with attempt 1 launched on the agent.
    async fn running_run(&self, config: Value) -> (Uuid, Uuid) {
        let r = self
            .post(
                &format!("/api/v1/projects/{}/runs", ProjectId::new()),
                json!({
                    "name": "p23", "algorithm_id": "sft",
                    "base_model_version_id": mrl_domain::ids::ModelVersionId::new().to_string(),
                    "dataset_version_ids": [mrl_domain::ids::DatasetVersionId::new().to_string()],
                    "config": config,
                }),
            )
            .await;
        assert_eq!(r.status, 201, "{}", r.text());
        let run = RunId::from_str(r.json()["id"].as_str().unwrap())
            .unwrap()
            .as_uuid();
        let job = JobId::from_str(r.json()["job_id"].as_str().unwrap())
            .unwrap()
            .as_uuid();
        let mut tx = self.db.db().begin().await.unwrap();
        let row = self.lock(&mut tx, job).await;
        let row = JobTransitions::apply(
            &mut tx,
            &row,
            JobEvent::Place,
            Owner::Scheduler,
            JobChange::default(),
        )
        .await
        .unwrap();
        let (row, attempt) = JobTransitions::launch(&mut tx, &row, None).await.unwrap();
        let attempt = JobTransitions::apply_attempt(
            &mut tx,
            &attempt,
            JobAttemptEvent::Acknowledge,
            AttemptChange::default(),
        )
        .await
        .unwrap();
        JobTransitions::apply_attempt(
            &mut tx,
            &attempt,
            JobAttemptEvent::Start,
            AttemptChange::default(),
        )
        .await
        .unwrap();
        let row = self.lock(&mut tx, row.id).await;
        JobTransitions::apply(
            &mut tx,
            &row,
            JobEvent::Start,
            Owner::Jobs,
            JobChange::default(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        self.give_attempt(job, attempt.id, attempt.epoch).await;
        *self.engine.run.lock().unwrap() = Some(run);
        (run, job)
    }

    /// Sends the LaunchAttempt for `attempt` to the agent (the node now runs it) and points
    /// the fake engine at it.
    async fn give_attempt(&self, job: Uuid, attempt: Uuid, epoch: i64) {
        let sent = CommandSender::new(&self.gw, &[CommandKind::LaunchAttempt])
            .send(
                NodeId::from_uuid(self.node),
                NewCommand::new(command::Kind::LaunchAttempt(Default::default()))
                    .attempt(AttemptId::from_uuid(attempt), epoch as u64),
            )
            .await
            .unwrap();
        self.until("launch completed", || async {
            let s: String = sqlx::query_scalar("SELECT state FROM agent_commands WHERE id = $1")
                .bind(sent.id)
                .fetch_one(self.db.pool())
                .await
                .unwrap();
            s == "COMPLETED"
        })
        .await;
        *self.engine.attempt.lock().unwrap() = Some(AttemptRef {
            job_id: JobId::from_uuid(job).to_string(),
            attempt_id: AttemptId::from_uuid(attempt).to_string(),
            fencing_epoch: epoch as u64,
        });
    }

    async fn lock(&self, tx: &mut mrl_server::db::Tx, job: Uuid) -> JobRow {
        JobTransitions::lock(tx, Uuid::nil(), job)
            .await
            .unwrap()
            .unwrap()
    }

    async fn until<F, Fut>(&self, what: &str, ok: F)
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        for _ in 0..300 {
            if ok().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for {what}");
    }

    async fn state(&self, ckpt: Uuid) -> String {
        sqlx::query_scalar("SELECT state FROM checkpoints WHERE id = $1")
            .bind(ckpt)
            .fetch_optional(self.db.pool())
            .await
            .unwrap()
            .unwrap_or_default()
    }

    async fn wait_state(&self, ckpt: Uuid, want: &str) {
        self.until(&format!("checkpoint {want}"), || async {
            self.service.tick(&self.chains).await;
            self.state(ckpt).await == want
        })
        .await;
    }

    async fn latest(&self, run: Uuid) -> Option<Uuid> {
        sqlx::query_scalar("SELECT latest_committed_checkpoint_id FROM runs WHERE id = $1")
            .bind(run)
            .fetch_one(self.db.pool())
            .await
            .unwrap()
    }

    async fn job_state(&self, job: Uuid) -> (String, Option<String>) {
        sqlx::query_as("SELECT state, failure_class FROM jobs WHERE id = $1")
            .bind(job)
            .fetch_one(self.db.pool())
            .await
            .unwrap()
    }

    /// Ends the attempt OK and moves the job to FINALIZING.
    async fn finish(&self, job: Uuid) {
        let mut tx = self.db.db().begin().await.unwrap();
        let row = self.lock(&mut tx, job).await;
        let attempt =
            JobTransitions::lock_attempt(&mut tx, Uuid::nil(), row.current_attempt_id.unwrap())
                .await
                .unwrap()
                .unwrap();
        JobTransitions::apply_attempt(
            &mut tx,
            &attempt,
            JobAttemptEvent::ExitOk,
            AttemptChange::default(),
        )
        .await
        .unwrap();
        let row = self.lock(&mut tx, job).await;
        JobTransitions::apply(
            &mut tx,
            &row,
            JobEvent::Finish,
            Owner::Jobs,
            JobChange::default(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }

    async fn object(&self, ckpt: Uuid, role: &str) -> String {
        sqlx::query_scalar(
            "SELECT object_key FROM checkpoint_files WHERE checkpoint_id = $1 AND role = $2",
        )
        .bind(ckpt)
        .bind(role)
        .fetch_one(self.db.pool())
        .await
        .unwrap()
    }

    async fn stop(mut self) {
        if let Some(r) = self.running.take() {
            r.stop().await;
        }
    }
}

/// The checkpoints module without its background task: tests drive the passes.
struct RoutesOnly(CheckpointsModule);
impl ServerModule for RoutesOnly {
    fn name(&self) -> &'static str {
        "checkpoints"
    }
    fn routes(&self) -> axum::Router<mrl_server::app::AppState> {
        self.0.routes()
    }
    fn openapi(&self) -> utoipa::openapi::OpenApi {
        self.0.openapi()
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn happy_path_commits_lists_verifies_and_completes_the_job() {
    let env = Env::start().await;
    let (run, job) = env
        .running_run(json!({"metrics": {"required": ["train/loss"]}, "common": {"max_steps": 20}}))
        .await;
    let first = env.engine.checkpoint(10, false, &[("train/loss", 1.0)]);
    env.wait_state(first, "COMMITTED").await;
    assert_eq!(env.latest(run).await, Some(first));
    let last = env.engine.checkpoint(20, true, &[("train/loss", 0.5)]);
    env.wait_state(last, "COMMITTED").await;
    assert_eq!(env.latest(run).await, Some(last));
    let fin: Option<Uuid> =
        sqlx::query_scalar("SELECT final_checkpoint_id FROM runs WHERE id = $1")
            .bind(run)
            .fetch_one(env.db.pool())
            .await
            .unwrap();
    assert_eq!(fin, Some(last));
    // Every step has its event, in order.
    let events: Vec<String> = sqlx::query_scalar(
        "SELECT type FROM outbox_events WHERE aggregate_id = $1 ORDER BY aggregate_seq",
    )
    .bind(last)
    .fetch_all(env.db.pool())
    .await
    .unwrap();
    assert_eq!(
        events,
        [
            "checkpoint.staged",
            "checkpoint.uploaded",
            "checkpoint.commit_requested",
            "checkpoint.committed"
        ]
    );

    let r = env
        .get(&format!(
            "/api/v1/runs/{}/checkpoints",
            RunId::from_uuid(run)
        ))
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let items = r.json()["items"].as_array().unwrap().clone();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["step"], 20);
    assert_eq!(items[0]["verification_level"], "manifest_head");
    let id = CheckpointId::from_uuid(last).to_string();
    let r = env.get(&format!("/api/v1/checkpoints/{id}")).await;
    assert_eq!(r.json()["files"].as_array().unwrap().len(), 3);
    let r = env
        .post(&format!("/api/v1/checkpoints/{id}:verify"), json!({}))
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["verification_level"], "full_rehash");
    env.service.require_full_verification(last).await.unwrap();

    env.finish(job).await;
    env.service.tick(&env.chains).await;
    assert_eq!(env.job_state(job).await.0, "COMPLETED");
    // Optimizer state goes after completion; the files are deleted after their rows.
    cleanup::strip_completed(&env.service).await.unwrap();
    let ckpt = env.service.find(last).await.unwrap().unwrap();
    assert_eq!(ckpt.kind, "weights_only");
    assert_eq!(env.service.files(last).await.unwrap().len(), 2);
    assert!(cleanup::delete_queued(&env.service).await.unwrap() >= 1);
    env.service.rehash(last).await.unwrap();
    env.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn missing_short_and_changed_files_make_checkpoints_corrupt_and_resume_falls_back() {
    let env = Env::start().await;
    let (run, _) = env.running_run(json!({})).await;
    let good = env.engine.checkpoint(10, false, &[]);
    env.wait_state(good, "COMMITTED").await;

    env.engine.set(Fault::Cut("weights.bin"));
    let short = env.engine.checkpoint(20, false, &[]);
    env.wait_state(short, "CORRUPT").await;
    let reason = env
        .service
        .find(short)
        .await
        .unwrap()
        .unwrap()
        .corrupt_reason
        .unwrap();
    assert!(reason.contains("bytes in storage"), "{reason}");

    env.engine.set(Fault::Skip("optimizer.pt"));
    let missing = env.engine.checkpoint(30, false, &[]);
    // Open until the staging TTL (3 s), then CORRUPT.
    tokio::time::sleep(Duration::from_millis(500)).await;
    env.service.tick(&env.chains).await;
    assert_eq!(env.state(missing).await, "WRITING");
    env.wait_state(missing, "CORRUPT").await;
    assert_eq!(env.latest(run).await, Some(good));

    env.engine.set(Fault::None);
    let newer = env.engine.checkpoint(40, false, &[]);
    env.wait_state(newer, "COMMITTED").await;
    assert_eq!(env.service.resume_point(run).await.unwrap(), Some(newer));
    // A byte flipped in storage: the rehash finds it, the run falls back to `good`.
    let store = env.store.org_store("local").await.unwrap();
    let key = env.object(newer, "weights").await;
    let mut bytes = store.get(&key).await.unwrap().to_vec();
    bytes[0] ^= 1;
    store.put(&key, bytes.into()).await.unwrap();
    let r = env
        .post(
            &format!(
                "/api/v1/checkpoints/{}:verify",
                CheckpointId::from_uuid(newer)
            ),
            json!({}),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["state"], "CORRUPT");
    assert_eq!(env.latest(run).await, Some(good));
    assert_eq!(env.service.resume_point(run).await.unwrap(), Some(good));
    // Changed after commit, found at load (no rehash): CORRUPT, nothing left to resume from.
    let key = env.object(good, "optimizer").await;
    store.put(&key, b"x".to_vec().into()).await.unwrap();
    assert_eq!(env.service.resume_point(run).await.unwrap(), None);
    assert_eq!(env.state(good).await, "CORRUPT");
    env.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn stale_epochs_and_cancelled_jobs_never_commit() {
    let env = Env::start().await;
    let (run, job) = env.running_run(json!({})).await;
    // The attempt's lease is lost after staging: the commit is refused.
    env.engine.set(Fault::Hold);
    let (stale, sha) = env.engine.stage(10, false, &[]);
    env.until("staged", || async { env.state(stale).await == "WRITING" })
        .await;
    let mut tx = env.db.db().begin().await.unwrap();
    let row = env.lock(&mut tx, job).await;
    let attempt =
        JobTransitions::lock_attempt(&mut tx, Uuid::nil(), row.current_attempt_id.unwrap())
            .await
            .unwrap()
            .unwrap();
    JobTransitions::apply_attempt(
        &mut tx,
        &attempt,
        JobAttemptEvent::LoseLease,
        AttemptChange::default(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    env.engine.set(Fault::None);
    env.engine.commit(stale, &sha);
    // The upload itself is never sent again by the core for a fenced attempt, so the commit
    // is decided by hand once the files are there.
    let files = env.service.files(stale).await.unwrap();
    let store = env.store.org_store("local").await.unwrap();
    for f in &files {
        let path = env.engine.staging.join(&f.path);
        store
            .put(&f.object_key, std::fs::read(path).unwrap().into())
            .await
            .unwrap();
    }
    env.until("commit requested", || async {
        env.service
            .find(stale)
            .await
            .unwrap()
            .unwrap()
            .commit_requested_at
            .is_some()
    })
    .await;
    env.wait_state(stale, "CORRUPT").await;
    let reason = env
        .service
        .find(stale)
        .await
        .unwrap()
        .unwrap()
        .corrupt_reason
        .unwrap();
    assert!(reason.contains("replaced attempt"), "{reason}");
    // A checkpoint staged with the old epoch afterwards is CORRUPT at once.
    let old = env.engine.checkpoint(20, false, &[]);
    env.until("old epoch refused", || async {
        env.state(old).await == "CORRUPT"
    })
    .await;
    assert_eq!(env.latest(run).await, None);

    // Cancel while uploading: the job is CANCELLING when the commit comes; never COMMITTED.
    let (run2, job2) = env.running_run(json!({})).await;
    env.engine.set(Fault::Hold);
    let (late, sha) = env.engine.stage(5, false, &[]);
    env.until("staged", || async { env.state(late).await == "WRITING" })
        .await;
    let mut tx = env.db.db().begin().await.unwrap();
    let row = env.lock(&mut tx, job2).await;
    JobTransitions::request_cancel(&mut tx, &row, "u")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for f in env.service.files(late).await.unwrap() {
        let path = env.engine.staging.join(&f.path);
        store
            .put(&f.object_key, std::fs::read(path).unwrap().into())
            .await
            .unwrap();
    }
    env.engine.commit(late, &sha);
    env.wait_state(late, "CORRUPT").await;
    let reason = env
        .service
        .find(late)
        .await
        .unwrap()
        .unwrap()
        .corrupt_reason
        .unwrap();
    assert!(reason.contains("CANCELLING"), "{reason}");
    assert_eq!(env.latest(run2).await, None);
    env.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn concurrent_commits_serialize_and_latest_is_the_highest_step() {
    let env = Env::start().await;
    let (run, _) = env.running_run(json!({})).await;
    env.engine.set(Fault::Hold);
    let staged: Vec<(Uuid, String)> = (1..=4)
        .map(|s| env.engine.stage(s * 10, false, &[]))
        .collect();
    for (id, _) in &staged {
        env.until("staged", || async { env.state(*id).await == "WRITING" })
            .await;
        let store = env.store.org_store("local").await.unwrap();
        for f in env.service.files(*id).await.unwrap() {
            let bytes = std::fs::read(env.engine.staging.join(&f.path)).unwrap();
            store.put(&f.object_key, bytes.into()).await.unwrap();
        }
        sqlx::query("UPDATE checkpoints SET commit_requested_at = now() WHERE id = $1")
            .bind(id)
            .execute(env.db.pool())
            .await
            .unwrap();
    }
    // Every checkpoint committed by several callers at once, newest first.
    let mut tasks = Vec::new();
    for (id, _) in staged.iter().rev() {
        for _ in 0..3 {
            let (svc, id) = (env.service.clone(), *id);
            tasks.push(tokio::spawn(async move { svc.try_commit(id).await }));
        }
    }
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    for (id, _) in &staged {
        assert_eq!(env.state(*id).await, "COMMITTED");
        let committed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND type = 'checkpoint.committed'",
        )
        .bind(id)
        .fetch_one(env.db.pool())
        .await
        .unwrap();
        assert_eq!(committed, 1);
    }
    assert_eq!(env.latest(run).await, Some(staged[3].0));
    let list = format!("/api/v1/runs/{}/checkpoints", RunId::from_uuid(run));
    let r = env.get(&format!("{list}?limit=3")).await;
    let steps: Vec<i64> = r.json()["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["step"].as_i64().unwrap())
        .collect();
    assert_eq!(steps, [40, 30, 20]);
    let next = r.json()["next_cursor"].as_str().unwrap().to_owned();
    let r = env.get(&format!("{list}?limit=3&cursor={next}")).await;
    assert_eq!(r.json()["items"][0]["step"], 10, "{}", r.text());
    assert!(r.json()["next_cursor"].is_null());

    // Retention keeps the newest two (and refuses to delete what is in use).
    cleanup::retention(&env.service).await.unwrap();
    assert_eq!(env.state(staged[0].0).await, "TOMBSTONED");
    assert_eq!(env.state(staged[1].0).await, "TOMBSTONED");
    assert_eq!(env.state(staged[2].0).await, "COMMITTED");
    let latest = CheckpointId::from_uuid(staged[3].0);
    let r = env
        .send(
            Method::DELETE,
            &format!("/api/v1/checkpoints/{latest}"),
            None,
        )
        .await;
    assert_eq!(r.status, 409, "{}", r.text());
    assert_eq!(r.json()["code"], "MRL-CONFLICT-230");
    let other = CheckpointId::from_uuid(staged[2].0);
    let r = env
        .send(
            Method::DELETE,
            &format!("/api/v1/checkpoints/{other}"),
            None,
        )
        .await;
    assert_eq!(r.status, 204, "{}", r.text());
    let r = env
        .send(
            Method::DELETE,
            &format!("/api/v1/checkpoints/{other}"),
            None,
        )
        .await;
    assert_eq!(r.status, 204);
    let r = env
        .post(&format!("/api/v1/checkpoints/{other}:verify"), json!({}))
        .await;
    assert_eq!(r.json()["code"], "MRL-STATE-230");
    let key = env.object(staged[0].0, "weights").await;
    assert!(cleanup::delete_queued(&env.service).await.unwrap() >= 9);
    let store = env.store.org_store("local").await.unwrap();
    assert!(
        store.head(&key).await.is_err(),
        "tombstoned objects are deleted"
    );
    let r = env.get(&list).await;
    assert_eq!(
        r.json()["items"].as_array().unwrap().len(),
        1,
        "{}",
        r.text()
    );
    env.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_finished_job_without_a_final_checkpoint_fails_verification() {
    let env = Env::start().await;
    let (_, job) = env.running_run(json!({})).await;
    let some = env.engine.checkpoint(10, false, &[]);
    env.wait_state(some, "COMMITTED").await;
    env.finish(job).await;
    env.service.tick(&env.chains).await;
    assert_eq!(
        env.job_state(job).await,
        ("FAILED".to_owned(), Some("VERIFICATION_FAILED".to_owned()))
    );

    // A final checkpoint that misses a required metric fails too.
    let (_, job) = env
        .running_run(json!({"metrics": {"required": ["train/loss"]}}))
        .await;
    let last = env.engine.checkpoint(10, true, &[]);
    env.wait_state(last, "COMMITTED").await;
    env.finish(job).await;
    env.service.tick(&env.chains).await;
    let (state, class) = env.job_state(job).await;
    assert_eq!(
        (state.as_str(), class.as_deref()),
        ("FAILED", Some("VERIFICATION_FAILED"))
    );
    let reason: Value = sqlx::query_scalar("SELECT state_reason FROM jobs WHERE id = $1")
        .bind(job)
        .fetch_one(env.db.pool())
        .await
        .unwrap();
    assert!(reason.to_string().contains("train/loss"), "{reason}");
    env.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn emergency_and_time_based_checkpoints_reach_the_worker() {
    let env = Env::start().await;
    let (_, job) = env
        .running_run(json!({"checkpoint": {"time_based": 1}}))
        .await;
    // The background tick commits once the upload is in.
    let (svc, chains) = (env.service.clone(), env.chains.clone());
    let ticker = tokio::spawn(async move {
        loop {
            svc.tick(&chains).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
    let out = env
        .service
        .emergency_checkpoint(job, "guard_soft_threshold", Duration::from_secs(20))
        .await
        .unwrap();
    let EmergencyOutcome::Committed(id) = out else {
        panic!("expected a committed checkpoint, got {out:?}");
    };
    assert_eq!(env.state(id).await, "COMMITTED");
    ticker.abort();
    let _ = ticker.await;
    // Nothing for a minute: the cadence asks for a checkpoint (once per period).
    env.db
        .seed(
            "UPDATE job_attempts SET started_at = now() - interval '1 hour', created_at = now() - interval '1 hour'; \
             UPDATE checkpoints SET created_at = now() - interval '1 hour'; \
             UPDATE agent_commands SET created_at = now() - interval '1 hour';",
        )
        .await;
    env.service.tick(&env.chains).await;
    env.service.tick(&env.chains).await;
    let asked: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT payload FROM agent_commands WHERE kind = 'CheckpointAttempt' AND created_at > now() - interval '1 minute'",
    )
    .fetch_all(env.db.pool())
    .await
    .unwrap();
    assert_eq!(asked.len(), 1);
    let cmd = <Command as prost::Message>::decode(asked[0].as_slice()).unwrap();
    let Some(command::Kind::CheckpointAttempt(c)) = cmd.kind else {
        panic!("not a checkpoint request");
    };
    assert_eq!(c.reason, "time_based");
    // A job with no running attempt: refused at once.
    let mut tx = env.db.db().begin().await.unwrap();
    let row = env.lock(&mut tx, job).await;
    JobTransitions::request_cancel(&mut tx, &row, "u")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let out = env
        .service
        .emergency_checkpoint(job, "guard", Duration::from_secs(1))
        .await
        .unwrap();
    assert!(matches!(out, EmergencyOutcome::Refused(_)), "{out:?}");
    env.stop().await;
}

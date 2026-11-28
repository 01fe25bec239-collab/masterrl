//! INT-6 integration tests: a built-in job through the real gateway and a real agent link.
//!
//! Compiled by `mrl-testkit` (see its `lib.rs`), not by this crate. `#[ignore]`d; run them
//! with `just dev && just test-integration`.
//!
//! The fake worker writes outputs and relays `OutputsStaged` on the agent's CONTROL link; the
//! agent side answers `UploadArtifacts` by PUTting each file with its ticket, and the test then
//! sends the upload's outcome (what the agent's runner reports once the files are sent).

use crate::agent::gateway::{fast_config, server_config, Running, TestAgent};
use crate::pg::TestDb;
use crate::seams::{self as ts, InMemorySecretStore};
use crate::server::{TestServer, TEST_SECRET};
use async_trait::async_trait;
use axum::http::Method;
use mrl_agent::session::{CommandHandler, Link};
use mrl_domain::ids::{AttemptId, JobId, NodeId, ProjectId};
use mrl_domain::state::{JobAttemptEvent, JobEvent, Owner};
use mrl_proto::agent::v1::{
    agent_message, command, Command, CommandResult, RoleLaunch, WorkerRelay,
};
use mrl_proto::common::v1::{AttemptRef, Failure};
use mrl_proto::worker::v1::{worker_message, OutputsStaged, WorkerMessage};
use mrl_server::config::Mode;
use mrl_server::modules::artifacts::{ArtifactStore, ArtifactsModule};
use mrl_server::modules::builtin::launch::BuiltinLaunch;
use mrl_server::modules::builtin::sink::{register_pass, Outputs};
use mrl_server::modules::builtin::{self, BuiltinJob, BuiltinModule, OutputsVerifier};
use mrl_server::modules::jobs::{
    AttemptChange, CompletionVerifier, JobChange, JobRow, JobTransitions, Verification,
};
use mrl_server::modules::nodes::commands::{CommandKind, CommandSender, NewCommand};
use mrl_server::modules::nodes::lanes::LaneRouter;
use mrl_server::modules::nodes::{Gateway, NodesModule};
use mrl_server::modules::scheduler::launch::Transfer;
use mrl_server::modules::scheduler::pipeline::{LaunchContributor, LaunchPlan, Placement};
use mrl_server::seams::{SecretStore, LAUNCH_SECRET_HEADER};
use mrl_storage::{sha256_hex, BackendConfig};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

/// The agent's upload side: PUTs every item with its ticket.
struct Uploader {
    staging: PathBuf,
    api: std::net::SocketAddr,
    uploads: Mutex<Vec<(String, usize)>>,
}

#[async_trait]
impl CommandHandler for Uploader {
    async fn handle(&self, c: Command) -> Result<(), Failure> {
        if let Some(command::Kind::UploadArtifacts(u)) = c.kind {
            for item in &u.items {
                let bytes = std::fs::read(self.staging.join(&item.local_path)).unwrap();
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
            self.uploads
                .lock()
                .unwrap()
                .push((c.command_id, u.items.len()));
        }
        Ok(())
    }
}

struct Env {
    _server: TestServer,
    db: TestDb,
    gw: Arc<Gateway>,
    outputs: Arc<Outputs>,
    store: Arc<ArtifactStore>,
    uploader: Arc<Uploader>,
    link: Arc<Link>,
    node: Uuid,
    running: Option<Running>,
    _agent: TestAgent,
    _dirs: tempfile::TempDir,
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
        let transfer = Transfer::new(store.clone(), artifacts.secrets(), secrets.clone());
        let outputs = Arc::new(Outputs::new(db.db().clone(), store.clone(), transfer, &gw));
        // Registers the outputs sink on `lanes`; the pass is driven by the tests.
        let _module = BuiltinModule::with_parts(outputs.clone(), &lanes);
        let seams = ts::build(
            ts::solo(TEST_SECRET).secrets_arc(secrets.clone()),
            Mode::Solo,
        );
        let server = TestServer::start(cfg, seams, vec![Box::new(nodes), Box::new(artifacts)])
            .await
            .unwrap_or_else(|e| panic!("server: {}", e.explain()));
        let uploader = Arc::new(Uploader {
            staging: dirs.path().join("staging"),
            api: server.api(),
            uploads: Mutex::default(),
        });
        let agent = TestAgent::new(1, |_| {});
        let pool = post(&server, "/api/v1/pools", json!({"name": "test"})).await;
        let pool = pool["id"].as_str().unwrap().to_owned();
        let token = post(
            &server,
            &format!("/api/v1/pools/{pool}/enrollment-tokens"),
            json!({}),
        )
        .await;
        let node = mrl_agent::enroll::enroll_dial(
            &agent.ctx,
            &gw.addr().unwrap().to_string(),
            token["token"].as_str().unwrap(),
            mrl_agent::transports::TransportKind::Direct,
            false,
        )
        .await
        .unwrap()
        .link
        .node_id;
        let link = Link::of(&agent.ctx).unwrap();
        for kind in ["LaunchAttempt", "UploadArtifacts"] {
            link.register(kind, uploader.clone());
        }
        let running = Some(agent.run());
        let env = Self {
            _server: server,
            db,
            gw,
            outputs,
            store,
            uploader,
            link,
            node: NodeId::from_str(&node).unwrap().as_uuid(),
            running,
            _agent: agent,
            _dirs: dirs,
        };
        env.until("node online", || async {
            sqlx::query_scalar::<_, String>("SELECT status FROM compute_nodes WHERE id = $1")
                .bind(env.node)
                .fetch_optional(env.db.pool())
                .await
                .unwrap()
                .as_deref()
                == Some("ONLINE")
        })
        .await;
        env
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

    async fn lock(&self, tx: &mut mrl_server::db::Tx, job: Uuid) -> JobRow {
        JobTransitions::lock(tx, Uuid::nil(), job)
            .await
            .unwrap()
            .unwrap()
    }

    /// A DATA_PREP job (task `hash_files`), QUEUED, pinned to the test node.
    async fn submit(&self) -> JobRow {
        let mut tx = self.db.db().begin().await.unwrap();
        let job = builtin::submit(
            &mut tx,
            BuiltinJob {
                org_id: Uuid::nil(),
                project_id: ProjectId::new().as_uuid(),
                kind: "DATA_PREP".into(),
                task: "hash_files".into(),
                inputs: json!({"roles": ["extra"]}),
                files: Vec::new(),
                subject: json!({"dataset_version_id": "dsv_test"}),
                created_by: "usr_test".into(),
                grants: Vec::new(),
                node_id: Some(self.node),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        job
    }

    /// Places, launches and starts the job's attempt on the test node; returns the worker's
    /// attempt reference.
    async fn start_job(&self, job: &JobRow) -> (AttemptRef, mrl_server::modules::jobs::AttemptRow) {
        let mut tx = self.db.db().begin().await.unwrap();
        let row = JobTransitions::apply(
            &mut tx,
            job,
            JobEvent::Place,
            Owner::Scheduler,
            JobChange::default(),
        )
        .await
        .unwrap();
        let (row, attempt) = JobTransitions::launch(&mut tx, &row, None).await.unwrap();
        sqlx::query(
            "INSERT INTO attempt_placements (attempt_id, node_id, org_id, node_rank, rank_lo, rank_hi, role)
             VALUES ($1, $2, $3, 0, 0, 0, 'service')",
        )
        .bind(attempt.id)
        .bind(self.node)
        .bind(attempt.org_id)
        .execute(&mut *tx)
        .await
        .unwrap();
        let attempt = JobTransitions::apply_attempt(
            &mut tx,
            &attempt,
            JobAttemptEvent::Acknowledge,
            AttemptChange::default(),
        )
        .await
        .unwrap();
        let attempt = JobTransitions::apply_attempt(
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
        let sent = CommandSender::new(&self.gw, &[CommandKind::LaunchAttempt])
            .send(
                NodeId::from_uuid(self.node),
                NewCommand::new(command::Kind::LaunchAttempt(Default::default()))
                    .attempt(AttemptId::from_uuid(attempt.id), attempt.epoch as u64),
            )
            .await
            .unwrap();
        self.until("launch completed", || async {
            self.command_state(sent.id).await == "COMPLETED"
        })
        .await;
        let at = AttemptRef {
            job_id: JobId::from_uuid(job.id).to_string(),
            attempt_id: AttemptId::from_uuid(attempt.id).to_string(),
            fencing_epoch: attempt.epoch as u64,
        };
        (at, attempt)
    }

    async fn command_state(&self, id: Uuid) -> String {
        sqlx::query_scalar("SELECT state FROM agent_commands WHERE id = $1")
            .bind(id)
            .fetch_one(self.db.pool())
            .await
            .unwrap()
    }

    /// Writes `outputs/hashes.json` and returns its artifact manifest.
    fn write_outputs(&self) -> Value {
        let out = self.uploader.staging.join("outputs");
        std::fs::create_dir_all(&out).unwrap();
        let bytes = br#"{"files": []}"#;
        std::fs::write(out.join("hashes.json"), bytes).unwrap();
        json!({
            "schema_version": "artifact-manifest/v1",
            "kind": "other",
            "created_at": "2026-11-27T00:00:00Z",
            "files": [{"path": "hashes.json", "size": bytes.len(), "sha256": sha256_hex(bytes), "role": "hashes"}],
            "source": {"task": "hash_files"}
        })
    }

    fn relay(&self, at: &AttemptRef, manifest_json: String) {
        self.link
            .send_control(agent_message::Body::Worker(WorkerRelay {
                attempt: Some(at.clone()),
                role_id: "service".into(),
                message: Some(WorkerMessage {
                    seq: 0,
                    sent_at: None,
                    body: Some(worker_message::Body::OutputsStaged(OutputsStaged {
                        manifest_json,
                    })),
                }),
            }))
            .unwrap();
    }

    /// The upload command sent for `job`, once the agent side answered it.
    async fn upload_command(&self, job: Uuid) -> Uuid {
        let mut id = None;
        for _ in 0..300 {
            id = sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT upload_command_id FROM builtin_jobs WHERE job_id = $1",
            )
            .bind(job)
            .fetch_one(self.db.pool())
            .await
            .unwrap();
            if let Some(id) = id {
                if self.command_state(id).await == "COMPLETED" {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        id.expect("an UploadArtifacts command")
    }

    /// Sends the upload's outcome, as the agent's runner does when the files are sent.
    async fn outcome(&self, command: Uuid) {
        self.link
            .send_control(agent_message::Body::CommandResult(CommandResult {
                command_id: command.to_string(),
                accepted: true,
                failure: None,
            }))
            .unwrap();
        self.until("outcome saved", || async {
            sqlx::query_scalar::<_, bool>(
                "SELECT outcome IS NOT NULL FROM agent_commands WHERE id = $1",
            )
            .bind(command)
            .fetch_one(self.db.pool())
            .await
            .unwrap()
        })
        .await;
    }

    async fn job(&self, id: Uuid) -> JobRow {
        let mut tx = self.db.db().begin().await.unwrap();
        let row = self.lock(&mut tx, id).await;
        tx.commit().await.unwrap();
        row
    }

    async fn stop(mut self) {
        if let Some(r) = self.running.take() {
            r.stop().await;
        }
    }
}

async fn post(server: &TestServer, path: &str, body: Value) -> Value {
    let r = server
        .request(
            Method::POST,
            path,
            &[
                (LAUNCH_SECRET_HEADER, TEST_SECRET),
                ("content-type", "application/json"),
            ],
            axum::body::Body::from(body.to_string()),
        )
        .await;
    assert!(r.status < 300, "{path}: {} {}", r.status, r.text());
    r.json()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_builtin_job_runs_its_task_in_its_sandbox_and_registers_its_outputs() {
    let env = Env::start().await;
    let job = env.submit().await;
    assert_eq!(job.state, "QUEUED");
    assert_eq!(
        job.resource_request["roles"][0]["node_ids"][0],
        NodeId::from_uuid(env.node).to_string()
    );

    // The launch: task, inputs and sandbox in the spec, the built-in engine and runtime.
    let (at, attempt) = env.start_job(&job).await;
    let mut plan = LaunchPlan::default();
    plan.launch.job_spec_json = json!({"job_kind": "DATA_PREP", "sandbox": "worker"}).to_string();
    plan.launch.roles = vec![RoleLaunch {
        role_id: "service-0".into(),
        kind: "service".into(),
        primary: true,
        ..Default::default()
    }];
    let placement = Placement {
        node_id: env.node,
        roles: Vec::new(),
        cpu_millis: 0,
        mem_bytes: 0,
    };
    BuiltinLaunch::new(env.db.db().clone())
        .contribute(&env.job(job.id).await, &attempt, &placement, &mut plan)
        .await
        .unwrap();
    let spec: Value = serde_json::from_str(&plan.launch.job_spec_json).unwrap();
    assert_eq!(spec["task"], "hash_files");
    assert_eq!(spec["inputs"], json!({"roles": ["extra"]}));
    assert_eq!(spec["sandbox"], "data_prep");
    assert_eq!(plan.launch.engine_name, "builtin");
    assert!(plan.launch.roles[0].runtime_ref.starts_with("uv:sha256:"));
    assert_eq!(plan.launch.environment_id, "cpu-uv");

    // Not finished yet: the verifier waits (an error keeps the job FINALIZING).
    let verifier = OutputsVerifier::new(env.db.db().clone());
    let manifest = env.write_outputs();
    env.relay(&at, manifest.to_string());
    let command = env.upload_command(job.id).await;
    assert_eq!(
        env.uploader.uploads.lock().unwrap().as_slice(),
        [(command.to_string(), 1)]
    );
    // A replayed OutputsStaged sends nothing new.
    env.relay(&at, manifest.to_string());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(env.uploader.uploads.lock().unwrap().len(), 1);
    register_pass(&env.outputs).await;
    let row = env.job(job.id).await;
    assert!(verifier.verify(&row).await.is_err());
    assert!(builtin::outputs(env.db.db(), Uuid::nil(), job.id)
        .await
        .unwrap()
        .is_none());

    // Uploaded: the pass registers one artifact, and the job may complete.
    env.outcome(command).await;
    register_pass(&env.outputs).await;
    let failure: Option<String> =
        sqlx::query_scalar("SELECT failure FROM builtin_jobs WHERE job_id = $1")
            .bind(job.id)
            .fetch_one(env.db.pool())
            .await
            .unwrap();
    assert_eq!(failure, None);
    let out = builtin::outputs(env.db.db(), Uuid::nil(), job.id)
        .await
        .unwrap()
        .expect("registered outputs");
    assert_eq!(out.manifest["files"][0]["path"], "hashes.json");
    let artifact = env
        .store
        .get(mrl_server::modules::artifacts::LOCAL_ORG, out.artifact_id)
        .await
        .unwrap()
        .expect("the artifact");
    assert_eq!(artifact.size_bytes, 13);
    assert_eq!(verifier.verify(&row).await.unwrap(), Verification::Abstain);
    // Another org sees nothing.
    assert!(builtin::outputs(env.db.db(), Uuid::now_v7(), job.id)
        .await
        .unwrap()
        .is_none());
    env.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn refused_outputs_release_the_worker_and_fail_the_job() {
    let env = Env::start().await;
    let verifier = OutputsVerifier::new(env.db.db().clone());
    let job = env.submit().await;
    let (at, _) = env.start_job(&job).await;
    // A job that sent no outputs can't complete.
    let Verification::Failed(why) = verifier.verify(&env.job(job.id).await).await.unwrap() else {
        panic!("a job without outputs must fail");
    };
    assert!(why.message.contains("sent none"), "{}", why.message);

    // A file outside the outputs folder: refused, answered with an empty upload.
    let mut manifest = env.write_outputs();
    manifest["files"][0]["path"] = json!("../escape.json");
    env.relay(&at, manifest.to_string());
    let command = env.upload_command(job.id).await;
    assert_eq!(
        env.uploader.uploads.lock().unwrap().as_slice(),
        [(command.to_string(), 0)]
    );
    let Verification::Failed(why) = verifier.verify(&env.job(job.id).await).await.unwrap() else {
        panic!("refused outputs must fail the job");
    };
    assert!(
        why.message.contains("results could not be saved"),
        "{}",
        why.message
    );
    env.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn expired_tickets_are_reissued_and_finished_staging_is_removed() {
    let env = Env::start().await;
    let job = env.submit().await;
    let (at, attempt) = env.start_job(&job).await;
    let manifest = env.write_outputs();
    env.relay(&at, manifest.to_string());
    let command = env.upload_command(job.id).await;
    sqlx::query("UPDATE agent_commands SET state = 'EXPIRED' WHERE id = $1")
        .bind(command)
        .execute(env.db.pool())
        .await
        .unwrap();
    register_pass(&env.outputs).await;
    let next = env.upload_command(job.id).await;
    assert_ne!(next, command);
    sqlx::query("UPDATE agent_commands SET outcome = $2 WHERE id = $1")
        .bind(next)
        .bind(json!({"accepted": true, "failure": {"sub_code": "MRL-STORE-008"}}))
        .execute(env.db.pool())
        .await
        .unwrap();
    register_pass(&env.outputs).await;
    let renewed = env.upload_command(job.id).await;
    assert_ne!(renewed, next);
    assert!(env
        .uploader
        .uploads
        .lock()
        .unwrap()
        .contains(&(renewed.to_string(), 0)));
    env.outcome(renewed).await;
    register_pass(&env.outputs).await;

    let prefix: String =
        sqlx::query_scalar("SELECT upload_prefix FROM builtin_jobs WHERE job_id = $1")
            .bind(job.id)
            .fetch_one(env.db.pool())
            .await
            .unwrap();
    sqlx::query("UPDATE builtin_output_staging SET last_ticket_at = now() - interval '2 days'")
        .execute(env.db.pool())
        .await
        .unwrap();
    let store = env
        .store
        .org_store(mrl_server::modules::artifacts::LOCAL_ORG)
        .await
        .unwrap();
    let prefix = format!("{}{prefix}", store.keys().prefix());
    register_pass(&env.outputs).await;
    assert_eq!(store.list(&prefix).await.unwrap().len(), 1);
    sqlx::query("UPDATE job_attempts SET state = 'EXITED_OK', ended_at = now() WHERE id = $1")
        .bind(attempt.id)
        .execute(env.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE jobs SET state = 'COMPLETED', finished_at = now() WHERE id = $1")
        .bind(job.id)
        .execute(env.db.pool())
        .await
        .unwrap();
    register_pass(&env.outputs).await;
    assert!(store.list(&prefix).await.unwrap().is_empty());
    let sha = manifest["files"][0]["sha256"].as_str().unwrap();
    assert_eq!(
        store
            .sha256(&store.keys().blob(sha).unwrap())
            .await
            .unwrap(),
        sha
    );
    env.stop().await;
}

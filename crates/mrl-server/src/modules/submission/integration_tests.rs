//! P28 end-to-end tests: a real server (gateway, scheduler, runs, submission) on a test
//! database with the TRL engine registered, and in-process agents with fake GPUs whose
//! dry runs are scripted (they answer `Prepared` like the worker would, over the real
//! protocol). Compiled by mrl-testkit under its P28 marker; they need PostgreSQL
//! (`just test-integration`).

use crate::agent::gateway::{fast_config, server_config, TestAgent};
use crate::engines::TinyFixture;
use crate::pg::TestDb;
use crate::seams::InMemorySecretStore;
use crate::server::{TestResponse, TestServer, TEST_SECRET};
use axum::body::Body;
use axum::http::Method;
use mrl_agent::hw::{GpuDevice, GpuVendor};
use mrl_agent::session::{CommandHandler, Link};
use mrl_domain::capability::manifest::Bundle;
use mrl_domain::ids::{AttemptId, NodeId, OrgId, PoolId, ProjectId, RunId};
use mrl_proto::agent::v1::agent_message::Body as Msg;
use mrl_proto::agent::v1::command::Kind;
use mrl_proto::agent::v1::{AgentMessage, AttemptPhase, AttemptStatus, Command, WorkerRelay};
use mrl_proto::common::v1::{AttemptRef, Failure};
use mrl_proto::worker::v1::worker_message::Body as WorkerBody;
use mrl_proto::worker::v1::{ExitKind, MetricPoint, Prepared, ResourceEstimate, WorkerMessage};
use mrl_server::config::Mode;
use mrl_server::modules::artifacts::ArtifactsModule;
use mrl_server::modules::compute::LocalAgentPool;
use mrl_server::modules::engines::launch::EngineRuntime;
use mrl_server::modules::engines::CapabilityValidator;
use mrl_server::modules::estimates::Calibrate;
use mrl_server::modules::events::EventConsumer;
use mrl_server::modules::jobs::{
    Chains, ProjectDirectory, Projects, RehomeLocalRows, SchemaChecks,
};
use mrl_server::modules::nodes::lanes::{LaneItem, LaneKind, LaneRouter};
use mrl_server::modules::nodes::NodesModule;
use mrl_server::modules::scheduler::admission::{MaxQueuedPerProject, PoolFlags, PoolMembership};
use mrl_server::modules::scheduler::launch::{RunConfigSpec, Transfer};
use mrl_server::modules::scheduler::pipeline::Pipeline;
use mrl_server::modules::scheduler::placement::SingleNodePacking;
use mrl_server::modules::scheduler::{SchedulerConfig, SchedulerModule};
use mrl_server::modules::submission::policy::{Guardian, MethodPolicy};
use mrl_server::modules::submission::probe::{self, PreparedSink, ProbeLaunch, ProbeVerifier};
use mrl_server::modules::submission::resolve::Presets;
use mrl_server::modules::submission::{SubmissionConfig, SubmissionModule};
use mrl_server::modules::telemetry::ingest::{Delivery, Owner};
use mrl_server::modules::telemetry::{Ingest, MetricObservers};
use mrl_server::seams::StartupStep;
use mrl_server::seams::{
    Authorizer, Decision, LocalOwnerAuthorizer, RequestContext, Resource, SeamError, SecretStore,
    LAUNCH_SECRET_HEADER,
};
use serde_json::{json, Value};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

const WAIT: Duration = Duration::from_secs(60);
const TRL: &str = include_str!("../../../engines/first_party/trl.json");

/// How a scripted machine answers a dry-run launch.
#[derive(Clone)]
enum Answer {
    /// `Prepared{ok, estimate lo..hi GiB}`, then `DRY_RUN_OK`.
    Estimate(f64, f64),
    /// Takes the launch and never answers.
    Silent,
    /// The worker rejects validation without sending issues.
    Reject,
}

struct Script {
    link: Arc<Link>,
    answer: Mutex<Answer>,
    running: Mutex<Vec<AttemptRef>>,
    /// The job spec of every dry run this machine was asked for.
    probes: Mutex<Vec<Value>>,
}

fn status(r: &AttemptRef, phase: AttemptPhase, exit: ExitKind) -> AttemptStatus {
    AttemptStatus {
        attempt: Some(r.clone()),
        phase: phase as i32,
        exit_kind: exit as i32,
        time: Some(prost_types::Timestamp::default()),
        ..AttemptStatus::default()
    }
}

fn prepared(r: &AttemptRef, lo: f64, hi: f64) -> Msg {
    Msg::Worker(WorkerRelay {
        attempt: Some(r.clone()),
        role_id: "trainer".into(),
        message: Some(WorkerMessage {
            seq: 1,
            sent_at: None,
            body: Some(WorkerBody::Prepared(Prepared {
                ok: true,
                issues: vec![],
                estimate: Some(ResourceEstimate {
                    gpu_mem_gib_lo: lo,
                    gpu_mem_gib_hi: hi,
                    total_steps: 120,
                    wall_seconds_lo: 300.0,
                    wall_seconds_hi: 600.0,
                    assumptions: vec!["sequence length 1024, micro-batch 4".into()],
                }),
                effective_config_sha256: String::new(),
            })),
        }),
    })
}

impl Script {
    fn send(&self, m: Msg) {
        self.link.send_control(m).unwrap();
    }

    fn publish(&self) {
        self.link.set_running(self.running.lock().unwrap().clone());
    }
}

struct Handler(Arc<Script>);

#[async_trait::async_trait]
impl CommandHandler for Handler {
    async fn handle(&self, c: Command) -> Result<(), Failure> {
        let s = &self.0;
        match c.kind {
            Some(Kind::LaunchAttempt(l)) => {
                let r = l.attempt.clone().unwrap_or_default();
                let answer = s.answer.lock().unwrap().clone();
                if !l.dry_run {
                    // A real run: it starts and keeps running.
                    s.running.lock().unwrap().push(r.clone());
                    s.publish();
                    s.send(Msg::AttemptStatus(status(
                        &r,
                        AttemptPhase::Staging,
                        ExitKind::Unspecified,
                    )));
                    s.send(Msg::AttemptStatus(status(
                        &r,
                        AttemptPhase::Running,
                        ExitKind::Unspecified,
                    )));
                    return Ok(());
                }
                assert!(
                    l.job_spec_json.contains("\"algorithm\":\"sft\""),
                    "the probe carries the resolved config: {}",
                    l.job_spec_json
                );
                s.probes
                    .lock()
                    .unwrap()
                    .push(serde_json::from_str(&l.job_spec_json).unwrap());
                if let Answer::Reject = answer {
                    s.send(Msg::AttemptStatus(status(
                        &r,
                        AttemptPhase::Staging,
                        ExitKind::Unspecified,
                    )));
                    s.send(Msg::Worker(WorkerRelay {
                        attempt: Some(r.clone()),
                        role_id: "trainer".into(),
                        message: Some(WorkerMessage {
                            body: Some(WorkerBody::Prepared(Prepared::default())),
                            ..WorkerMessage::default()
                        }),
                    }));
                    s.send(Msg::AttemptStatus(status(
                        &r,
                        AttemptPhase::Exited,
                        ExitKind::Failed,
                    )));
                    return Ok(());
                }
                if let Answer::Estimate(lo, hi) = answer {
                    s.send(Msg::AttemptStatus(status(
                        &r,
                        AttemptPhase::Staging,
                        ExitKind::Unspecified,
                    )));
                    s.send(prepared(&r, lo, hi));
                    s.send(Msg::AttemptStatus(status(
                        &r,
                        AttemptPhase::Exited,
                        ExitKind::DryRunOk,
                    )));
                } else {
                    s.running.lock().unwrap().push(r.clone());
                    s.publish();
                }
                Ok(())
            }
            Some(Kind::CancelAttempt(x)) => {
                let r = x.attempt.unwrap_or_default();
                let known = {
                    let mut running = s.running.lock().unwrap();
                    let before = running.len();
                    running.retain(|a| a.attempt_id != r.attempt_id);
                    running.len() != before
                };
                if known {
                    s.publish();
                    s.send(Msg::AttemptStatus(status(
                        &r,
                        AttemptPhase::Exited,
                        ExitKind::Cancelled,
                    )));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

struct Machine {
    _agent: TestAgent,
    _running: crate::agent::gateway::Running,
    node: Uuid,
    script: Arc<Script>,
}

struct CheckpointAccess {
    decision: Arc<Mutex<Decision>>,
}

#[async_trait::async_trait]
impl Authorizer for CheckpointAccess {
    async fn authorize(
        &self,
        ctx: &RequestContext,
        action: &str,
        resource: &Resource,
    ) -> Result<Decision, SeamError> {
        if action == "checkpoints.read" {
            assert_eq!(resource.kind, "run");
            assert!(resource.project.is_some());
            Ok(*self.decision.lock().unwrap())
        } else {
            LocalOwnerAuthorizer.authorize(ctx, action, resource).await
        }
    }
}

struct World {
    server: TestServer,
    db: TestDb,
    pool: Uuid,
    project: ProjectId,
    fixture: TinyFixture,
    lanes: Arc<LaneRouter>,
    ingest: Ingest,
    checkpoint_access: Arc<Mutex<Decision>>,
    _objects: tempfile::TempDir,
}

fn sched_config() -> SchedulerConfig {
    SchedulerConfig {
        tick_seconds: 1,
        lease_grace_seconds: 1,
        outage_threshold_seconds: 5,
        launch_deadline_seconds: 60,
        ..SchedulerConfig::default()
    }
}

impl World {
    async fn start(probe_timeout_seconds: u64) -> Self {
        Self::start_with(SubmissionConfig {
            probe_timeout_seconds,
            ..SubmissionConfig::default()
        })
        .await
    }

    /// A core with these `[submission]` settings (its own database).
    async fn start_with(settings: SubmissionConfig) -> Self {
        let db = TestDb::new().await;
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let objects = tempfile::tempdir().unwrap();
        let mut gateway = fast_config();
        gateway.min_disk_free_bytes = 0;
        let mut cfg = server_config(gateway, Mode::Solo, 0);
        cfg.object_store.url = Some(
            reqwest::Url::from_directory_path(objects.path().join("objects"))
                .unwrap()
                .into(),
        );
        let trl = Bundle::parse(TRL).unwrap();
        mrl_server::modules::engines::store::register(db.db(), &[&trl], false)
            .await
            .unwrap();
        // Probes passed on CPU and NVIDIA (P24 checks them before a run may queue).
        db.seed(
            "UPDATE engine_versions SET status = 'ENABLED',
               probe_results = '{\"cpu\": {\"status\": \"ENABLED\"}, \"nvidia\": {\"status\": \"ENABLED\"}}'",
        )
        .await;
        let lanes = LaneRouter::new();
        let artifacts = ArtifactsModule::with_backend(
            db.db().clone(),
            mrl_storage::BackendConfig::Local {
                root: objects.path().join("objects"),
            },
            Mode::Solo,
            objects.path().join("scratch"),
        );
        let transfer = Transfer::new(artifacts.store(), artifacts.secrets(), secrets.clone());
        let ingest = Ingest::new(
            db.db().clone(),
            artifacts.store(),
            MetricObservers::new(),
            100,
        );
        let nodes = NodesModule::new(db.db().clone(), &cfg, secrets.clone(), lanes.clone());
        let gw = nodes.gateway();
        let d = db.db().clone();
        let fixture = TinyFixture::default();
        let chains = Chains::new(
            vec![
                Arc::new(SchemaChecks),
                Arc::new(CapabilityValidator::with_records(
                    d.clone(),
                    fixture.records(),
                )),
                Arc::new(MethodPolicy::with_capability(
                    d.clone(),
                    CapabilityValidator::with_records(d.clone(), fixture.records()),
                )),
                Arc::new(Guardian::new(d.clone(), settings)),
            ],
            vec![Arc::new(ProbeVerifier)],
            vec![Arc::new(Presets::with_capability(
                d.clone(),
                CapabilityValidator::with_records(d.clone(), fixture.records()),
                settings,
            ))],
        );
        let pipeline = Pipeline::new(
            vec![
                Arc::new(PoolMembership),
                Arc::new(PoolFlags),
                Arc::new(MaxQueuedPerProject { max: 100 }),
            ],
            vec![Arc::new(SingleNodePacking)],
            vec![
                Arc::new(RunConfigSpec::new(d.clone())),
                Arc::new(EngineRuntime::new(d.clone())),
                Arc::new(ProbeLaunch::new(d.clone())),
            ],
            vec![],
            vec![Arc::new(LocalAgentPool::new(d.clone()))],
        );
        let sched = SchedulerModule::with_parts(
            d.clone(),
            sched_config(),
            &gw,
            &lanes,
            chains.clone(),
            pipeline,
            transfer,
        );
        let submission = SubmissionModule::with(
            d.clone(),
            settings,
            chains.clone(),
            Projects::default(),
            &lanes,
            &MetricObservers::new(),
        );
        let modules: Vec<Box<dyn mrl_server::app::ServerModule>> = vec![
            Box::new(nodes),
            Box::new(artifacts),
            Box::new(sched),
            Box::new(mrl_server::modules::runs::RunsModule::with_chains(
                d, chains,
            )),
            Box::new(submission),
        ];
        let checkpoint_access = Arc::new(Mutex::new(Decision::Allow));
        let seams = crate::seams::build(
            crate::seams::solo(TEST_SECRET)
                .secrets_arc(secrets)
                .authorizer(CheckpointAccess {
                    decision: checkpoint_access.clone(),
                }),
            Mode::Solo,
        );
        let server = match TestServer::start(cfg, seams, modules).await {
            Ok(s) => s,
            Err(e) => panic!("server did not start: {}", e.explain()),
        };
        let mut w = Self {
            server,
            db,
            pool: Uuid::nil(),
            project: ProjectId::new(),
            fixture,
            lanes,
            ingest,
            checkpoint_access,
            _objects: objects,
        };
        let r = w
            .call(
                Method::POST,
                "/api/v1/pools",
                json!({"name": "gpus", "attempt_lease_ttl_seconds": 10}),
            )
            .await;
        assert_eq!(r.status, 201, "pool: {}", r.text());
        w.pool = PoolId::from_str(r.json()["id"].as_str().unwrap())
            .unwrap()
            .as_uuid();
        w
    }

    async fn call(&self, method: Method, path: &str, body: Value) -> TestResponse {
        self.call_with(method, path, body, &[]).await
    }

    async fn call_with(
        &self,
        method: Method,
        path: &str,
        body: Value,
        extra: &[(&str, &str)],
    ) -> TestResponse {
        let mut headers = vec![
            (LAUNCH_SECRET_HEADER, TEST_SECRET),
            ("content-type", "application/json"),
        ];
        headers.extend_from_slice(extra);
        let body = if body.is_null() {
            Body::empty()
        } else {
            Body::from(body.to_string())
        };
        self.server.request(method, path, &headers, body).await
    }

    fn pool(&self) -> &sqlx::PgPool {
        self.db.pool()
    }

    /// A machine with one fake NVIDIA GPU of `gib` GiB that answers dry runs with `answer`.
    /// Its platform reads as Linux (the TRL CUDA runtime is Linux-only) on every host.
    async fn machine(&self, gib: u64, answer: Answer) -> Machine {
        let agent = TestAgent::new(0, |_| {});
        agent.hw.add_device(GpuDevice {
            index: 0,
            backend: "fake".into(),
            vendor: GpuVendor::Nvidia,
            uuid: format!("GPU-fake-{gib}g-{}", Uuid::now_v7()),
            model: format!("Fake {gib} GB"),
            compute_capability: Some("8.6".into()),
            memory_total_bytes: gib << 30,
            memory_free_bytes: gib << 30,
            driver_version: Some("fake-1.0".into()),
            cuda_version: Some("13.0".into()),
            ecc_enabled: None,
            pci_bus_id: Some("00000000:01:00.0".into()),
            nvlink_peers: vec![],
            mig: None,
            experimental: false,
        });
        let r = self
            .call(
                Method::POST,
                &format!(
                    "/api/v1/pools/{}/enrollment-tokens",
                    PoolId::from_uuid(self.pool)
                ),
                json!({}),
            )
            .await;
        assert_eq!(r.status, 201, "token: {}", r.text());
        let token = r.json()["token"].as_str().unwrap().to_owned();
        let grpc = self.server.running().grpc_addr().expect("grpc listener");
        let node = mrl_agent::enroll::enroll_dial(
            &agent.ctx,
            &grpc.to_string(),
            &token,
            mrl_agent::transports::TransportKind::Direct,
            false,
        )
        .await
        .unwrap_or_else(|e| panic!("enroll: {}", e.explain()))
        .link
        .node_id;
        let node = NodeId::from_str(&node).unwrap().as_uuid();
        let link = Link::of(&agent.ctx).unwrap();
        let script = Arc::new(Script {
            link: link.clone(),
            answer: Mutex::new(answer),
            running: Mutex::default(),
            probes: Mutex::default(),
        });
        for kind in [
            "LaunchAttempt",
            "CancelAttempt",
            "StageArtifacts",
            "UpdateCredentials",
        ] {
            link.register(kind, Arc::new(Handler(script.clone())));
        }
        let running = agent.run();
        until("the machine is online with its GPU", || async {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM compute_nodes n JOIN gpu_devices g ON g.node_id = n.id
                  WHERE n.id = $1 AND n.status = 'ONLINE')",
            )
            .bind(node)
            .fetch_one(self.pool())
            .await
            .unwrap()
        })
        .await;
        // The inventory is sent once per connection, so this stays.
        sqlx::query(
            "UPDATE compute_nodes SET inventory = jsonb_set(jsonb_set(inventory, '{platform}',
               '\"PLATFORM_LINUX\"'), '{arch}', '\"x86_64\"') WHERE id = $1",
        )
        .bind(node)
        .execute(self.pool())
        .await
        .unwrap();
        Machine {
            _agent: agent,
            _running: running,
            node,
            script,
        }
    }

    async fn validate(&self, body: Value) -> Value {
        let r = self
            .call(
                Method::POST,
                &format!("/api/v1/projects/{}/runs:validate", self.project),
                body,
            )
            .await;
        assert_eq!(r.status, 200, "validate: {}", r.text());
        r.json()
    }

    async fn count(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_owned()))
            .fetch_one(self.pool())
            .await
            .unwrap()
    }
}

async fn until<F, Fut>(what: &str, f: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + WAIT;
    while !f().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The TRL SFT LoRA preset on the tiny fixture, on one NVIDIA GPU.
fn sft(f: &TinyFixture, nodes: &[Uuid]) -> Value {
    let nodes: Vec<String> = nodes
        .iter()
        .map(|n| NodeId::from_uuid(*n).to_string())
        .collect();
    json!({
        "name": "sft-lora",
        "algorithm_id": "sft",
        "base_model_version_id": f.model.to_string(),
        "dataset_version_ids": [f.dataset.to_string()],
        "config": {"preset": "sft-lora-beginner"},
        "resources": {"roles": [{"role": "trainer", "replicas": 1, "gpu_count": 1,
            "gpu": {"vendor": "nvidia"}, "node_ids": nodes,
            "cpu_millis": 1000, "mem_bytes": 1u64 << 30}]}
    })
}

fn codes(v: &Value) -> Vec<String> {
    v["issues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["code"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn validate_without_machines_answers_from_the_app_alone() {
    let w = World::start(60).await;
    let f = w.fixture;
    let v = w.validate(sft(&f, &[])).await;
    assert_eq!(v["ok"], false, "{v}");
    assert_eq!(v["checked"], "l0");
    assert!(
        codes(&v).contains(&"MRL-CAP-245".to_owned()),
        "no machine of this kind: {v}"
    );
    assert_eq!(v["estimate"]["verdict"], "unknown");
    // The preset, the schema's defaults, and the engine pinned.
    let cfg = &v["resolved_config"];
    assert_eq!(cfg["common"]["update"]["lora"]["r"], 8, "{cfg}");
    assert_eq!(cfg["engine"]["learning_rate"], 0.0002);
    assert_eq!(cfg["engine"]["logging_steps"], 10, "a default");
    assert!(cfg["common"]["engine"]
        .as_str()
        .unwrap()
        .starts_with("trl@"));
    assert!(cfg.get("preset").is_none());
    assert_eq!(v["preset"], "sft-lora-beginner");
    // No side effects: no run, no job.
    assert_eq!(w.count("SELECT count(*) FROM runs").await, 0);
    assert_eq!(w.count("SELECT count(*) FROM jobs").await, 0);
    // An unknown preset is a field problem, not a server error.
    let mut bad = sft(&f, &[]);
    bad["config"]["preset"] = json!("no-such-preset");
    let v = w.validate(bad).await;
    assert_eq!(codes(&v), ["MRL-VAL-280"], "{v}");
    // A malformed or missing project answers 404 before anything runs.
    let r = w
        .call(
            Method::POST,
            "/api/v1/projects/prj_bad/runs:validate",
            sft(&f, &[]),
        )
        .await;
    assert_eq!(r.status, 404, "{}", r.text());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn dry_run_fits_24_gb_and_not_4_gb_and_submit_agrees() {
    let w = World::start(60).await;
    let f = w.fixture;
    let big = w.machine(24, Answer::Estimate(6.0, 9.0)).await;
    let small = w.machine(4, Answer::Estimate(5.0, 7.0)).await;
    let v = w.validate(sft(&f, &[big.node])).await;
    assert_eq!(v["ok"], true, "{v}");
    assert_eq!(v["checked"], "l1");
    assert_eq!(v["estimate"]["verdict"], "fits");
    assert_eq!(v["estimate"]["confidence"], "low");
    assert_eq!(
        v["estimate"]["per_gpu_memory_gib"],
        json!({"lo": 6.0, "expected": 7.5, "hi": 9.0})
    );
    assert_eq!(v["estimate"]["total_steps"], 120);
    assert_eq!(v["preflight"]["machines"][0]["usable_gib"], 21.6);
    // The probe carries the run's inputs and the GPU it estimates for, on top of the
    // scheduler's own keys (`subject_ref.launch_spec` goes in before every contributor).
    let probe = big.script.probes.lock().unwrap()[0].clone();
    assert_eq!(
        probe["base_model_version_id"],
        f.model.to_string(),
        "{probe}"
    );
    assert_eq!(probe["dataset_version_ids"], json!([f.dataset.to_string()]));
    assert_eq!(
        probe["target_device"],
        json!({"kind": "cuda", "name": "Fake 24 GB", "memory_gib": 24.0, "bf16": true})
    );
    assert_eq!(probe["sandbox"], "worker", "the scheduler's keys stay");
    assert_eq!(
        probe["engine"]["learning_rate"], 0.0002,
        "the resolved config"
    );
    let v4 = w.validate(sft(&f, &[small.node])).await;
    assert_eq!(v4["ok"], false, "{v4}");
    assert_eq!(v4["estimate"]["verdict"], "does_not_fit");
    assert_eq!(
        v4["estimate"]["per_gpu_memory_gib"]["expected"], 6.0,
        "used the pinned node"
    );
    let placements: Vec<Uuid> =
        sqlx::query_scalar("SELECT node_id FROM resource_estimates ORDER BY created_at")
            .fetch_all(w.pool())
            .await
            .unwrap();
    assert_eq!(placements, [big.node, small.node]);
    assert!(codes(&v4).contains(&"MRL-CAP-280".to_owned()), "{v4}");
    let kinds: Vec<&str> = v4["preflight"]["alternatives"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["qlora", "smaller_model", "machine"], "{v4}");
    assert_eq!(
        v4["preflight"]["alternatives"][2]["node_id"],
        NodeId::from_uuid(big.node).to_string()
    );
    // The probe jobs are internal: no run, and they complete.
    assert_eq!(w.count("SELECT count(*) FROM runs").await, 0);
    until("both probes completed", || async {
        w.count("SELECT count(*) FROM jobs WHERE state = 'COMPLETED' AND run_id IS NULL")
            .await
            == 2
    })
    .await;
    // Submit with the resolved config: the same estimate is found. On the small machine
    // the Guardian refuses it; on the big one it queues.
    let mut on_small = sft(&f, &[small.node]);
    on_small["config"] = v["resolved_config"].clone();
    let r = w
        .call_with(
            Method::POST,
            &format!("/api/v1/projects/{}/runs", w.project),
            on_small,
            &[("idempotency-key", "small")],
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    assert_eq!(r.json()["state"], "REJECTED", "{}", r.text());
    assert_eq!(
        r.json()["state_reason"]["problems"][0]["code"],
        "MRL-CAP-280"
    );
    let mut on_big = sft(&f, &[big.node]);
    on_big["config"] = v["resolved_config"].clone();
    let r = w
        .call_with(
            Method::POST,
            &format!("/api/v1/projects/{}/runs", w.project),
            on_big,
            &[("idempotency-key", "big")],
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    assert_ne!(r.json()["state"], "REJECTED", "{}", r.text());
    let run = RunId::from_str(r.json()["id"].as_str().unwrap()).unwrap();
    // It runs on the big machine; inject the measured peaks of its attempt.
    until("the run is running", || async {
        w.count(&format!(
            "SELECT count(*) FROM jobs WHERE run_id = '{}' AND state = 'RUNNING'",
            run.as_uuid()
        ))
        .await
            == 1
    })
    .await;
    let (job, attempt): (Uuid, Uuid) =
        sqlx::query_as("SELECT id, current_attempt_id FROM jobs WHERE run_id = $1")
            .bind(run.as_uuid())
            .fetch_one(w.pool())
            .await
            .unwrap();
    let device: String =
        sqlx::query_scalar("SELECT device_uuid FROM gpu_devices WHERE node_id = $1")
            .bind(big.node)
            .fetch_one(w.pool())
            .await
            .unwrap();
    for (phase, signal, gib) in [
        ("update", "process_peak", 9.0),
        ("update", "torch_reserved_peak", 8.0),
        ("eval", "process_peak", 5.0),
    ] {
        sqlx::query(
            "INSERT INTO resource_actuals (attempt_id, org_id, run_id, device, phase, signal, peak)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(attempt)
        .bind(Uuid::nil())
        .bind(run.as_uuid())
        .bind(&device)
        .bind(phase)
        .bind(signal)
        .bind(gib * f64::from(1u32 << 30))
        .execute(w.pool())
        .await
        .unwrap();
    }
    let r = w
        .call(
            Method::GET,
            &format!("/api/v1/runs/{run}/resources"),
            Value::Null,
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let res = r.json();
    assert_eq!(res["estimate"]["verdict"], "fits", "{res}");
    let original = res["estimate"]["estimate_id"].clone();
    *big.script.answer.lock().unwrap() = Answer::Estimate(10.0, 12.0);
    let later = w.validate(sft(&f, &[big.node])).await;
    assert_ne!(later["estimate"]["estimate_id"], original, "{later}");
    let historical = w
        .call(
            Method::GET,
            &format!("/api/v1/runs/{run}/resources"),
            Value::Null,
        )
        .await;
    assert_eq!(historical.status, 200, "{}", historical.text());
    assert_eq!(historical.json()["estimate"]["estimate_id"], original);
    let update = res["phases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["phase"] == "update")
        .unwrap();
    assert_eq!(update["actual_peak_gib"], 9.0);
    assert_eq!(update["ratio"], 1.2);
    assert_eq!(res["actual"].as_array().unwrap().len(), 3);
    // Completing the run adds one calibration sample, once.
    let event = mrl_server::modules::events::Event {
        id: 0,
        event_id: Uuid::now_v7(),
        org_id: None,
        aggregate_kind: "job".into(),
        aggregate_id: job,
        aggregate_seq: 9,
        event_type: "job.completed".into(),
        schema_version: 1,
        payload: json!({}),
        trace_context: None,
        created_at: time::OffsetDateTime::now_utc(),
        xid: 0,
    };
    sqlx::query("DELETE FROM resource_actuals WHERE attempt_id = $1")
        .bind(attempt)
        .execute(w.pool())
        .await
        .unwrap();
    let cal = Calibrate::new(w.db.db().clone());
    for _ in 0..2 {
        let mut tx = w.db.db().begin().await.unwrap();
        cal.handle(&mut tx, &event).await.unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(
        w.count("SELECT count(*) FROM estimator_calibration_inputs")
            .await,
        1
    );
    let ingest_peak = |sequence, gib: f64| {
        let device = &device;
        let w = &w;
        async move {
            w.ingest
                .metrics(
                    Owner {
                        org: Uuid::nil(),
                        run: run.as_uuid(),
                        attempt,
                        attempt_no: 1,
                    },
                    Delivery {
                        node: big.node,
                        stream: mrl_proto::agent::v1::Lane::Telemetry,
                        seq: sequence,
                    },
                    None,
                    &[MetricPoint {
                        name: format!("resource/process_peak/{device}"),
                        split: "update".into(),
                        step: sequence,
                        value: gib * f64::from(1u32 << 30),
                        ..MetricPoint::default()
                    }],
                )
                .await
                .unwrap();
        }
    };
    ingest_peak(10, 5.0).await;
    cal.reconcile().await.unwrap();
    let first: f64 = sqlx::query_scalar("SELECT ratio FROM estimator_calibration_samples")
        .fetch_one(w.pool())
        .await
        .unwrap();
    assert!((first - 5.0 / 7.5).abs() < 1e-9);
    ingest_peak(11, 9.0).await;
    cal.reconcile().await.unwrap();
    cal.reconcile().await.unwrap();
    assert_eq!(
        w.count("SELECT count(*) FROM estimator_calibration_samples")
            .await,
        1
    );
    let (n, sum): (i32, f64) = sqlx::query_as("SELECT n, sum_ln FROM estimator_calibration")
        .fetch_one(w.pool())
        .await
        .unwrap();
    assert_eq!(n, 1, "one sample per attempt");
    assert!((sum - 1.2f64.ln()).abs() < 1e-9, "{sum}");
    sqlx::query("UPDATE estimator_calibration SET n = 5, sum_ln = $1, sum_ln_sq = $2")
        .bind(5.0 * 1.2f64.ln())
        .bind(5.0 * 1.2f64.ln().powi(2))
        .execute(w.pool())
        .await
        .unwrap();
    let next = w.validate(sft(&f, &[big.node])).await;
    assert_eq!(next["estimate"]["confidence"], "medium", "{next}");
    assert!(
        (next["estimate"]["per_gpu_memory_gib"]["expected"]
            .as_f64()
            .unwrap()
            - 13.2)
            .abs()
            < 1e-9
    );
    let historical = w
        .call(
            Method::GET,
            &format!("/api/v1/runs/{run}/resources"),
            Value::Null,
        )
        .await;
    assert_eq!(historical.json()["estimate"]["confidence"], "low");
    assert_eq!(
        historical.json()["estimate"]["per_gpu_memory_gib"]["expected"],
        7.5
    );

    // The resume check: a checkpoint of another method can't be resumed.
    sqlx::query("UPDATE runs SET algorithm_id = 'cpt' WHERE id = $1")
        .bind(run.as_uuid())
        .execute(w.pool())
        .await
        .unwrap();
    let ckpt = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO checkpoints (id, org_id, storage_org, run_id, job_id, attempt_id,
           attempt_epoch, global_step, kind, layout, state)
         VALUES ($1, $2, 'local', $3, $4, $5, 1, 10, 'adapter_only', 'hf-trainer-v1', 'COMMITTED')",
    )
    .bind(ckpt)
    .bind(Uuid::nil())
    .bind(run.as_uuid())
    .bind(job)
    .bind(attempt)
    .execute(w.pool())
    .await
    .unwrap();
    let mut branch = sft(&f, &[big.node]);
    branch["parent_checkpoint_id"] =
        json!(mrl_domain::ids::CheckpointId::from_uuid(ckpt).to_string());
    // A checkpoint the caller can't read answers like a missing one, as submit does.
    for (decision, expected) in [(Decision::Hide, 404), (Decision::Deny, 404)] {
        *w.checkpoint_access.lock().unwrap() = decision;
        let other_project = ProjectId::new();
        let rejected = w
            .call(
                Method::POST,
                &format!("/api/v1/projects/{other_project}/runs:validate"),
                branch.clone(),
            )
            .await;
        assert_eq!(rejected.status, expected, "{}", rejected.text());
        let rejected = w
            .call(
                Method::POST,
                "/api/v1/runs:preflight",
                json!({"project_id": other_project.to_string(), "run": branch}),
            )
            .await;
        assert_eq!(rejected.status, expected, "{}", rejected.text());
    }
    *w.checkpoint_access.lock().unwrap() = Decision::Allow;
    let v = w.validate(branch.clone()).await;
    assert!(codes(&v).contains(&"MRL-CAP-281".to_owned()), "{v}");
    // Submit refuses it the same way, before anything is queued.
    let r = w
        .call_with(
            Method::POST,
            &format!("/api/v1/projects/{}/runs", w.project),
            branch,
            &[("idempotency-key", "branch")],
        )
        .await;
    assert_eq!(r.status, 422, "{}", r.text());
    assert!(r.text().contains("MRL-CAP-281"), "{}", r.text());
    // Probe mode: a run with no estimate of its config starts; once its first steps measure
    // a peak above the GPU's usable memory it is cancelled. Below it, it goes on.
    let mut unmeasured = sft(&f, &[small.node]);
    unmeasured["config"] = v["resolved_config"].clone();
    unmeasured["config"]["engine"]["num_train_epochs"] = json!(3);
    let r = w
        .call_with(
            Method::POST,
            &format!("/api/v1/projects/{}/runs", w.project),
            unmeasured.clone(),
            &[("idempotency-key", "unmeasured")],
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    assert_ne!(
        r.json()["state"],
        "REJECTED",
        "no estimate: unknown, not refused"
    );
    let probe_run = RunId::from_str(r.json()["id"].as_str().unwrap()).unwrap();
    let later = w.validate(unmeasured).await;
    assert!(later["estimate"]["estimate_id"].is_string(), "{later}");
    until("the unmeasured run is running", || async {
        w.count(&format!(
            "SELECT count(*) FROM jobs WHERE run_id = '{}' AND state = 'RUNNING'",
            probe_run.as_uuid()
        ))
        .await
            == 1
    })
    .await;
    let attempt: Uuid = sqlx::query_scalar("SELECT current_attempt_id FROM jobs WHERE run_id = $1")
        .bind(probe_run.as_uuid())
        .fetch_one(w.pool())
        .await
        .unwrap();
    let device: String =
        sqlx::query_scalar("SELECT device_uuid FROM gpu_devices WHERE node_id = $1")
            .bind(small.node)
            .fetch_one(w.pool())
            .await
            .unwrap();
    let guard = mrl_server::modules::preflight::ProbeModeGuard::new(
        w.db.db().clone(),
        mrl_server::modules::preflight::Headroom::default(),
        5,
    );
    let points: Vec<_> = (1..=5)
        .map(|step| MetricPoint {
            name: format!("resource/process_peak/{device}"),
            split: "update".into(),
            step,
            value: 3.9 * f64::from(1u32 << 30),
            ..MetricPoint::default()
        })
        .collect();
    let owner = Owner {
        org: Uuid::nil(),
        run: probe_run.as_uuid(),
        attempt,
        attempt_no: 1,
    };
    let delivery = Delivery {
        node: small.node,
        stream: mrl_proto::agent::v1::Lane::Telemetry,
        seq: 11,
    };
    w.ingest
        .metrics(owner, delivery, None, &points[..4])
        .await
        .unwrap();
    guard.reconcile().await.unwrap();
    assert_eq!(
        w.count(&format!(
            "SELECT count(*) FROM submission_probe_decisions WHERE attempt_id = '{attempt}'"
        ))
        .await,
        0
    );

    // The batch commits with no observer. A new guard recovers it from storage.
    w.ingest
        .metrics(
            owner,
            Delivery {
                seq: 12,
                ..delivery
            },
            None,
            &points[4..],
        )
        .await
        .unwrap();
    let restarted = mrl_server::modules::preflight::ProbeModeGuard::new(
        w.db.db().clone(),
        mrl_server::modules::preflight::Headroom::default(),
        5,
    );
    restarted.reconcile().await.unwrap();
    restarted.reconcile().await.unwrap();
    assert_eq!(
        w.count("SELECT count(*) FROM submission_probe_decisions WHERE cancelled")
            .await,
        1
    );
    assert_eq!(
        w.count(&format!(
            "SELECT count(*) FROM jobs WHERE run_id = '{}' AND state IN ('CANCELLING', 'CANCELLED')",
            probe_run.as_uuid()
        ))
        .await,
        1
    );
    // The measured run (with an estimate) is left alone even when it is close.
    assert!(!guard
        .decide(run.as_uuid(), job_attempt(&w, run).await)
        .await
        .unwrap());
}

async fn job_attempt(w: &World, run: RunId) -> Uuid {
    sqlx::query_scalar("SELECT current_attempt_id FROM jobs WHERE run_id = $1")
        .bind(run.as_uuid())
        .fetch_one(w.pool())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_probe_timeout_falls_back_to_l0_and_cancels_the_probe() {
    let w = World::start(5).await;
    let f = w.fixture;
    let m = w.machine(24, Answer::Silent).await;
    let started = tokio::time::Instant::now();
    let v = w.validate(sft(&f, &[])).await;
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "the budget holds"
    );
    assert_eq!(v["ok"], true, "{v}");
    assert_eq!(v["checked"], "l0");
    assert_eq!(codes(&v), ["MRL-CAP-282"], "{v}");
    assert_eq!(v["issues"][0]["severity"], "info");
    assert_eq!(v["estimate"]["verdict"], "unknown");
    assert_eq!(v["preflight"]["probe_steps"], 5);
    until("the probe is cancelled", || async {
        w.count("SELECT count(*) FROM jobs WHERE state IN ('CANCELLING', 'CANCELLED')")
            .await
            == 1
    })
    .await;
    assert_eq!(
        w.count("SELECT count(*) FROM resource_estimates WHERE state = 'EXPIRED'")
            .await,
        1,
        "the probe no longer accepts answers"
    );
    let (attempt, epoch): (Uuid, i64) = sqlx::query_as("SELECT id, epoch FROM job_attempts")
        .fetch_one(w.pool())
        .await
        .unwrap();
    assert!(!PreparedSink::new(w.db.db().clone())
        .store(
            m.node,
            attempt,
            epoch,
            &Prepared {
                ok: true,
                ..Prepared::default()
            },
        )
        .await
        .unwrap());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn replayed_or_foreign_answers_change_nothing() {
    let w = World::start(60).await;
    let m = w.machine(24, Answer::Silent).await;
    let other = w.machine(24, Answer::Silent).await;
    // A probe the silent machines take but never answer.
    let engine: Uuid = sqlx::query_scalar("SELECT id FROM engine_versions LIMIT 1")
        .fetch_one(w.pool())
        .await
        .unwrap();
    let spec = probe::ProbeSpec {
        algorithm: "sft".into(),
        base_model_version_id: "mv_x".into(),
        dataset_version_ids: vec![],
        reward_version_id: None,
        target_resources: None,
        seeds: json!({}),
        resolved: json!({"common": {}, "engine": {}}),
    };
    let (estimate, job) = probe::start(
        w.db.db(),
        &probe::NewProbe {
            org: Uuid::nil(),
            project: w.project.as_uuid(),
            created_by: "test",
            spec: &spec,
            engine_version_id: engine,
            estimator_version: "trl@x".into(),
            calibration_key: "k".into(),
            inputs_sha256: vec![0; 32],
            node_ids: Vec::new(),
            timeout_seconds: 60,
            max_runtime_seconds: 600,
            target_device: None,
        },
    )
    .await
    .unwrap();
    let placed = || async {
        sqlx::query_as::<_, (Uuid, i64, Uuid)>(
            "SELECT a.id, a.epoch, p.node_id FROM job_attempts a
               JOIN attempt_placements p ON p.attempt_id = a.id WHERE a.job_id = $1",
        )
        .bind(job)
        .fetch_optional(w.pool())
        .await
        .unwrap()
    };
    until("the probe is placed", || async { placed().await.is_some() }).await;
    let (attempt, epoch, node) = placed().await.unwrap();
    let wrong = if node == m.node { other.node } else { m.node };
    let sink = w
        .lanes
        .control_sink("worker.prepared")
        .expect("the P28 sink");
    let r = AttemptRef {
        attempt_id: AttemptId::from_uuid(attempt).to_string(),
        fencing_epoch: u64::try_from(epoch).unwrap(),
        ..AttemptRef::default()
    };
    let item = |node: Uuid, r: &AttemptRef, lo: f64| LaneItem {
        node_id: node,
        session_epoch: 1,
        lane: LaneKind::Control,
        stream: mrl_proto::agent::v1::Lane::Control,
        seq: 1,
        dropped_before: 0,
        message: AgentMessage {
            body: Some(prepared(r, lo, lo + 1.0)),
            ..AgentMessage::default()
        },
    };
    let state = || async {
        sqlx::query_as::<_, (String, Option<f64>)>(
            "SELECT state, gpu_mem_lo_gib FROM resource_estimates WHERE id = $1",
        )
        .bind(estimate)
        .fetch_one(w.pool())
        .await
        .unwrap()
    };
    sink.deliver(item(wrong, &r, 1.0)).await.unwrap();
    assert_eq!(state().await, ("PENDING".into(), None), "another machine");
    let stale = AttemptRef {
        fencing_epoch: r.fencing_epoch + 1,
        ..r.clone()
    };
    sink.deliver(item(node, &stale, 2.0)).await.unwrap();
    assert_eq!(state().await, ("PENDING".into(), None), "another epoch");
    sqlx::query("UPDATE jobs SET epoch = epoch + 1 WHERE id = $1")
        .bind(job)
        .execute(w.pool())
        .await
        .unwrap();
    sink.deliver(item(node, &r, 2.5)).await.unwrap();
    assert_eq!(
        state().await,
        ("PENDING".into(), None),
        "an attempt behind its job fence"
    );
    sqlx::query("UPDATE jobs SET epoch = $2 WHERE id = $1")
        .bind(job)
        .bind(epoch)
        .execute(w.pool())
        .await
        .unwrap();
    sink.deliver(item(node, &r, 3.0)).await.unwrap();
    assert_eq!(
        state().await,
        ("READY".into(), Some(3.0)),
        "the right machine"
    );
    sink.deliver(item(node, &r, 4.0)).await.unwrap();
    assert_eq!(
        state().await,
        ("READY".into(), Some(3.0)),
        "a replay changes nothing"
    );
    assert!(PreparedSink::new(w.db.db().clone())
        .store(node, attempt, epoch, &Prepared::default())
        .await
        .map(|changed| !changed)
        .unwrap());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn unsupported_combinations_and_experimental_methods_are_refused() {
    let w = World::start(60).await;
    let f = w.fixture;
    let _m = w.machine(24, Answer::Estimate(1.0, 2.0)).await;
    // QLoRA on a processor-only run: P24's checks name it, nothing is probed.
    let mut cpu = sft(&f, &[]);
    cpu["config"] = json!({"preset": "sft-qlora-24gb"});
    cpu["resources"]["roles"][0]["gpu_count"] = json!(0);
    cpu["resources"]["roles"][0]["gpu"] = json!({});
    let v = w.validate(cpu).await;
    assert_eq!(v["ok"], false, "{v}");
    let cap: Vec<String> = codes(&v)
        .into_iter()
        .filter(|c| c.starts_with("MRL-CAP-"))
        .collect();
    assert!(
        !cap.is_empty() && !cap.contains(&"MRL-CAP-282".to_owned()),
        "{v}"
    );
    assert_eq!(
        w.count("SELECT count(*) FROM jobs").await,
        0,
        "no probe for a refused run"
    );
    // An experimental method needs the workspace toggle.
    sqlx::query(
        "UPDATE algorithm_definitions SET maturity = 'experimental' WHERE algorithm_id = 'sft'",
    )
    .execute(w.pool())
    .await
    .unwrap();
    let v = w.validate(sft(&f, &[])).await;
    assert!(codes(&v).contains(&"MRL-CAP-022".to_owned()), "{v}");
    sqlx::query(
        "INSERT INTO organizations (id, slug, name, kind, settings)
         VALUES ($1, 'p28', 'P28', 'team', '{\"experimental_methods\": true}')",
    )
    .bind(Uuid::nil())
    .execute(w.pool())
    .await
    .unwrap();
    let v = w.validate(sft(&f, &[])).await;
    assert!(!codes(&v).contains(&"MRL-CAP-022".to_owned()), "{v}");
    let mut submission = sft(&f, &[]);
    submission["config"] = v["resolved_config"].clone();
    submission["config"]["common"]
        .as_object_mut()
        .unwrap()
        .remove("engine");

    let mut experimental: Value = serde_json::from_str(TRL).unwrap();
    experimental["manifest"]["metadata"]["name"] = json!("zz-experimental");
    experimental["manifest"]["spec"]["algorithms"][0]["maturity"] = json!("experimental");
    let experimental = Bundle::parse(&experimental.to_string()).unwrap();
    mrl_server::modules::engines::store::register(w.db.db(), &[&experimental], false)
        .await
        .unwrap();
    w.db.seed(
        r#"UPDATE organizations SET settings = '{}';
         UPDATE algorithm_definitions ad SET maturity = 'stable'
           FROM engine_versions ev JOIN training_engines e ON e.id = ev.engine_id
          WHERE ad.engine_version_id = ev.id AND e.name = 'trl';
         UPDATE engine_versions ev SET status = 'ENABLED', probe_results = CASE
           WHEN e.name = 'trl' THEN '{"cpu": {"status": "ENABLED"}}'::jsonb
           ELSE '{"nvidia": {"status": "ENABLED"}}'::jsonb END
           FROM training_engines e WHERE e.id = ev.engine_id;"#,
    )
    .await;
    let r = w
        .call_with(
            Method::POST,
            &format!("/api/v1/projects/{}/runs", w.project),
            submission,
            &[("idempotency-key", "mixed-maturity")],
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    let result = r.json();
    assert_eq!(result["state"], "REJECTED", "{result}");
    assert!(
        result["state_reason"]["problems"]
            .as_array()
            .unwrap()
            .iter()
            .any(|issue| issue["code"] == "MRL-CAP-022"),
        "{result}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn preflight_keeps_worker_rejections_and_validation_explains_empty_replies() {
    let w = World::start(60).await;
    let m = w.machine(24, Answer::Reject).await;
    let body = sft(&w.fixture, &[m.node]);
    let result = w.validate(body.clone()).await;
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(result["checked"], "l1", "{result}");
    assert_eq!(result["issues"][0]["severity"], "error", "{result}");
    let r = w
        .call(
            Method::POST,
            "/api/v1/runs:preflight",
            json!({"project_id": w.project.to_string(), "run": body}),
        )
        .await;
    assert_eq!(r.status, 422, "{}", r.text());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn audit_failure_rolls_back_probe_creation_and_train_requires_a_run_or_probe() {
    let w = World::start(60).await;
    let m = w.machine(24, Answer::Estimate(6.0, 9.0)).await;
    for subject in [None, Some(json!({})), Some(json!({"kind": null}))] {
        let error = sqlx::query(
            "INSERT INTO jobs (id, org_id, project_id, kind, subject_ref, state, resource_request,
               retry_policy, created_by) VALUES ($1, $2, $3, 'TRAIN', $4, 'CREATED', '{}', '{}', 'test')"
        )
        .bind(Uuid::now_v7()).bind(Uuid::nil()).bind(w.project.as_uuid()).bind(subject)
        .execute(w.pool()).await.unwrap_err();
        assert_eq!(
            error.as_database_error().and_then(|e| e.constraint()),
            Some("jobs_check")
        );
    }
    w.db.seed(
        "CREATE FUNCTION fail_p28_audit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NEW.action = 'submission.probe.create' THEN
             RAISE EXCEPTION 'test audit failure';
           END IF;
           RETURN NEW;
         END $$;
         CREATE TRIGGER fail_p28_audit BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION fail_p28_audit();"
    ).await;
    let response = w
        .call(
            Method::POST,
            &format!("/api/v1/projects/{}/runs:validate", w.project),
            sft(&w.fixture, &[m.node]),
        )
        .await;
    assert!((500..600).contains(&response.status), "{}", response.text());
    assert_eq!(w.count("SELECT count(*) FROM jobs").await, 0);
    assert_eq!(w.count("SELECT count(*) FROM resource_estimates").await, 0);
}

async fn submit(w: &World, body: Value, key: &str) -> (Value, Value, Option<Uuid>) {
    let r = w
        .call_with(
            Method::POST,
            &format!("/api/v1/projects/{}/runs", w.project),
            body,
            &[("idempotency-key", key)],
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    let run = RunId::from_str(r.json()["id"].as_str().unwrap()).unwrap();
    let (resolved, estimate): (Value, Option<Uuid>) =
        sqlx::query_as("SELECT resolved, estimate_id FROM run_configs WHERE run_id = $1")
            .bind(run.as_uuid())
            .fetch_one(w.pool())
            .await
            .unwrap();
    (r.json(), resolved, estimate)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn submit_applies_method_defaults_and_tight_safety_settings() {
    let w = World::start(60).await;
    let f = w.fixture;
    // 9 GiB, 10 % kept free: 8.1 usable. 6–9 GiB expected 7.5: tight.
    let m = w.machine(9, Answer::Estimate(6.0, 9.0)).await;
    let v = w.validate(sft(&f, &[m.node])).await;
    assert_eq!(v["ok"], true, "{v}");
    assert_eq!(v["preflight"]["verdict"], "tight", "{v}");
    assert_eq!(v["estimate"]["verdict"], "unknown", "{v}");
    assert!(v["estimate"]["per_gpu_memory_gib"].is_null());
    assert!(codes(&v).contains(&"MRL-CAP-284".to_owned()), "{v}");
    assert_eq!(
        v["resolved_config"]["engine"]["gradient_checkpointing"],
        true
    );
    assert_eq!(
        v["resolved_config"]["common"]["precision"], "auto",
        "common defaults"
    );
    let estimate: Uuid = sqlx::query_scalar("SELECT id FROM resource_estimates")
        .fetch_one(w.pool())
        .await
        .unwrap();

    let (run, resolved, linked) = submit(&w, sft(&f, &[m.node]), "raw").await;
    assert_ne!(run["state"], "REJECTED", "{run}");
    assert_eq!(resolved, v["resolved_config"]);
    assert_eq!(linked, None, "the probe measured the original settings");
    let details: Value = sqlx::query_scalar(
        "SELECT details FROM audit_events WHERE action = 'submission.safety_settings'",
    )
    .fetch_one(w.pool())
    .await
    .unwrap();
    assert_eq!(details["source_estimate_id"], estimate.to_string());
    assert_eq!(details["changes"], v["preflight"]["changes"]);
    assert_eq!(details["message"], v["preflight"]["message"]);

    let mut adjusted = sft(&f, &[m.node]);
    adjusted["config"] = v["resolved_config"].clone();
    let (run, resolved, linked) = submit(&w, adjusted, "adjusted").await;
    assert_ne!(run["state"], "REJECTED", "{run}");
    assert_eq!(resolved, v["resolved_config"]);
    assert_eq!(linked, None);
    let resources = w
        .call(
            Method::GET,
            &format!("/api/v1/runs/{}/resources", run["id"].as_str().unwrap()),
            Value::Null,
        )
        .await;
    assert_eq!(resources.status, 200, "{}", resources.text());
    assert!(
        resources.json()["estimate"].is_null(),
        "{}",
        resources.text()
    );

    // No preset: the method's defaults still apply at submit, and P24 accepts them.
    let mut plain = sft(&f, &[m.node]);
    plain["config"] = json!({"common": {"update": {"strategy": "lora"}}});
    let (run, resolved, linked) = submit(&w, plain, "plain").await;
    assert_ne!(run["state"], "REJECTED", "{run}");
    assert_eq!(resolved["common"]["precision"], "auto", "{resolved}");
    assert_eq!(resolved["engine"]["logging_steps"], 10, "{resolved}");
    assert!(resolved["common"]["engine"]
        .as_str()
        .unwrap()
        .starts_with("trl@"));
    assert_eq!(linked, None, "no estimate of this config yet");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn an_engine_name_selects_a_version_that_can_run_the_resolved_preset() {
    let w = World::start(60).await;
    let m = w.machine(24, Answer::Estimate(6.0, 9.0)).await;
    let mut version: Value = serde_json::from_str(TRL).unwrap();
    version["manifest"]["metadata"]["version"] = json!("0.1.1");
    let version = Bundle::parse(&version.to_string()).unwrap();
    mrl_server::modules::engines::store::register(w.db.db(), &[&version], false)
        .await
        .unwrap();
    w.db.seed(
        r#"UPDATE engine_versions SET status = 'ENABLED',
        probe_results = CASE WHEN version = '0.1.0' THEN '{"cpu": {"status": "ENABLED"}}'::jsonb
            ELSE '{"nvidia": {"status": "ENABLED"}}'::jsonb END"#,
    )
    .await;
    let mut request = sft(&w.fixture, &[m.node]);
    request["config"]["common"] = json!({"engine": "trl"});
    let (run, resolved, _) = submit(&w, request, "later-engine").await;
    assert_ne!(run["state"], "REJECTED", "{run}");
    assert_eq!(resolved["common"]["engine"], "trl@0.1.1");
    assert_eq!(resolved["common"]["update"]["strategy"], "lora");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_legacy_probe_and_its_estimate_move_to_the_same_org() {
    struct Directory(ProjectId, OrgId);
    #[async_trait::async_trait]
    impl ProjectDirectory for Directory {
        async fn owner(&self, project: ProjectId) -> Result<Option<OrgId>, SeamError> {
            Ok((project == self.0).then_some(self.1))
        }
    }

    let w = World::start(60).await;
    let engine: Uuid = sqlx::query_scalar("SELECT id FROM engine_versions")
        .fetch_one(w.pool())
        .await
        .unwrap();
    let (estimate, job) = probe::start(
        w.db.db(),
        &probe::NewProbe {
            org: Uuid::nil(),
            project: w.project.as_uuid(),
            created_by: "test",
            spec: &probe::ProbeSpec {
                algorithm: "sft".into(),
                base_model_version_id: "mv_x".into(),
                dataset_version_ids: vec![],
                reward_version_id: None,
                target_resources: None,
                seeds: json!({}),
                resolved: json!({}),
            },
            engine_version_id: engine,
            estimator_version: "trl@0.1.0".into(),
            calibration_key: "test".into(),
            inputs_sha256: vec![0; 32],
            node_ids: Vec::new(),
            timeout_seconds: 60,
            max_runtime_seconds: 120,
            target_device: None,
        },
    )
    .await
    .unwrap();
    let org = OrgId::new();
    let projects = Projects::with(Arc::new(Directory(w.project, org)));
    RehomeLocalRows::new(w.db.db().clone(), projects)
        .run()
        .await
        .unwrap();
    let owners: (Uuid, Uuid) = sqlx::query_as(
        "SELECT e.org_id, j.org_id FROM resource_estimates e
        JOIN jobs j ON j.id = e.job_id WHERE e.id = $1 AND j.id = $2",
    )
    .bind(estimate)
    .bind(job)
    .fetch_one(w.pool())
    .await
    .unwrap();
    assert_eq!(owners, (org.as_uuid(), org.as_uuid()));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn two_cores_apply_their_own_headroom_at_submit() {
    // 12 GiB GPU, 6–9 GiB: with 10 % kept free (10.8 usable) it fits; with 30 % (8.4) tight.
    let roomy = World::start(60).await;
    let strict = World::start_with(SubmissionConfig {
        headroom: mrl_server::modules::preflight::Headroom {
            gpu_free_fraction: 0.3,
            ..mrl_server::modules::preflight::Headroom::default()
        },
        ..SubmissionConfig::default()
    })
    .await;
    let mut configs = Vec::new();
    for w in [&roomy, &strict] {
        let m = w.machine(12, Answer::Estimate(6.0, 9.0)).await;
        let v = w.validate(sft(&w.fixture, &[m.node])).await;
        assert_eq!(v["ok"], true, "{v}");
        let (run, resolved, _) = submit(w, sft(&w.fixture, &[m.node]), "raw").await;
        assert_ne!(run["state"], "REJECTED", "{run}");
        configs.push((v["preflight"]["verdict"].clone(), resolved));
    }
    assert_eq!(configs[0].0, "fits");
    assert_eq!(configs[0].1["engine"]["gradient_checkpointing"], false);
    assert_eq!(configs[1].0, "tight");
    assert_eq!(
        configs[1].1["engine"]["gradient_checkpointing"], true,
        "the strict core's resolver applied its own headroom"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_7_5_gib_run_is_placed_on_the_gpu_that_holds_it() {
    let w = World::start(60).await;
    let f = w.fixture;
    let small = w.machine(4, Answer::Estimate(6.0, 9.0)).await;
    let big = w.machine(24, Answer::Estimate(6.0, 9.0)).await;
    // Both machines idle, no pin: the estimate (expected 7.5 GiB) fits only the 24 GiB GPU.
    let v = w.validate(sft(&f, &[])).await;
    assert_eq!(v["ok"], true, "{v}");
    assert_eq!(v["estimate"]["per_gpu_memory_gib"]["expected"], 7.5, "{v}");
    let (run, _, estimate) = submit(&w, sft(&f, &[]), "unpinned").await;
    assert_ne!(run["state"], "REJECTED", "{run}");
    assert!(estimate.is_some());
    let run = RunId::from_str(run["id"].as_str().unwrap()).unwrap();
    let asked: Value = sqlx::query_scalar("SELECT resource_request FROM jobs WHERE run_id = $1")
        .bind(run.as_uuid())
        .fetch_one(w.pool())
        .await
        .unwrap();
    assert_eq!(
        asked["roles"][0]["gpu"]["min_vram_bytes"],
        (7.5 * f64::from(1u32 << 30)).ceil() as u64,
        "{asked}"
    );
    until("the run is placed", || async {
        w.count(&format!(
            "SELECT count(*) FROM jobs WHERE run_id = '{}' AND state = 'RUNNING'",
            run.as_uuid()
        ))
        .await
            == 1
    })
    .await;
    let node: Uuid = sqlx::query_scalar(
        "SELECT r.node_id FROM resource_reservations r JOIN jobs j ON j.current_attempt_id = r.attempt_id
          WHERE j.run_id = $1",
    )
    .bind(run.as_uuid())
    .fetch_one(w.pool())
    .await
    .unwrap();
    assert_eq!(node, big.node);
    assert_ne!(node, small.node);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn validate_and_submit_agree_on_body_rules_and_hidden_inputs() {
    let w = World::start(60).await;
    let f = w.fixture;
    let mut bad_priority = sft(&f, &[]);
    bad_priority["priority"] = json!(5000);
    let mut missing_parent = sft(&f, &[]);
    missing_parent["parent_run_id"] = json!(RunId::new().to_string());
    let mut missing_checkpoint = sft(&f, &[]);
    missing_checkpoint["parent_checkpoint_id"] =
        json!(mrl_domain::ids::CheckpointId::new().to_string());
    for (i, (body, status)) in [
        (bad_priority, 422),
        (missing_parent, 404),
        (missing_checkpoint, 404),
    ]
    .into_iter()
    .enumerate()
    {
        let checked = w
            .call(
                Method::POST,
                &format!("/api/v1/projects/{}/runs:validate", w.project),
                body.clone(),
            )
            .await;
        let submitted = w
            .call_with(
                Method::POST,
                &format!("/api/v1/projects/{}/runs", w.project),
                body,
                &[("idempotency-key", &format!("agree-{i}"))],
            )
            .await;
        assert_eq!(checked.status, status, "validate: {}", checked.text());
        assert_eq!(submitted.status, status, "submit: {}", submitted.text());
        assert_eq!(checked.json()["code"], submitted.json()["code"]);
    }
    assert_eq!(w.count("SELECT count(*) FROM runs").await, 0);
}

//! P21 end-to-end tests: a real server (gateway + scheduler) on a test database and in-process
//! agents with fake GPUs whose attempts are scripted (no Python), over the real protocol.
//! Compiled by mrl-testkit under its P21 marker; they need PostgreSQL (`just test-integration`).

use crate::agent::gateway::{fast_config, server_config, TestAgent};
use crate::pg::TestDb;
use crate::seams::InMemorySecretStore;
use crate::server::{TestResponse, TestServer, TEST_SECRET};
use axum::body::Body;
use axum::http::Method;
use mrl_agent::session::{CommandHandler, Link};
use mrl_domain::ids::{AttemptId, JobId, PoolId};
use mrl_domain::resources::ReservationOwner;
use mrl_domain::state::{JobEvent, Owner};
use mrl_proto::agent::v1::agent_message::Body as Msg;
use mrl_proto::agent::v1::command::Kind;
use mrl_proto::agent::v1::{AttemptPhase, AttemptStatus, Command, ReconcileReport};
use mrl_proto::common::v1::{AttemptRef, Failure, FailureClass};
use mrl_proto::worker::v1::ExitKind;
use mrl_server::config::Mode;
use mrl_server::db::Db;
use mrl_server::modules::artifacts::{ArtifactStore, ArtifactsModule};
use mrl_server::modules::compute::LocalAgentPool;
use mrl_server::modules::jobs::{
    Chains, CompletionVerifier, JobChange, JobRow, JobTransitions, NewJob, Verification,
};
use mrl_server::modules::nodes::lanes::LaneRouter;
use mrl_server::modules::nodes::{Gateway, NodesModule};
use mrl_server::modules::scheduler::admission::{MaxQueuedPerProject, PoolFlags, PoolMembership};
use mrl_server::modules::scheduler::launch::{RunConfigSpec, Transfer};
use mrl_server::modules::scheduler::pipeline::{
    Demand, LaunchContributor, LaunchError, LaunchPlan, PauseSupport, Pipeline, Placement,
};
use mrl_server::modules::scheduler::placement::SingleNodePacking;
use mrl_server::modules::scheduler::{
    invariants, ReservationRequest, Reserved, SchedError, Scheduler, SchedulerConfig,
    SchedulerModule,
};
use mrl_server::seams::{SecretStore, LAUNCH_SECRET_HEADER};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

const WAIT: Duration = Duration::from_secs(60);
const RUNTIME: &str = "uv:sha256:0000000000000000000000000000000000000000000000000000000000000000";

/// Names the fake engine and its runtime, so launches can be built.
struct FakeEngine;

#[async_trait::async_trait]
impl LaunchContributor for FakeEngine {
    fn name(&self) -> &'static str {
        "fake_engine"
    }
    async fn contribute(
        &self,
        job: &JobRow,
        _attempt: &mrl_server::modules::jobs::AttemptRow,
        _placement: &Placement,
        plan: &mut LaunchPlan,
    ) -> Result<(), LaunchError> {
        if job.resource_request["stage_without_expiry"] == true {
            plan.stage.push(mrl_proto::agent::v1::StageItem {
                name: "fixture".into(),
                ..Default::default()
            });
        }
        if job.resource_request["stage_fixture"] == true {
            plan.stage.push(mrl_proto::agent::v1::StageItem {
                name: "fixture".into(),
                expires_at: Some(prost_types::Timestamp {
                    seconds: time::OffsetDateTime::now_utc().unix_timestamp() + 600,
                    nanos: 0,
                }),
                ..Default::default()
            });
        }
        plan.launch.engine_name = "fake".into();
        let runtime = if let Some(runtime) = job.resource_request["runtime_fixture"].as_str() {
            plan.launch.job_spec_json = json!({
                "run_id": "run_t", "job_kind": "TRAIN", "algorithm": "x-fake.steps",
                "common": {"seed": 7}, "engine": {"steps": 300, "step_seconds": 0.2},
                "switch": {}, "base_model_sha256": "0".repeat(64)
            })
            .to_string();
            runtime
        } else {
            RUNTIME
        };
        for r in &mut plan.launch.roles {
            r.runtime_ref = runtime.into();
        }
        Ok(())
    }
}

struct CanPause(bool);

#[async_trait::async_trait]
impl PauseSupport for CanPause {
    fn name(&self) -> &'static str {
        "test_pause"
    }
    async fn can_pause(&self, _job: &JobRow) -> Result<Option<bool>, SchedError> {
        Ok(Some(self.0))
    }
}

#[async_trait::async_trait]
impl mrl_server::modules::jobs::JobValidator for FakeEngine {
    fn name(&self) -> &'static str {
        "fake_engine"
    }

    async fn validate(
        &self,
        _: &mrl_server::modules::jobs::JobSpec,
    ) -> Result<mrl_server::modules::jobs::Verdict, mrl_server::seams::SeamError> {
        Ok(mrl_server::modules::jobs::Verdict::Accept)
    }
}

struct VerifyAll;

#[async_trait::async_trait]
impl CompletionVerifier for VerifyAll {
    fn name(&self) -> &'static str {
        "verify_all"
    }
    async fn verify(&self, _job: &JobRow) -> Result<Verification, mrl_server::seams::SeamError> {
        Ok(Verification::Verified)
    }
}

fn sched_config() -> SchedulerConfig {
    SchedulerConfig {
        tick_seconds: 1,
        lease_grace_seconds: 1,
        outage_threshold_seconds: 5,
        launch_deadline_seconds: 60,
        max_jobs_per_tick: 1,
        ..SchedulerConfig::default()
    }
}

fn pipeline(db: &Db, pause: bool) -> Pipeline {
    Pipeline::new(
        vec![
            Arc::new(PoolMembership),
            Arc::new(PoolFlags),
            Arc::new(MaxQueuedPerProject { max: 100 }),
        ],
        vec![Arc::new(SingleNodePacking)],
        vec![
            Arc::new(RunConfigSpec::new(db.clone())),
            Arc::new(FakeEngine),
        ],
        vec![Arc::new(CanPause(pause))],
        vec![Arc::new(LocalAgentPool::new(db.clone()))],
    )
}

const HIDDEN_PROJECT: Uuid = Uuid::from_u128(910);

struct QueueViewer;

#[async_trait::async_trait]
impl mrl_server::seams::Authorizer for QueueViewer {
    async fn authorize(
        &self,
        _: &mrl_server::seams::RequestContext,
        _: &str,
        resource: &mrl_server::seams::Resource,
    ) -> Result<mrl_server::seams::Decision, mrl_server::seams::SeamError> {
        use mrl_domain::ids::ProjectId;
        use mrl_server::seams::Decision;
        let hidden = ProjectId::from_uuid(HIDDEN_PROJECT).to_string();
        Ok(if resource.project.as_deref() == Some(hidden.as_str()) {
            Decision::Hide
        } else {
            Decision::Allow
        })
    }
}

/// A server with the gateway and the scheduler on one test database.
struct World {
    server: Option<TestServer>,
    gw: Arc<Gateway>,
    sched: Scheduler,
    db: TestDb,
    secrets: Arc<dyn SecretStore>,
    objects: tempfile::TempDir,
    store: Arc<ArtifactStore>,
    pool: Uuid,
    pause: bool,
    org: Uuid,
    project: Uuid,
}

async fn boot(
    db: &TestDb,
    secrets: Arc<dyn SecretStore>,
    objects: &std::path::Path,
    port: u16,
    pause: bool,
) -> (TestServer, Arc<Gateway>, Scheduler, Arc<ArtifactStore>) {
    let mut cfg = server_config(fast_config(), Mode::Solo, port);
    cfg.object_store.url = Some(
        reqwest::Url::from_directory_path(objects.join("objects"))
            .unwrap()
            .into(),
    );

    // A fresh router per boot: a restarted server never delivers to the old module's sink.
    let lanes = LaneRouter::new();
    let artifacts = ArtifactsModule::with_backend(
        db.db().clone(),
        mrl_storage::BackendConfig::Local {
            root: objects.join("objects"),
        },
        Mode::Solo,
        objects.join("scratch"),
    );
    let store = artifacts.store();
    let transfer = Transfer::new(store.clone(), artifacts.secrets(), secrets.clone());
    let nodes = NodesModule::new(db.db().clone(), &cfg, secrets.clone(), lanes.clone());
    let gw = nodes.gateway();
    let chains = Chains::new(
        vec![Arc::new(FakeEngine)],
        vec![Arc::new(VerifyAll)],
        vec![],
    );
    let module = SchedulerModule::with_parts(
        db.db().clone(),
        sched_config(),
        &gw,
        &lanes,
        chains.clone(),
        pipeline(db.db(), pause),
        transfer,
    );
    let sched = module.scheduler();
    let seams = crate::seams::build(
        crate::seams::solo(TEST_SECRET)
            .authorizer(QueueViewer)
            .secrets_arc(secrets),
        Mode::Solo,
    );
    let modules: Vec<Box<dyn mrl_server::app::ServerModule>> = vec![
        Box::new(nodes),
        Box::new(artifacts),
        Box::new(module),
        Box::new(mrl_server::modules::runs::RunsModule::with_chains(
            db.db().clone(),
            chains,
        )),
    ];
    match TestServer::start(cfg, seams, modules).await {
        Ok(s) => (s, gw, sched, store),
        Err(e) => panic!("server did not start: {}", e.explain()),
    }
}

impl World {
    async fn start(pause: bool) -> Self {
        Self::start_for_org(pause, Uuid::now_v7()).await
    }

    async fn start_for_org(pause: bool, org: Uuid) -> Self {
        let db = TestDb::new().await;
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let objects = tempfile::tempdir().unwrap();
        let (server, gw, sched, store) = boot(&db, secrets.clone(), objects.path(), 0, pause).await;
        let mut w = Self {
            server: Some(server),
            gw,
            sched,
            db,
            secrets,
            objects,
            store,
            pool: Uuid::nil(),
            pause,
            org,
            project: Uuid::now_v7(),
        };
        let r = w
            .post(
                "/api/v1/pools",
                json!({"name": "gpus", "attempt_lease_ttl_seconds": 10}),
            )
            .await;
        assert_eq!(r.status, 201, "pool: {}", r.text());
        let id = r.json()["id"].as_str().unwrap_or_default().to_owned();
        w.pool = PoolId::from_str(&id).unwrap().as_uuid();
        w
    }

    fn server(&self) -> &TestServer {
        self.server.as_ref().unwrap()
    }

    async fn post(&self, path: &str, body: Value) -> TestResponse {
        self.server()
            .request(
                Method::POST,
                path,
                &[
                    (LAUNCH_SECRET_HEADER, TEST_SECRET),
                    ("content-type", "application/json"),
                ],
                Body::from(body.to_string()),
            )
            .await
    }

    async fn get(&self, path: &str) -> TestResponse {
        self.server()
            .request(
                Method::GET,
                path,
                &[(LAUNCH_SECRET_HEADER, TEST_SECRET)],
                Body::empty(),
            )
            .await
    }

    /// Stops the server and starts a new one on the same database, CA and gRPC port.
    async fn restart(&mut self) {
        let port = self.gw.addr().unwrap().port();
        if let Some(s) = self.server.take() {
            s.shutdown()
                .await
                .unwrap_or_else(|e| panic!("shutdown: {}", e.explain()));
        }
        let (server, gw, sched, store) = boot(
            &self.db,
            self.secrets.clone(),
            self.objects.path(),
            port,
            self.pause,
        )
        .await;
        self.server = Some(server);
        self.gw = gw;
        self.sched = sched;
        self.store = store;
    }

    /// An enrolled agent with `gpus` fake GPUs and a scripted attempt runtime.
    async fn agent(&self, gpus: u32) -> Machine {
        let agent = TestAgent::new(gpus, |_| {});
        let r = self
            .post(
                &format!(
                    "/api/v1/pools/{}/enrollment-tokens",
                    PoolId::from_uuid(self.pool)
                ),
                json!({}),
            )
            .await;
        assert_eq!(r.status, 201, "token: {}", r.text());
        let token = r.json()["token"].as_str().unwrap_or_default().to_owned();
        let node = mrl_agent::enroll::enroll_dial(
            &agent.ctx,
            &self.gw.addr().unwrap().to_string(),
            &token,
            mrl_agent::transports::TransportKind::Direct,
            false,
        )
        .await
        .unwrap_or_else(|e| panic!("enroll: {}", e.explain()))
        .link
        .node_id;
        let node = mrl_domain::ids::NodeId::from_str(&node).unwrap().as_uuid();
        let script = Script::new(&agent);
        let m = Machine {
            running: Some(agent.run()),
            agent,
            node,
            script,
        };
        self.until("the machine is online with its GPUs", || async {
            let n: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM compute_nodes n JOIN gpu_devices g ON g.node_id = n.id
                 WHERE n.id = $1 AND n.status = 'ONLINE'",
            )
            .bind(m.node)
            .fetch_one(self.db.pool())
            .await
            .unwrap();
            n == i64::from(gpus)
        })
        .await;
        m
    }

    async fn until<F, Fut>(&self, what: &str, f: F)
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

    async fn job(&self, id: Uuid) -> JobRow {
        sqlx::query_as(
            "SELECT id, org_id, project_id, kind, run_id, subject_ref, state, state_version, version, \
             state_reason, failure_class, epoch, priority, pool_id, resource_request, retry_policy, \
             retries_used, current_attempt_id, queued_at, started_at, finished_at, next_eligible_at, \
             max_runtime_seconds, created_by, created_at, updated_at FROM jobs WHERE id = $1",
        )
        .bind(id)
        .fetch_one(self.db.pool())
        .await
        .unwrap()
    }

    async fn wait_state(&self, job: Uuid, state: &str) -> JobRow {
        self.until(&format!("job state {state}"), || async {
            self.job(job).await.state == state
        })
        .await;
        self.job(job).await
    }

    async fn attempt_state(&self, attempt: Uuid) -> String {
        sqlx::query_scalar("SELECT state FROM job_attempts WHERE id = $1")
            .bind(attempt)
            .fetch_one(self.db.pool())
            .await
            .unwrap()
    }

    async fn wait_attempt(&self, attempt: Uuid, state: &str) {
        self.until(&format!("attempt state {state}"), || async {
            self.attempt_state(attempt).await == state
        })
        .await;
    }

    /// A queued job (no run) asking for `gpus` GPUs on one machine.
    async fn queue(&self, gpus: u32) -> Uuid {
        self.queue_with(
            json!({"roles": [{"role": "trainer", "replicas": 1, "gpu_count": gpus,
            "cpu_millis": 100, "mem_bytes": 1 << 20}]}),
            None,
        )
        .await
    }

    async fn queue_with(&self, resources: Value, run: Option<Uuid>) -> Uuid {
        let db = self.db.db();
        let mut tx = db.begin().await.unwrap();
        let job = NewJob {
            id: Uuid::now_v7(),
            org_id: self.org,
            project_id: self.project,
            kind: if run.is_some() { "TRAIN" } else { "DATA_PREP" }.into(),
            run_id: run,
            subject_ref: None,
            priority: 0,
            resource_request: resources,
            retry_policy: json!({}),
            max_runtime_seconds: None,
            created_by: "test".into(),
        };
        let row = JobTransitions::create(&mut tx, &job).await.unwrap();
        let row = JobTransitions::apply(
            &mut tx,
            &row,
            JobEvent::Submit,
            Owner::Jobs,
            JobChange::default(),
        )
        .await
        .unwrap();
        let row = JobTransitions::apply(
            &mut tx,
            &row,
            JobEvent::Accept,
            Owner::Jobs,
            JobChange::default(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        row.id
    }

    /// A run (experiment, run, config) for a TRAIN job.
    async fn run(&self) -> Uuid {
        let (exp, run) = (Uuid::now_v7(), Uuid::now_v7());
        sqlx::query(
            "INSERT INTO experiments (id, org_id, project_id, name, created_by) VALUES ($1, $2, $3, 'e', 't')",
        )
        .bind(exp)
        .bind(self.org)
        .bind(self.project)
        .execute(self.db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runs (id, org_id, project_id, experiment_id, name, kind, algorithm_id,
               base_model_version_id, submit_key, submit_fingerprint, created_by)
             VALUES ($1, $2, $3, $4, 'r', 'TRAIN', 'sft', $5, $6, '\\x00', 't')",
        )
        .bind(run)
        .bind(self.org)
        .bind(self.project)
        .bind(exp)
        .bind(Uuid::now_v7())
        .bind(run.to_string())
        .execute(self.db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO run_configs (run_id, org_id, submitted, resolved, resolved_sha256,
               dataset_version_ids, seeds)
             VALUES ($1, $2, '{}', '{\"engine\": {\"steps\": 5}}', decode(repeat('00', 32), 'hex'), '{}', '{\"global\": 1}')",
        )
        .bind(run)
        .bind(self.org)
        .execute(self.db.pool())
        .await
        .unwrap();
        run
    }

    /// Waits until the job's current attempt is RUNNING; returns it.
    async fn running(&self, job: Uuid) -> Uuid {
        let row = self.wait_state(job, "RUNNING").await;
        let a = row.current_attempt_id.unwrap();
        self.wait_attempt(a, "RUNNING").await;
        a
    }

    async fn invariants(&self) {
        let broken = invariants::check(self.db.db()).await.unwrap();
        assert!(broken.is_empty(), "invariants broken: {broken:?}");
    }

    async fn events(&self, aggregate: Uuid) -> Vec<String> {
        sqlx::query_scalar("SELECT type FROM outbox_events WHERE aggregate_id = $1 ORDER BY id")
            .bind(aggregate)
            .fetch_all(self.db.pool())
            .await
            .unwrap()
    }

    async fn reservations(&self, job: Uuid) -> Vec<(String, Uuid)> {
        sqlx::query_as("SELECT state, node_id FROM resource_reservations WHERE job_id = $1 ORDER BY created_at")
            .bind(job)
            .fetch_all(self.db.pool())
            .await
            .unwrap()
    }

    async fn commands(&self, attempt: Uuid, kind: &str) -> Vec<(String, Vec<u8>)> {
        sqlx::query_as("SELECT state, payload FROM agent_commands WHERE attempt_id = $1 AND kind = $2 ORDER BY seq")
            .bind(attempt)
            .bind(kind)
            .fetch_all(self.db.pool())
            .await
            .unwrap()
    }

    /// Makes a waiting retry due now.
    async fn retry_now(&self, job: Uuid) {
        self.wait_state(job, "RETRY_WAIT").await;
        sqlx::query("UPDATE jobs SET next_eligible_at = now() WHERE id = $1")
            .bind(job)
            .execute(self.db.pool())
            .await
            .unwrap();
    }
}

/// The scripted attempt runtime of one agent: launches start at once, cancels and pauses end
/// the attempt, and the test ends attempts with [`Script::exit`].
struct Script {
    link: Arc<Link>,
    running: Mutex<BTreeMap<String, AttemptRef>>,
    seen: Mutex<Vec<Command>>,
    refuse_unknown: bool,
}

struct Handler(Arc<Script>);

fn status(
    r: &AttemptRef,
    phase: AttemptPhase,
    exit: ExitKind,
    failure: Option<FailureClass>,
) -> AttemptStatus {
    AttemptStatus {
        attempt: Some(r.clone()),
        phase: phase as i32,
        exit_kind: exit as i32,
        failure: failure.map(|c| Failure {
            class: c as i32,
            message: "scripted failure".into(),
            ..Failure::default()
        }),
        time: Some(prost_types::Timestamp::default()),
        ..AttemptStatus::default()
    }
}

impl Script {
    fn new(agent: &TestAgent) -> Arc<Self> {
        let link = Link::of(&agent.ctx).unwrap_or_else(|e| panic!("link: {}", e.explain()));
        let s = Arc::new(Self {
            link: link.clone(),
            running: Mutex::default(),
            seen: Mutex::default(),
            refuse_unknown: true,
        });
        for kind in [
            "LaunchAttempt",
            "CancelAttempt",
            "PauseAttempt",
            "StageArtifacts",
            "UpdateCredentials",
        ] {
            link.register(kind, Arc::new(Handler(s.clone())));
        }
        s
    }

    fn publish(&self) {
        let refs = self.running.lock().unwrap().values().cloned().collect();
        self.link.set_running(refs);
    }

    fn send(&self, s: AttemptStatus) {
        self.link.send_control(Msg::AttemptStatus(s)).unwrap();
    }

    /// Ends a running attempt as the worker would.
    fn exit(&self, attempt: Uuid, kind: ExitKind, failure: Option<FailureClass>) {
        let id = AttemptId::from_uuid(attempt).to_string();
        let r = self
            .running
            .lock()
            .unwrap()
            .remove(&id)
            .unwrap_or_else(|| panic!("{id} is not running"));
        self.publish();
        self.send(status(&r, AttemptPhase::Exited, kind, failure));
    }

    /// The reconcile report an agent sends when it starts.
    fn report(&self) {
        let attempts = self
            .running
            .lock()
            .unwrap()
            .values()
            .map(|r| status(r, AttemptPhase::Running, ExitKind::Unspecified, None))
            .collect();
        self.link
            .send_control(Msg::Reconcile(ReconcileReport {
                attempts,
                deployments: vec![],
            }))
            .unwrap();
    }

    fn launches(&self) -> Vec<mrl_proto::agent::v1::LaunchAttempt> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter_map(|c| match &c.kind {
                Some(Kind::LaunchAttempt(l)) => Some(l.clone()),
                _ => None,
            })
            .collect()
    }

    fn cancels(&self) -> Vec<mrl_proto::agent::v1::CancelAttempt> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter_map(|c| match &c.kind {
                Some(Kind::CancelAttempt(l)) => Some(l.clone()),
                _ => None,
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl CommandHandler for Handler {
    async fn handle(&self, c: Command) -> Result<(), Failure> {
        let s = &self.0;
        s.seen.lock().unwrap().push(c.clone());
        let unknown = || Failure {
            message: "no such attempt here".into(),
            ..Failure::default()
        };
        match c.kind {
            Some(Kind::LaunchAttempt(l)) => {
                let r = l.attempt.unwrap_or_default();
                s.running
                    .lock()
                    .unwrap()
                    .insert(r.attempt_id.clone(), r.clone());
                s.publish();
                s.send(status(
                    &r,
                    AttemptPhase::Staging,
                    ExitKind::Unspecified,
                    None,
                ));
                s.send(status(
                    &r,
                    AttemptPhase::Running,
                    ExitKind::Unspecified,
                    None,
                ));
                Ok(())
            }
            Some(Kind::CancelAttempt(x)) => {
                let r = x.attempt.unwrap_or_default();
                let known = s.running.lock().unwrap().remove(&r.attempt_id);
                match known {
                    Some(r) => {
                        s.publish();
                        s.send(status(&r, AttemptPhase::Exited, ExitKind::Cancelled, None));
                        Ok(())
                    }
                    None if s.refuse_unknown => Err(unknown()),
                    None => Ok(()),
                }
            }
            Some(Kind::PauseAttempt(x)) => {
                let r = x.attempt.unwrap_or_default();
                let known = s.running.lock().unwrap().remove(&r.attempt_id);
                match known {
                    Some(r) => {
                        s.publish();
                        s.send(status(&r, AttemptPhase::Exited, ExitKind::Paused, None));
                        Ok(())
                    }
                    None => Err(unknown()),
                }
            }
            _ => Ok(()),
        }
    }
}

/// One agent and its script.
struct Machine {
    agent: TestAgent,
    running: Option<crate::agent::gateway::Running>,
    node: Uuid,
    script: Arc<Script>,
}

impl Machine {
    /// Cuts the network (the agent process lives on, with its attempts).
    async fn disconnect(&mut self) {
        if let Some(r) = self.running.take() {
            r.stop().await;
        }
    }

    /// Reconnects after a network cut (no reconcile report: the agent did not restart).
    fn reconnect(&mut self) {
        self.running = Some(self.agent.run());
    }

    /// Restarts the agent: it reconnects and reports what it still runs.
    async fn restart(&mut self) {
        self.disconnect().await;
        self.script.report();
        self.reconnect();
    }
}

// ---------------------------------------------------------------------------------------

/// Queued → placed → launched → RUNNING → completed; the GPU is held while it runs and given
/// back after; the launch carries the placement, lease and job spec.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_queued_job_is_placed_launched_and_completed() {
    let w = World::start(false).await;
    let m = w.agent(2).await;
    let run = w.run().await;
    let job = w
        .queue_with(
            json!({"roles": [{"role": "trainer", "replicas": 1, "gpu_count": 2,
            "cpu_millis": 100, "mem_bytes": 1 << 20}]}),
            Some(run),
        )
        .await;
    let attempt = w.running(job).await;
    let row = w.job(job).await;
    assert_eq!(row.pool_id, Some(w.pool));
    assert_eq!(row.epoch, 1);
    assert_eq!(
        w.reservations(job).await,
        vec![("ACTIVE".to_owned(), m.node)]
    );
    let l = m.script.launches();
    assert_eq!(l.len(), 1);
    let l = &l[0];
    assert_eq!(l.attempt.as_ref().unwrap().fencing_epoch, 1);
    assert_eq!(l.allocation.as_ref().unwrap().gpu_indices, vec![0, 1]);
    assert_eq!(l.lease_duration_s, 10);
    assert_eq!(l.roles[0].runtime_ref, RUNTIME);
    let spec: Value = serde_json::from_str(&l.job_spec_json).unwrap();
    assert_eq!(spec["engine"]["steps"], 5);
    assert_eq!(spec["job_id"], JobId::from_uuid(job).to_string());
    assert_eq!(spec["algorithm"], "sft");
    w.invariants().await;

    m.script.exit(attempt, ExitKind::Completed, None);
    w.wait_state(job, "COMPLETED").await;
    w.until("the hold is released", || async {
        w.reservations(job)
            .await
            .iter()
            .all(|(s, _)| s == "RELEASED")
    })
    .await;
    assert_eq!(w.attempt_state(attempt).await, "EXITED_OK");
    w.invariants().await;
}

/// Kill the worker: the attempt errors, the job waits to retry, a new attempt (next epoch)
/// starts from the run's latest committed checkpoint.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_killed_worker_retries_from_the_latest_checkpoint() {
    let w = World::start(false).await;
    let m = w.agent(1).await;
    let run = w.run().await;
    let job = w
        .queue_with(
            json!({"roles": [{"role": "trainer", "replicas": 1, "gpu_count": 1,
            "cpu_millis": 100, "mem_bytes": 1 << 20}]}),
            Some(run),
        )
        .await;
    let first = w.running(job).await;
    let ckpt = Uuid::now_v7();
    sqlx::query("UPDATE runs SET latest_committed_checkpoint_id = $2 WHERE id = $1")
        .bind(run)
        .bind(ckpt)
        .execute(w.db.pool())
        .await
        .unwrap();
    m.script
        .exit(first, ExitKind::Failed, Some(FailureClass::EngineError));
    w.retry_now(job).await;
    assert_eq!(w.attempt_state(first).await, "EXITED_ERROR");
    w.invariants().await;
    let second = w.running(job).await;
    assert_ne!(first, second);
    let row = w.job(job).await;
    assert_eq!(row.epoch, 2);
    let resume: Option<Uuid> =
        sqlx::query_scalar("SELECT resume_from_checkpoint_id FROM job_attempts WHERE id = $1")
            .bind(second)
            .fetch_one(w.db.pool())
            .await
            .unwrap();
    assert_eq!(resume, Some(ckpt));
    let l = m.script.launches();
    let spec: Value = serde_json::from_str(&l.last().unwrap().job_spec_json).unwrap();
    assert_eq!(
        spec["resume_from_checkpoint_id"],
        mrl_domain::ids::CheckpointId::from_uuid(ckpt).to_string()
    );
    assert_eq!(row.retries_used["ENGINE_ERROR"], 1);
    // ENGINE_ERROR allows one retry: the next failure fails the job.
    m.script
        .exit(second, ExitKind::Failed, Some(FailureClass::EngineError));
    let row = w.wait_state(job, "FAILED").await;
    assert_eq!(row.failure_class.as_deref(), Some("ENGINE_ERROR"));
    w.invariants().await;
}

/// The same terminal status sent again (new seq), an older status after it and a reconcile
/// listing the ended attempt change nothing (A1 ruling: idempotent by content).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn repeated_and_stale_reports_make_no_second_transition() {
    let w = World::start(false).await;
    let m = w.agent(1).await;
    let job = w.queue(1).await;
    let attempt = w.running(job).await;
    let r = AttemptRef {
        job_id: JobId::from_uuid(job).to_string(),
        attempt_id: AttemptId::from_uuid(attempt).to_string(),
        fencing_epoch: 1,
    };
    m.script
        .exit(attempt, ExitKind::Failed, Some(FailureClass::EngineError));
    w.wait_state(job, "RETRY_WAIT").await;
    // Re-sent facts: the exit again, an older RUNNING, and a reconcile listing it ended.
    m.script.send(status(
        &r,
        AttemptPhase::Exited,
        ExitKind::Failed,
        Some(FailureClass::EngineError),
    ));
    m.script.send(status(
        &r,
        AttemptPhase::Running,
        ExitKind::Unspecified,
        None,
    ));
    m.script
        .link
        .send_control(Msg::Reconcile(ReconcileReport {
            attempts: vec![status(
                &r,
                AttemptPhase::Exited,
                ExitKind::Failed,
                Some(FailureClass::EngineError),
            )],
            deployments: vec![],
        }))
        .unwrap();
    // The RUNNING one is a live claim for an ended attempt: the machine is told to stop it.
    w.until("a fenced cancel for the ended attempt", || async {
        !w.commands(attempt, "CancelAttempt").await.is_empty()
    })
    .await;
    w.until("the reconcile report was applied", || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM scheduler_node_reconciles WHERE node_id = $1)",
        )
        .bind(m.node)
        .fetch_one(w.db.pool())
        .await
        .unwrap()
    })
    .await;
    let events = w.events(attempt).await;
    assert_eq!(
        events.iter().filter(|e| *e == "attempt.errored").count(),
        1,
        "{events:?}"
    );
    let row = w.job(job).await;
    assert_eq!(row.state, "RETRY_WAIT");
    assert_eq!(row.retries_used["ENGINE_ERROR"], 1);
    w.invariants().await;
}

/// Partition an agent past its lease: LOST, epoch bumped, GPU quarantined, job rescheduled on
/// the other machine; on reconnect the stale attempt is cancelled (FENCED) and the GPU freed.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_partitioned_agent_is_fenced_and_its_job_rescheduled() {
    let w = World::start(false).await;
    let mut a = w.agent(1).await;
    let job = w.queue(1).await;
    let first = w.running(job).await;
    let b = w.agent(1).await;
    assert_eq!(w.reservations(job).await[0].1, a.node);

    a.disconnect().await;
    w.wait_attempt(first, "LOST").await;
    let row = w.job(job).await;
    assert_eq!(row.epoch, 2, "a lost lease moves the epoch on");
    assert_eq!(row.failure_class.as_deref(), Some("NODE_LOST"));
    assert_eq!(w.reservations(job).await[0].0, "QUARANTINED");
    w.invariants().await;

    w.retry_now(job).await;
    let second = w.running(job).await;
    let holds = w.reservations(job).await;
    assert_eq!(holds.len(), 2);
    assert_eq!(
        holds[1],
        ("ACTIVE".to_owned(), b.node),
        "rescheduled on the other machine"
    );
    assert_eq!(w.job(job).await.epoch, 3);

    // Back online without a restart: the queued fenced cancel stops the stale attempt.
    a.reconnect();
    w.until("the stale attempt is cancelled", || async {
        a.script.cancels().iter().any(|c| {
            c.reason == "FENCED"
                && c.attempt
                    .as_ref()
                    .is_some_and(|r| r.attempt_id == AttemptId::from_uuid(first).to_string())
        })
    })
    .await;
    w.until("the quarantined GPU is released", || async {
        w.reservations(job).await[0].0 == "RELEASED"
    })
    .await;
    assert_eq!(w.attempt_state(first).await, "LOST");
    assert_eq!(w.attempt_state(second).await, "RUNNING");
    w.invariants().await;
}

/// Restart the server mid-run: the attempt keeps running at the same epoch and completes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_server_restart_mid_run_changes_nothing() {
    let mut w = World::start(false).await;
    let m = w.agent(1).await;
    let job = w.queue(1).await;
    let attempt = w.running(job).await;
    w.restart().await;
    // Longer than a lease: the agent reconnected and its heartbeats keep the lease.
    tokio::time::sleep(Duration::from_secs(13)).await;
    assert_eq!(w.attempt_state(attempt).await, "RUNNING");
    let row = w.job(job).await;
    assert_eq!((row.state.as_str(), row.epoch), ("RUNNING", 1));
    m.script.exit(attempt, ExitKind::Completed, None);
    w.wait_state(job, "COMPLETED").await;
    let events = w.events(job).await;
    assert!(
        !events.iter().any(|e| e == "job.retry_scheduled"),
        "{events:?}"
    );
    w.invariants().await;
}

/// Restart the agent within its lease: its reconcile report lists the attempt running, which
/// is re-adopted (same attempt, same epoch, no retry).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn an_agent_restarted_within_its_lease_keeps_the_attempt() {
    let w = World::start(false).await;
    let mut m = w.agent(1).await;
    let job = w.queue(1).await;
    let attempt = w.running(job).await;
    m.restart().await;
    w.until("the reconcile report is applied", || async {
        let n: i64 =
            sqlx::query_scalar("SELECT count(*) FROM scheduler_node_reconciles WHERE node_id = $1")
                .bind(m.node)
                .fetch_one(w.db.pool())
                .await
                .unwrap();
        n == 1
    })
    .await;
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert_eq!(w.attempt_state(attempt).await, "RUNNING");
    let row = w.job(job).await;
    assert_eq!(
        (row.state.as_str(), row.epoch, row.current_attempt_id),
        ("RUNNING", 1, Some(attempt))
    );
    assert!(m.script.cancels().is_empty());
    w.invariants().await;
}

/// An agent restarted without the attempt (its state was lost): LOST at once, no lease wait.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn an_agent_that_forgot_its_attempt_loses_it_at_once() {
    let w = World::start(false).await;
    let mut m = w.agent(1).await;
    let job = w.queue(1).await;
    let attempt = w.running(job).await;
    m.disconnect().await;
    m.script.running.lock().unwrap().clear();
    m.script.publish();
    m.script.report();
    m.reconnect();
    w.wait_attempt(attempt, "LOST").await;
    // The machine said nothing runs there: the GPU is free at once.
    assert_eq!(w.reservations(job).await[0].0, "RELEASED");
    w.wait_state(job, "RETRY_WAIT").await;
    w.invariants().await;
}

/// D-04-09: after a control-plane gap every live lease is extended first; the attempt is not
/// lost for the outage, only once its extended lease runs out with no agent.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_control_plane_gap_extends_leases_first() {
    let mut w = World::start(false).await;
    let mut m = w.agent(1).await;
    let job = w.queue(1).await;
    let attempt = w.running(job).await;
    w.server.take().unwrap().shutdown().await.unwrap();
    m.disconnect().await;
    // Seed the outage while the scheduler is stopped so the first tick sees it
    // before checking leases.
    let outage_end: time::OffsetDateTime = sqlx::query_scalar(
        "WITH t AS (UPDATE scheduler_ticks SET last_tick_at = now() - interval '1 hour'),
              g AS (UPDATE agent_commands SET updated_at = now() - interval '1 minute'
                    WHERE attempt_id = $1 AND kind = 'LaunchAttempt')
         UPDATE job_attempts SET lease_expires_at = now() - interval '1 minute'
         WHERE id = $1 AND state = 'RUNNING' RETURNING now()",
    )
    .bind(attempt)
    .fetch_one(w.db.pool())
    .await
    .unwrap();
    w.restart().await;
    w.wait_attempt(attempt, "LOST").await;

    let (lease, ended): (time::OffsetDateTime, time::OffsetDateTime) =
        sqlx::query_as("SELECT lease_expires_at, ended_at FROM job_attempts WHERE id = $1")
            .bind(attempt)
            .fetch_one(w.db.pool())
            .await
            .unwrap();
    assert!(
        lease >= outage_end + time::Duration::seconds(10),
        "the gap should grant a full lease, got {lease} after outage at {outage_end}"
    );
    assert!(
        ended >= lease,
        "the attempt ended at {ended} before the extended lease expired at {lease}"
    );
    w.invariants().await;
}

/// Cancel: CancelAttempt with grace, the attempt ends, the job is CANCELLED, the GPU free.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_cancel_reaches_the_attempt_and_frees_the_gpu() {
    let w = World::start(false).await;
    let m = w.agent(1).await;
    let job = w.queue(1).await;
    let attempt = w.running(job).await;
    let mut tx = w.db.db().begin().await.unwrap();
    let row = JobTransitions::lock(&mut tx, w.org, job)
        .await
        .unwrap()
        .unwrap();
    JobTransitions::request_cancel(&mut tx, &row, "tester")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    w.wait_state(job, "CANCELLED").await;
    let c = m.script.cancels();
    assert_eq!(c.len(), 1);
    assert_eq!((c[0].reason.as_str(), c[0].grace_s), ("USER", 30));
    assert_eq!(w.attempt_state(attempt).await, "KILLED");
    w.until("released", || async {
        w.reservations(job).await[0].0 == "RELEASED"
    })
    .await;
    w.invariants().await;
}

/// Pause: sent only when the engine says it can pause; otherwise refused at once.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_pause_is_delivered_only_when_the_engine_can_pause() {
    for can in [true, false] {
        let w = World::start(can).await;
        let m = w.agent(1).await;
        let job = w.queue(1).await;
        let attempt = w.running(job).await;
        let mut tx = w.db.db().begin().await.unwrap();
        let row = JobTransitions::lock(&mut tx, w.org, job)
            .await
            .unwrap()
            .unwrap();
        JobTransitions::apply(
            &mut tx,
            &row,
            JobEvent::RequestPause,
            Owner::Jobs,
            JobChange::default(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        if can {
            w.wait_state(job, "PAUSED").await;
            assert_eq!(w.attempt_state(attempt).await, "EXITED_OK");
            assert_eq!(w.reservations(job).await[0].0, "RELEASED");
        } else {
            let row = w.wait_state(job, "RUNNING").await;
            let events = w.events(job).await;
            assert!(
                events.contains(&"job.pause_refused".to_owned()),
                "{events:?}"
            );
            assert!(row.state_reason.unwrap()["message"]
                .as_str()
                .unwrap()
                .contains("can't pause"));
            assert!(w.commands(attempt, "PauseAttempt").await.is_empty());
        }
        let _ = &m;
        w.invariants().await;
    }
}

/// A job no machine can run stays QUEUED with a plain reason; GET /queue shows its position.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn unsatisfiable_jobs_wait_with_a_reason_and_the_queue_shows_them() {
    let w = World::start(false).await;
    let _m = w.agent(1).await;
    let busy = w.queue(1).await;
    w.running(busy).await;
    let waits = w.queue(1).await;
    let never = w.queue(9).await;
    w.until("both reasons", || async {
        w.job(waits).await.state_reason.is_some() && w.job(never).await.state_reason.is_some()
    })
    .await;
    let r = w.job(waits).await.state_reason.unwrap();
    assert_eq!(r["code"], "MRL-QUOTA-210", "{r}");
    let r = w.job(never).await.state_reason.unwrap();
    assert_eq!(r["code"], "MRL-CAP-210", "{r}");
    assert!(r["message"].as_str().unwrap().contains("9 GPU(s)"), "{r}");

    let q = w
        .get(&format!("/api/v1/queue?pool={}", PoolId::from_uuid(w.pool)))
        .await;
    assert_eq!(q.status, 200, "{}", q.text());
    let q = q.json();
    assert_eq!(q["total"], 2);
    assert_eq!(q["jobs"][0]["job_id"], JobId::from_uuid(waits).to_string());
    assert_eq!(q["jobs"][0]["position"], 1);
    assert_eq!(q["jobs"][1]["reason_code"], "MRL-CAP-210");
    let missing = w
        .get(&format!("/api/v1/queue?pool={}", PoolId::new()))
        .await;
    assert_eq!(missing.status, 404);
    let bad = w.get("/api/v1/queue?pool=nope").await;
    assert_eq!(bad.status, 422);
    w.invariants().await;
}

/// `one_holder_per_gpu` refuses a forced second holder of a GPU.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn one_holder_per_gpu_refuses_a_forced_double_allocation() {
    let w = World::start(false).await;
    let _m = w.agent(1).await;
    let job = w.queue(1).await;
    w.running(job).await;
    let r = sqlx::query(
        "WITH g AS (SELECT gpu_device_id FROM reservation_devices WHERE active LIMIT 1),
              r AS (INSERT INTO resource_reservations (id, org_id, owner_kind, deployment_revision_id,
                      pool_id, node_id, state, epoch, lease_expires_at)
                    SELECT $1, $2, 'deployment', $3, pool_id, node_id, 'HELD', 0, now() + interval '1 hour'
                    FROM resource_reservations WHERE job_id = $4 RETURNING id)
         INSERT INTO reservation_devices (reservation_id, gpu_device_id) SELECT r.id, g.gpu_device_id FROM r, g",
    )
    .bind(Uuid::now_v7())
    .bind(w.org)
    .bind(Uuid::now_v7())
    .bind(job)
    .execute(w.db.pool())
    .await;
    let e = r.expect_err("a second live holder of one GPU");
    assert!(e.to_string().contains("one_holder_per_gpu"), "{e}");
    w.invariants().await;
}

/// The generic reserve/release, for a deployment owner: holds GPUs and CPU/memory counters,
/// is fenced by its epoch, and gives everything back.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_deployment_reserves_and_releases_through_the_scheduler() {
    let w = World::start(false).await;
    let m = w.agent(2).await;
    let demand = Demand::from_request(
        &json!({"roles": [{"role": "service", "replicas": 1, "gpu_count": 2,
        "cpu_millis": 500, "mem_bytes": 1 << 20}]}),
    )
    .unwrap();
    let req = ReservationRequest {
        request_id: Uuid::now_v7(),
        owner: ReservationOwner::Deployment(mrl_domain::ids::DeploymentId::new()),
        org_id: w.org,
        pool_id: w.pool,
        demand: demand.clone(),
        epoch: 7,
        lease: Some(Duration::from_secs(600)),
    };
    let held = match w.sched.reserve(&req).await.unwrap() {
        Reserved::Held(r) => r,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        (held.state.as_str(), held.node_id, held.cpu_millis),
        ("HELD", m.node, 500)
    );
    assert_eq!(
        w.sched.reserve(&req).await.unwrap(),
        Reserved::Held(held.clone())
    );
    let other = ReservationRequest {
        request_id: Uuid::now_v7(),
        ..req.clone()
    };
    assert!(matches!(
        w.sched.reserve(&other).await.unwrap(),
        Reserved::Busy(_)
    ));
    let mut changed = req.clone();
    changed.demand.roles[0].gpu_count = 1;
    assert!(w.sched.reserve(&changed).await.is_err());
    let job = w.queue(1).await;
    w.until("the job waits", || async {
        w.job(job).await.state_reason.is_some()
    })
    .await;
    assert_eq!(w.job(job).await.state, "QUEUED");
    w.invariants().await;
    assert!(w
        .sched
        .release(Uuid::now_v7(), held.id, 7, "wrong org")
        .await
        .is_err());
    assert!(w
        .sched
        .renew(Uuid::now_v7(), held.id, 7, Duration::from_secs(60))
        .await
        .is_err());
    // An older epoch can't touch it.
    let stale = w
        .sched
        .release(w.org, held.id, 6, "test")
        .await
        .unwrap_err();
    assert!(stale.to_string().contains("MRL-CONFLICT-210"), "{stale}");
    w.sched
        .renew(w.org, held.id, 7, Duration::from_secs(60))
        .await
        .unwrap();
    let gone = w.sched.release(w.org, held.id, 7, "done").await.unwrap();
    assert_eq!(
        w.sched.reserve(&req).await.unwrap(),
        Reserved::Ended(Box::new(gone.clone()))
    );
    assert_eq!(gone.state, "EXPIRED", "a hold never activated expires");
    let active: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM reservation_devices WHERE reservation_id = $1 AND active",
    )
    .bind(held.id)
    .fetch_one(w.db.pool())
    .await
    .unwrap();
    assert_eq!(active, 0);
    w.running(job).await;
    w.invariants().await;
}

// An impossible first page cannot hide a later fitting job. The agent's actual
// wire decoder must keep the helper's GPU list empty.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn queue_backfills_a_mixed_cpu_gpu_job() {
    let w = World::start(false).await;
    let m = w.agent(1).await;
    let first = w.queue(2).await;
    let mixed = w.queue_with(
        json!({"roles": [
            {"role": "trainer", "replicas": 1, "gpu_count": 1, "cpu_millis": 100, "mem_bytes": 1024},
            {"role": "service", "replicas": 1, "gpu_count": 0, "cpu_millis": 100, "mem_bytes": 1024}
        ]}),
        None,
    ).await;
    w.running(mixed).await;
    assert_eq!(w.job(first).await.state, "QUEUED");
    let launches = m.script.launches();
    let dir = tempfile::tempdir().unwrap();
    let spec =
        mrl_agent::attempts::spec::AttemptSpec::from_launch(&launches[0], &[], dir.path()).unwrap();
    assert_eq!(
        spec.roles
            .iter()
            .find(|r| r.kind == "trainer")
            .unwrap()
            .gpus,
        [0]
    );
    assert!(spec
        .roles
        .iter()
        .find(|r| r.kind == "service")
        .unwrap()
        .gpus
        .is_empty());
    let audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_events WHERE actor = 'system:scheduler'
           AND target = $1 AND action IN ('scheduler.place','scheduler.attempt_status')",
    )
    .bind(JobId::from_uuid(mixed).to_string())
    .fetch_one(w.db.pool())
    .await
    .unwrap();
    assert!(audits >= 2);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn queue_does_not_count_hidden_jobs() {
    let mut w = World::start(false).await;
    let visible_project = w.project;
    w.project = HIDDEN_PROJECT;
    let hidden = w.queue(9).await;
    w.project = visible_project;
    let visible = w.queue(9).await;
    assert_ne!(hidden, visible);
    let q = w
        .get(&format!(
            "/api/v1/queue?pool={}&limit=1",
            PoolId::from_uuid(w.pool)
        ))
        .await;
    assert_eq!(q.status, 200, "{}", q.text());
    let q = q.json();
    assert_eq!(q["total"], 1);
    assert_eq!(
        q["jobs"][0]["job_id"],
        JobId::from_uuid(visible).to_string()
    );
    assert_eq!(q["jobs"][0]["position"], 1);
}

struct DelayedCommand {
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    handler: Handler,
}

#[async_trait::async_trait]
impl CommandHandler for DelayedCommand {
    async fn handle(&self, command: Command) -> Result<(), Failure> {
        self.entered.notify_one();
        self.resume.notified().await;
        self.handler.handle(command).await
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn delayed_launch_keeps_its_gpu_after_a_cancel_refusal() {
    let w = World::start(false).await;
    let m = w.agent(1).await;
    let launch = Arc::new(DelayedCommand {
        entered: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
        handler: Handler(m.script.clone()),
    });
    m.script.link.register("LaunchAttempt", launch.clone());
    let job = w.queue(1).await;
    tokio::time::timeout(WAIT, launch.entered.notified())
        .await
        .unwrap();
    let attempt = w.job(job).await.current_attempt_id.unwrap();
    w.until("the launch has reached the agent", || async {
        w.commands(attempt, "LaunchAttempt").await[0].0 == "RECEIVED"
    })
    .await;
    let mut tx = w.db.db().begin().await.unwrap();
    let row = JobTransitions::lock(&mut tx, w.org, job)
        .await
        .unwrap()
        .unwrap();
    JobTransitions::request_cancel(&mut tx, &row, "tester")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    w.until("the first cancel was refused", || async {
        w.commands(attempt, "CancelAttempt")
            .await
            .iter()
            .any(|c| c.0 == "FAILED")
    })
    .await;
    let next = w.queue(1).await;
    w.until("the second job waits for the held GPU", || async {
        w.job(next).await.state_reason.is_some()
    })
    .await;
    assert_eq!(w.job(job).await.state, "CANCELLING");
    assert_eq!(w.job(next).await.state, "QUEUED");
    assert_eq!(w.reservations(job).await[0].0, "RELEASING");
    m.script
        .link
        .register("LaunchAttempt", Arc::new(Handler(m.script.clone())));
    launch.resume.notify_one();
    w.wait_state(job, "CANCELLED").await;
    w.running(next).await;
    w.invariants().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn launch_waits_for_the_staging_result() {
    let w = World::start(false).await;
    let m = w.agent(1).await;
    let stage = Arc::new(DelayedCommand {
        entered: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
        handler: Handler(m.script.clone()),
    });
    m.script.link.register("StageArtifacts", stage.clone());
    let job = w.queue_with(json!({
        "stage_fixture": true,
        "roles": [{"role": "trainer", "replicas": 1, "gpu_count": 1, "cpu_millis": 100, "mem_bytes": 1024}]
    }), None).await;
    tokio::time::timeout(WAIT, stage.entered.notified())
        .await
        .unwrap();
    let attempt = w.job(job).await.current_attempt_id.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(w.commands(attempt, "LaunchAttempt").await.is_empty());
    stage.resume.notify_one();
    w.running(job).await;
    assert_eq!(w.commands(attempt, "LaunchAttempt").await.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_completed_stage_past_its_deadline_cannot_launch() {
    let w = World::start(false).await;
    let m = w.agent(1).await;
    let stage = Arc::new(DelayedCommand {
        entered: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
        handler: Handler(m.script.clone()),
    });
    m.script.link.register("StageArtifacts", stage.clone());
    let job = w.queue_with(json!({
        "stage_fixture": true,
        "roles": [{"role": "trainer", "replicas": 1, "gpu_count": 1, "cpu_millis": 100, "mem_bytes": 1024}]
    }), None).await;
    tokio::time::timeout(WAIT, stage.entered.notified())
        .await
        .unwrap();
    let attempt = w.job(job).await.current_attempt_id.unwrap();
    sqlx::query("UPDATE agent_commands SET state = 'COMPLETED', result = jsonb_build_object('accepted', true), deadline_at = now() - interval '1 second', not_after = now() - interval '1 second' WHERE attempt_id = $1 AND kind = 'StageArtifacts'")
        .bind(attempt).execute(w.db.pool()).await.unwrap();
    w.wait_attempt(attempt, "KILLED").await;
    assert!(w.commands(attempt, "LaunchAttempt").await.is_empty());
    stage.resume.notify_one();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn reconcile_frees_a_lost_cancelling_attempt() {
    let w = World::start(false).await;
    let mut m = w.agent(1).await;
    let job = w.queue(1).await;
    let attempt = w.running(job).await;
    m.disconnect().await;
    let mut tx = w.db.db().begin().await.unwrap();
    let row = JobTransitions::lock(&mut tx, w.org, job)
        .await
        .unwrap()
        .unwrap();
    JobTransitions::request_cancel(&mut tx, &row, "tester")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    w.until("the reservation begins releasing", || async {
        w.reservations(job).await[0].0 == "RELEASING"
    })
    .await;
    w.wait_attempt(attempt, "LOST").await;
    m.script.running.lock().unwrap().clear();
    m.script.publish();
    m.script.report();
    m.reconnect();
    w.until("the reconciled reservation was released", || async {
        w.reservations(job).await[0].0 == "RELEASED"
    })
    .await;
    w.wait_state(job, "CANCELLED").await;
    w.invariants().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL and uv (just test-integration)"]
async fn real_worker_survives_readoption_and_a_kill_retries() {
    use mrl_agent::attempts::record::{AttemptRecord, ProcIdentity};
    use mrl_agent::attempts::service::AttemptsService;
    use mrl_agent::registry::AgentService;
    use tokio_util::sync::CancellationToken;

    if mrl_agent::envs::which("uv").is_none() {
        assert!(
            std::env::var_os("MRL_TEST_ATTEMPTS_REQUIRED").is_none(),
            "uv is required"
        );
        return;
    }
    let w = World::start(false).await;
    let m = w.agent(0).await;
    let ctx = &m.agent.ctx;
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let hash = mrl_storage::sha256_hex(&std::fs::read(repo.join("uv.lock")).unwrap());
    let runtimes = ctx.state.root().join("runtimes");
    std::fs::create_dir_all(&runtimes).unwrap();
    std::os::unix::fs::symlink(&repo, runtimes.join(format!("uv-sha256-{hash}"))).unwrap();

    let start_service = || {
        let stop = CancellationToken::new();
        let task = tokio::spawn({
            let ctx = ctx.clone();
            let stop = stop.clone();
            async move { AttemptsService::new().run(ctx, stop).await }
        });
        (stop, task)
    };
    let reconcile_seq = || async {
        sqlx::query_scalar::<_, i64>("SELECT seq FROM scheduler_node_reconciles WHERE node_id = $1")
            .bind(m.node)
            .fetch_optional(w.db.pool())
            .await
            .unwrap()
            .unwrap_or(0)
    };
    let (stop, service) = start_service();
    w.until("the real attempt runtime is ready", || async {
        reconcile_seq().await > 0
    })
    .await;
    let job = w.queue_with(json!({
        "runtime_fixture": format!("uv:sha256:{hash}"),
        "roles": [{"role": "trainer", "replicas": 1, "gpu_count": 0, "cpu_millis": 100, "mem_bytes": 1024}]
    }), None).await;
    let first = w.running(job).await;
    let dir = ctx.state.job_dir(&AttemptId::from_uuid(first).to_string());
    let worker = || {
        AttemptRecord::load(&dir).unwrap().unwrap().workers[0]
            .identity
            .clone()
    };
    let identity = worker();
    stop.cancel();
    service.await.unwrap().unwrap();

    let before = reconcile_seq().await;
    let (stop, service) = start_service();
    w.until("the restarted attempt runtime reconciled", || async {
        reconcile_seq().await > before
    })
    .await;
    assert_eq!(worker(), identity);
    assert_eq!(w.job(job).await.current_attempt_id, Some(first));
    assert_eq!(w.attempt_state(first).await, "RUNNING");
    let ProcIdentity::Pid { pid, .. } = identity else {
        panic!("expected a process worker")
    };
    assert!(std::process::Command::new("kill")
        .args(["-KILL", &pid.to_string()])
        .status()
        .unwrap()
        .success());
    w.until("a fresh attempt runs after the worker kill", || async {
        let row = w.job(job).await;
        row.state == "RUNNING" && row.current_attempt_id != Some(first)
    })
    .await;
    let row = w.job(job).await;
    assert_eq!(row.epoch, 2);
    assert_eq!(row.retries_used["HARNESS_ERROR"], 1);
    assert_eq!(w.attempt_state(first).await, "EXITED_ERROR");

    let mut tx = w.db.db().begin().await.unwrap();
    let row = JobTransitions::lock(&mut tx, w.org, job)
        .await
        .unwrap()
        .unwrap();
    JobTransitions::request_cancel(&mut tx, &row, "tester")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    w.wait_state(job, "CANCELLED").await;
    stop.cancel();
    service.await.unwrap().unwrap();
    w.invariants().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn the_reaper_keeps_a_hold_renewed_while_it_waits_for_the_lock() {
    let w = World::start(false).await;
    let _m = w.agent(1).await;
    let req = ReservationRequest {
        request_id: Uuid::now_v7(),
        owner: ReservationOwner::Deployment(mrl_domain::ids::DeploymentId::new()),
        org_id: w.org,
        pool_id: w.pool,
        demand: Demand::from_request(&json!({"roles": [{
            "role": "service", "replicas": 1, "gpu_count": 1, "cpu_millis": 100, "mem_bytes": 1024
        }]}))
        .unwrap(),
        epoch: 1,
        lease: Some(Duration::from_secs(600)),
    };
    assert!(matches!(
        w.sched.reserve(&req).await.unwrap(),
        Reserved::Held(_)
    ));
    sqlx::query("UPDATE resource_reservations SET lease_expires_at = clock_timestamp() + interval '2 seconds' WHERE id = $1")
        .bind(req.request_id).execute(w.db.pool()).await.unwrap();
    let mut tx = w.db.db().begin().await.unwrap();
    sqlx::query("SELECT id FROM resource_reservations WHERE id = $1 FOR UPDATE")
        .bind(req.request_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    let reaper_waiting = || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
             WHERE datname = current_database() AND wait_event_type = 'Lock'
               AND query LIKE '%resource_reservations WHERE id = $1%' AND query LIKE '%FOR UPDATE%')",
        )
        .fetch_one(w.db.pool()).await.unwrap()
    };
    w.until("the reaper sees the expired hold and waits", reaper_waiting)
        .await;
    sqlx::query("UPDATE resource_reservations SET lease_expires_at = clock_timestamp() + interval '5 minutes' WHERE id = $1")
        .bind(req.request_id).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    w.until("the reaper releases the row lock", || async {
        !reaper_waiting().await
    })
    .await;
    assert!(matches!(w.sched.reserve(&req).await.unwrap(), Reserved::Held(r) if r.state == "HELD"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn launch_carries_download_tickets_and_granted_secrets() {
    use mrl_proto::agent::v1::{StageArtifacts, UpdateCredentials};
    use prost::Message;
    let w = World::start_for_org(false, Uuid::nil()).await;
    let m = w.agent(1).await;
    let bytes = b"fake weights";
    let sha = sha256_hex(bytes);
    let upload = w
        .post(
            "/api/v1/uploads",
            json!({
                "filename": "weights.bin", "sha256": sha, "size": bytes.len()
            }),
        )
        .await;
    assert_eq!(upload.status, 201, "{}", upload.text());
    let up = upload.json()["id"].as_str().unwrap().to_owned();
    let headers = [
        (LAUNCH_SECRET_HEADER, TEST_SECRET),
        ("content-type", "application/json"),
    ];
    let part = w
        .server()
        .request(
            Method::PUT,
            &format!("/api/v1/uploads/{up}/parts/0"),
            &headers,
            Body::from(bytes.as_slice()),
        )
        .await;
    assert_eq!(part.status, 200, "{}", part.text());
    let complete = w
        .post(&format!("/api/v1/uploads/{up}:complete"), json!({}))
        .await;
    assert_eq!(complete.status, 200, "{}", complete.text());
    let artifact = w.post("/api/v1/artifacts", json!({
        "name": "launch-model",
        "manifest": {"schema_version": "artifact-manifest/v1", "kind": "model",
            "created_at": "2026-11-16T00:00:00Z",
            "files": [{"path": "weights.bin", "size": bytes.len(), "sha256": sha, "role": "weights"}]},
        "uploads": {"weights.bin": up},
    })).await;
    assert_eq!(artifact.status, 201, "{}", artifact.text());
    let artifact = artifact.json()["id"].as_str().unwrap().to_owned();
    let secret = w
        .post(
            "/api/v1/secrets",
            json!({
                "name": "hf_hub", "kind": "hf_token", "allowed_uses": ["run"]
            }),
        )
        .await;
    assert_eq!(secret.status, 201, "{}", secret.text());
    let secret = secret.json()["id"].as_str().unwrap().to_owned();
    let value = w
        .server()
        .request(
            Method::PUT,
            &format!("/api/v1/secrets/{secret}/value"),
            &headers,
            Body::from(r#"{"value":"canary-7f3a"}"#),
        )
        .await;
    assert!(matches!(value.status, 200 | 204), "{}", value.text());
    let project = mrl_domain::ids::ProjectId::from_uuid(w.project);
    let body = json!({
        "name": "with-inputs", "algorithm_id": "sft",
        "base_model_version_id": mrl_domain::ids::ModelVersionId::new().to_string(),
        "artifact_inputs": [{"artifact_id": artifact, "path": "weights.bin", "name": "model.bin"}],
        "secret_ids": [secret],
        "resources": {"roles": [{"role": "trainer", "replicas": 1, "gpu_count": 1,
            "cpu_millis": 100, "mem_bytes": 1024}]},
    });
    let run = w
        .server()
        .request(
            Method::POST,
            &format!("/api/v1/projects/{project}/runs"),
            &[
                (LAUNCH_SECRET_HEADER, TEST_SECRET),
                ("content-type", "application/json"),
                ("idempotency-key", "submitted-with-artifact-and-grant"),
            ],
            Body::from(body.to_string()),
        )
        .await;
    assert_eq!(run.status, 201, "{}", run.text());
    let job = JobId::from_str(run.json()["job_id"].as_str().unwrap())
        .unwrap()
        .as_uuid();
    let attempt = w.running(job).await;

    // The staged input carries a ticket the transfer endpoint honours.
    let stage = m
        .script
        .seen
        .lock()
        .unwrap()
        .iter()
        .find_map(|c| match &c.kind {
            Some(Kind::StageArtifacts(s)) => Some(s.clone()),
            _ => None,
        })
        .unwrap_or_else(StageArtifacts::default);
    let item = &stage.items[0];
    assert_eq!(
        (
            item.name.as_str(),
            item.sha256.as_str(),
            item.size,
            item.local_path.as_str()
        ),
        ("model.bin", sha.as_str(), bytes.len() as u64, "model.bin")
    );
    let dir = tempfile::tempdir().unwrap();
    let secret_file = dir.path().join("launch-secret");
    std::fs::write(&secret_file, TEST_SECRET).unwrap();
    let agent_input = mrl_agent::attempts::spec::AttemptSpec::from_launch(
        &m.script.launches()[0],
        &stage.items,
        dir.path(),
    )
    .unwrap();
    let ticket = agent_input.inputs[0].ticket().unwrap();
    let client = mrl_agent::transfer::TransferClient::new(&mrl_agent::config::TransferConfig {
        server_url: Some(format!("http://{}", w.server().api())),
        server_launch_secret_file: Some(secret_file),
        ..Default::default()
    })
    .unwrap();
    let dest = dir.path().join("downloaded-model.bin");
    client.download(&ticket, &dest, Some(&sha)).await.unwrap();
    assert_eq!(std::fs::read(dest).unwrap(), bytes);

    // The granted secret reached the agent, before the launch.
    let seen = m.script.seen.lock().unwrap().clone();
    let creds = seen
        .iter()
        .position(|c| matches!(c.kind, Some(Kind::UpdateCredentials(_))))
        .unwrap();
    let launch = seen
        .iter()
        .position(|c| matches!(c.kind, Some(Kind::LaunchAttempt(_))))
        .unwrap();
    assert!(creds < launch);
    let Some(Kind::UpdateCredentials(UpdateCredentials { credentials, .. })) =
        seen[creds].kind.clone()
    else {
        unreachable!()
    };
    assert_eq!(credentials.len(), 1);
    assert_eq!(credentials[0].name, "hf_hub");
    assert_eq!(credentials[0].value, b"canary-7f3a");

    // Once delivered, the value is gone from the database.
    w.until("the delivered credentials are scrubbed", || async {
        w.commands(attempt, "UpdateCredentials")
            .await
            .iter()
            .all(|(state, payload)| state == "COMPLETED" && payload.is_empty())
    })
    .await;
    let leaked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent_commands WHERE position('canary-7f3a'::bytea IN payload) > 0",
    )
    .fetch_one(w.db.pool())
    .await
    .unwrap();
    assert_eq!(leaked, 0);
    assert!(mrl_proto::agent::v1::Command::decode(&[][..]).is_ok());

    let audited: Vec<String> = sqlx::query_scalar(
        "SELECT action FROM audit_events
         WHERE action IN ('transfer.ticket_issued', 'scheduler.credentials_sent') ORDER BY action",
    )
    .fetch_all(w.db.pool())
    .await
    .unwrap();
    assert_eq!(
        audited,
        ["scheduler.credentials_sent", "transfer.ticket_issued"]
    );
    w.invariants().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_missing_secret_value_fails_the_launch_as_user_config() {
    let w = World::start(false).await;
    let _m = w.agent(1).await;
    let run = w.run().await;
    let secret = Uuid::now_v7();
    let org = mrl_domain::ids::OrgId::from_uuid(w.org).to_string();
    sqlx::query(
        "INSERT INTO secrets_metadata (id, org_id, name, backend, ref, kind, allowed_uses, created_by)
         VALUES ($1, $2, 'hf_hub', 'secret_store', 'mrl.secret.unset', 'hf_token', '{run}', 't')",
    )
    .bind(secret)
    .bind(&org)
    .execute(w.db.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO run_secret_grants (run_id, secret_id, mount) VALUES ($1, $2, 'env')")
        .bind(run)
        .bind(secret)
        .execute(w.db.pool())
        .await
        .unwrap();
    let job = w
        .queue_with(
            json!({"roles": [{"role": "trainer", "replicas": 1, "gpu_count": 1, "cpu_millis": 100, "mem_bytes": 1024}]}),
            Some(run),
        )
        .await;
    let row = w.wait_state(job, "FAILED").await;
    assert_eq!(row.failure_class.as_deref(), Some("USER_CONFIG"));
    let attempt = row.current_attempt_id.unwrap();
    assert!(w.commands(attempt, "LaunchAttempt").await.is_empty());
    assert!(w.commands(attempt, "UpdateCredentials").await.is_empty());
    assert!(w.reservations(job).await.iter().all(|r| r.0 == "RELEASED"));
    w.invariants().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn queue_hides_a_foreign_pool() {
    use crate::supabase::core::{Auth, CoreServer};
    use crate::supabase::{MockSupabase, MockUser};
    use mrl_domain::ids::{OrgId, UserId};
    use mrl_domain::rbac::Role;
    use mrl_server::modules::tenancy::{self, Scope};

    let sb = MockSupabase::start().await;
    let core = CoreServer::start(Mode::Team, &sb, TestDb::new().await, Arc::default()).await;
    let org = OrgId::new();
    sqlx::query(
        "INSERT INTO organizations (id, slug, name, kind) VALUES ($1, 'home', 'Home', 'team')",
    )
    .bind(org.as_uuid())
    .execute(core.db.pool())
    .await
    .unwrap();
    let user = MockUser::new("queue@example.com", "Queue");
    let auth = Auth::Bearer(sb.access_token(&user, 600));
    let me = core.call(Method::GET, "/api/v1/auth/me", &auth, None).await;
    assert_eq!(me.status, 200, "{}", me.text());
    let user_id: Uuid = sqlx::query_scalar("SELECT id FROM users WHERE supabase_user_id = $1")
        .bind(user.id)
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    let mut tx = core.db.db().begin().await.unwrap();
    tenancy::bind_role(
        &mut tx,
        org,
        UserId::from_uuid(user_id),
        Role::Owner,
        Scope::Org,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let pool = mrl_server::modules::nodes::store::create_pool(
        core.db.db(),
        Some(OrgId::new().as_uuid()),
        "foreign-queue",
        "agent",
        90,
        &[],
    )
    .await
    .unwrap()
    .unwrap();
    let response = core
        .call(
            Method::GET,
            &format!("/api/v1/queue?pool={}", pool.id),
            &auth,
            None,
        )
        .await;
    assert_eq!(response.status, 404, "{}", response.text());
    core.stop().await;
}

async fn seed_grant(w: &World, run: Uuid, name: &str, mount: &str) {
    let org = mrl_domain::ids::OrgId::from_uuid(w.org).to_string();
    let secret = Uuid::now_v7();
    let reference = format!("mrl.secret.{secret}");
    sqlx::query(
        "INSERT INTO secrets_metadata (id, org_id, name, backend, ref, kind, allowed_uses, created_by)
         VALUES ($1, $2, $3, 'secret_store', $4, 'hf_token', '{run}', 't')",
    )
    .bind(secret).bind(&org).bind(name).bind(&reference)
    .execute(w.db.pool()).await.unwrap();
    sqlx::query("INSERT INTO run_secret_grants (run_id, secret_id, mount) VALUES ($1, $2, $3)")
        .bind(run)
        .bind(secret)
        .bind(mount)
        .execute(w.db.pool())
        .await
        .unwrap();
    w.secrets
        .set(
            &reference,
            mrl_server::seams::Secret::new("canary-value".to_owned()),
        )
        .await
        .unwrap();
}

async fn queue_granted_run(w: &World, run: Uuid) -> Uuid {
    w.queue_with(
        json!({"roles": [{
            "role": "trainer", "replicas": 1, "gpu_count": 1, "cpu_millis": 100, "mem_bytes": 1024
        }]}),
        Some(run),
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn received_credentials_are_scrubbed_without_a_result() {
    let w = World::start(false).await;
    let m = w.agent(1).await;
    let delayed = Arc::new(DelayedCommand {
        entered: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
        handler: Handler(m.script.clone()),
    });
    m.script.link.register("UpdateCredentials", delayed.clone());
    let run = w.run().await;
    seed_grant(&w, run, "hf_hub", "env").await;
    let job = queue_granted_run(&w, run).await;
    tokio::time::timeout(WAIT, delayed.entered.notified())
        .await
        .unwrap();
    let attempt = w.job(job).await.current_attempt_id.unwrap();
    w.until("the received credential payload is scrubbed", || async {
        w.commands(attempt, "UpdateCredentials").await == vec![("RECEIVED".to_owned(), Vec::new())]
    })
    .await;
    assert!(w.commands(attempt, "LaunchAttempt").await.is_empty());
    delayed.resume.notify_one();
}

struct LoseCredentialsOnce(Handler, std::sync::atomic::AtomicBool);

#[async_trait::async_trait]
impl CommandHandler for LoseCredentialsOnce {
    async fn handle(&self, command: Command) -> Result<(), Failure> {
        if !self.1.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err(mrl_agent::attempts::AttemptError::CredentialsLost.to_failure());
        }
        self.0.handle(command).await
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn lost_credentials_retry_without_quarantining_the_gpu() {
    let w = World::start(false).await;
    let m = w.agent(1).await;
    m.script.link.register(
        "LaunchAttempt",
        Arc::new(LoseCredentialsOnce(
            Handler(m.script.clone()),
            std::sync::atomic::AtomicBool::new(false),
        )),
    );
    let run = w.run().await;
    seed_grant(&w, run, "hf_hub", "env").await;
    let job = queue_granted_run(&w, run).await;
    let next = w.running(job).await;
    assert_eq!(w.job(job).await.epoch, 2);
    assert_eq!(w.commands(next, "LaunchAttempt").await.len(), 1);
    assert_eq!(w.commands(next, "UpdateCredentials").await.len(), 1);
    let reservations = w.reservations(job).await;
    assert_eq!(reservations.len(), 2);
    assert!(reservations.iter().any(|row| row.0 == "RELEASED"));
    w.invariants().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn unsupported_and_aliased_credentials_never_reach_the_agent() {
    for grants in [
        vec![("file-token", "file")],
        vec![("hf-token", "env"), ("hf_token", "env")],
    ] {
        let w = World::start(false).await;
        let _m = w.agent(1).await;
        let run = w.run().await;
        for (name, mount) in grants {
            seed_grant(&w, run, name, mount).await;
        }
        let job = queue_granted_run(&w, run).await;
        let row = w.wait_state(job, "FAILED").await;
        assert_eq!(row.failure_class.as_deref(), Some("USER_CONFIG"));
        let attempt = row.current_attempt_id.unwrap();
        assert!(w.commands(attempt, "UpdateCredentials").await.is_empty());
        assert!(w.commands(attempt, "LaunchAttempt").await.is_empty());
    }
}

fn sha256_hex(b: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_high_priority_job_queued_mid_scan_does_not_wait_for_the_scan_to_wrap() {
    let w = World::start(false).await;
    let _m = w.agent(1).await;
    // 30 jobs that never fit (2 GPUs on a 1-GPU machine); one job is looked at per tick.
    let never = json!({"roles": [{"role": "trainer", "replicas": 1, "gpu_count": 2,
        "cpu_millis": 100, "mem_bytes": 1024}]});
    let mut low = Vec::new();
    for _ in 0..30 {
        low.push(w.queue_with(never.clone(), None).await);
    }
    w.until("the scan is past the first queued jobs", || async {
        w.job(low[2]).await.state_reason.is_some()
    })
    .await;
    let high = w.queue(1).await;
    sqlx::query("UPDATE jobs SET priority = 10 WHERE id = $1")
        .bind(high)
        .execute(w.db.pool())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), w.running(high))
        .await
        .expect("the new high-priority job waited for the whole queue scan");
    assert!(
        w.job(low[29]).await.state_reason.is_none(),
        "the scan wrapped first"
    );
    w.invariants().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_staged_item_without_an_expiry_is_not_treated_as_expired() {
    let w = World::start(false).await;
    let _m = w.agent(1).await;
    // The scripted agent takes it; a real agent refuses the launch with its own reason.
    let job = w
        .queue_with(
            json!({"stage_without_expiry": true, "roles": [{"role": "trainer", "replicas": 1,
                "gpu_count": 1, "cpu_millis": 100, "mem_bytes": 1024}]}),
            None,
        )
        .await;
    let attempt = w.running(job).await;
    assert_eq!(w.commands(attempt, "StageArtifacts").await.len(), 1);
    assert_eq!(w.commands(attempt, "LaunchAttempt").await.len(), 1);
    w.invariants().await;
}

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
use mrl_server::modules::compute::LocalAgentPool;
use mrl_server::modules::jobs::{
    Chains, CompletionVerifier, JobChange, JobRow, JobTransitions, NewJob, Verification,
};
use mrl_server::modules::nodes::lanes::LaneRouter;
use mrl_server::modules::nodes::{Gateway, NodesModule};
use mrl_server::modules::scheduler::admission::{MaxQueuedPerProject, PoolFlags, PoolMembership};
use mrl_server::modules::scheduler::launch::RunConfigSpec;
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
        _job: &JobRow,
        _attempt: &mrl_server::modules::jobs::AttemptRow,
        _placement: &Placement,
        plan: &mut LaunchPlan,
    ) -> Result<(), LaunchError> {
        plan.launch.engine_name = "fake".into();
        for r in &mut plan.launch.roles {
            r.runtime_ref = RUNTIME.into();
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

/// A server with the gateway and the scheduler on one test database.
struct World {
    server: Option<TestServer>,
    gw: Arc<Gateway>,
    sched: Scheduler,
    db: TestDb,
    secrets: Arc<dyn SecretStore>,
    pool: Uuid,
    pause: bool,
    org: Uuid,
    project: Uuid,
}

async fn boot(
    db: &TestDb,
    secrets: Arc<dyn SecretStore>,
    port: u16,
    pause: bool,
) -> (TestServer, Arc<Gateway>, Scheduler) {
    let cfg = server_config(fast_config(), Mode::Solo, port);
    // A fresh router per boot: a restarted server never delivers to the old module's sink.
    let lanes = LaneRouter::new();
    let nodes = NodesModule::new(db.db().clone(), &cfg, secrets, lanes.clone());
    let gw = nodes.gateway();
    let chains = Chains::new(vec![], vec![Arc::new(VerifyAll)], vec![]);
    let module = SchedulerModule::with_parts(
        db.db().clone(),
        sched_config(),
        &gw,
        &lanes,
        chains,
        pipeline(db.db(), pause),
    );
    let sched = module.scheduler();
    let seams = crate::seams::build(crate::seams::solo(TEST_SECRET), Mode::Solo);
    let modules: Vec<Box<dyn mrl_server::app::ServerModule>> =
        vec![Box::new(nodes), Box::new(module)];
    match TestServer::start(cfg, seams, modules).await {
        Ok(s) => (s, gw, sched),
        Err(e) => panic!("server did not start: {}", e.explain()),
    }
}

impl World {
    async fn start(pause: bool) -> Self {
        let db = TestDb::new().await;
        let secrets: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        let (server, gw, sched) = boot(&db, secrets.clone(), 0, pause).await;
        let mut w = Self {
            server: Some(server),
            gw,
            sched,
            db,
            secrets,
            pool: Uuid::nil(),
            pause,
            org: Uuid::now_v7(),
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
        let (server, gw, sched) = boot(&self.db, self.secrets.clone(), port, self.pause).await;
        self.server = Some(server);
        self.gw = gw;
        self.sched = sched;
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
    let w = World::start(false).await;
    let mut m = w.agent(1).await;
    let job = w.queue(1).await;
    let attempt = w.running(job).await;
    m.disconnect().await;
    // As if the scheduler had been down for an hour: no grant since, and the lease ran out.
    sqlx::query(
        "WITH t AS (UPDATE scheduler_ticks SET last_tick_at = now() - interval '1 hour'),
              g AS (UPDATE agent_commands SET updated_at = now() - interval '1 minute'
                    WHERE attempt_id = $1 AND kind = 'LaunchAttempt')
         UPDATE job_attempts SET lease_expires_at = now() - interval '1 minute' WHERE id = $1",
    )
    .bind(attempt)
    .execute(w.db.pool())
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(
        w.attempt_state(attempt).await,
        "RUNNING",
        "the gap extended the lease"
    );
    w.wait_attempt(attempt, "LOST").await;
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
    // Both GPUs are held: a job now waits, and a second deployment is busy.
    assert!(matches!(
        w.sched.reserve(&req).await.unwrap(),
        Reserved::Busy(_)
    ));
    let job = w.queue(1).await;
    w.until("the job waits", || async {
        w.job(job).await.state_reason.is_some()
    })
    .await;
    assert_eq!(w.job(job).await.state, "QUEUED");
    w.invariants().await;
    // An older epoch can't touch it.
    let stale = w.sched.release(held.id, 6, "test").await.unwrap_err();
    assert!(stale.to_string().contains("MRL-CONFLICT-210"), "{stale}");
    w.sched
        .renew(held.id, 7, Duration::from_secs(60))
        .await
        .unwrap();
    let gone = w.sched.release(held.id, 7, "done").await.unwrap();
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

//! P22 integration tests: lane sinks → history, logs, samples, comparison, streams and alerts,
//! over real HTTP on a test database.
//!
//! Compiled by `mrl-testkit` (its `lib.rs`, P22 marker), because they need `TestServer` and
//! `TestDb`. `#[ignore]`d; run them with `just dev && just test-integration`. Worker messages
//! enter through the sinks registered on a [`LaneRouter`], as the gateway hands them over.

use crate::pg::TestDb;
use crate::seams::{self as ts, user};
use crate::server::{TestResponse, TestServer, TEST_SECRET};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::Method;
use mrl_domain::ids::{AttemptId, NodeId, OrgId, RunId};
use mrl_proto::agent::v1::{agent_message::Body as AgentBody, AgentMessage, Lane, WorkerRelay};
use mrl_proto::common::v1::AttemptRef;
use mrl_proto::worker::v1::worker_message::Body as WorkerBody;
use mrl_proto::worker::v1::{
    LogChunk, LogStream, MetricPoint, MetricsBatch, Progress, Sample, SampleBatch, WorkerMessage,
};
use mrl_server::config::{Config, Mode, Role};
use mrl_server::modules::artifacts::ArtifactsModule;
use mrl_server::modules::nodes::lanes::{LaneItem, LaneKind, LaneRouter};
use mrl_server::modules::telemetry::{
    rollup, IngestedBatch, LiveConfig, MetricObserver, MetricObservers, Telemetry, TelemetryModule,
};
use mrl_server::seams::{
    Authorizer, Decision, PrincipalKind, RequestContext, Resource, SeamError, LAUNCH_SECRET_HEADER,
};
use mrl_storage::BackendConfig;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

/// Who sends a request.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Who {
    /// Solo owner (launch secret).
    Solo,
    /// Team user in org A.
    A,
    /// Team user in org B.
    B,
}

/// Allows the solo owner everything and team users their own org only (others: Hide).
struct OrgRules;

#[async_trait]
impl Authorizer for OrgRules {
    async fn authorize(
        &self,
        ctx: &RequestContext,
        _action: &str,
        resource: &Resource,
    ) -> Result<Decision, SeamError> {
        if ctx.principal.kind == PrincipalKind::LocalOwner || resource.org == ctx.org {
            Ok(Decision::Allow)
        } else {
            Ok(Decision::Hide)
        }
    }
}

/// A test server with the P22 modules on its own database and object folder.
pub(crate) struct World {
    pub db: TestDb,
    pub srv: TestServer,
    pub tel: Arc<Telemetry>,
    pub lanes: Arc<LaneRouter>,
    pub observers: Arc<MetricObservers>,
    /// Org of `Who::A` and `Who::B` (team mode).
    pub orgs: [Uuid; 2],
    _dir: tempfile::TempDir,
}

/// Fast settings for tests.
pub(crate) fn fast_live() -> LiveConfig {
    LiveConfig {
        maintenance_interval_seconds: 3600,
        stream_heartbeat_seconds: 1,
        ..LiveConfig::default()
    }
}

impl World {
    /// A solo server.
    pub async fn solo() -> Self {
        Self::start(Mode::Solo, fast_live()).await
    }

    /// A team server with two orgs.
    pub async fn team() -> Self {
        Self::start(Mode::Team, fast_live()).await
    }

    /// A server in `mode` with `live` settings.
    pub async fn start(mode: Mode, live: LiveConfig) -> Self {
        let db = TestDb::new().await;
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
        let store = ArtifactsModule::with_backend(
            db.db().clone(),
            BackendConfig::Local {
                root: dir.path().join("objects"),
            },
            mode,
            dir.path().join("scratch"),
        )
        .store();
        let lanes = LaneRouter::new();
        let observers = MetricObservers::new();
        let module = TelemetryModule::with_parts(
            db.db().clone(),
            live.clone(),
            store,
            &lanes,
            observers.clone(),
        );
        let tel = module.telemetry();
        let orgs = [OrgId::new(), OrgId::new()];
        let seams = match mode {
            Mode::Solo => ts::build(ts::solo(TEST_SECRET), Mode::Solo),
            Mode::Team => ts::build(
                ts::team(&[
                    ("tok-a", user("usr-a", &orgs[0].to_string())),
                    ("tok-b", user("usr-b", &orgs[1].to_string())),
                ])
                .authorizer(OrgRules),
                Mode::Team,
            ),
        };
        let mut c = Config {
            live: live.clone(),
            ..Config::default()
        };
        c.server.mode = mode;
        // Tests run maintenance passes themselves.
        c.server.roles = vec![Role::Api];
        let any = "127.0.0.1:0".parse().unwrap_or_else(|e| panic!("{e}"));
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
        let modules: Vec<Box<dyn mrl_server::app::ServerModule>> = vec![Box::new(module)];
        let srv = TestServer::start(c, seams, modules)
            .await
            .unwrap_or_else(|e| panic!("test server did not start: {}", e.explain()));
        Self {
            db,
            srv,
            tel,
            lanes,
            observers,
            orgs: orgs.map(|o| o.as_uuid()),
            _dir: dir,
        }
    }

    /// The org rows of `who` are stored under.
    pub fn org(&self, who: Who) -> Uuid {
        match who {
            Who::Solo => Uuid::nil(),
            Who::A => self.orgs[0],
            Who::B => self.orgs[1],
        }
    }

    /// Sign-in headers of `who`.
    pub fn auth(who: Who) -> (&'static str, &'static str) {
        match who {
            Who::Solo => (LAUNCH_SECRET_HEADER, TEST_SECRET),
            Who::A => ("authorization", "Bearer tok-a"),
            Who::B => ("authorization", "Bearer tok-b"),
        }
    }

    /// GET as `who`.
    pub async fn get(&self, who: Who, path: &str) -> TestResponse {
        self.srv
            .request(Method::GET, path, &[Self::auth(who)], Body::empty())
            .await
    }

    /// POST JSON as `who`.
    pub async fn post(&self, who: Who, path: &str, body: Value) -> TestResponse {
        self.srv
            .request(
                Method::POST,
                path,
                &[Self::auth(who), ("content-type", "application/json")],
                Body::from(body.to_string()),
            )
            .await
    }

    /// Hands `msg` (from `node`, stream seq `seq`) to the sink of `lane`.
    pub async fn deliver(
        &self,
        node: Uuid,
        lane: LaneKind,
        seq: u64,
        dropped: u64,
        msg: AgentMessage,
    ) {
        let sink = match lane {
            LaneKind::Control => self
                .lanes
                .control_sink(mrl_server::modules::nodes::lanes::control_kind(&msg)),
            other => self.lanes.sink_for(other),
        }
        .unwrap_or_else(|| panic!("no sink for {lane:?}"));
        let item = LaneItem {
            node_id: node,
            session_epoch: 1,
            lane,
            stream: match lane {
                LaneKind::Control => Lane::Control,
                _ => Lane::Bulk,
            },
            seq,
            dropped_before: dropped,
            message: msg,
        };
        sink.deliver(item)
            .await
            .unwrap_or_else(|e| panic!("sink failed: {e}"));
    }
}

/// One run with one running attempt placed on a node.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Seeded {
    pub org: Uuid,
    pub project: Uuid,
    pub run: Uuid,
    pub job: Uuid,
    pub attempt: Uuid,
    pub attempt_no: i32,
    pub node: Uuid,
}

impl Seeded {
    pub fn run_id(&self) -> String {
        RunId::from_uuid(self.run).to_string()
    }

    pub fn attempt_id(&self) -> String {
        AttemptId::from_uuid(self.attempt).to_string()
    }
}

/// Inserts a run (resolved config `config`) with one RUNNING attempt on a new node.
pub(crate) async fn seed_run(db: &TestDb, org: Uuid, config: Value) -> Seeded {
    let s = Seeded {
        org,
        project: Uuid::now_v7(),
        run: Uuid::now_v7(),
        job: Uuid::now_v7(),
        attempt: Uuid::now_v7(),
        attempt_no: 1,
        node: Uuid::now_v7(),
    };
    seed_run_in(db, s, config).await
}

/// [`seed_run`] in an existing project.
pub(crate) async fn seed_run_in(db: &TestDb, s: Seeded, config: Value) -> Seeded {
    let p = db.pool();
    let exp = Uuid::now_v7();
    let run_q = |q: &'static str| sqlx::query(q);
    run_q("INSERT INTO experiments (id, org_id, project_id, name, created_by) VALUES ($1, $2, $3, $4, 'usr-a')")
        .bind(exp).bind(s.org).bind(s.project).bind(format!("e-{exp}"))
        .execute(p).await.unwrap_or_else(|e| panic!("experiment: {e}"));
    run_q(
        "INSERT INTO runs (id, org_id, project_id, experiment_id, name, kind, algorithm_id,
                base_model_version_id, submit_key, submit_fingerprint, created_by)
         VALUES ($1, $2, $3, $4, 'demo', 'TRAIN', 'sft', $5, $6, '\\x00', 'usr-a')",
    )
    .bind(s.run)
    .bind(s.org)
    .bind(s.project)
    .bind(exp)
    .bind(Uuid::now_v7())
    .bind(s.run.to_string())
    .execute(p)
    .await
    .unwrap_or_else(|e| panic!("run: {e}"));
    run_q(
        "INSERT INTO run_configs (run_id, org_id, submitted, resolved, resolved_sha256, dataset_version_ids, seeds)
         VALUES ($1, $2, '{}', $3, decode(repeat('00', 32), 'hex'), '{}', '{}')",
    )
    .bind(s.run).bind(s.org).bind(&config)
    .execute(p).await.unwrap_or_else(|e| panic!("config: {e}"));
    run_q(
        "INSERT INTO jobs (id, org_id, project_id, kind, run_id, state, epoch, resource_request, retry_policy, created_by)
         VALUES ($1, $2, $3, 'TRAIN', $4, 'RUNNING', 1, '{}', '{}', 'usr-a')",
    )
    .bind(s.job).bind(s.org).bind(s.project).bind(s.run)
    .execute(p).await.unwrap_or_else(|e| panic!("job: {e}"));
    add_attempt(db, &s).await;
    s
}

async fn add_attempt(db: &TestDb, s: &Seeded) {
    let p = db.pool();
    sqlx::query(
        "INSERT INTO job_attempts (id, org_id, job_id, attempt_no, epoch, state)
         VALUES ($1, $2, $3, $4, $4, 'RUNNING')",
    )
    .bind(s.attempt)
    .bind(s.org)
    .bind(s.job)
    .bind(s.attempt_no)
    .execute(p)
    .await
    .unwrap_or_else(|e| panic!("attempt: {e}"));
    sqlx::query(
        "INSERT INTO attempt_placements (attempt_id, node_id, org_id, node_rank, rank_lo, rank_hi)
         VALUES ($1, $2, $3, 0, 0, 0)",
    )
    .bind(s.attempt)
    .bind(s.node)
    .bind(s.org)
    .execute(p)
    .await
    .unwrap_or_else(|e| panic!("placement: {e}"));
}

/// Ends the current attempt (LOST) and starts the next one on the same node.
pub(crate) async fn next_attempt(db: &TestDb, s: &Seeded) -> Seeded {
    sqlx::query("UPDATE job_attempts SET state = 'LOST' WHERE id = $1")
        .bind(s.attempt)
        .execute(db.pool())
        .await
        .unwrap_or_else(|e| panic!("lose: {e}"));
    let n = Seeded {
        attempt: Uuid::now_v7(),
        attempt_no: s.attempt_no + 1,
        ..*s
    };
    add_attempt(db, &n).await;
    n
}

/// A relayed worker message of `s`'s attempt (its fencing epoch = its attempt number).
pub(crate) fn relay(s: &Seeded, seq: u64, body: WorkerBody) -> AgentMessage {
    AgentMessage {
        body: Some(AgentBody::Worker(WorkerRelay {
            attempt: Some(AttemptRef {
                job_id: mrl_domain::ids::JobId::from_uuid(s.job).to_string(),
                attempt_id: s.attempt_id(),
                fencing_epoch: u64::try_from(s.attempt_no).unwrap_or(0),
            }),
            role_id: "trainer".into(),
            message: Some(WorkerMessage {
                seq,
                body: Some(body),
                ..WorkerMessage::default()
            }),
        })),
        ..AgentMessage::default()
    }
}

/// `name` points at `steps`, each `value(step)`.
pub(crate) fn points(
    name: &str,
    steps: std::ops::Range<u64>,
    value: impl Fn(u64) -> f64,
) -> Vec<MetricPoint> {
    steps
        .map(|step| MetricPoint {
            name: name.into(),
            step,
            value: value(step),
            split: "train".into(),
            ..MetricPoint::default()
        })
        .collect()
}

/// Sends a metrics batch as worker message `seq`.
pub(crate) async fn send_metrics(w: &World, s: &Seeded, seq: u64, pts: Vec<MetricPoint>) {
    let msg = relay(s, seq, WorkerBody::Metrics(MetricsBatch { points: pts }));
    w.deliver(s.node, LaneKind::Metrics, seq, 0, msg).await;
}

/// Sends a log chunk as worker message `seq`.
pub(crate) async fn send_log(
    w: &World,
    s: &Seeded,
    seq: u64,
    rank: u32,
    stream: LogStream,
    offset: u64,
    text: &[u8],
) {
    let msg = relay(
        s,
        seq,
        WorkerBody::Log(LogChunk {
            stream: stream as i32,
            data: text.to_vec(),
            rank,
            offset,
        }),
    );
    w.deliver(s.node, LaneKind::Logs, seq, 0, msg).await;
}

async fn count(db: &TestDb, sql: &'static str, id: Uuid) -> i64 {
    sqlx::query_scalar(sql)
        .bind(id)
        .fetch_one(db.pool())
        .await
        .unwrap_or_else(|e| panic!("count: {e}"))
}

/// Steps and values of one series in a query answer.
fn line(v: &Value) -> Vec<(i64, f64)> {
    v["points"]
        .as_array()
        .map(|p| {
            p.iter()
                .map(|p| {
                    (
                        p["x"].as_i64().unwrap_or(-1),
                        p["value"].as_f64().unwrap_or(f64::NAN),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

struct Collect(Mutex<Vec<i64>>);

#[async_trait]
impl MetricObserver for Collect {
    fn name(&self) -> &'static str {
        "collect"
    }
    async fn observe(&self, batch: Arc<IngestedBatch>) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(batch.batch_id);
    }
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn metric_replays_are_dropped_and_the_latest_attempt_wins_per_step() {
    let w = World::solo().await;
    let seen = Arc::new(Collect(Mutex::default()));
    w.observers.register(seen.clone());
    let a1 = seed_run(&w.db, Uuid::nil(), json!({})).await;
    send_metrics(&w, &a1, 1, points("loss", 0..100, |_| 1.0)).await;
    // The same worker message again (agent spool replay): nothing new.
    send_metrics(&w, &a1, 1, points("loss", 0..100, |_| 1.0)).await;
    // The attempt is resumed from step 50 and runs to 150.
    let a2 = next_attempt(&w.db, &a1).await;
    send_metrics(&w, &a2, 1, points("loss", 50..150, |_| 2.0)).await;

    assert_eq!(
        count(
            &w.db,
            "SELECT count(*) FROM metric_batches WHERE run_id = $1",
            a1.run
        )
        .await,
        2
    );
    let raw = count(
        &w.db,
        "SELECT count(*) FROM metric_points p JOIN metric_series s ON s.id = p.series_id WHERE s.run_id = $1",
        a1.run,
    )
    .await;
    assert_eq!(
        raw, 200,
        "both attempts' points are kept, the replay is not"
    );

    let r = w
        .get(
            Who::Solo,
            &format!("/api/v1/runs/{}/metrics/query?series=loss", a1.run_id()),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let got = line(&r.json()["series"][0]);
    assert_eq!(got.len(), 150);
    for (x, v) in got {
        assert_eq!(v, if x < 50 { 1.0 } else { 2.0 }, "step {x}");
    }

    let list = w
        .get(Who::Solo, &format!("/api/v1/runs/{}/metrics", a1.run_id()))
        .await
        .json();
    assert_eq!(list["series"][0]["name"], "loss");
    assert_eq!(list["series"][0]["split"], "train");
    assert_eq!(list["series"][0]["declared"], true);

    for _ in 0..50 {
        if seen.0.lock().unwrap_or_else(|p| p.into_inner()).len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        seen.0.lock().unwrap_or_else(|p| p.into_inner()).len(),
        2,
        "one observer call per new batch"
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn undeclared_names_are_flagged_and_resource_peaks_kept() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    let mut pts = points("custom/thing", 0..3, |s| s as f64);
    pts[1].undeclared = true;
    pts.extend(points("resource/torch_allocated_peak/gpu0", 0..3, |s| {
        [5.0, 9.0, 7.0][s as usize]
    }));
    send_metrics(&w, &s, 1, pts).await;
    let list = w
        .get(Who::Solo, &format!("/api/v1/runs/{}/metrics", s.run_id()))
        .await
        .json();
    let custom = list["series"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["name"] == "custom/thing")
        .cloned()
        .unwrap();
    assert_eq!(custom["declared"], false);
    assert_eq!(list["resources"][0]["signal"], "torch_allocated_peak");
    assert_eq!(list["resources"][0]["device"], "gpu0");
    assert_eq!(list["resources"][0]["phase"], "train");
    assert_eq!(list["resources"][0]["peak"], 9.0);
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn a_query_over_a_million_points_returns_at_most_2000_with_an_envelope() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    let owner = w
        .tel
        .ingest
        .owner(
            s.node,
            match &relay(&s, 1, WorkerBody::Progress(Progress::default())).body {
                Some(AgentBody::Worker(r)) => r,
                _ => panic!("relay"),
            },
        )
        .await
        .unwrap_or_else(|e| panic!("owner: {e}"))
        .unwrap_or_else(|| panic!("the attempt belongs to the node"));
    // A sawtooth: value = step % 1000, so every group of 500 steps spans 0..499 or 500..999.
    for (i, chunk) in (0..1_000_000u64)
        .collect::<Vec<_>>()
        .chunks(250_000)
        .enumerate()
    {
        let pts = points("loss", chunk[0]..chunk[chunk.len() - 1] + 1, |s| {
            (s % 1000) as f64
        });
        w.tel
            .ingest
            .metrics(owner, i as u64 + 1, None, &pts)
            .await
            .unwrap_or_else(|e| panic!("ingest: {e}"));
    }
    let path = format!(
        "/api/v1/runs/{}/metrics/query?series=loss&max_points=2000",
        s.run_id()
    );
    let r = w.get(Who::Solo, &path).await;
    assert_eq!(r.status, 200, "{}", r.text());
    let body = r.json();
    let pts = body["series"][0]["points"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        pts.len() <= 2000 && pts.len() >= 1000,
        "{} points",
        pts.len()
    );
    let n: i64 = pts.iter().map(|p| p["n"].as_i64().unwrap_or(0)).sum();
    assert_eq!(n, 1_000_000, "every point is in a group");
    assert_eq!(pts[0]["min"], 0.0);
    assert_eq!(pts[0]["max"], 499.0);
    assert_eq!(pts[1]["min"], 500.0);

    let t = w.get(Who::Solo, &format!("{path}&axis=time")).await.json();
    let tn: i64 = t["series"][0]["points"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["n"].as_i64().unwrap_or(0))
        .sum();
    assert_eq!(tn, 1_000_000, "the time axis counts every point too");

    // Roll up, then lose the raw points of the first half: the rollups answer for them.
    while rollup::roll_up(&w.tel.db, 1000)
        .await
        .unwrap_or_else(|e| panic!("roll up: {e}"))
        > 0
    {}
    sqlx::query("DELETE FROM metric_points WHERE step < 500000")
        .execute(w.db.pool())
        .await
        .unwrap_or_else(|e| panic!("delete: {e}"));
    let body = w.get(Who::Solo, &path).await.json();
    let pts = body["series"][0]["points"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(pts.len() <= 2000, "{} points", pts.len());
    let n: i64 = pts.iter().map(|p| p["n"].as_i64().unwrap_or(0)).sum();
    assert_eq!(n, 1_000_000, "rollups stand in for the dropped raw points");
    assert_eq!(
        (pts[0]["min"].clone(), pts[0]["max"].clone()),
        (json!(0.0), json!(499.0))
    );

    let bad = w
        .get(
            Who::Solo,
            &format!("/api/v1/runs/{}/metrics/query?max_points=1", s.run_id()),
        )
        .await;
    assert_eq!(bad.status, 422, "{}", bad.text());
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn old_partitions_are_dropped() {
    let w = World::solo().await;
    let long_ago = time::OffsetDateTime::now_utc() - time::Duration::days(60);
    rollup::ensure_partitions(&w.tel.db, long_ago)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let parts = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_inherits WHERE inhparent = 'metric_points'::regclass",
        )
        .fetch_one(w.db.pool())
        .await
        .unwrap_or_else(|e| panic!("{e}"))
    };
    let before = parts().await;
    let n = rollup::drop_old_partitions(
        &w.tel.db,
        time::OffsetDateTime::now_utc() - time::Duration::days(7),
    )
    .await
    .unwrap_or_else(|e| panic!("{e}"));
    assert!(n >= 4, "dropped {n}");
    assert_eq!(parts().await, before - i64::try_from(n).unwrap_or(0));
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    send_metrics(&w, &s, 1, points("loss", 0..3, |_| 1.0)).await;
    assert_eq!(
        count(
            &w.db,
            "SELECT count(*) FROM metric_batches WHERE run_id = $1",
            s.run
        )
        .await,
        1,
        "current weeks stay"
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn messages_for_attempts_the_node_does_not_run_are_dropped() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    // Another node, a stale epoch, a bad id: all dropped.
    let other = Seeded {
        node: Uuid::now_v7(),
        ..s
    };
    send_metrics(&w, &other, 1, points("loss", 0..3, |_| 1.0)).await;
    let stale = Seeded { attempt_no: 7, ..s };
    send_metrics(&w, &stale, 2, points("loss", 0..3, |_| 1.0)).await;
    let mut bad = relay(
        &s,
        3,
        WorkerBody::Metrics(MetricsBatch {
            points: points("loss", 0..3, |_| 1.0),
        }),
    );
    if let Some(AgentBody::Worker(r)) = &mut bad.body {
        r.attempt = Some(AttemptRef {
            attempt_id: "att_nope".into(),
            ..AttemptRef::default()
        });
    }
    w.deliver(s.node, LaneKind::Metrics, 3, 0, bad).await;
    assert_eq!(
        count(
            &w.db,
            "SELECT count(*) FROM metric_batches WHERE run_id = $1",
            s.run
        )
        .await,
        0
    );
    // The attempt's own node, epoch and id: stored.
    send_metrics(&w, &s, 4, points("loss", 0..3, |_| 1.0)).await;
    assert_eq!(
        count(
            &w.db,
            "SELECT count(*) FROM metric_batches WHERE run_id = $1",
            s.run
        )
        .await,
        1
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn logs_seal_into_segments_and_read_and_search_across_both() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    let mut expected = Vec::new();
    for i in 0..12u64 {
        let (rank, stream) = if i % 3 == 0 {
            (1, LogStream::Stderr)
        } else {
            (0, LogStream::Stdout)
        };
        let text = format!(
            "line {i} {}\n",
            if i % 4 == 0 { "ERROR boom" } else { "ok" }
        );
        send_log(&w, &s, i + 1, rank, stream, i * 100, text.as_bytes()).await;
        expected.push(text);
    }
    // A chunk over 64 KiB is split; a replay of a chunk is dropped.
    let big = vec![b'x'; 70 * 1024];
    send_log(&w, &s, 13, 0, LogStream::Stdout, 5000, &big).await;
    send_log(&w, &s, 13, 0, LogStream::Stdout, 5000, &big).await;
    expected.push("x".repeat(64 * 1024));
    expected.push("x".repeat(6 * 1024));
    assert_eq!(
        count(
            &w.db,
            "SELECT count(*) FROM log_chunks WHERE run_id = $1",
            s.run
        )
        .await,
        14
    );

    // Seal everything so far, then more lines arrive hot.
    let sealed = mrl_server::modules::telemetry::logs::seal(&w.tel, Duration::ZERO)
        .await
        .unwrap_or_else(|e| panic!("seal: {e}"));
    assert!(sealed >= 2, "{sealed} segments");
    assert_eq!(
        count(
            &w.db,
            "SELECT count(*) FROM log_chunks WHERE run_id = $1",
            s.run
        )
        .await,
        0
    );
    for i in 20..25u64 {
        let text = format!("hot {i} {}\n", if i == 22 { "error late" } else { "fine" });
        send_log(&w, &s, i, 0, LogStream::Stdout, i * 100, text.as_bytes()).await;
        expected.push(text);
    }
    // A gap reported by the gateway becomes an agent line.
    let msg = relay(
        &s,
        30,
        WorkerBody::Log(LogChunk {
            stream: LogStream::Stdout as i32,
            data: b"after gap\n".to_vec(),
            rank: 0,
            offset: 9000,
        }),
    );
    w.deliver(s.node, LaneKind::Logs, 777, 3, msg).await;
    expected.push(
        "[masterrl] 3 log chunk(s) were dropped here because the connection was too slow.\n".into(),
    );
    expected.push("after gap\n".into());

    // Paging 3 at a time sees every chunk once, in order.
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..100 {
        let path = match &cursor {
            Some(c) => format!("/api/v1/runs/{}/logs?limit=3&cursor={c}", s.run_id()),
            None => format!("/api/v1/runs/{}/logs?limit=3", s.run_id()),
        };
        let r = w.get(Who::Solo, &path).await;
        assert_eq!(r.status, 200, "{}", r.text());
        let page = r.json();
        for it in page["items"].as_array().cloned().unwrap_or_default() {
            seen.push(it["text"].as_str().unwrap_or_default().to_owned());
        }
        match page["next_cursor"].as_str() {
            Some(c) => cursor = Some(c.to_owned()),
            None => break,
        }
    }
    assert_eq!(seen, expected);

    let hits = w
        .get(
            Who::Solo,
            &format!("/api/v1/runs/{}/logs?q=error", s.run_id()),
        )
        .await
        .json();
    let hits: Vec<String> = hits["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["text"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        hits,
        vec![
            "line 0 ERROR boom\n",
            "line 4 ERROR boom\n",
            "line 8 ERROR boom\n",
            "hot 22 error late\n"
        ]
    );
    let rank1 = w
        .get(
            Who::Solo,
            &format!("/api/v1/runs/{}/logs?rank=1&stream=stderr", s.run_id()),
        )
        .await
        .json();
    assert_eq!(rank1["items"].as_array().unwrap().len(), 4);

    let dl = w
        .get(
            Who::Solo,
            &format!("/api/v1/runs/{}/logs:download", s.run_id()),
        )
        .await;
    assert_eq!(dl.status, 200);
    assert_eq!(dl.text(), expected.concat());
    assert!(dl.headers["content-disposition"]
        .to_str()
        .unwrap_or_default()
        .contains(&s.run_id()));

    // A damaged segment is reported, not shown.
    let key: String = sqlx::query_scalar(
        "SELECT object_key FROM log_index WHERE run_id = $1 ORDER BY id LIMIT 1",
    )
    .bind(s.run)
    .fetch_one(w.db.pool())
    .await
    .unwrap_or_else(|e| panic!("{e}"));
    let store = w
        .tel
        .store
        .org_store("local")
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    store
        .put(&key, bytes_of(b"{}\n"))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let r = w
        .get(Who::Solo, &format!("/api/v1/runs/{}/logs", s.run_id()))
        .await;
    assert_eq!(r.status, 503, "{}", r.text());
    assert_eq!(r.json()["code"], "MRL-STORE-220");
}

fn bytes_of(b: &[u8]) -> axum::body::Bytes {
    axum::body::Bytes::copy_from_slice(b)
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn samples_are_indexed_deduped_and_filtered() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    let sample = |id: &str, prompt: &str, reward: f64| Sample {
        sample_id: id.into(),
        group_id: "g".into(),
        prompt: prompt.into(),
        completion: format!("answer to {prompt}"),
        reward_total: reward,
        ..Sample::default()
    };
    for (seq, step, list) in [
        (
            1,
            10,
            vec![
                sample("a", "two plus two", 1.0),
                sample("b", "capital of France", 0.2),
            ],
        ),
        (2, 20, vec![sample("c", "two times three", 0.9)]),
    ] {
        let msg = relay(
            &s,
            seq,
            WorkerBody::Samples(SampleBatch {
                step,
                samples: list,
                overflow_ref: String::new(),
            }),
        );
        w.deliver(s.node, LaneKind::Metrics, seq, 0, msg.clone())
            .await;
        w.deliver(s.node, LaneKind::Metrics, seq, 0, msg).await;
    }
    assert_eq!(
        count(
            &w.db,
            "SELECT count(*) FROM samples WHERE run_id = $1",
            s.run
        )
        .await,
        2
    );
    let all = w
        .get(Who::Solo, &format!("/api/v1/runs/{}/samples", s.run_id()))
        .await
        .json();
    assert_eq!(all["items"].as_array().unwrap().len(), 3);
    let good = w
        .get(
            Who::Solo,
            &format!("/api/v1/runs/{}/samples?min_reward=0.5&q=TWO", s.run_id()),
        )
        .await
        .json();
    let ids: Vec<&str> = good["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["sample_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["a", "c"]);
    let step = w
        .get(
            Who::Solo,
            &format!("/api/v1/runs/{}/samples?step=20", s.run_id()),
        )
        .await
        .json();
    assert_eq!(step["items"][0]["sample_id"], "c");
    let page = w
        .get(
            Who::Solo,
            &format!("/api/v1/runs/{}/samples?limit=1", s.run_id()),
        )
        .await
        .json();
    assert_eq!(
        page["items"].as_array().unwrap().len(),
        2,
        "a page holds whole batches"
    );
    let next = page["next_cursor"].as_str().unwrap();
    let rest = w
        .get(
            Who::Solo,
            &format!("/api/v1/runs/{}/samples?limit=1&cursor={next}", s.run_id()),
        )
        .await
        .json();
    assert_eq!(rest["items"][0]["sample_id"], "c");
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn progress_keeps_the_newest_attempt_and_message() {
    let w = World::solo().await;
    let a1 = seed_run(&w.db, Uuid::nil(), json!({})).await;
    let progress = |step| {
        WorkerBody::Progress(Progress {
            step,
            total_steps: 100,
            phase: "train".into(),
            ..Progress::default()
        })
    };
    w.deliver(
        a1.node,
        LaneKind::Control,
        1,
        0,
        relay(&a1, 5, progress(50)),
    )
    .await;
    w.deliver(
        a1.node,
        LaneKind::Control,
        2,
        0,
        relay(&a1, 4, progress(40)),
    )
    .await;
    let a2 = next_attempt(&w.db, &a1).await;
    w.deliver(
        a2.node,
        LaneKind::Control,
        3,
        0,
        relay(&a2, 1, progress(30)),
    )
    .await;
    // A late message of the old attempt changes nothing.
    w.deliver(
        a1.node,
        LaneKind::Control,
        4,
        0,
        relay(&a1, 9, progress(60)),
    )
    .await;
    let p = w
        .get(Who::Solo, &format!("/api/v1/runs/{}/metrics", a1.run_id()))
        .await
        .json();
    assert_eq!(p["progress"]["step"], 30);
    assert_eq!(p["progress"]["attempt_id"], a2.attempt_id());
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn runs_compare_shows_differing_config_and_aligned_overlays() {
    let w = World::solo().await;
    let a = seed_run(
        &w.db,
        Uuid::nil(),
        json!({"lr": 0.1, "batch": 8, "opt": {"name": "adamw"}}),
    )
    .await;
    let b = seed_run(
        &w.db,
        Uuid::nil(),
        json!({"lr": 0.3, "batch": 8, "opt": {"name": "sgd"}}),
    )
    .await;
    send_metrics(&w, &a, 1, points("loss", 0..100, |s| s as f64)).await;
    send_metrics(&w, &b, 1, points("loss", 50..300, |s| s as f64)).await;
    send_metrics(&w, &b, 2, points("only_b", 0..10, |_| 1.0)).await;
    let r = w
        .post(
            Who::Solo,
            "/api/v1/runs:compare",
            json!({"run_ids": [a.run_id(), b.run_id()], "max_points": 30}),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let c = r.json();
    let paths: Vec<&str> = c["config_diff"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["/lr", "/opt/name"]);
    assert_eq!(
        c["metrics"].as_array().unwrap().len(),
        1,
        "only metrics both runs have"
    );
    let lines = &c["metrics"][0]["runs"];
    let of = |id: String| line(&json!({ "points": lines[id] }));
    let (la, lb) = (of(a.run_id()), of(b.run_id()));
    assert!(la.len() <= 30 && lb.len() <= 30);
    // Both lines sit on one grid of width 10 starting at step 0.
    assert!(
        la.iter().chain(lb.iter()).all(|(x, _)| x % 10 == 0),
        "{la:?} {lb:?}"
    );
    assert_eq!(lb[0].0, 50);
    let one = w
        .post(
            Who::Solo,
            "/api/v1/runs:compare",
            json!({"run_ids": [a.run_id()]}),
        )
        .await;
    assert_eq!(one.status, 422);
    let twice = w
        .post(
            Who::Solo,
            "/api/v1/runs:compare",
            json!({"run_ids": [a.run_id(), a.run_id()]}),
        )
        .await;
    assert_eq!(twice.status, 422);
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn another_orgs_run_data_is_hidden() {
    let w = World::team().await;
    let a = seed_run(&w.db, w.org(Who::A), json!({})).await;
    let b = seed_run(&w.db, w.org(Who::B), json!({})).await;
    send_metrics(&w, &a, 1, points("loss", 0..3, |_| 1.0)).await;
    send_log(&w, &a, 2, 0, LogStream::Stdout, 0, b"secret line\n").await;
    for path in [
        "metrics",
        "metrics/query",
        "logs",
        "logs:download",
        "samples",
    ] {
        let mine = w
            .get(Who::A, &format!("/api/v1/runs/{}/{path}", a.run_id()))
            .await;
        assert_eq!(mine.status, 200, "{path}: {}", mine.text());
        let theirs = w
            .get(Who::B, &format!("/api/v1/runs/{}/{path}", a.run_id()))
            .await;
        assert_eq!(theirs.status, 404, "{path}: {}", theirs.text());
        assert!(!theirs.text().contains("secret line"));
    }
    let r = w
        .post(
            Who::B,
            "/api/v1/runs:compare",
            json!({"run_ids": [a.run_id(), b.run_id()]}),
        )
        .await;
    assert_eq!(r.status, 404, "{}", r.text());
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn metrics_and_logs_from_a_real_agent_reach_the_history() {
    use crate::agent::gateway::{fast_config, GatewayHarness, TestAgent};
    let db = TestDb::new().await;
    let lanes = LaneRouter::new();
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let store = ArtifactsModule::with_backend(
        db.db().clone(),
        BackendConfig::Local {
            root: dir.path().join("o"),
        },
        Mode::Solo,
        dir.path().join("s"),
    )
    .store();
    let module = TelemetryModule::with_parts(
        db.db().clone(),
        fast_live(),
        store,
        &lanes,
        MetricObservers::new(),
    );
    rollup::ensure_partitions(&module.telemetry().db, time::OffsetDateTime::now_utc())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let gw = GatewayHarness::start_with(
        db,
        fast_config(),
        Arc::new(crate::seams::InMemorySecretStore::default()),
        lanes,
    )
    .await;
    let token = gw.token(json!({})).await;
    let agent = TestAgent::new(1, |_| {});
    let node = agent
        .enroll(&gw, &token)
        .await
        .unwrap_or_else(|e| panic!("enroll: {e}"));
    let running = agent.run();
    gw.wait_status(&node, "ONLINE").await;
    let node_uuid = node
        .parse::<NodeId>()
        .unwrap_or_else(|e| panic!("{e}"))
        .as_uuid();
    let s = seed_run(&gw.db, Uuid::nil(), json!({})).await;
    let s = Seeded {
        node: node_uuid,
        ..s
    };
    sqlx::query("UPDATE attempt_placements SET node_id = $2 WHERE attempt_id = $1")
        .bind(s.attempt)
        .bind(node_uuid)
        .execute(gw.db.pool())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let link =
        mrl_agent::session::Link::of(&agent.ctx).unwrap_or_else(|e| panic!("{}", e.explain()));
    let to_relay = |m: AgentMessage| match m.body {
        Some(AgentBody::Worker(r)) => r,
        _ => panic!("relay"),
    };
    link.send_metrics(to_relay(relay(
        &s,
        1,
        WorkerBody::Metrics(MetricsBatch {
            points: points("loss", 0..5, |x| x as f64),
        }),
    )));
    link.send_logs(to_relay(relay(
        &s,
        2,
        WorkerBody::Log(LogChunk {
            stream: LogStream::Stdout as i32,
            data: b"hello\n".to_vec(),
            rank: 0,
            offset: 0,
        }),
    )));
    let mut got = (0, 0);
    for _ in 0..100 {
        got = (
            count(
                &gw.db,
                "SELECT count(*) FROM metric_batches WHERE run_id = $1",
                s.run,
            )
            .await,
            count(
                &gw.db,
                "SELECT count(*) FROM log_chunks WHERE run_id = $1",
                s.run,
            )
            .await,
        );
        if got == (1, 1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(got, (1, 1), "metrics and logs arrived through the gateway");
    running.stop().await;
}

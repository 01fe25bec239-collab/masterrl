//! P24 integration tests: startup registration, probes over a real agent link, L0 validation
//! of submitted runs and the engines API.
//!
//! Compiled by `mrl-testkit` (see its `lib.rs`, P24 marker), not by this crate: they need
//! `TestServer`, `TestDb` and `TestAgent`. `#[ignore]`d; run them with
//! `just dev && just test-integration`.

use crate::agent::gateway::{Running, TestAgent};
use crate::engines::{start_server as start, TinyFixture};
use crate::pg::TestDb;
use crate::seams::InMemorySecretStore;
use crate::server::{TestResponse, TestServer, TEST_SECRET};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::Method;
use mrl_agent::session::{CommandHandler, Link};
use mrl_domain::capability::matcher::{DatasetFacts, ModelFacts};
use mrl_domain::ids::{DatasetVersionId, ModelVersionId, NodeId, ProjectId};
use mrl_proto::agent::v1::{
    agent_message::Body as Msg, command::Kind, AgentMessage, Command, ProbeCheckResult,
    ProbeOutcome, ProbeResult,
};
use mrl_proto::common::v1::Failure;
use mrl_server::modules::engines::probes::ProbeSink;
use mrl_server::modules::engines::{
    CapabilityValidator, EnginesConfig, Pick, Records, RegistryRecords,
};
use mrl_server::modules::jobs::JobSpec;
use mrl_server::modules::nodes::lanes::{LaneItem, LaneKind, LaneSink};
use mrl_server::seams::{SeamError, LAUNCH_SECRET_HEADER};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const GOLDEN: &str = include_str!("../../../../../tests/fixtures/rejections/engines/golden.json");

/// Model and dataset records the test knows.
#[derive(Default)]
struct TestRecords {
    models: BTreeMap<ModelVersionId, ModelFacts>,
    datasets: BTreeMap<DatasetVersionId, DatasetFacts>,
}

#[async_trait]
impl RegistryRecords for TestRecords {
    async fn model(&self, _: Uuid, id: ModelVersionId) -> Result<Option<ModelFacts>, SeamError> {
        Ok(self.models.get(&id).cloned())
    }
    async fn dataset(
        &self,
        _: Uuid,
        id: DatasetVersionId,
    ) -> Result<Option<DatasetFacts>, SeamError> {
        Ok(self.datasets.get(&id).cloned())
    }
}

struct World {
    model: ModelVersionId,
    dataset: DatasetVersionId,
    records: Records,
}

fn world() -> World {
    let (model, dataset) = (ModelVersionId::new(), DatasetVersionId::new());
    let mut r = TestRecords::default();
    r.models.insert(
        model,
        ModelFacts {
            family: "decoder".into(),
            is_moe: false,
            params: Some(1.0e8),
            has_chat_template: true,
            requires_remote_code: false,
        },
    );
    r.datasets.insert(
        dataset,
        DatasetFacts {
            schema: "sft.messages.v1".into(),
            rows: Some(10),
            streaming: false,
        },
    );
    World {
        model,
        dataset,
        records: Records::new(vec![Arc::new(r)]),
    }
}

fn engines_config(dev: bool) -> EnginesConfig {
    EnginesConfig {
        dev_engines: dev,
        probe_interval_seconds: 1,
        probe_timeout_seconds: 10,
        probe_retry_seconds: 10,
    }
}

async fn call(s: &TestServer, method: Method, path: &str, body: Option<Value>) -> TestResponse {
    let key = Uuid::new_v4().to_string();
    let mut headers = vec![
        (LAUNCH_SECRET_HEADER, TEST_SECRET),
        ("content-type", "application/json"),
    ];
    if method == Method::POST && path.ends_with("/runs") {
        headers.push(("idempotency-key", key.as_str()));
    }
    let body = body.map_or_else(Body::empty, |b| Body::from(b.to_string()));
    s.request(method, path, &headers, body).await
}

async fn get(s: &TestServer, path: &str) -> TestResponse {
    call(s, Method::GET, path, None).await
}

/// Answers every Probe with every asked check OK (or with `outcome`).
struct AnswerProbes {
    link: Arc<Link>,
    outcome: ProbeOutcome,
}

#[async_trait]
impl CommandHandler for AnswerProbes {
    async fn handle(&self, command: Command) -> Result<(), Failure> {
        if let Some(Kind::Probe(p)) = command.kind {
            let checks = p
                .checks
                .iter()
                .map(|c| ProbeCheckResult {
                    check: *c,
                    outcome: self.outcome as i32,
                    summary: "checked by the test agent".into(),
                    ..ProbeCheckResult::default()
                })
                .collect();
            let _ = self.link.send_control(Msg::ProbeResult(ProbeResult {
                request_id: p.request_id,
                checks,
                inventory: None,
            }));
        }
        Ok(())
    }
}

/// A CPU machine enrolled in a new pool that answers every probe OK: its agent, its loop
/// (keep both alive while the test needs it), node id and pool id.
async fn join_cpu_machine(s: &TestServer) -> (TestAgent, Running, String, String) {
    let pool = call(
        s,
        Method::POST,
        "/api/v1/pools",
        Some(json!({"name": "p24"})),
    )
    .await;
    assert_eq!(pool.status, 201, "{}", pool.text());
    let pool = pool.json()["id"].as_str().unwrap().to_owned();
    let token = call(
        s,
        Method::POST,
        &format!("/api/v1/pools/{pool}/enrollment-tokens"),
        Some(json!({})),
    )
    .await;
    assert_eq!(token.status, 201, "{}", token.text());
    let token = token.json()["token"].as_str().unwrap().to_owned();
    let agent = TestAgent::new(0, |_| {});
    let link = Link::of(&agent.ctx).unwrap();
    link.register(
        "Probe",
        Arc::new(AnswerProbes {
            link: link.clone(),
            outcome: ProbeOutcome::Ok,
        }),
    );
    let grpc = s.running().grpc_addr().expect("grpc listener");
    let node = mrl_agent::enroll::enroll_dial(
        &agent.ctx,
        &grpc.to_string(),
        &token,
        mrl_agent::transports::TransportKind::Direct,
        false,
    )
    .await
    .unwrap()
    .link
    .node_id;
    let running = agent.run();
    (agent, running, node, pool)
}

async fn wait_for(what: &str, mut ok: impl AsyncFnMut() -> bool) {
    for _ in 0..300 {
        if ok().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

async fn scalar_text(db: &TestDb, sql: &str) -> Option<String> {
    sqlx::query_scalar::<_, Option<String>>(sqlx::AssertSqlSafe(sql.to_owned()))
        .fetch_optional(db.pool())
        .await
        .unwrap()
        .flatten()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn startup_registers_bundles_once_and_lists_dev_engines_only_when_asked() {
    let db = TestDb::new().await;
    let secrets = Arc::new(InMemorySecretStore::default());
    let w = world();
    for _ in 0..2 {
        let s = start(
            &db,
            &secrets,
            engines_config(true),
            vec![GOLDEN.into()],
            w.records.clone(),
        )
        .await
        .unwrap();
        s.shutdown().await.unwrap();
    }
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT te.name, ev.version, ev.status FROM engine_versions ev
         JOIN training_engines te ON te.id = ev.engine_id ORDER BY te.name",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let names: Vec<_> = rows.iter().map(|r| r.0.as_str()).collect();
    assert_eq!(names, ["fake", "golden", "mrl-builtin"]);
    assert!(rows.iter().all(|r| r.2 == "PROBING"), "{rows:?}");
    let registered: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM outbox_events WHERE type = 'engine_version.registered'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        registered, 3,
        "one registration event per version, across restarts"
    );
    let algos: Vec<String> =
        sqlx::query_scalar("SELECT algorithm_id FROM algorithm_definitions ORDER BY algorithm_id")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(algos, ["dpo", "grpo", "kto", "sft", "x-fake.steps"]);

    // Without the dev flag the dev-only engines (FakeEngine, the golden fixture) are hidden.
    let s = start(
        &db,
        &secrets,
        engines_config(false),
        vec![GOLDEN.into()],
        w.records.clone(),
    )
    .await
    .unwrap();
    let list = get(&s, "/api/v1/engines").await;
    assert_eq!(list.status, 200, "{}", list.text());
    let names: Vec<_> = list.json()["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["mrl-builtin"]);
    assert_eq!(
        get(&s, "/api/v1/engines/fake/versions/0.1.0").await.status,
        404
    );
    assert_eq!(
        get(&s, "/api/v1/algorithms/x-fake.steps/schema")
            .await
            .status,
        404
    );
    s.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_changed_manifest_under_the_same_version_or_a_reserved_metric_stops_start() {
    let db = TestDb::new().await;
    let secrets = Arc::new(InMemorySecretStore::default());
    start(
        &db,
        &secrets,
        engines_config(true),
        vec![GOLDEN.into()],
        Records::default(),
    )
    .await
    .unwrap()
    .shutdown()
    .await
    .unwrap();
    let mut changed: Value = serde_json::from_str(GOLDEN).unwrap();
    changed["manifest"]["metadata"]["description"] = json!("Something else.");
    let err = start(
        &db,
        &secrets,
        engines_config(true),
        vec![changed.to_string()],
        Records::default(),
    )
    .await
    .err()
    .expect("start must fail");
    assert!(err.contains("MRL-VAL-130"), "{err}");
    assert!(
        err.contains("golden 1.0.0 is already registered with a different manifest"),
        "{err}"
    );

    let mut reserved: Value = serde_json::from_str(GOLDEN).unwrap();
    reserved["manifest"]["metadata"]["name"] = json!("reserved");
    reserved["manifest"]["spec"]["algorithms"][0]["metrics"][0]["name"] =
        json!("resource/gpu_peak");
    let err = start(
        &TestDb::new().await,
        &secrets,
        engines_config(false),
        vec![reserved.to_string()],
        Records::default(),
    )
    .await
    .err()
    .expect("start must fail");
    assert!(
        err.contains("metric names starting with `resource/` belong to the platform (resource use per phase); rename resource/gpu_peak"),
        "{err}"
    );
}

fn submission(w: &World, algorithm: &str, config: Value, gpus: u32) -> Value {
    json!({
        "name": format!("p24-{algorithm}"),
        "algorithm_id": algorithm,
        "base_model_version_id": w.model.to_string(),
        "dataset_version_ids": [w.dataset.to_string()],
        "config": config,
        "resources": {"roles": [{"role": "trainer", "replicas": 1, "gpu_count": gpus,
            "cpu_millis": 1000, "mem_bytes": 1073741824}]},
    })
}

async fn submit(s: &TestServer, body: Value) -> Value {
    let prj = ProjectId::new();
    let r = call(
        s,
        Method::POST,
        &format!("/api/v1/projects/{prj}/runs"),
        Some(body),
    )
    .await;
    assert_eq!(r.status, 201, "{}", r.text());
    r.json()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_passed_probe_enables_cpu_then_runs_are_queued_or_rejected_by_l0() {
    let db = TestDb::new().await;
    let secrets = Arc::new(InMemorySecretStore::default());
    let w = world();
    let s = start(
        &db,
        &secrets,
        engines_config(true),
        vec![GOLDEN.into()],
        w.records.clone(),
    )
    .await
    .unwrap();

    // Before any machine: L0 rejects (no machine of this kind connected).
    let early = submit(&s, submission(&w, "x-fake.steps", json!({}), 0)).await;
    assert_eq!(early["state"], "REJECTED");
    assert_eq!(early["state_reason"]["code"], "MRL-CAP-245", "{early}");

    // A CPU machine joins and answers probes.
    let (_agent, running, node, pool) = join_cpu_machine(&s).await;

    // Probes reach it; every engine version with a CPU setup is enabled on `cpu`.
    wait_for("fake ENABLED", async || {
        scalar_text(&db, "SELECT ev.status FROM engine_versions ev JOIN training_engines te ON te.id = ev.engine_id WHERE te.name = 'fake'")
            .await
            .as_deref()
            == Some("ENABLED")
    })
    .await;
    wait_for("golden enabled on cpu", async || {
        scalar_text(&db, "SELECT ev.probe_results #>> '{cpu,status}' FROM engine_versions ev JOIN training_engines te ON te.id = ev.engine_id WHERE te.name = 'golden'")
            .await
            .as_deref()
            == Some("ENABLED")
    })
    .await;
    let node_uuid = NodeId::from_str(&node).unwrap().as_uuid();
    let passed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM capability_evidence WHERE result = 'PASSED' AND node_id = $1 AND hardware_class = 'cpu'",
    )
    .bind(node_uuid)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(passed >= 2, "{passed}");
    let enabled: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM outbox_events WHERE type = 'engine_version.enabled'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(enabled >= 2);
    let detail = get(&s, "/api/v1/engines/fake/versions/0.1.0").await;
    assert_eq!(detail.status, 200, "{}", detail.text());
    assert_eq!(detail.json()["probe_results"]["cpu"]["status"], "ENABLED");
    assert_eq!(
        detail.json()["algorithms"][0]["verified_hw"],
        json!(["cpu"])
    );

    // The FakeEngine run passes L0 and is queued.
    let run = submit(&s, submission(&w, "x-fake.steps", json!({}), 0)).await;
    assert_eq!(run["state"], "QUEUED", "{run}");

    // QLoRA on a CPU pool is rejected with the documented code.
    let qlora = submit(
        &s,
        submission(
            &w,
            "sft",
            json!({"common": {"update_strategy": "qlora"}}),
            0,
        ),
    )
    .await;
    assert_eq!(qlora["state"], "REJECTED");
    assert_eq!(qlora["state_reason"]["code"], "MRL-CAP-018", "{qlora}");
    assert_eq!(qlora["state_reason"]["problems"][0]["path"], "/resources");
    assert!(qlora["state_reason"]["problems"][0]["hint"]
        .as_str()
        .unwrap()
        .contains("Environment Doctor"));

    // An NVIDIA run: no such machine connected.
    let gpu = submit(&s, submission(&w, "sft", json!({}), 1)).await;
    assert_eq!(gpu["state_reason"]["code"], "MRL-CAP-245", "{gpu}");

    // No engine offers the method: the chain's own MRL-CAP-001, once; a named engine without
    // it: MRL-CAP-001 at the engine choice.
    let ppo = submit(&s, submission(&w, "ppo", json!({}), 0)).await;
    let problems = ppo["state_reason"]["problems"].as_array().unwrap().clone();
    assert_eq!(problems.len(), 1, "{ppo}");
    assert_eq!(problems[0]["code"], "MRL-CAP-001");
    assert_eq!(problems[0]["path"], "/algorithm_id");
    let named = submit(
        &s,
        submission(&w, "sft", json!({"common": {"engine": "fake"}}), 0),
    )
    .await;
    assert_eq!(
        named["state_reason"]["problems"][0]["path"], "/config/common/engine",
        "{named}"
    );

    // Unknown model and dataset records.
    let mut unknown = submission(&w, "x-fake.steps", json!({}), 0);
    unknown["base_model_version_id"] = json!(ModelVersionId::new().to_string());
    unknown["dataset_version_ids"] = json!([DatasetVersionId::new().to_string()]);
    let unknown = submit(&s, unknown).await;
    let codes: Vec<_> = unknown["state_reason"]["problems"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["code"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(codes, ["MRL-DATA-240", "MRL-MODEL-240"]);

    // Capabilities for the pool drive the UI: the fake method runs, QLoRA is not offered.
    let caps = get(
        &s,
        &format!(
            "/api/v1/capabilities?model_version={}&dataset_version={}&pool={pool}",
            w.model, w.dataset
        ),
    )
    .await;
    assert_eq!(caps.status, 200, "{}", caps.text());
    let caps = caps.json();
    assert_eq!(caps["hardware"], json!(["cpu"]));
    let item = |algo: &str| {
        caps["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["algorithm"] == algo)
            .cloned()
            .unwrap()
    };
    assert_eq!(item("x-fake.steps")["runnable"], true);
    assert_eq!(item("sft")["runnable"], true);
    assert_eq!(item("sft")["update_strategies"], json!(["full", "lora"]));
    assert_eq!(item("dpo")["runnable"], false);
    assert_eq!(item("dpo")["problems"][0]["code"], "MRL-CAP-003");
    running.stop().await;
    s.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn probe_answers_count_once_and_only_from_the_machine_asked() {
    let db = TestDb::new().await;
    let secrets = Arc::new(InMemorySecretStore::default());
    let s = start(
        &db,
        &secrets,
        engines_config(true),
        vec![],
        Records::default(),
    )
    .await
    .unwrap();
    let version: Uuid = sqlx::query_scalar(
        "SELECT ev.id FROM engine_versions ev JOIN training_engines te ON te.id = ev.engine_id WHERE te.name = 'fake'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let (asked, other, evidence) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    db.seed(&format!(
        "INSERT INTO capability_evidence (id, engine_version_id, hardware_class, source, node_id, result, detail)
         VALUES ('{evidence}', '{version}', 'cpu', 'probe', '{asked}', 'PENDING', '{{\"checks\": [1]}}')"
    ))
    .await;
    let sink = ProbeSink::new(db.db().clone());
    let item = |node: Uuid, outcome: ProbeOutcome| LaneItem {
        node_id: node,
        session_epoch: 1,
        lane: LaneKind::Control,
        stream: mrl_proto::agent::v1::Lane::Control,
        seq: 1,
        dropped_before: 0,
        message: AgentMessage {
            seq: 1,
            body: Some(Msg::ProbeResult(ProbeResult {
                request_id: evidence.to_string(),
                checks: vec![ProbeCheckResult {
                    check: 1,
                    outcome: outcome as i32,
                    ..ProbeCheckResult::default()
                }],
                inventory: None,
            })),
            ..AgentMessage::default()
        },
    };
    let state = || async {
        scalar_text(
            &db,
            &format!("SELECT result FROM capability_evidence WHERE id = '{evidence}'"),
        )
        .await
    };
    // Another machine can't answer for it; a request id that isn't ours is ignored.
    sink.deliver(item(other, ProbeOutcome::Ok)).await.unwrap();
    assert_eq!(state().await.as_deref(), Some("PENDING"));
    let mut foreign = item(asked, ProbeOutcome::Ok);
    if let Some(Msg::ProbeResult(p)) = &mut foreign.message.body {
        p.request_id = "doctor-from-cli".into();
    }
    sink.deliver(foreign).await.unwrap();
    assert_eq!(state().await.as_deref(), Some("PENDING"));
    // A failed answer counts; a later (replayed or forged) passing one changes nothing.
    sink.deliver(item(asked, ProbeOutcome::Failed))
        .await
        .unwrap();
    sink.deliver(item(asked, ProbeOutcome::Ok)).await.unwrap();
    assert_eq!(state().await.as_deref(), Some("FAILED"));
    let status = scalar_text(
        &db,
        &format!("SELECT status FROM engine_versions WHERE id = '{version}'"),
    )
    .await;
    assert_eq!(status.as_deref(), Some("PROBING"));
    let cpu = scalar_text(
        &db,
        &format!(
            "SELECT probe_results #>> '{{cpu,status}}' FROM engine_versions WHERE id = '{version}'"
        ),
    )
    .await;
    assert_eq!(cpu.as_deref(), Some("FAILED"));
    s.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn algorithm_schema_and_version_routes_answer_and_refuse_bad_input() {
    let db = TestDb::new().await;
    let secrets = Arc::new(InMemorySecretStore::default());
    let s = start(
        &db,
        &secrets,
        engines_config(true),
        vec![GOLDEN.into()],
        Records::default(),
    )
    .await
    .unwrap();
    let doc = get(
        &s,
        "/api/v1/algorithms/sft/schema?engine_version=golden@1.0.0",
    )
    .await;
    assert_eq!(doc.status, 200, "{}", doc.text());
    let doc = doc.json();
    assert_eq!(
        doc["config_schema"]["properties"]["learning_rate"]["x-mrl-ui"]["tier"],
        "beginner"
    );
    assert_eq!(doc["presets"][0]["id"], "qlora-low-vram");
    let grpo = get(&s, "/api/v1/algorithms/grpo/schema").await.json();
    assert_eq!(grpo["switch"], json!({"loss_type": "grpo"}));
    assert_eq!(grpo["config_schema"], Value::Null);
    assert_eq!(
        get(&s, "/api/v1/algorithms/sft/schema?engine_version=golden")
            .await
            .status,
        422
    );
    assert_eq!(get(&s, "/api/v1/algorithms/ppo/schema").await.status, 404);
    assert_eq!(
        get(&s, "/api/v1/engines/golden/versions/9.9.9")
            .await
            .status,
        404
    );
    assert_eq!(
        get(&s, "/api/v1/capabilities?model_version=nope")
            .await
            .status,
        422
    );
    let pool = mrl_domain::ids::PoolId::new();
    assert_eq!(
        get(&s, &format!("/api/v1/capabilities?pool={pool}"))
            .await
            .status,
        404
    );
    let caps = get(
        &s,
        &format!(
            "/api/v1/capabilities?model_version={}",
            ModelVersionId::new()
        ),
    )
    .await;
    assert_eq!(caps.status, 200, "{}", caps.text());
    let caps = caps.json();
    let first = &caps["items"][0];
    assert_eq!(first["runnable"], false);
    let codes: Vec<_> = first["problems"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["code"].clone())
        .collect();
    assert!(codes.contains(&json!("MRL-MODEL-240")), "{codes:?}");
    assert!(codes.contains(&json!("MRL-CAP-245")), "{codes:?}");
    s.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn an_sft_run_on_the_tiny_fixtures_passes_l0_and_picks_the_fake_engine() {
    let db = TestDb::new().await;
    let secrets = Arc::new(InMemorySecretStore::default());
    let tiny = TinyFixture::default();
    let s = start(&db, &secrets, engines_config(true), vec![], tiny.records())
        .await
        .unwrap();
    let (_agent, running, _, _) = join_cpu_machine(&s).await;
    wait_for("fake ENABLED", async || {
        scalar_text(&db, "SELECT ev.status FROM engine_versions ev JOIN training_engines te ON te.id = ev.engine_id WHERE te.name = 'fake'")
            .await
            .as_deref()
            == Some("ENABLED")
    })
    .await;

    let w = World {
        model: tiny.model,
        dataset: tiny.dataset,
        records: tiny.records(),
    };
    let run = submit(&s, submission(&w, "x-fake.steps", json!({}), 0)).await;
    assert_eq!(run["state"], "QUEUED", "{run}");

    // The pick names the FakeEngine version, the same decision the chain accepted.
    let org: Uuid = sqlx::query_scalar("SELECT org_id FROM runs")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let fake: Uuid = sqlx::query_scalar("SELECT ev.id FROM engine_versions ev JOIN training_engines te ON te.id = ev.engine_id WHERE te.name = 'fake'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let spec = JobSpec {
        org_id: org,
        project_id: Uuid::new_v4(),
        kind: "TRAIN".into(),
        algorithm_id: "x-fake.steps".into(),
        base_model_version_id: tiny.model.as_uuid(),
        reward_version_id: None,
        dataset_version_ids: vec![tiny.dataset.as_uuid()],
        resolved: json!({}),
        resources: serde_json::from_value(json!({"roles": [{"role": "trainer", "replicas": 1,
            "gpu_count": 0, "cpu_millis": 1000, "mem_bytes": 1073741824}]}))
        .unwrap(),
        max_runtime_seconds: None,
    };
    let v = CapabilityValidator::with_records(db.db().clone(), tiny.records());
    assert_eq!(v.pick(&spec).await.unwrap(), Pick::Engine(fake));
    // Another model is unknown to the fixture records.
    let other = JobSpec {
        base_model_version_id: Uuid::new_v4(),
        ..spec
    };
    match v.pick(&other).await.unwrap() {
        Pick::Rejected(r) => assert_eq!(r[0].code.to_string(), "MRL-MODEL-240"),
        p => panic!("expected a rejection, got {p:?}"),
    }
    running.stop().await;
}

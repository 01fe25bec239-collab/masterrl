//! P29 integration tests: reward versions over the real team core (P16 auth and RBAC, P17
//! projects, P19 artifacts), the publish gate, the immutability triggers, the completion
//! verifier on fixture reports and the launch contributor.
//!
//! Compiled by `mrl-testkit` (see its `lib.rs`, P29 marker), not by this crate. `#[ignore]`d;
//! run them with `just dev && just test-integration` (or `cargo test -p mrl-testkit p29_ --
//! --ignored`).

use crate::pg::TestDb;
use crate::supabase::core::{Auth, CoreServer};
use crate::supabase::{MockSupabase, MockUser};
use async_trait::async_trait;
use axum::http::Method;
use mrl_domain::ids::{JobId, OrgId, ProjectId, RewardVersionId, UserId, WorkspaceId};
use mrl_domain::rbac::Role;
use mrl_server::config::Mode;
use mrl_server::modules::jobs::{CompletionVerifier, JobRow, Verification};
use mrl_server::modules::rewards::verifier::{JobFiles, ReportSource, ReportVerifier};
use mrl_server::modules::tenancy::{bind_role, Scope};
use mrl_server::seams::SeamError;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

struct Env {
    sb: MockSupabase,
    core: CoreServer,
    org: OrgId,
    prj: ProjectId,
    owner: Auth,
}

async fn person(env: &Env, name: &str, org: OrgId, role: Role, scope: Scope) -> Auth {
    let user = MockUser::new(&format!("{name}@example.com"), name);
    let auth = Auth::Bearer(env.sb.access_token(&user, 600));
    let me = env
        .core
        .call(Method::GET, "/api/v1/auth/me", &auth, None)
        .await;
    assert_eq!(me.status, 200, "{}", me.text());
    let id: Uuid = sqlx::query_scalar("SELECT id FROM users WHERE supabase_user_id = $1")
        .bind(user.id)
        .fetch_one(env.core.db.pool())
        .await
        .unwrap();
    let mut tx = env.core.db.db().begin().await.unwrap();
    bind_role(&mut tx, org, UserId::from_uuid(id), role, scope)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    auth
}

async fn add_project(env: &Env) -> ProjectId {
    let project = ProjectId::new();
    sqlx::query(
        "INSERT INTO projects (id, org_id, workspace_id, slug, name)
         SELECT $1, org_id, workspace_id, 'other', 'Other' FROM projects WHERE id = $2",
    )
    .bind(project.as_uuid())
    .bind(env.prj.as_uuid())
    .execute(env.core.db.pool())
    .await
    .unwrap();
    project
}

async fn seed_org(core: &CoreServer, org: OrgId, prj: ProjectId) {
    let (o, w, p) = (org.as_uuid(), WorkspaceId::new().as_uuid(), prj.as_uuid());
    core.db
        .seed(&format!(
            "INSERT INTO organizations (id, slug, name, kind) VALUES ('{o}', 'o{n}', 'Lab', 'team');
             INSERT INTO workspaces (id, org_id, slug, name) VALUES ('{w}', '{o}', 'main', 'Main');
             INSERT INTO projects (id, org_id, workspace_id, slug, name)
             VALUES ('{p}', '{o}', '{w}', 'rewards', 'Rewards');",
            n = o.simple()
        ))
        .await;
}

async fn env() -> Env {
    let sb = MockSupabase::start().await;
    let core = CoreServer::start(Mode::Team, &sb, TestDb::new().await, Arc::default()).await;
    let (org, prj) = (OrgId::new(), ProjectId::new());
    seed_org(&core, org, prj).await;
    let mut env = Env {
        sb,
        core,
        org,
        prj,
        owner: Auth::None,
    };
    env.owner = person(&env, "owner", org, Role::Owner, Scope::Org).await;
    env
}

impl Env {
    async fn call(
        &self,
        m: Method,
        path: &str,
        body: Option<Value>,
    ) -> crate::server::TestResponse {
        self.core.call(m, path, &self.owner, body).await
    }
}

fn policy(weight: f64) -> Value {
    json!({
        "apiVersion": "mrl.dev/reward/v1",
        "kind": "RewardPolicy",
        "metadata": {"name": "math", "signal": "scalar.sequence"},
        "spec": {
            "components": [
                {"id": "correct", "use": "builtin/math.verify@1", "weight": weight},
                {"id": "format", "use": "builtin/format.think_answer_tags@1", "weight": 0.2}
            ],
            "aggregation": {"method": "weighted_sum", "where": "platform"},
            "failure": {"on_timeout": {"action": "mask"}, "on_error": {"action": "mask"},
                        "max_failure_rate": 0.2},
            "tests": "tests.yaml"
        }
    })
}

fn cases() -> Value {
    json!([
        {"name": "right", "sample": {"prompt": "1+1?", "completion": "<think>x</think><answer>2</answer>",
            "reference": "2"}, "expect": {"components": {"correct": 1.0}}},
        {"name": "wrong", "sample": {"prompt": "1+1?", "completion": "<think>x</think><answer>3</answer>",
            "reference": "2"}, "expect": {"components": {"correct": 0.0}}}
    ])
}

/// A report source that hands out whatever the test put in for a job.
#[derive(Default)]
struct Fixtures(Mutex<BTreeMap<Uuid, JobFiles>>);

#[async_trait]
impl ReportSource for Fixtures {
    async fn outputs(&self, job: &JobRow) -> Result<Option<JobFiles>, SeamError> {
        Ok(self.0.lock().unwrap().get(&job.id).cloned())
    }
}

impl Fixtures {
    fn put(&self, job: Uuid, report: Value, rows: Option<&str>) {
        let mut files =
            BTreeMap::from([("report.json".to_owned(), report.to_string().into_bytes())]);
        if let Some(r) = rows {
            files.insert("rows.jsonl".into(), r.as_bytes().to_vec());
        }
        self.0.lock().unwrap().insert(
            job,
            JobFiles {
                artifact_id: Uuid::now_v7(),
                files,
                problems: Vec::new(),
            },
        );
    }
}

/// A FINALIZING REWARD_PREVIEW job with a PENDING report for `version` (what `:test` makes).
async fn preview_job(env: &Env, version: Uuid, mode: &str, other: Option<Uuid>) -> JobRow {
    let (job, report) = (Uuid::now_v7(), Uuid::now_v7());
    let (o, p) = (env.org.as_uuid(), env.prj.as_uuid());
    let other = other.map_or("NULL".to_owned(), |u| format!("'{u}'"));
    env.core
        .db
        .seed(&format!(
            "INSERT INTO jobs (id, org_id, project_id, kind, state, resource_request, retry_policy, created_by)
             VALUES ('{job}', '{o}', '{p}', 'REWARD_PREVIEW', 'FINALIZING', '{{}}', '{{}}', 'test');
             INSERT INTO reward_reports (id, org_id, project_id, version_id, other_version_id, mode, job_id, state, created_by)
             VALUES ('{report}', '{o}', '{p}', '{version}', {other}, '{mode}', '{job}', 'PENDING', 'test');"
        ))
        .await;
    if mode == "test" {
        env.core
            .db
            .seed(&format!(
                "UPDATE reward_versions SET state = 'TESTING', state_version = state_version + 1,
                    test_job_id = '{job}', test_summary = NULL WHERE id = '{version}';"
            ))
            .await;
    }
    sqlx::query_as::<_, JobRow>("SELECT * FROM jobs WHERE id = $1")
        .bind(job)
        .fetch_one(env.core.db.pool())
        .await
        .unwrap()
}

async fn settle_check(env: &Env, job: &JobRow, outcome: &str) {
    use mrl_server::modules::events::{Event, EventConsumer};
    use mrl_server::modules::rewards::verifier::EndedEarly;
    let db = env.core.db.db().clone();
    let mut tx = db.begin().await.unwrap();
    let event = Event {
        id: 0,
        event_id: Uuid::now_v7(),
        org_id: Some(job.org_id),
        aggregate_kind: "job".into(),
        aggregate_id: job.id,
        aggregate_seq: job.state_version + 1,
        event_type: outcome.to_owned(),
        schema_version: 1,
        payload: json!({}),
        trace_context: None,
        created_at: time::OffsetDateTime::now_utc(),
        xid: 0,
    };
    EndedEarly::new(db).handle(&mut tx, &event).await.unwrap();
    tx.commit().await.unwrap();
}

fn case(position: i32, correct: f64) -> Value {
    json!({"position": position, "total": correct, "resolution": "SCORED",
           "components": {"correct": correct, "format": 1.0}, "flags": [],
           "statuses": {"correct": "OK", "format": "OK"}, "repeatable": true})
}

fn report(version: &str, mode: &str, cases: Value) -> Value {
    json!({"format": "mrl-reward-report/1", "mode": mode, "reward_version_id": version,
           "content_sha256": "e".repeat(64), "cases": cases, "sandbox": {"profile": "reward"}})
}

fn uuid_of(id: &str) -> Uuid {
    RewardVersionId::from_str(id).unwrap().as_uuid()
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn p29_versions_are_content_hashed_idempotent_validated_and_org_scoped() {
    let env = env().await;
    let prj = env.prj;
    let made = env
        .call(
            Method::POST,
            &format!("/api/v1/projects/{prj}/rewards"),
            Some(json!({
            "name": "math", "description": "Correct answers.",
            "version": {"policy": policy(1.0), "test_cases": cases()}})),
        )
        .await;
    assert_eq!(made.status, 201, "{}", made.text());
    let made = made.json();
    let rwd = made["id"].as_str().unwrap().to_owned();
    let v1 = made["versions"][0]["id"].as_str().unwrap().to_owned();
    assert_eq!(made["versions"][0]["state"], "DRAFT");
    // The same content (keys in another order, 1 written as 1.0) is the same version.
    let reordered: Value = serde_json::from_str(
        &policy(1.0)
            .to_string()
            .replace("\"weight\":1.0", "\"weight\":1"),
    )
    .unwrap();
    let again = env
        .call(
            Method::POST,
            &format!("/api/v1/rewards/{rwd}/versions"),
            Some(json!({"test_cases": cases(), "policy": reordered})),
        )
        .await;
    assert_eq!(again.status, 200, "{}", again.text());
    assert_eq!(again.json()["id"], v1.as_str());
    let v2 = env
        .call(
            Method::POST,
            &format!("/api/v1/rewards/{rwd}/versions"),
            Some(json!({"policy": policy(2.0), "test_cases": cases()})),
        )
        .await;
    assert_eq!(v2.status, 201, "{}", v2.text());
    let v2 = v2.json();
    assert_eq!(v2["version_no"], 2);
    assert_ne!(v2["content_sha256"], again.json()["content_sha256"]);
    assert_eq!(
        v2["hash_inputs"]["executor_sha256"].as_str().unwrap().len(),
        64
    );
    assert_eq!(v2["test_cases"].as_array().unwrap().len(), 2);
    assert_eq!(v2["publishable"]["ok"], false);
    // A taken name, a judge model, a learned reward model, a bad test case.
    let dup = env
        .call(
            Method::POST,
            &format!("/api/v1/projects/{prj}/rewards"),
            Some(json!({"name": "math"})),
        )
        .await;
    assert_eq!(
        (dup.status, dup.json()["code"].clone()),
        (409, json!("MRL-CONFLICT-290"))
    );
    for r in ["judge/helpfulness@1", "builtin/model.reward@1"] {
        let mut p = policy(1.0);
        p["spec"]["components"][0]["use"] = json!(r);
        let bad = env
            .call(
                Method::POST,
                &format!("/api/v1/rewards/{rwd}/versions"),
                Some(json!({"policy": p})),
            )
            .await;
        assert_eq!(bad.status, 422, "{}", bad.text());
        assert_eq!(bad.json()["code"], "MRL-RWD-290", "{}", bad.text());
        assert_eq!(
            bad.json()["errors"][0]["path"],
            "/policy/spec/components/0/use"
        );
    }
    let bad_case = env
        .call(Method::POST, &format!("/api/v1/rewards/{rwd}/versions"),
              Some(json!({"policy": policy(3.0), "test_cases": [{"name": "x", "sample": {"prompt": "p", "completion": "c"}, "expect": {}}]})))
        .await;
    assert_eq!(
        bad_case.json()["code"],
        "MRL-VAL-290",
        "{}",
        bad_case.text()
    );
    let unknown = env
        .call(
            Method::POST,
            &format!("/api/v1/rewards/{rwd}/versions"),
            Some(json!({"policy": {"apiVersion": "mrl.dev/reward/v1"}})),
        )
        .await;
    assert_eq!(unknown.json()["code"], "MRL-RWD-140", "{}", unknown.text());
    // Lists and the component list.
    let list = env
        .call(
            Method::GET,
            &format!("/api/v1/projects/{prj}/rewards"),
            None,
        )
        .await;
    assert_eq!(
        list.json()["items"][0]["versions"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let comps = env
        .call(Method::GET, "/api/v1/reward-components", None)
        .await;
    assert_eq!(comps.status, 200);
    let items = comps.json()["items"].as_array().unwrap().clone();
    let unit = items
        .iter()
        .find(|c| c["ref"] == "builtin/code.unit_tests@1")
        .unwrap();
    assert_eq!(
        (unit["tier"].clone(), unit["listed"].clone()),
        (json!(2), json!(true))
    );
    // Another org's person sees none of it (404), and a viewer can't write (403).
    let (other_org, other_prj) = (OrgId::new(), ProjectId::new());
    seed_org(&env.core, other_org, other_prj).await;
    let stranger = person(&env, "stranger", other_org, Role::Owner, Scope::Org).await;
    for (m, path, body) in [
        (Method::GET, format!("/api/v1/rewards/{rwd}"), None),
        (Method::GET, format!("/api/v1/reward-versions/{v1}"), None),
        (Method::GET, format!("/api/v1/projects/{prj}/rewards"), None),
        (
            Method::POST,
            format!("/api/v1/rewards/{rwd}/versions"),
            Some(json!({"policy": policy(4.0)})),
        ),
        (
            Method::POST,
            format!("/api/v1/reward-versions/{v1}:publish"),
            None,
        ),
    ] {
        let r = env.core.call(m.clone(), &path, &stranger, body).await;
        assert_eq!(r.status, 404, "{m} {path}: {}", r.text());
    }
    let viewer = person(&env, "viewer", env.org, Role::Viewer, Scope::Org).await;
    let r = env
        .core
        .call(
            Method::POST,
            &format!("/api/v1/rewards/{rwd}/versions"),
            &viewer,
            Some(json!({"policy": policy(5.0)})),
        )
        .await;
    assert_eq!(r.status, 403, "{}", r.text());
    assert_eq!(
        env.core
            .call(
                Method::GET,
                &format!("/api/v1/rewards/{rwd}"),
                &viewer,
                None
            )
            .await
            .status,
        200
    );
    // Creation is audited inside its transaction.
    let audited: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit_events WHERE action LIKE 'rewards.%'")
            .fetch_one(env.core.db.pool())
            .await
            .unwrap();
    assert_eq!(audited, 3, "create + two new versions");
    env.core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn p29_publish_needs_passing_sandbox_tests_and_a_regression_report() {
    let env = env().await;
    let db = env.core.db.db().clone();
    let fixtures = Arc::new(Fixtures::default());
    let verifier = ReportVerifier::new(db.clone(), fixtures.clone(), 0.9);
    let prj = env.prj;
    let made = env
        .call(
            Method::POST,
            &format!("/api/v1/projects/{prj}/rewards"),
            Some(
                json!({"name": "gate", "version": {"policy": policy(1.0), "test_cases": cases()}}),
            ),
        )
        .await
        .json();
    let rwd = made["id"].as_str().unwrap().to_owned();
    let v1 = made["versions"][0]["id"].as_str().unwrap().to_owned();
    let publish = |v: String| {
        let env = &env;
        async move {
            env.call(
                Method::POST,
                &format!("/api/v1/reward-versions/{v}:publish"),
                None,
            )
            .await
        }
    };
    // No tests yet: refused.
    let r = publish(v1.clone()).await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (409, json!("MRL-STATE-291")),
        "{}",
        r.text()
    );
    // A failing case → FAILED_TESTS; the job itself is verified (its report was sound).
    let job = preview_job(&env, uuid_of(&v1), "test", None).await;
    fixtures.put(
        job.id,
        report(&v1, "test", json!([case(0, 1.0), case(1, 1.0)])),
        None,
    );
    assert_eq!(verifier.verify(&job).await.unwrap(), Verification::Verified);
    settle_check(&env, &job, "job.completed").await;
    let got = env
        .call(Method::GET, &format!("/api/v1/reward-versions/{v1}"), None)
        .await
        .json();
    assert_eq!(got["state"], "FAILED_TESTS");
    assert_eq!(got["test_cases"][1]["last_result"]["passed"], false);
    assert_eq!(
        got["test_cases"][1]["last_result"]["failures"][0],
        "component correct was 1"
    );
    let r = publish(v1.clone()).await;
    assert_eq!(r.json()["code"], "MRL-STATE-291");
    // A report the job's own pass/fail would accept but that leaves a case out is refused.
    sqlx::query("UPDATE reward_versions SET state = 'DRAFT' WHERE id = $1")
        .bind(uuid_of(&v1))
        .execute(env.core.db.pool())
        .await
        .unwrap();
    let job = preview_job(&env, uuid_of(&v1), "test", None).await;
    fixtures.put(job.id, report(&v1, "test", json!([case(0, 1.0)])), None);
    assert!(matches!(
        verifier.verify(&job).await.unwrap(),
        Verification::Failed(_)
    ));
    settle_check(&env, &job, "job.failed").await;
    let st: (String, String) = sqlx::query_as("SELECT r.state, v.state FROM reward_reports r JOIN reward_versions v ON v.id = r.version_id WHERE r.job_id = $1")
        .bind(job.id).fetch_one(env.core.db.pool()).await.unwrap();
    assert_eq!(st, ("INVALID".into(), "FAILED_TESTS".into()));
    // Passing cases → stays TESTING with a passing summary → publish works (and repeats).
    sqlx::query("UPDATE reward_versions SET state = 'DRAFT' WHERE id = $1")
        .bind(uuid_of(&v1))
        .execute(env.core.db.pool())
        .await
        .unwrap();
    let job = preview_job(&env, uuid_of(&v1), "test", None).await;
    fixtures.put(
        job.id,
        report(&v1, "test", json!([case(0, 1.0), case(1, 0.0)])),
        None,
    );
    assert_eq!(verifier.verify(&job).await.unwrap(), Verification::Verified);
    assert_eq!(publish(v1.clone()).await.status, 409);
    settle_check(&env, &job, "job.completed").await;
    // A replay after a crash finds the record settled: same answer, nothing written twice.
    let events = |id: Uuid| {
        let db = env.core.db.pool().clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM outbox_events WHERE aggregate_id = $1",
            )
            .bind(id)
            .fetch_one(&db)
            .await
            .unwrap()
        }
    };
    let before = events(uuid_of(&v1)).await;
    assert_eq!(verifier.verify(&job).await.unwrap(), Verification::Verified);
    settle_check(&env, &job, "job.completed").await;
    assert_eq!(events(uuid_of(&v1)).await, before);
    let got = env
        .call(Method::GET, &format!("/api/v1/reward-versions/{v1}"), None)
        .await
        .json();
    assert_eq!(
        (got["state"].clone(), got["test_summary"]["passed"].clone()),
        (json!("TESTING"), json!(true))
    );
    assert_eq!(got["exec_sha256"], "e".repeat(64));
    assert_eq!(got["publishable"]["ok"], true);
    let r = publish(v1.clone()).await;
    assert_eq!(
        (r.status, r.json()["state"].clone()),
        (200, json!("PUBLISHED")),
        "{}",
        r.text()
    );
    assert_eq!(
        publish(v1.clone()).await.status,
        200,
        "publishing again answers the same"
    );
    // v2 passes its tests but needs a regression report against v1 first.
    let v2 = env
        .call(
            Method::POST,
            &format!("/api/v1/rewards/{rwd}/versions"),
            Some(json!({"policy": policy(2.0), "test_cases": cases()})),
        )
        .await
        .json()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let job = preview_job(&env, uuid_of(&v2), "test", None).await;
    fixtures.put(
        job.id,
        report(&v2, "test", json!([case(0, 1.0), case(1, 0.0)])),
        None,
    );
    assert_eq!(verifier.verify(&job).await.unwrap(), Verification::Verified);
    settle_check(&env, &job, "job.completed").await;
    let r = publish(v2.clone()).await;
    assert_eq!(r.json()["code"], "MRL-STATE-291", "{}", r.text());
    assert!(r.json()["detail"].as_str().unwrap().contains("regression"));
    let job = preview_job(&env, uuid_of(&v2), "regression", Some(uuid_of(&v1))).await;
    let mut reg = report(&v2, "regression", json!([case(0, 1.0), case(1, 1.0)]));
    reg["other_content_sha256"] = json!("e".repeat(64));
    reg["baseline_cases"] = json!([case(0, 1.0), case(1, 0.0)]);
    fixtures.put(job.id, reg, None);
    assert_eq!(verifier.verify(&job).await.unwrap(), Verification::Verified);
    settle_check(&env, &job, "job.completed").await;
    let got = env
        .call(Method::GET, &format!("/api/v1/reward-versions/{v2}"), None)
        .await
        .json();
    assert_eq!(got["regression"]["newly_failing"], json!(["wrong"]));
    assert_eq!(got["regression"]["against_version_id"], v1.as_str());
    assert_eq!(publish(v2.clone()).await.status, 200);
    // Deprecate: only published versions; a deprecated one stays deprecated.
    let r = env
        .call(
            Method::POST,
            &format!("/api/v1/reward-versions/{v1}:deprecate"),
            None,
        )
        .await;
    assert_eq!(
        (r.status, r.json()["state"].clone()),
        (200, json!("DEPRECATED"))
    );
    let v3 = env
        .call(
            Method::POST,
            &format!("/api/v1/rewards/{rwd}/versions"),
            Some(json!({"policy": policy(3.0)})),
        )
        .await
        .json()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let r = env
        .call(
            Method::POST,
            &format!("/api/v1/reward-versions/{v3}:deprecate"),
            None,
        )
        .await;
    assert_eq!(r.json()["code"], "MRL-STATE-290", "{}", r.text());
    let r = env
        .call(
            Method::POST,
            &format!("/api/v1/reward-versions/{v3}:frobnicate"),
            None,
        )
        .await;
    assert_eq!(r.status, 404);
    // Preview and compare reports get server-computed statistics; other jobs are not ours.
    let job = preview_job(&env, uuid_of(&v2), "preview", None).await;
    let rows = (0..8)
        .map(|i| {
            json!({"i": i, "group_id": format!("g{}", i / 4), "total": 1.0, "resolution": "SCORED",
                        "status": "OK", "completion_tokens": i})
            .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    fixtures.put(job.id, report(&v2, "preview", json!([])), Some(&rows));
    assert_eq!(verifier.verify(&job).await.unwrap(), Verification::Verified);
    settle_check(&env, &job, "job.completed").await;
    let results: Value = sqlx::query_scalar("SELECT results FROM reward_reports WHERE job_id = $1")
        .bind(job.id)
        .fetch_one(env.core.db.pool())
        .await
        .unwrap();
    assert_eq!(results["summary"]["learnability"]["warning"], true);
    assert_eq!(results["summary"]["tag"], "sampled");
    let mut other = job.clone();
    other.kind = "DATA_PREP".into();
    assert_eq!(
        verifier.verify(&other).await.unwrap(),
        Verification::Abstain
    );
    let publishes: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit_events WHERE action = 'rewards.publish'")
            .fetch_one(env.core.db.pool())
            .await
            .unwrap();
    assert_eq!(publishes, 2);
    let _ = JobId::from_uuid(job.id);
    env.core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn p29_published_versions_and_their_cases_are_immutable_in_the_database() {
    let env = env().await;
    let prj = env.prj;
    let made = env
        .call(Method::POST, &format!("/api/v1/projects/{prj}/rewards"),
              Some(json!({"name": "frozen", "version": {"policy": policy(1.0), "test_cases": cases()}})))
        .await
        .json();
    let v = uuid_of(made["versions"][0]["id"].as_str().unwrap());
    let pool = env.core.db.pool();
    let refused = |sql: &'static str| async move {
        let e = sqlx::query(sql).bind(v).execute(pool).await;
        assert!(e.is_err(), "{sql} must be refused");
        assert!(e.unwrap_err().to_string().contains("reward version"));
    };
    // Content never changes, even as a DRAFT.
    refused("UPDATE reward_versions SET notes = 'x' WHERE id = $1").await;
    refused("UPDATE reward_test_cases SET expect = '{}' WHERE version_id = $1").await;
    sqlx::query("UPDATE reward_versions SET state = 'PUBLISHED', test_summary = '{\"passed\": true}' WHERE id = $1")
        .bind(v).execute(pool).await.unwrap();
    refused("UPDATE reward_versions SET policy = '{}' WHERE id = $1").await;
    refused("UPDATE reward_versions SET state = 'DRAFT' WHERE id = $1").await;
    refused("UPDATE reward_versions SET test_summary = NULL WHERE id = $1").await;
    refused("UPDATE reward_test_cases SET last_result = '{}' WHERE version_id = $1").await;
    refused("DELETE FROM reward_versions WHERE id = $1").await;
    sqlx::query("UPDATE reward_versions SET state = 'DEPRECATED' WHERE id = $1")
        .bind(v)
        .execute(pool)
        .await
        .unwrap();
    refused("UPDATE reward_versions SET state = 'PUBLISHED' WHERE id = $1").await;
    env.core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn p29_report_pages_hide_other_projects_and_exclude_their_cursors() {
    let env = env().await;
    let other = add_project(&env).await;
    let make = |project: ProjectId| {
        let env = &env;
        async move {
            let response = env
                .call(
                    Method::POST,
                    &format!("/api/v1/projects/{project}/rewards"),
                    Some(json!({"name": "math", "version": {"policy": policy(1.0)}})),
                )
                .await;
            assert_eq!(response.status, 201, "{}", response.text());
            response.json()["versions"][0]["id"]
                .as_str()
                .unwrap()
                .to_owned()
        }
    };
    let a = make(env.prj).await;
    let b = make(other).await;
    for _ in 0..2 {
        preview_job(&env, uuid_of(&a), "compare", Some(uuid_of(&b))).await;
    }
    let path = format!("/api/v1/reward-versions/{b}/reports?limit=1");
    let accessible = env.call(Method::GET, &path, None).await;
    assert_eq!(accessible.status, 200, "{}", accessible.text());
    assert_eq!(accessible.json()["items"].as_array().unwrap().len(), 1);
    assert!(accessible.json()["next_cursor"].is_string());
    let reader = person(
        &env,
        "other-viewer",
        env.org,
        Role::Viewer,
        Scope::Project(other),
    )
    .await;
    let hidden = env.core.call(Method::GET, &path, &reader, None).await;
    assert_eq!(hidden.status, 200, "{}", hidden.text());
    assert_eq!(hidden.json()["items"], json!([]));
    assert!(hidden.json()["next_cursor"].is_null());
    env.core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn p29_a_run_can_launch_only_with_a_published_reward() {
    use mrl_domain::ids::{DatasetVersionId, ModelVersionId};
    use mrl_server::modules::jobs::AttemptRow;
    use mrl_server::modules::rewards::launch::RewardInputs;
    use mrl_server::modules::scheduler::pipeline::{LaunchContributor, LaunchPlan, Placement};
    let env = env().await;
    let prj = env.prj;
    let made = env
        .call(
            Method::POST,
            &format!("/api/v1/projects/{prj}/rewards"),
            Some(json!({"name": "pin", "version": {"policy": policy(1.0)}})),
        )
        .await
        .json();
    let vtext = made["versions"][0]["id"].as_str().unwrap().to_owned();
    let body = json!({
        "name": "r", "algorithm_id": "grpo", "reward_version_id": vtext,
        "base_model_version_id": ModelVersionId::new().to_string(),
        "dataset_version_ids": [DatasetVersionId::new().to_string()], "config": {}
    });
    let submit = |body: Value, key: &'static str| {
        let env = &env;
        async move {
            env.core
                .call_with(
                    Method::POST,
                    &format!("/api/v1/projects/{prj}/runs"),
                    &env.owner,
                    Some(body),
                    &[("idempotency-key", key)],
                )
                .await
        }
    };
    let mut missing = body.clone();
    missing["reward_version_id"] = json!(RewardVersionId::new().to_string());
    for (input, key) in [(missing, "p29-missing"), (body.clone(), "p29-draft")] {
        let response = submit(input, key).await;
        assert_eq!(response.status, 422, "{}", response.text());
        assert_eq!(response.json()["errors"][0]["path"], "/reward_version_id");
    }
    sqlx::query("UPDATE reward_versions SET state = 'PUBLISHED', exec_sha256 = $2 WHERE id = $1")
        .bind(uuid_of(&vtext))
        .bind("d".repeat(64))
        .execute(env.core.db.pool())
        .await
        .unwrap();
    let other_project = add_project(&env).await;
    let refused = env
        .core
        .call_with(
            Method::POST,
            &format!("/api/v1/projects/{other_project}/runs"),
            &env.owner,
            Some(body.clone()),
            &[("idempotency-key", "p29-other-project")],
        )
        .await;
    assert_eq!(refused.status, 422, "{}", refused.text());
    assert_eq!(refused.json()["errors"][0]["path"], "/reward_version_id");

    let run = submit(body.clone(), "p29-run").await;
    assert_eq!(run.status, 201, "{}", run.text());
    let run_id: mrl_domain::ids::RunId = run.json()["id"].as_str().unwrap().parse().unwrap();
    let edge: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM lineage_edges WHERE org_id = $1 AND from_kind = 'run'
           AND from_id = $2 AND relation = 'rewarded_by' AND to_kind = 'rwdv' AND to_id = $3",
    )
    .bind(env.org.to_string())
    .bind(run_id.as_uuid())
    .bind(uuid_of(&vtext))
    .fetch_one(env.core.db.pool())
    .await
    .unwrap();
    assert_eq!(edge, 1);
    let deprecated = env
        .call(
            Method::POST,
            &format!("/api/v1/reward-versions/{vtext}:deprecate"),
            None,
        )
        .await;
    assert_eq!(deprecated.status, 200, "{}", deprecated.text());
    let new_pin = submit(body, "p29-deprecated").await;
    assert_eq!(new_pin.status, 422, "{}", new_pin.text());
    assert_eq!(new_pin.json()["errors"][0]["path"], "/reward_version_id");
    let job_id = mrl_domain::ids::JobId::from_str(run.json()["job_id"].as_str().unwrap())
        .unwrap()
        .as_uuid();
    let job: JobRow = sqlx::query_as("SELECT * FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(env.core.db.pool())
        .await
        .unwrap();
    let (a, o) = (Uuid::now_v7(), env.org.as_uuid());
    env.core.db.seed(&format!(
        "INSERT INTO job_attempts (id, org_id, job_id, attempt_no, epoch, state) VALUES ('{a}', '{o}', '{job_id}', 1, 1, 'PENDING');"
    )).await;
    let attempt: AttemptRow = sqlx::query_as("SELECT * FROM job_attempts WHERE id = $1")
        .bind(a)
        .fetch_one(env.core.db.pool())
        .await
        .unwrap();
    let placement = Placement {
        node_id: Uuid::nil(),
        roles: vec![],
        cpu_millis: 0,
        mem_bytes: 0,
    };
    let store = mrl_server::modules::artifacts::ArtifactsModule::new(
        env.core.db.db().clone(),
        &env.core.config,
    )
    .store();
    let contributor = RewardInputs::with_store(env.core.db.db().clone(), store);
    let mut plan = LaunchPlan::default();
    plan.launch.job_spec_json = json!({"run_id": "x"}).to_string();
    contributor
        .contribute(&job, &attempt, &placement, &mut plan)
        .await
        .unwrap();
    let spec: Value = serde_json::from_str(&plan.launch.job_spec_json).unwrap();
    assert_eq!(spec["run_id"], "x");
    assert_eq!(spec["reward"]["version_id"], vtext.as_str());
    assert_eq!(spec["reward"]["exec_sha256"], "d".repeat(64));
    assert_eq!(spec["reward"]["policy"]["kind"], "RewardPolicy");
    assert!(plan.inputs.is_empty(), "built-ins only: no code files");
    env.core.stop().await;
}

// ---- Milestone 2: checks as REWARD_PREVIEW built-in jobs (INT-6) ----

fn store_of(env: &Env) -> Arc<mrl_server::modules::artifacts::ArtifactStore> {
    mrl_server::modules::artifacts::ArtifactsModule::new(env.core.db.db().clone(), &env.core.config)
        .store()
}

fn org_text(env: &Env) -> String {
    mrl_server::modules::jobs::scope::org_text(env.org.as_uuid()).unwrap()
}

/// A COMMITTED artifact of the project holding `files` (stored as checked blob copies, as P19
/// leaves them).
async fn seed_artifact<P: AsRef<str>>(
    env: &Env,
    name: &str,
    kind: &str,
    files: &[(P, Vec<u8>)],
) -> Uuid {
    let org = org_text(env);
    let st = store_of(env).org_store(&org).await.unwrap();
    let mut listed = Vec::new();
    let mut total = 0;
    for (path, bytes) in files {
        let sha = mrl_storage::sha256_hex(bytes);
        let key = st.keys().blob(&sha).unwrap();
        st.put(&key, bytes.clone().into()).await.unwrap();
        listed.push(
            json!({"path": path.as_ref(), "size": bytes.len(), "sha256": sha, "role": "data"}),
        );
        total += bytes.len();
    }
    let manifest = json!({"schema_version": "artifact-manifest/v1", "kind": kind,
        "created_at": "2026-11-27T00:00:00Z", "files": listed});
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO artifacts (id, org_id, project_id, name, kind, prefix, manifest, size_bytes,
            state, created_by, committed_at, files_checked)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'COMMITTED', 'test', now(), true)",
    )
    .bind(id)
    .bind(&org)
    .bind(env.prj.to_string())
    .bind(name)
    .bind(kind)
    .bind(format!("test/{id}/"))
    .bind(&manifest)
    .bind(total as i64)
    .execute(env.core.db.pool())
    .await
    .unwrap();
    id
}

/// What INT-6 records once a job's outputs are uploaded: their artifact on the job's row.
async fn register_outputs<P: AsRef<str>>(env: &Env, job: Uuid, files: &[(P, Vec<u8>)]) -> Uuid {
    let art = seed_artifact(env, &format!("outputs-{job}"), "eval", files).await;
    sqlx::query("UPDATE builtin_jobs SET artifact_id = $2 WHERE job_id = $1")
        .bind(job)
        .bind(art)
        .execute(env.core.db.pool())
        .await
        .unwrap();
    art
}

async fn job_row(env: &Env, id: Uuid) -> JobRow {
    sqlx::query_as::<_, JobRow>("SELECT * FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_one(env.core.db.pool())
        .await
        .unwrap()
}

/// `(task, inputs, files)` of a built-in job.
async fn builtin_row(env: &Env, job: Uuid) -> (String, Value, Value) {
    sqlx::query_as("SELECT task, inputs, files FROM builtin_jobs WHERE job_id = $1")
        .bind(job)
        .fetch_one(env.core.db.pool())
        .await
        .unwrap()
}

fn job_of(report: &Value) -> Uuid {
    JobId::from_str(report["job_id"].as_str().unwrap())
        .unwrap()
        .as_uuid()
}

/// The workspace Python with mrl-runtime and mrl-rewards (`uv sync`), or `None` (the test
/// then writes the report itself). `MRL_TEST_REWARDS_PYTHON_REQUIRED=1` makes it required.
fn task_python() -> Option<std::path::PathBuf> {
    let found = std::env::var_os("MRL_TEST_PYTHON")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            [".venv/bin/python", ".venv/Scripts/python.exe"]
                .iter()
                .map(|p| root.join(p))
                .find(|p| p.is_file())
        })
        .filter(|_| !cfg!(windows)); // the reward service listens on a Unix socket
    if found.is_none() && std::env::var("MRL_TEST_REWARDS_PYTHON_REQUIRED").as_deref() == Ok("1") {
        panic!("MRL_TEST_REWARDS_PYTHON_REQUIRED=1 but no Python with mrl-runtime (run uv sync)");
    }
    found
}

const RUN_TASK: &str = r#"
import json, sys
from pathlib import Path
from mrl_runtime.builtin.reward_preview import REWARD_PREVIEW
from mrl_runtime.builtin.tasks import TaskContext, TaskOutput
inputs = json.loads(Path(sys.argv[1]).read_text())
staged = {k: Path(v) for k, v in json.loads(Path(sys.argv[2]).read_text()).items()}
issues = REWARD_PREVIEW.validate(inputs, staged)
assert not issues, issues
events = list(REWARD_PREVIEW.run(TaskContext(inputs, staged, Path(sys.argv[3]), lambda: False)))
print(json.dumps([e.path for e in events if isinstance(e, TaskOutput)]))
"#;

/// Runs the job's real `reward_preview` task: stages its files from storage the way the
/// agent does (a folder input when the file sits under `<name>/`), then returns its outputs.
async fn run_task(env: &Env, python: &std::path::Path, job: Uuid) -> Vec<(String, Vec<u8>)> {
    let (task, inputs, files) = builtin_row(env, job).await;
    assert_eq!(task, "reward_preview");
    let dir = tempfile::Builder::new()
        .prefix("p29")
        .tempdir_in("/tmp")
        .unwrap();
    let st = store_of(env).org_store(&org_text(env)).await.unwrap();
    let mut staged = serde_json::Map::new();
    for f in files.as_array().unwrap() {
        let (name, local) = (
            f["name"].as_str().unwrap(),
            f["local_path"].as_str().unwrap(),
        );
        let dest = dir.path().join("inputs").join(local);
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        let bytes = st.get(f["key"].as_str().unwrap()).await.unwrap();
        assert_eq!(
            mrl_storage::sha256_hex(&bytes),
            f["sha256"].as_str().unwrap()
        );
        std::fs::write(&dest, &bytes).unwrap();
        let folder = local.split('/').count() > 1 && local.starts_with(&format!("{name}/"));
        let shown = if folder {
            dir.path().join("inputs").join(name)
        } else {
            dest
        };
        staged.insert(name.to_owned(), json!(shown.display().to_string()));
    }
    let (i, s, out) = (
        dir.path().join("inputs.json"),
        dir.path().join("staged.json"),
        dir.path().join("out"),
    );
    std::fs::write(&i, inputs.to_string()).unwrap();
    std::fs::write(&s, Value::Object(staged).to_string()).unwrap();
    let ran = tokio::process::Command::new(python)
        .args(["-c", RUN_TASK])
        .arg(&i)
        .arg(&s)
        .arg(&out)
        .env("TMPDIR", "/tmp")
        .output()
        .await
        .unwrap();
    assert!(
        ran.status.success(),
        "the task failed: {}",
        String::from_utf8_lossy(&ran.stderr)
    );
    let paths: Vec<String> = serde_json::from_slice(
        ran.stdout
            .split(|b| *b == b'\n')
            .rfind(|l| !l.is_empty())
            .unwrap(),
    )
    .unwrap();
    paths
        .into_iter()
        .map(|p| {
            let bytes = std::fs::read(out.join(&p)).unwrap();
            (p, bytes)
        })
        .collect()
}

async fn wait_for(what: &str, mut ok: impl AsyncFnMut() -> bool) {
    for _ in 0..300 {
        if ok().await {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs DATABASE_URL"]
async fn p29_a_test_run_is_a_reward_preview_job_whose_report_publishes_the_version() {
    let env = env().await;
    let prj = env.prj;
    let made = env
        .call(
            Method::POST,
            &format!("/api/v1/projects/{prj}/rewards"),
            Some(json!({"name": "e2e", "version": {"policy": policy(1.0), "test_cases": cases()}})),
        )
        .await
        .json();
    let v1 = made["versions"][0]["id"].as_str().unwrap().to_owned();
    let r = env
        .call(
            Method::POST,
            &format!("/api/v1/reward-versions/{v1}:test"),
            None,
        )
        .await;
    assert_eq!(r.status, 202, "{}", r.text());
    let started = r.json();
    assert_eq!(
        (started["mode"].clone(), started["state"].clone()),
        (json!("test"), json!("PENDING"))
    );
    let job = job_of(&started);

    // One QUEUED built-in job running the task with the version and its cases.
    let row = job_row(&env, job).await;
    assert_eq!(
        (row.kind.as_str(), row.state.as_str()),
        ("REWARD_PREVIEW", "QUEUED")
    );
    let (_, inputs, files) = builtin_row(&env, job).await;
    assert_eq!(inputs["mode"], "test");
    assert_eq!(inputs["reward"]["version_id"], v1.as_str());
    assert_eq!(inputs["reward"]["policy"]["kind"], "RewardPolicy");
    assert_eq!(inputs["cases"].as_array().unwrap().len(), 2);
    assert_eq!(inputs["cases"][0]["expect"]["components"]["correct"], 1.0);
    assert_eq!(files, json!([]), "built-in components only: no code files");
    let got = env
        .call(Method::GET, &format!("/api/v1/reward-versions/{v1}"), None)
        .await
        .json();
    assert_eq!(got["state"], "TESTING");
    let again = env
        .call(
            Method::POST,
            &format!("/api/v1/reward-versions/{v1}:test"),
            None,
        )
        .await;
    assert_eq!(
        (again.status, again.json()["code"].clone()),
        (409, json!("MRL-STATE-290"))
    );

    // The chain member waits (an error keeps the job FINALIZING) until the outputs are in.
    let verifier = ReportVerifier::registered(env.core.db.db().clone());
    assert!(verifier.verify(&row).await.is_err());

    // The real task, when this machine has the Python environment; else a report like it.
    let outputs = match task_python() {
        Some(py) => run_task(&env, &py, job).await,
        None => vec![(
            "report.json".to_owned(),
            report(&v1, "test", json!([case(0, 1.0), case(1, 0.0)]))
                .to_string()
                .into_bytes(),
        )],
    };
    assert!(
        outputs.iter().any(|(p, _)| p == "report.json"),
        "{outputs:?}"
    );
    let art = register_outputs(&env, job, &outputs).await;
    sqlx::query("UPDATE jobs SET state = 'FINALIZING' WHERE id = $1")
        .bind(job)
        .execute(env.core.db.pool())
        .await
        .unwrap();
    let row = job_row(&env, job).await;
    assert_eq!(verifier.verify(&row).await.unwrap(), Verification::Verified);
    settle_check(&env, &row, "job.completed").await;
    let got = env
        .call(Method::GET, &format!("/api/v1/reward-versions/{v1}"), None)
        .await
        .json();
    assert_eq!(got["state"], "TESTING");
    assert_eq!(got["test_summary"]["passed"], true, "{got}");
    assert_eq!(got["exec_sha256"].as_str().unwrap().len(), 64);
    assert_eq!(got["publishable"]["ok"], true, "{got}");
    let reports = env
        .call(
            Method::GET,
            &format!("/api/v1/reward-versions/{v1}/reports"),
            None,
        )
        .await
        .json();
    assert_eq!(reports["items"][0]["state"], "READY", "{reports}");
    assert_eq!(
        reports["items"][0]["artifact_id"],
        mrl_domain::ids::ArtifactId::from_uuid(art).to_string()
    );
    let r = env
        .call(
            Method::POST,
            &format!("/api/v1/reward-versions/{v1}:publish"),
            None,
        )
        .await;
    assert_eq!(
        (r.status, r.json()["state"].clone()),
        (200, json!("PUBLISHED")),
        "{}",
        r.text()
    );
    let audited: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_events WHERE action = 'rewards.test' AND target = $1",
    )
    .bind(&v1)
    .fetch_one(env.core.db.pool())
    .await
    .unwrap();
    assert_eq!(audited, 1);
    env.core.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs DATABASE_URL"]
async fn p29_checks_need_their_inputs_and_a_job_that_ends_early_fails_its_tests() {
    use mrl_domain::ids::ArtifactId;
    use mrl_server::modules::jobs::JobTransitions;
    let env = env().await;
    let prj = env.prj;
    let call = |m: Method, path: String, body: Option<Value>| {
        let env = &env;
        async move { env.call(m, &format!("/api/v1{path}"), body).await }
    };
    let made = call(
        Method::POST,
        format!("/projects/{prj}/rewards"),
        Some(json!({"name": "checks", "version": {"policy": policy(1.0)}})),
    )
    .await
    .json();
    let rwd = made["id"].as_str().unwrap().to_owned();
    let v0 = made["versions"][0]["id"].as_str().unwrap().to_owned();

    // Nothing to check against: no test cases; no published earlier version.
    for action in ["test", "regression"] {
        let r = call(
            Method::POST,
            format!("/reward-versions/{v0}:{action}"),
            None,
        )
        .await;
        assert_eq!(
            (r.status, r.json()["code"].clone()),
            (409, json!("MRL-STATE-292")),
            "{action}: {}",
            r.text()
        );
    }
    // Preview settings are checked before anything starts.
    let r = call(
        Method::POST,
        format!("/reward-versions/{v0}:preview"),
        Some(json!({"dataset_artifact_id": "art_x", "n": 0, "completions": "bogus"})),
    )
    .await;
    assert_eq!(r.status, 422, "{}", r.text());
    let paths: Vec<String> = r.json()["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(paths, ["/n", "/completions"]);
    let r = call(
        Method::POST,
        format!("/reward-versions/{v0}:preview"),
        Some(json!({"dataset_artifact_id": ArtifactId::new().to_string()})),
    )
    .await;
    assert_eq!(r.status, 404, "{}", r.text());
    assert!(r.text().contains("No artifact"), "{}", r.text());

    // A preview stages the dataset as the folder input `dataset`.
    let rows: Vec<u8> = [
        ("1+1?", "<answer>2</answer>", "2"),
        ("2+2?", "<answer>5</answer>", "4"),
    ]
    .iter()
    .map(|(p, c, r)| json!({"prompt": p, "completion": c, "reference": r}).to_string() + "\n")
    .collect::<String>()
    .into_bytes();
    let ds = seed_artifact(&env, "rows", "dataset", &[("part-0.jsonl", rows)]).await;
    let ds_id = ArtifactId::from_uuid(ds).to_string();
    let r = call(
        Method::POST,
        format!("/reward-versions/{v0}:preview"),
        Some(json!({"dataset_artifact_id": ds_id, "n": 2, "seed": 7})),
    )
    .await;
    assert_eq!(r.status, 202, "{}", r.text());
    let preview = job_of(&r.json());
    let (_, inputs, files) = builtin_row(&env, preview).await;
    assert_eq!(inputs["mode"], "preview");
    assert_eq!(
        (
            inputs["preview"]["dataset"].clone(),
            inputs["preview"]["n"].clone()
        ),
        (json!("dataset"), json!(2))
    );
    assert_eq!(
        (files[0]["name"].clone(), files[0]["local_path"].clone()),
        (json!("dataset"), json!("dataset/part-0.jsonl"))
    );
    if let Some(py) = task_python() {
        let outputs = run_task(&env, &py, preview).await;
        register_outputs(&env, preview, &outputs).await;
        let verifier = ReportVerifier::registered(env.core.db.db().clone());
        sqlx::query("UPDATE jobs SET state = 'FINALIZING' WHERE id = $1")
            .bind(preview)
            .execute(env.core.db.pool())
            .await
            .unwrap();
        let row = job_row(&env, preview).await;
        assert_eq!(verifier.verify(&row).await.unwrap(), Verification::Verified);
        settle_check(&env, &row, "job.completed").await;
        let listed = call(Method::GET, format!("/reward-versions/{v0}/reports"), None)
            .await
            .json();
        let summary = &listed["items"][0]["results"]["summary"];
        assert_eq!(summary["samples"], 2, "{listed}");
        assert!(summary["learnability"].is_object(), "{listed}");
    }

    // Comparing two versions of one org; a version with itself is refused.
    let v1 = call(
        Method::POST,
        format!("/rewards/{rwd}/versions"),
        Some(json!({"policy": policy(2.0), "test_cases": cases()})),
    )
    .await
    .json()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let r = call(
        Method::POST,
        "/rewards:compare".into(),
        Some(json!({"a": v0, "b": v1, "samples": {"dataset_artifact_id": ds_id}})),
    )
    .await;
    assert_eq!(r.status, 202, "{}", r.text());
    assert_eq!(r.json()["other_version_id"], v1.as_str());
    let (_, inputs, _) = builtin_row(&env, job_of(&r.json())).await;
    assert_eq!(
        (
            inputs["mode"].clone(),
            inputs["other"]["version_id"].clone()
        ),
        (json!("compare"), json!(v1))
    );
    let r = call(
        Method::POST,
        "/rewards:compare".into(),
        Some(json!({"a": v0, "b": v0, "samples": {"dataset_artifact_id": ds_id}})),
    )
    .await;
    assert_eq!(r.status, 422, "{}", r.text());

    // A test job that ends without a report (here: cancelled) fails the version's tests, so
    // it can be tested again.
    let r = call(Method::POST, format!("/reward-versions/{v1}:test"), None).await;
    assert_eq!(r.status, 202, "{}", r.text());
    let test_job = job_of(&r.json());
    let mut tx = env.core.db.db().begin().await.unwrap();
    let row = JobTransitions::lock(&mut tx, env.org.as_uuid(), test_job)
        .await
        .unwrap()
        .unwrap();
    let row = JobTransitions::request_cancel(&mut tx, &row, "test")
        .await
        .unwrap();
    assert_eq!(row.state, "CANCELLED", "no attempt yet: cancelled at once");
    tx.commit().await.unwrap();
    wait_for("the cancelled test run settled", async || {
        let st: (String, String) = sqlx::query_as(
            "SELECT r.state, v.state FROM reward_reports r JOIN reward_versions v ON v.id = r.version_id WHERE r.job_id = $1",
        )
        .bind(test_job)
        .fetch_one(env.core.db.pool())
        .await
        .unwrap();
        st == ("INVALID".to_owned(), "FAILED_TESTS".to_owned())
    })
    .await;
    let reason: String = sqlx::query_scalar("SELECT reason FROM reward_reports WHERE job_id = $1")
        .bind(test_job)
        .fetch_one(env.core.db.pool())
        .await
        .unwrap();
    assert!(reason.contains("cancelled"), "{reason}");
    let r = call(Method::POST, format!("/reward-versions/{v1}:test"), None).await;
    assert_eq!(r.status, 202, "{}", r.text());

    // With a published earlier version, a new version's regression run scores its cases.
    sqlx::query("UPDATE reward_versions SET state = 'PUBLISHED', published_at = now(), exec_sha256 = $2 WHERE id = $1")
        .bind(uuid_of(&v1))
        .bind("d".repeat(64))
        .execute(env.core.db.pool())
        .await
        .unwrap();
    let v2 = call(
        Method::POST,
        format!("/rewards/{rwd}/versions"),
        Some(json!({"policy": policy(3.0), "test_cases": cases()})),
    )
    .await
    .json()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let r = call(
        Method::POST,
        format!("/reward-versions/{v2}:regression"),
        None,
    )
    .await;
    assert_eq!(r.status, 202, "{}", r.text());
    let reg = r.json();
    assert_eq!(reg["other_version_id"], v1.as_str());
    assert_eq!(reg["request"]["against_version_id"], v1.as_str());
    let (_, inputs, _) = builtin_row(&env, job_of(&reg)).await;
    assert_eq!(inputs["other"]["version_id"], v1.as_str());
    assert_eq!(inputs["baseline_cases"].as_array().unwrap().len(), 2);
    let r = call(
        Method::POST,
        format!("/reward-versions/{v1}:regression"),
        None,
    )
    .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (409, json!("MRL-STATE-290"))
    );

    // v1's reports: its tests, plus the comparison and the regression run naming it.
    let page = call(
        Method::GET,
        format!("/reward-versions/{v1}/reports?limit=2"),
        None,
    )
    .await
    .json();
    assert_eq!(page["items"].as_array().unwrap().len(), 2);
    let cursor = page["next_cursor"].as_str().unwrap().to_owned();
    let rest = call(
        Method::GET,
        format!("/reward-versions/{v1}/reports?limit=10&cursor={cursor}"),
        None,
    )
    .await
    .json();
    assert_eq!(rest["items"].as_array().unwrap().len(), 2, "{rest}");
    for (action, n) in [
        ("rewards.preview", 1),
        ("rewards.compare", 1),
        ("rewards.regression", 1),
    ] {
        let got: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM audit_events WHERE action = $1 AND org_id = $2",
        )
        .bind(action)
        .bind(org_text(&env))
        .fetch_one(env.core.db.pool())
        .await
        .unwrap();
        assert_eq!(got, n, "{action}");
    }
    env.core.stop().await;
}

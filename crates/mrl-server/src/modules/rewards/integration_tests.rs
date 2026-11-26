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

async fn person(env: &Env, name: &str, org: OrgId, role: Role) -> Auth {
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
    bind_role(&mut tx, org, UserId::from_uuid(id), role, Scope::Org)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    auth
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
    env.owner = person(&env, "owner", org, Role::Owner).await;
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

fn case(position: i32, correct: f64) -> Value {
    json!({"position": position, "total": correct, "resolution": "SCORED",
           "components": {"correct": correct, "format": 1.0}, "flags": [],
           "statuses": {"correct": "OK", "format": "OK"}})
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
    let stranger = person(&env, "stranger", other_org, Role::Owner).await;
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
    let viewer = person(&env, "viewer", env.org, Role::Viewer).await;
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
    let r = publish(v2.clone()).await;
    assert_eq!(r.json()["code"], "MRL-STATE-291", "{}", r.text());
    assert!(r.json()["detail"].as_str().unwrap().contains("regression"));
    let job = preview_job(&env, uuid_of(&v2), "regression", Some(uuid_of(&v1))).await;
    let mut reg = report(&v2, "regression", json!([case(0, 1.0), case(1, 1.0)]));
    reg["baseline_cases"] = json!([case(0, 1.0), case(1, 0.0)]);
    fixtures.put(job.id, reg, None);
    assert_eq!(verifier.verify(&job).await.unwrap(), Verification::Verified);
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
async fn p29_a_run_can_launch_only_with_a_published_reward() {
    use mrl_domain::ids::{DatasetVersionId, ModelVersionId};
    use mrl_server::modules::jobs::AttemptRow;
    use mrl_server::modules::rewards::launch::RewardInputs;
    use mrl_server::modules::scheduler::pipeline::{
        LaunchContributor, LaunchError, LaunchPlan, Placement,
    };
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
    let run = env
        .core
        .call_with(
            Method::POST,
            &format!("/api/v1/projects/{prj}/runs"),
            &env.owner,
            Some(json!({
            "name": "r", "algorithm_id": "grpo", "reward_version_id": vtext,
            "base_model_version_id": ModelVersionId::new().to_string(),
            "dataset_version_ids": [DatasetVersionId::new().to_string()], "config": {}})),
            &[("idempotency-key", "p29-run")],
        )
        .await;
    assert_eq!(run.status, 201, "{}", run.text());
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
    let draft = contributor
        .contribute(&job, &attempt, &placement, &mut plan)
        .await;
    assert!(
        matches!(&draft, Err(LaunchError::Fail(mrl_domain::failure::FailureClass::UserConfig, why)) if why.contains("DRAFT")),
        "{draft:?}"
    );
    sqlx::query("UPDATE reward_versions SET state = 'PUBLISHED', exec_sha256 = $2 WHERE id = $1")
        .bind(uuid_of(&vtext))
        .bind("d".repeat(64))
        .execute(env.core.db.pool())
        .await
        .unwrap();
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

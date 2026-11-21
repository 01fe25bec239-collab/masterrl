//! Runs and jobs against PostgreSQL and a real team-mode server.

use crate::support::sequences::{ops, World};
use crate::support::*;
use axum::body::Body;
use axum::http::Method;
use mrl_domain::failure::{FailureClass, FailureCode};
use mrl_domain::ids::{JobId, RunId};
use mrl_domain::ids::{OrgId, ProjectId};
use mrl_domain::rbac::Role;
use mrl_domain::state::{JobAttemptEvent, JobEvent, Owner};
use mrl_server::modules::jobs::{AttemptChange, JobChange, JobTransitions};
use mrl_server::modules::jobs::{Chains, JobRow};
use mrl_testkit::pg::TestDb;
use mrl_testkit::server::{TestResponse, TestServer};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

struct Env {
    db: TestDb,
    srv: TestServer,
    org: OrgId,
    project: ProjectId,
    rules: Arc<Mutex<AuthRules>>,
}

fn users(org: OrgId, other: OrgId) -> Vec<TestUser> {
    vec![
        test_user("owner", "usr_owner", org, Role::Owner),
        test_user("maint", "usr_maint", org, Role::Maintainer),
        test_user("res", "usr_res", org, Role::Researcher),
        test_user("res2", "usr_res2", org, Role::Researcher),
        test_user("viewer", "usr_viewer", org, Role::Viewer),
        test_user("outsider", "usr_out", other, Role::Owner),
    ]
}

/// A server whose chains queue everything (`accepting`) or the build's registered chains.
async fn env(accepting: bool) -> Env {
    let db = TestDb::new().await;
    let chains = if accepting {
        accepting_chains()
    } else {
        Chains::registered(db.db())
    };
    let org = OrgId::new();
    let project = ProjectId::new();
    let harness = Harness::default();
    *harness.rules.lock().unwrap() = AuthRules::with_projects(&[(project, org)]);
    let rules = harness.rules.clone();
    let srv = runs_server_with(db.db(), chains, &users(org, OrgId::new()), vec![], &harness).await;
    Env {
        db,
        srv,
        org,
        project,
        rules,
    }
}

impl Env {
    /// Another project in this env's org.
    fn add_project(&self, project: ProjectId) {
        let mut rules = self.rules.lock().unwrap();
        let owners = rules.projects.get_or_insert_with(HashMap::new);
        owners.insert(project.to_string(), self.org.to_string());
    }

    fn runs_path(&self) -> String {
        format!("/api/v1/projects/{}/runs", self.project)
    }

    async fn submit(&self, token: &str, key: &str, body: &Value) -> TestResponse {
        call(
            &self.srv,
            Method::POST,
            &self.runs_path(),
            token,
            Some(key),
            Some(body),
        )
        .await
    }

    async fn get(&self, token: &str, path: &str) -> TestResponse {
        call(&self.srv, Method::GET, path, token, None, None).await
    }

    async fn post(&self, token: &str, path: &str) -> TestResponse {
        call(&self.srv, Method::POST, path, token, None, None).await
    }

    async fn job_row(&self, job: &str) -> JobRow {
        let id: JobId = job.parse().unwrap();
        let mut tx = self.db.db().begin().await.unwrap();
        JobTransitions::lock(&mut tx, self.org.as_uuid(), id.as_uuid())
            .await
            .unwrap()
            .unwrap()
    }

    /// The run's event types in order. Also checks each job's and attempt's events are
    /// numbered 0, 1, 2, … with no gaps.
    async fn timeline(&self, run: &str) -> Vec<String> {
        let res = self
            .get("viewer", &format!("/api/v1/runs/{run}/timeline?limit=200"))
            .await;
        assert_eq!(res.status, 200, "{}", res.text());
        let mut next: HashMap<String, i64> = HashMap::new();
        let items = res.json()["items"].as_array().unwrap().clone();
        for e in &items {
            let seq = next
                .entry(e["aggregate_id"].as_str().unwrap().to_owned())
                .or_default();
            assert_eq!(e["aggregate_seq"].as_i64(), Some(*seq), "{e}");
            *seq += 1;
        }
        items
            .iter()
            .map(|e| e["type"].as_str().unwrap().to_owned())
            .collect()
    }
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn default_chains_reject_with_no_engine_and_a_hint() {
    let e = env(false).await;
    let res = e.submit("res", "k1", &submission("first")).await;
    assert_eq!(res.status, 201, "{}", res.text());
    assert!(res.headers["location"]
        .to_str()
        .unwrap()
        .starts_with("/api/v1/runs/run_"));
    let run = res.json();
    assert_eq!(run["state"], "REJECTED");
    let reason = &run["state_reason"];
    assert_eq!(reason["code"], "MRL-CAP-001");
    let p = &reason["problems"][0];
    assert_eq!(p["code"], "MRL-CAP-001");
    assert_eq!(p["path"], "/algorithm_id");
    assert!(p["hint"].as_str().unwrap().contains("Pick another method"));
    let id = run["id"].as_str().unwrap();
    assert_eq!(
        e.timeline(id).await,
        ["job.created", "job.submitted", "job.rejected"]
    );
    // The config snapshot is stored and can't change.
    let cfg = e.get("viewer", &format!("/api/v1/runs/{id}/config")).await;
    assert_eq!(cfg.status, 200, "{}", cfg.text());
    let c = cfg.json();
    assert_eq!(c["submitted"]["name"], "first");
    assert_eq!(c["resolved"], json!({"epochs": 1}));
    assert_eq!(c["resolved_sha256"].as_str().unwrap().len(), 64);
    assert!(c["seeds"]["global"].is_u64());
    assert!(c["dataset_version_ids"][0]
        .as_str()
        .unwrap()
        .starts_with("dsv_"));
    let run_uuid = id.parse::<RunId>().unwrap().as_uuid();
    let changed = sqlx::query("UPDATE run_configs SET seeds = '{}' WHERE run_id = $1")
        .bind(run_uuid)
        .execute(e.db.pool())
        .await;
    assert!(changed.is_err(), "run_configs must be immutable");
    // The job mirrors it.
    let job = e
        .get(
            "viewer",
            &format!("/api/v1/jobs/{}", run["job_id"].as_str().unwrap()),
        )
        .await;
    assert_eq!(job.status, 200);
    assert_eq!(job.json()["state"], "REJECTED");
    assert!(job.headers.contains_key("etag"));
    // The schema checks still list their problems (and then no MRL-CAP-001).
    let mut bad = submission("bad");
    bad["dataset_version_ids"] = json!([]);
    let res = e.submit("res", "k2", &bad).await;
    assert_eq!(
        res.json()["state_reason"]["problems"][0]["path"],
        "/dataset_version_ids"
    );
    assert_eq!(
        res.json()["state_reason"]["problems"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn submit_needs_a_key_and_replays_durably() {
    let e = env(true).await;
    let body = submission("once");
    let res = call(
        &e.srv,
        Method::POST,
        &e.runs_path(),
        "res",
        None,
        Some(&body),
    )
    .await;
    assert_eq!(res.status, 400);
    assert_eq!(res.json()["code"], "MRL-IDEM-170");
    assert_eq!(res.headers["content-type"], "application/problem+json");

    let first = e.submit("res", "same", &body).await;
    assert_eq!(first.status, 201);
    let again = e.submit("res", "same", &body).await;
    assert_eq!(again.status, 201);
    assert_eq!(again.json()["id"], first.json()["id"]);
    assert_eq!(e.db.count("runs").await, 1);
    let other = e.submit("res", "same", &submission("different")).await;
    assert_eq!(other.status, 422);
    assert_eq!(other.json()["code"], "MRL-IDEM-002");

    // A second server (fresh in-memory key store) still recognises the key: the run row is the record.
    let srv2 = runs_server(
        &e.db,
        accepting_chains(),
        &users(e.org, OrgId::new()),
        vec![],
    )
    .await;
    let replay = call(
        &srv2,
        Method::POST,
        &e.runs_path(),
        "res",
        Some("same"),
        Some(&body),
    )
    .await;
    assert_eq!(replay.status, 201, "{}", replay.text());
    assert_eq!(replay.json()["id"], first.json()["id"]);
    let mismatch = call(
        &srv2,
        Method::POST,
        &e.runs_path(),
        "res",
        Some("same"),
        Some(&submission("x")),
    )
    .await;
    assert_eq!(mismatch.json()["code"], "MRL-IDEM-002");
    // Keys are per principal.
    let theirs = e.submit("res2", "same", &body).await;
    assert_ne!(theirs.json()["id"], first.json()["id"]);
    assert_eq!(e.db.count("runs").await, 2);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn other_orgs_see_404_everywhere() {
    let e = env(true).await;
    let run = e.submit("owner", "k", &submission("mine")).await.json();
    let (rid, jid) = (run["id"].as_str().unwrap(), run["job_id"].as_str().unwrap());
    let exp = run["experiment_id"].as_str().unwrap();
    for path in [
        format!("/api/v1/runs/{rid}"),
        format!("/api/v1/runs/{rid}/config"),
        format!("/api/v1/runs/{rid}/attempts"),
        format!("/api/v1/runs/{rid}/timeline"),
        format!("/api/v1/jobs/{jid}"),
        format!("/api/v1/experiments/{exp}"),
    ] {
        let res = e.get("outsider", &path).await;
        assert_eq!(res.status, 404, "{path}: {}", res.text());
        assert_eq!(res.json()["code"], "MRL-RES-001", "{path}");
    }
    for path in [
        format!("/api/v1/runs/{rid}:cancel"),
        format!("/api/v1/jobs/{jid}:cancel"),
    ] {
        assert_eq!(e.post("outsider", &path).await.status, 404, "{path}");
    }
    let patch = call(
        &e.srv,
        Method::PATCH,
        &format!("/api/v1/jobs/{jid}"),
        "outsider",
        None,
        Some(&json!({"priority": 1})),
    )
    .await;
    assert_eq!(patch.status, 404);
    let jobs = e.get("outsider", "/api/v1/jobs").await.json();
    assert_eq!(jobs["items"].as_array().unwrap().len(), 0);
    // Another org's project is hidden like a missing one.
    let theirs = e.get("outsider", &e.runs_path()).await;
    assert_eq!(theirs.status, 404, "{}", theirs.text());
    // Still running for its owner.
    assert_eq!(
        e.get("owner", &format!("/api/v1/jobs/{jid}")).await.json()["state"],
        "QUEUED"
    );
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn rbac_per_endpoint() {
    let e = env(true).await;
    let mine = e.submit("res", "a", &submission("res-run")).await.json();
    let maints = e
        .submit("maint", "b", &submission("maint-run"))
        .await
        .json();
    let exp = mine["experiment_id"].as_str().unwrap().to_owned();
    let (rid, jid) = (
        mine["id"].as_str().unwrap().to_owned(),
        mine["job_id"].as_str().unwrap().to_owned(),
    );
    let other_run = maints["id"].as_str().unwrap().to_owned();
    let other_job = maints["job_id"].as_str().unwrap().to_owned();
    let exps = format!("/api/v1/projects/{}/experiments", e.project);

    // Viewer reads everything and changes nothing.
    for path in [
        e.runs_path(),
        format!("/api/v1/runs/{rid}"),
        format!("/api/v1/runs/{rid}/config"),
        format!("/api/v1/runs/{rid}/attempts"),
        format!("/api/v1/runs/{rid}/timeline"),
        "/api/v1/jobs".to_owned(),
        format!("/api/v1/jobs/{jid}"),
        exps.clone(),
        format!("/api/v1/experiments/{exp}"),
    ] {
        assert_eq!(e.get("viewer", &path).await.status, 200, "{path}");
    }
    assert_eq!(e.submit("viewer", "v", &submission("no")).await.status, 403);
    let new_exp = json!({"name": "sweep"});
    assert_eq!(
        call(&e.srv, Method::POST, &exps, "viewer", None, Some(&new_exp))
            .await
            .status,
        403
    );
    assert_eq!(
        call(
            &e.srv,
            Method::PATCH,
            &format!("/api/v1/experiments/{exp}"),
            "viewer",
            None,
            Some(&json!({"description": "x"}))
        )
        .await
        .status,
        403
    );
    assert_eq!(
        e.post("viewer", &format!("/api/v1/runs/{rid}:cancel"))
            .await
            .status,
        403
    );
    assert_eq!(
        e.post("viewer", &format!("/api/v1/jobs/{jid}:cancel"))
            .await
            .status,
        403
    );
    let prio = json!({"priority": 3});
    assert_eq!(
        call(
            &e.srv,
            Method::PATCH,
            &format!("/api/v1/jobs/{jid}"),
            "viewer",
            None,
            Some(&prio)
        )
        .await
        .status,
        403
    );

    // Researchers create experiments, cancel their own runs only and can't change priority.
    assert_eq!(
        call(&e.srv, Method::POST, &exps, "res", None, Some(&new_exp))
            .await
            .status,
        201
    );
    let denied = e
        .post("res", &format!("/api/v1/runs/{other_run}:cancel"))
        .await;
    assert_eq!(denied.status, 403);
    assert_eq!(denied.json()["code"], "MRL-PERM-001");
    assert_eq!(
        call(
            &e.srv,
            Method::PATCH,
            &format!("/api/v1/jobs/{jid}"),
            "res",
            None,
            Some(&prio)
        )
        .await
        .status,
        403
    );
    let own = e.post("res", &format!("/api/v1/runs/{rid}:cancel")).await;
    assert_eq!(own.status, 200, "{}", own.text());
    assert_eq!(own.json()["state"], "CANCELLED");

    // Maintainers cancel anything and set priority.
    let p = call(
        &e.srv,
        Method::PATCH,
        &format!("/api/v1/jobs/{other_job}"),
        "maint",
        None,
        Some(&prio),
    )
    .await;
    assert_eq!(p.status, 200, "{}", p.text());
    assert_eq!(p.json()["priority"], 3);
    let mine2 = e.submit("res", "c", &submission("res-run-2")).await.json();
    let any = e
        .post(
            "maint",
            &format!("/api/v1/jobs/{}:cancel", mine2["job_id"].as_str().unwrap()),
        )
        .await;
    assert_eq!(any.status, 200);
    assert_eq!(any.json()["state"], "CANCELLED");
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn cancel_timeline_and_terminal_states() {
    let e = env(true).await;
    let run = e.submit("res", "k", &submission("to-cancel")).await.json();
    assert_eq!(run["state"], "QUEUED");
    let rid = run["id"].as_str().unwrap();
    let res = e.post("res", &format!("/api/v1/runs/{rid}:cancel")).await;
    assert_eq!(res.json()["state"], "CANCELLED");
    assert_eq!(res.json()["failure_class"], "CANCELLED_BY_USER");
    // Cancelling again is a no-op.
    assert_eq!(
        e.post("res", &format!("/api/v1/runs/{rid}:cancel"))
            .await
            .status,
        200
    );
    assert_eq!(
        e.timeline(rid).await,
        [
            "job.created",
            "job.submitted",
            "job.queued",
            "job.cancel_requested",
            "job.cancelled"
        ]
    );
    // A rejected run can't be cancelled; a finished job's priority can't change.
    let mut bad = submission("bad");
    bad["dataset_version_ids"] = json!([]);
    let rejected = e.submit("res", "r", &bad).await.json();
    assert_eq!(rejected["state"], "REJECTED");
    let res = e
        .post(
            "res",
            &format!("/api/v1/runs/{}:cancel", rejected["id"].as_str().unwrap()),
        )
        .await;
    assert_eq!(res.status, 409);
    assert_eq!(res.json()["code"], "MRL-STATE-170");
    assert_eq!(
        res.json()["title"],
        "This job can't take that step right now."
    );
    let p = call(
        &e.srv,
        Method::PATCH,
        &format!("/api/v1/jobs/{}", run["job_id"].as_str().unwrap()),
        "maint",
        None,
        Some(&json!({"priority": 1})),
    )
    .await;
    assert_eq!(p.json()["code"], "MRL-STATE-170");
    // Unknown actions and ids.
    assert_eq!(
        e.post("res", &format!("/api/v1/runs/{rid}:explode"))
            .await
            .status,
        404
    );
    assert_eq!(e.get("res", "/api/v1/runs/not-an-id").await.status, 404);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn cas_conflict_is_reported_not_retried() {
    let e = env(true).await;
    let run = e.submit("res", "k", &submission("cas")).await.json();
    let stale = e.job_row(run["job_id"].as_str().unwrap()).await;
    // Someone else moves the job first.
    let mut tx = e.db.db().begin().await.unwrap();
    JobTransitions::apply(
        &mut tx,
        &stale,
        JobEvent::Place,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    // A writer with the old version loses and nothing changes.
    let mut tx = e.db.db().begin().await.unwrap();
    let err = JobTransitions::apply(
        &mut tx,
        &stale,
        JobEvent::Place,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), "MRL-CONFLICT-170");
    drop(tx);
    let now = e.job_row(run["job_id"].as_str().unwrap()).await;
    assert_eq!(
        (now.state.as_str(), now.state_version),
        ("ALLOCATING", stale.state_version + 1)
    );
    // The wrong owner is refused before any write.
    let mut tx = e.db.db().begin().await.unwrap();
    let err = JobTransitions::apply(
        &mut tx,
        &now,
        JobEvent::Requeue,
        Owner::Jobs,
        JobChange::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), "MRL-STATE-170");
    assert!(err.to_string().contains("scheduler"), "{err}");
    let err = JobTransitions::apply(
        &mut tx,
        &now,
        JobEvent::Launch,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("launch"), "{err}");
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn live_attempt_cancel_retry_and_verification() {
    let e = env(true).await;
    let org = e.org.as_uuid();
    let run = e.submit("res", "k", &submission("attempts")).await.json();
    let jid = run["job_id"].as_str().unwrap().to_owned();
    let rid = run["id"].as_str().unwrap().to_owned();
    let job = e.job_row(&jid).await;

    let mut tx = e.db.db().begin().await.unwrap();
    let job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Place,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap();
    let (job, att) = JobTransitions::launch(&mut tx, &job, None).await.unwrap();
    assert_eq!(
        (job.state.as_str(), job.epoch, att.attempt_no, att.epoch),
        ("PREPARING", 1, 1, 1)
    );
    tx.commit().await.unwrap();

    // Cancel waits for the live attempt; killing it finishes the cancel.
    let res = e.post("res", &format!("/api/v1/jobs/{jid}:cancel")).await;
    assert_eq!(res.json()["state"], "CANCELLING");
    let mut tx = e.db.db().begin().await.unwrap();
    let att = JobTransitions::lock_attempt(&mut tx, org, att.id)
        .await
        .unwrap()
        .unwrap();
    JobTransitions::apply_attempt(
        &mut tx,
        &att,
        JobAttemptEvent::Kill,
        AttemptChange::default(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(e.job_row(&jid).await.state, "CANCELLED");
    let attempts = e
        .get("res", &format!("/api/v1/runs/{rid}/attempts"))
        .await
        .json();
    assert_eq!(attempts["items"][0]["state"], "KILLED");
    assert_eq!(
        e.timeline(&rid).await,
        [
            "job.created",
            "job.submitted",
            "job.queued",
            "job.placed",
            "job.launched",
            "attempt.created",
            "job.cancel_requested",
            "attempt.killed",
            "job.cancelled",
        ]
    );

    // Retries follow the job's budget, then fail.
    let mut body = submission("retry");
    body["retry"] = json!({"budgets": {"ENGINE_ERROR": 1}});
    let run = e.submit("res", "k2", &body).await.json();
    let jid = run["job_id"].as_str().unwrap().to_owned();
    let mut tx = e.db.db().begin().await.unwrap();
    let job = e.job_row(&jid).await;
    let job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Place,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap();
    let engine = FailureCode::class_only(FailureClass::EngineError);
    let job = JobTransitions::fail_or_retry(&mut tx, &job, engine, "boom")
        .await
        .unwrap();
    assert_eq!(job.state, "RETRY_WAIT");
    assert!(job.next_eligible_at.is_some());
    assert_eq!(job.retries_used, json!({"ENGINE_ERROR": 1}));
    let job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Retry,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap();
    assert!(job.next_eligible_at.is_none());
    let job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Place,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap();
    let job = JobTransitions::fail_or_retry(&mut tx, &job, engine, "boom again")
        .await
        .unwrap();
    assert_eq!(
        (job.state.as_str(), job.failure_class.as_deref()),
        ("FAILED", Some("ENGINE_ERROR"))
    );
    tx.commit().await.unwrap();

    // With no verifier registered, a finished job fails verification.
    let run = e.submit("res", "k3", &submission("verify")).await.json();
    let mut tx = e.db.db().begin().await.unwrap();
    let mut job = e.job_row(run["job_id"].as_str().unwrap()).await;
    job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Place,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap();
    let (j, att) = JobTransitions::launch(&mut tx, &job, None).await.unwrap();
    job = JobTransitions::apply(
        &mut tx,
        &j,
        JobEvent::Start,
        Owner::Jobs,
        JobChange::default(),
    )
    .await
    .unwrap();
    job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Finish,
        Owner::Jobs,
        JobChange::default(),
    )
    .await
    .unwrap();
    let none = Chains::new(vec![], vec![], vec![]);
    let failed = JobTransitions::verify_completion(&mut tx, &job, none.verify(&job).await.unwrap())
        .await
        .unwrap();
    assert_eq!(failed.failure_class.as_deref(), Some("VERIFICATION_FAILED"));
    assert_eq!(
        failed.state_reason.as_ref().unwrap()["code"],
        "VERIFICATION_FAILED"
    );
    // Old-epoch reports are fenced; a lost lease bumps the epoch.
    let a = JobTransitions::lock_attempt(&mut tx, org, att.id)
        .await
        .unwrap()
        .unwrap();
    let a = JobTransitions::apply_attempt(
        &mut tx,
        &a,
        JobAttemptEvent::Acknowledge,
        AttemptChange::default(),
    )
    .await
    .unwrap();
    let a = JobTransitions::apply_attempt(
        &mut tx,
        &a,
        JobAttemptEvent::LoseLease,
        AttemptChange::default(),
    )
    .await
    .unwrap();
    assert_eq!(a.state, "LOST");
    let after = JobTransitions::lock(&mut tx, org, job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.epoch, job.epoch + 1);
    tx.commit().await.unwrap();
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn experiments_and_list_pages() {
    let e = env(true).await;
    let exps = format!("/api/v1/projects/{}/experiments", e.project);
    let made = call(
        &e.srv,
        Method::POST,
        &exps,
        "res",
        None,
        Some(&json!({"name": "sweep", "tags": ["lr"]})),
    )
    .await;
    assert_eq!(made.status, 201, "{}", made.text());
    let exp = made.json();
    let dup = call(
        &e.srv,
        Method::POST,
        &exps,
        "res",
        None,
        Some(&json!({"name": "sweep"})),
    )
    .await;
    assert_eq!(dup.status, 409);
    assert_eq!(dup.json()["code"], "MRL-CONFLICT-171");
    let path = format!("/api/v1/experiments/{}", exp["id"].as_str().unwrap());
    let stale = [
        ("authorization", "Bearer res"),
        ("if-match", "\"99\""),
        ("content-type", "application/json"),
    ];
    let res = e
        .srv
        .request(
            Method::PATCH,
            &path,
            &stale,
            Body::from(json!({"description": "d"}).to_string()),
        )
        .await;
    assert_eq!(res.status, 412);
    let ok = call(
        &e.srv,
        Method::PATCH,
        &path,
        "res",
        None,
        Some(&json!({"description": "d"})),
    )
    .await;
    assert_eq!((ok.status, ok.json()["version"].as_i64()), (200, Some(1)));

    // Runs into the named experiment and the default one; filters and pages.
    let mut body = submission("in-sweep");
    body["experiment_id"] = exp["id"].clone();
    assert_eq!(
        e.submit("res", "1", &body).await.json()["experiment_id"],
        exp["id"]
    );
    for i in 0..3 {
        e.submit("res", &format!("d{i}"), &submission(&format!("d{i}")))
            .await;
    }
    let in_sweep = e
        .get(
            "res",
            &format!(
                "{}?experiment={}",
                e.runs_path(),
                exp["id"].as_str().unwrap()
            ),
        )
        .await
        .json();
    assert_eq!(in_sweep["items"].as_array().unwrap().len(), 1);
    let listed = e.get("res", &exps).await.json();
    assert_eq!(
        listed["items"].as_array().unwrap().len(),
        2,
        "sweep + default"
    );
    let first = e
        .get("res", &format!("{}?limit=2", e.runs_path()))
        .await
        .json();
    assert_eq!(first["items"].as_array().unwrap().len(), 2);
    let cursor = first["next_cursor"].as_str().unwrap();
    let second = e
        .get("res", &format!("{}?limit=2&cursor={cursor}", e.runs_path()))
        .await
        .json();
    assert_eq!(second["items"].as_array().unwrap().len(), 2);
    assert!(second["next_cursor"].is_null());
    assert_ne!(first["items"][0]["id"], second["items"][0]["id"]);
    let queued = e
        .get("res", "/api/v1/jobs?state=QUEUED&kind=TRAIN")
        .await
        .json();
    assert_eq!(queued["items"].as_array().unwrap().len(), 4);
    assert_eq!(
        e.get("res", "/api/v1/jobs?state=NOPE").await.json()["code"],
        "MRL-VAL-006"
    );
    assert_eq!(
        e.get("res", "/api/v1/jobs?limit=0").await.json()["code"],
        "MRL-VAL-005"
    );
    let missing = call(
        &e.srv,
        Method::POST,
        &e.runs_path(),
        "res",
        Some("m"),
        Some(&json!({"name": "x"})),
    )
    .await;
    assert_eq!(missing.status, 400, "{}", missing.text());
}

#[test]
#[ignore = "needs DATABASE_URL"]
fn random_sequences_keep_the_invariants() {
    use proptest::test_runner::{Config as PtConfig, TestRunner};
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let (db, srv, org) = rt.block_on(async {
        let db = TestDb::new().await;
        let org = OrgId::new();
        let srv = runs_server(&db, accepting_chains(), &users(org, OrgId::new()), vec![]).await;
        (db, srv, org)
    });
    let mut runner = TestRunner::new(PtConfig {
        cases: 24,
        ..PtConfig::default()
    });
    let result = runner.run(&ops(30), |steps| {
        rt.block_on(async {
            let mut world = World {
                srv: &srv,
                db: &db,
                token: "maint",
                org: org.as_uuid(),
                project: ProjectId::new(),
                runs: Vec::new(),
                seen: HashMap::new(),
            };
            for step in &steps {
                world.step(step).await;
                world.check().await;
            }
        });
        Ok(())
    });
    rt.block_on(async { srv.shutdown().await.unwrap() });
    if let Err(e) = result {
        panic!("{e}");
    }
}

/// Forces `job` into `state` behind `JobTransitions`' back (test set-up only).
async fn force_job(db: &TestDb, job: uuid::Uuid, state: &str) {
    sqlx::query("UPDATE jobs SET state = $2 WHERE id = $1")
        .bind(job)
        .bind(state)
        .execute(db.pool())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn every_transition_matches_the_p06_tables() {
    use mrl_domain::state::{Job, JobAttempt, JobState, StateMachine};
    let e = env(true).await;
    let org = e.org.as_uuid();
    let run = e.submit("res", "k", &submission("rules")).await.json();
    let job_id = run["job_id"]
        .as_str()
        .unwrap()
        .parse::<JobId>()
        .unwrap()
        .as_uuid();
    let owners = [Owner::Jobs, Owner::Scheduler, Owner::ArtifactsVerifier];
    let special = [
        JobEvent::Launch,
        JobEvent::Complete,
        JobEvent::Interrupt,
        JobEvent::RequestCancel,
        JobEvent::ConfirmCancel,
    ];
    let mut checked = 0;
    for &from in Job::STATES {
        force_job(&e.db, job_id, from.as_str()).await;
        for &event in Job::EVENTS {
            for by in owners {
                let mut tx = e.db.db().begin().await.unwrap();
                let row = JobTransitions::lock(&mut tx, org, job_id)
                    .await
                    .unwrap()
                    .unwrap();
                let got =
                    JobTransitions::apply(&mut tx, &row, event, by, JobChange::default()).await;
                let want = Job::transition(from, event)
                    .filter(|t| t.owner == by && !special.contains(&event));
                match (got, want) {
                    (Ok(next), Some(t)) => {
                        assert_eq!(next.state, t.to.as_str(), "{from} {event:?} by {by}")
                    }
                    (Err(err), None) => {
                        assert_eq!(err.code(), "MRL-STATE-170", "{from} {event:?} by {by}")
                    }
                    (got, want) => {
                        panic!("{from} {event:?} by {by}: got {got:?}, P06 says {want:?}")
                    }
                }
                checked += 1;
                drop(tx); // roll back
            }
        }
        // The special steps, through their own methods.
        let mut tx = e.db.db().begin().await.unwrap();
        let row = JobTransitions::lock(&mut tx, org, job_id)
            .await
            .unwrap()
            .unwrap();
        let launched = JobTransitions::launch(&mut tx, &row, None).await;
        assert_eq!(
            launched.is_ok(),
            from == JobState::Allocating,
            "launch from {from}"
        );
        drop(tx);
        let mut tx = e.db.db().begin().await.unwrap();
        let verified = JobTransitions::verify_completion(
            &mut tx,
            &row,
            accepting_chains().verify(&row).await.unwrap(),
        )
        .await;
        assert_eq!(
            verified.is_ok(),
            from == JobState::Finalizing,
            "verify from {from}"
        );
        drop(tx);
    }
    assert_eq!(
        checked,
        Job::STATES.len() * Job::EVENTS.len() * owners.len()
    );

    // Attempts: an attempt in every state, at the job's epoch, against every event.
    force_job(&e.db, job_id, "ALLOCATING").await;
    let mut tx = e.db.db().begin().await.unwrap();
    let row = JobTransitions::lock(&mut tx, org, job_id)
        .await
        .unwrap()
        .unwrap();
    let (_, att) = JobTransitions::launch(&mut tx, &row, None).await.unwrap();
    tx.commit().await.unwrap();
    for &from in JobAttempt::STATES {
        sqlx::query("UPDATE job_attempts SET state = $2 WHERE id = $1")
            .bind(att.id)
            .bind(from.as_str())
            .execute(e.db.pool())
            .await
            .unwrap();
        for &event in JobAttempt::EVENTS {
            let mut tx = e.db.db().begin().await.unwrap();
            let a = JobTransitions::lock_attempt(&mut tx, org, att.id)
                .await
                .unwrap()
                .unwrap();
            let got =
                JobTransitions::apply_attempt(&mut tx, &a, event, AttemptChange::default()).await;
            match (got, JobAttempt::transition(from, event)) {
                (Ok(next), Some(t)) => assert_eq!(next.state, t.to.as_str(), "{from} {event:?}"),
                (Err(err), None) => assert_eq!(err.code(), "MRL-STATE-170", "{from} {event:?}"),
                (got, want) => panic!("attempt {from} {event:?}: got {got:?}, P06 says {want:?}"),
            }
            drop(tx);
        }
    }
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_second_live_attempt_is_always_refused() {
    let e = env(true).await;
    let org = e.org.as_uuid();
    let run = e.submit("res", "k", &submission("two")).await.json();
    let mut job = e.job_row(run["job_id"].as_str().unwrap()).await;
    let mut tx = e.db.db().begin().await.unwrap();
    job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Place,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap();
    let (j, first) = JobTransitions::launch(&mut tx, &job, None).await.unwrap();
    // The job goes back to ALLOCATING while its first attempt is still PENDING.
    let lost = FailureCode::class_only(FailureClass::NodeLost);
    job = JobTransitions::fail_or_retry(&mut tx, &j, lost, "node gone")
        .await
        .unwrap();
    assert_eq!(job.state, "RETRY_WAIT");
    job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Retry,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap();
    job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Place,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let mut tx = e.db.db().begin().await.unwrap();
    let err = JobTransitions::launch(&mut tx, &job, None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-STATE-170");
    assert!(err.to_string().contains("already has an attempt"), "{err}");
    drop(tx);
    // A writer going around JobTransitions hits the database rule too.
    let raw = sqlx::query(
        "INSERT INTO job_attempts (id, org_id, job_id, attempt_no, epoch, state) \
         VALUES ($1, $2, $3, 99, $4, 'RUNNING')",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(org)
    .bind(job.id)
    .bind(job.epoch)
    .execute(e.db.pool())
    .await
    .unwrap_err();
    let db_err = raw.as_database_error().unwrap();
    assert_eq!(db_err.constraint(), Some("one_live_attempt"));
    // Once the first attempt ends, the next launch works.
    let mut tx = e.db.db().begin().await.unwrap();
    let a = JobTransitions::lock_attempt(&mut tx, org, first.id)
        .await
        .unwrap()
        .unwrap();
    JobTransitions::apply_attempt(&mut tx, &a, JobAttemptEvent::Kill, AttemptChange::default())
        .await
        .unwrap();
    let job = JobTransitions::lock(&mut tx, org, job.id)
        .await
        .unwrap()
        .unwrap();
    let (_, second) = JobTransitions::launch(&mut tx, &job, None).await.unwrap();
    assert_eq!((second.attempt_no, second.epoch), (2, 2));
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn guarded_steps_and_stale_rows_cannot_bypass_the_rules() {
    let e = env(true).await;
    let org = e.org.as_uuid();
    let run = e.submit("res", "k", &submission("guards")).await.json();
    let jid = run["job_id"].as_str().unwrap().to_owned();
    let queued = e.job_row(&jid).await;

    // Cancel steps only go through request_cancel, which checks for live attempts.
    let mut tx = e.db.db().begin().await.unwrap();
    for event in [JobEvent::RequestCancel, JobEvent::ConfirmCancel] {
        let err = JobTransitions::apply(&mut tx, &queued, event, Owner::Jobs, JobChange::default())
            .await
            .unwrap_err();
        assert_eq!(err.code(), "MRL-STATE-170", "{event:?}");
        assert!(err.to_string().contains("request_cancel"), "{err}");
    }
    let job = JobTransitions::apply(
        &mut tx,
        &queued,
        JobEvent::Place,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap();
    let (job, _) = JobTransitions::launch(&mut tx, &job, None).await.unwrap();
    let cancelling = JobTransitions::request_cancel(&mut tx, &job, "usr_res")
        .await
        .unwrap();
    assert_eq!(cancelling.state, "CANCELLING", "the attempt is still live");
    let err = JobTransitions::apply(
        &mut tx,
        &cancelling,
        JobEvent::ConfirmCancel,
        Owner::Jobs,
        JobChange::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), "MRL-STATE-170");
    tx.commit().await.unwrap();
    assert_eq!(e.job_row(&jid).await.state, "CANCELLING");

    // A job row read before a lost lease can't write the old epoch back.
    let run = e.submit("res", "k2", &submission("fence")).await.json();
    let jid = run["job_id"].as_str().unwrap().to_owned();
    let mut tx = e.db.db().begin().await.unwrap();
    let job = e.job_row(&jid).await;
    let job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Place,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap();
    let (stale, att2) = JobTransitions::launch(&mut tx, &job, None).await.unwrap();
    let a = JobTransitions::lock_attempt(&mut tx, org, att2.id)
        .await
        .unwrap()
        .unwrap();
    let a = JobTransitions::apply_attempt(
        &mut tx,
        &a,
        JobAttemptEvent::Acknowledge,
        AttemptChange::default(),
    )
    .await
    .unwrap();
    JobTransitions::apply_attempt(
        &mut tx,
        &a,
        JobAttemptEvent::LoseLease,
        AttemptChange::default(),
    )
    .await
    .unwrap();
    let err = JobTransitions::apply(
        &mut tx,
        &stale,
        JobEvent::Start,
        Owner::Jobs,
        JobChange::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), "MRL-CONFLICT-170");
    tx.commit().await.unwrap();
    let now = e.job_row(&jid).await;
    assert_eq!(
        (now.state.as_str(), now.epoch),
        ("PREPARING", stale.epoch + 1),
        "the fence holds"
    );
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn hidden_parents_and_jobs_stay_hidden() {
    let e = env(true).await;
    let secret = ProjectId::new();
    e.add_project(secret);
    e.rules
        .lock()
        .unwrap()
        .hidden
        .insert(("usr_res".into(), secret.to_string()));

    // A parent in a project the submitter can't read answers like a missing one.
    let secret_runs = format!("/api/v1/projects/{secret}/runs");
    let hidden_parent = call(
        &e.srv,
        Method::POST,
        &secret_runs,
        "maint",
        Some("p"),
        Some(&submission("hidden-parent")),
    )
    .await
    .json();
    let mut body = submission("child");
    body["parent_run_id"] = hidden_parent["id"].clone();
    let hidden = e.submit("res", "c1", &body).await;
    body["parent_run_id"] = json!(RunId::new().to_string());
    let missing = e.submit("res", "c2", &body).await;
    assert_eq!(
        (hidden.status, missing.status),
        (404, 404),
        "{}",
        hidden.text()
    );
    assert_eq!(hidden.json()["code"], "MRL-RES-001");
    assert_eq!(hidden.json()["detail"], missing.json()["detail"]);
    let mine = e
        .submit("res", "c3", &submission("own-parent"))
        .await
        .json();
    body["parent_run_id"] = mine["id"].clone();
    assert_eq!(e.submit("res", "c4", &body).await.status, 201);

    // Jobs, oldest first: the "res" runs are visible, the secret one is hidden. A page of
    // two must hold two visible jobs and continue after the last visible one.
    let e2 = env(true).await;
    e2.add_project(secret);
    e2.rules
        .lock()
        .unwrap()
        .hidden
        .insert(("usr_res".into(), secret.to_string()));
    let mut visible = Vec::new();
    for (i, key) in ["v1", "v2", "h", "v3"].into_iter().enumerate() {
        let job = if key == "h" {
            let b = submission("secret");
            call(
                &e2.srv,
                Method::POST,
                &secret_runs,
                "maint",
                Some(key),
                Some(&b),
            )
            .await
            .json()["job_id"]
                .clone()
        } else {
            let run = e2
                .submit("res", key, &submission(&format!("run-{i}")))
                .await;
            let id = run.json()["job_id"].clone();
            visible.push(id.clone());
            id
        };
        assert!(job.is_string());
    }
    visible.reverse(); // newest first
    let page = e2.get("res", "/api/v1/jobs?limit=2").await.json();
    let ids = |p: &Value| -> Vec<Value> {
        p["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|j| j["id"].clone())
            .collect()
    };
    assert_eq!(ids(&page), visible[..2].to_vec(), "{page}");
    let cursor = page["next_cursor"].as_str().unwrap().to_owned();
    let rest = e2
        .get("res", &format!("/api/v1/jobs?limit=2&cursor={cursor}"))
        .await
        .json();
    assert_eq!(ids(&rest), visible[2..].to_vec(), "{rest}");
    assert!(rest["next_cursor"].is_null(), "{rest}");
    let all = e2.get("maint", "/api/v1/jobs?limit=10").await.json();
    assert_eq!(all["items"].as_array().unwrap().len(), 4);

    // An authorizer outage is an error, not an empty page.
    e2.rules.lock().unwrap().down = true;
    let res = e2.get("res", "/api/v1/jobs").await;
    assert_eq!(res.status, 503, "{}", res.text());
    let res = e2.submit("res", "c5", &body).await;
    assert_eq!(res.status, 503, "{}", res.text());
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn one_connection_is_enough_and_audits_name_the_project() {
    let db = TestDb::new().await;
    let org = OrgId::new();
    let project = ProjectId::new();
    // One pool connection, shared by the runs module and an authorizer that reads the
    // database (like P16's): authorizing while a write transaction holds it would time out.
    let small = db.replica_with(&mrl_server::config::DatabaseConfig {
        max_connections: 1,
        acquire_timeout_seconds: 3,
        ..mrl_testkit::pg::test_config()
    });
    let harness = Harness::default();
    *harness.rules.lock().unwrap() = AuthRules {
        pool: Some(small.clone()),
        ..AuthRules::with_projects(&[(project, org)])
    };
    let srv = runs_server_with(
        &small,
        accepting_chains(),
        &users(org, OrgId::new()),
        vec![],
        &harness,
    )
    .await;
    let runs = format!("/api/v1/projects/{project}/runs");
    let post = |token: &'static str, path: String, key: Option<&'static str>, body: Value| {
        let srv = &srv;
        async move {
            let body = (!body.is_null()).then_some(body);
            call(srv, Method::POST, &path, token, key, body.as_ref()).await
        }
    };
    let parent = post("res", runs.clone(), Some("a"), submission("parent")).await;
    assert_eq!(parent.status, 201, "{}", parent.text());
    let parent = parent.json();
    let mut child = submission("child");
    child["parent_run_id"] = parent["id"].clone();
    let child = post("res", runs.clone(), Some("b"), child).await;
    assert_eq!(child.status, 201, "{}", child.text());
    let child = child.json();
    let third = post("res", runs.clone(), Some("c"), submission("third"))
        .await
        .json();

    let job = parent["job_id"].as_str().unwrap();
    let res = post(
        "res",
        format!("/api/v1/jobs/{job}:cancel"),
        None,
        Value::Null,
    )
    .await;
    assert_eq!(res.status, 200, "{}", res.text());
    let run = child["id"].as_str().unwrap();
    let res = post(
        "res",
        format!("/api/v1/runs/{run}:cancel"),
        None,
        Value::Null,
    )
    .await;
    assert_eq!(res.status, 200, "{}", res.text());
    let job = third["job_id"].as_str().unwrap();
    let res = call(
        &srv,
        Method::PATCH,
        &format!("/api/v1/jobs/{job}"),
        "maint",
        None,
        Some(&json!({"priority": 5})),
    )
    .await;
    assert_eq!(res.status, 200, "{}", res.text());
    let exps = format!("/api/v1/projects/{project}/experiments");
    let res = post("res", exps, None, json!({"name": "sweep"})).await;
    assert_eq!(res.status, 201, "{}", res.text());

    let events = harness.audit.events();
    let got: Vec<(&str, Option<&str>)> = events
        .iter()
        .map(|e| (e.action.as_str(), e.project.as_deref()))
        .collect();
    let p = project.to_string();
    let p = Some(p.as_str());
    assert_eq!(
        got,
        [
            ("runs.submit", p),
            ("runs.submit", p),
            ("runs.submit", p),
            ("runs.cancel", p),
            ("runs.cancel", p),
            ("jobs.set_priority", p),
            ("experiments.create", p),
        ]
    );
    srv.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn foreign_and_missing_projects_are_404_with_no_writes() {
    let e = env(true).await;
    let foreign = ProjectId::new();
    e.rules
        .lock()
        .unwrap()
        .projects
        .get_or_insert_with(HashMap::new)
        .insert(foreign.to_string(), OrgId::new().to_string());
    let missing = ProjectId::new();
    for project in [foreign, missing] {
        let runs = format!("/api/v1/projects/{project}/runs");
        let exps = format!("/api/v1/projects/{project}/experiments");
        for path in [&runs, &exps] {
            let res = e.get("owner", path).await;
            assert_eq!(res.status, 404, "GET {path}: {}", res.text());
            assert_eq!(res.json()["code"], "MRL-RES-001");
        }
        let key = project.to_string();
        let res = call(
            &e.srv,
            Method::POST,
            &runs,
            "owner",
            Some(&key),
            Some(&submission("x")),
        )
        .await;
        assert_eq!(res.status, 404, "POST {runs}: {}", res.text());
        let body = json!({"name": "sweep"});
        let res = call(&e.srv, Method::POST, &exps, "owner", None, Some(&body)).await;
        assert_eq!(res.status, 404, "POST {exps}: {}", res.text());
    }
    for table in [
        "experiments",
        "runs",
        "run_configs",
        "jobs",
        "job_attempts",
        "outbox_events",
    ] {
        let n: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
                .fetch_one(e.db.pool())
                .await
                .unwrap();
        assert_eq!(n, 0, "{table} has rows");
    }
    // GET /jobs?project=: a hidden project answers exactly like a foreign or missing one.
    let hidden = ProjectId::new();
    e.add_project(hidden);
    e.rules
        .lock()
        .unwrap()
        .hidden
        .insert(("usr_owner".into(), hidden.to_string()));
    let mut answers = Vec::new();
    for project in [hidden, foreign, missing] {
        let res = e
            .get("owner", &format!("/api/v1/jobs?project={project}"))
            .await;
        assert_eq!(res.status, 404, "{project}: {}", res.text());
        let p = res.json();
        answers.push((p["code"].clone(), p["title"].clone(), p["detail"].clone()));
    }
    assert!(answers.windows(2).all(|w| w[0] == w[1]), "{answers:?}");
    // The org's own project still works.
    assert_eq!(
        e.submit("owner", "ok", &submission("mine")).await.status,
        201
    );
    let own = e
        .get("owner", &format!("/api/v1/jobs?project={}", e.project))
        .await;
    assert_eq!(
        own.json()["items"].as_array().unwrap().len(),
        1,
        "{}",
        own.text()
    );
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn solo_runs_are_stored_under_the_project_org_for_every_credential() {
    use mrl_server::seams::{Principal, PrincipalKind};
    let db = TestDb::new().await;
    let org = OrgId::new();
    let project = ProjectId::new();
    let person = |token: &str, id: &str, kind, org: Option<String>, role| TestUser {
        token: token.to_owned(),
        principal: Principal {
            id: id.to_owned(),
            kind,
            org,
        },
        role,
    };
    let people = vec![
        // The solo launch secret: the local owner, no org.
        person(
            "solo",
            "usr_local",
            PrincipalKind::LocalOwner,
            None,
            Role::Owner,
        ),
        // A token bound to the project: P16 gives it the project's org.
        person(
            "pat",
            "tok_pat",
            PrincipalKind::Service,
            Some(org.to_string()),
            Role::Viewer,
        ),
    ];
    let runs = format!("/api/v1/projects/{project}/runs");
    let nil_rows = || async {
        let n: i64 = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM runs WHERE org_id = $1) \
                  + (SELECT count(*) FROM jobs WHERE org_id = $1) \
                  + (SELECT count(*) FROM run_configs WHERE org_id = $1) \
                  + (SELECT count(*) FROM experiments WHERE org_id = $1)",
        )
        .bind(uuid::Uuid::nil())
        .fetch_one(db.pool())
        .await
        .unwrap();
        n
    };

    // A build without a project directory (solo, before P16) stores under the nil org.
    let old = runs_server_with(
        db.db(),
        accepting_chains(),
        &people,
        vec![],
        &Harness::default(),
    )
    .await;
    let first = call(
        &old,
        Method::POST,
        &runs,
        "solo",
        Some("a"),
        Some(&submission("old")),
    )
    .await;
    assert_eq!(first.status, 201, "{}", first.text());
    let first = first.json();
    old.shutdown().await.unwrap();
    assert_eq!(
        nil_rows().await,
        4,
        "run, job, config and default experiment"
    );

    // With the directory, startup moves those rows to the project's org ...
    let harness = Harness::default();
    *harness.rules.lock().unwrap() = AuthRules::with_projects(&[(project, org)]);
    let srv = runs_server_with(db.db(), accepting_chains(), &people, vec![], &harness).await;
    assert_eq!(nil_rows().await, 0);
    // ... and new solo submissions are stored there too.
    let second = call(
        &srv,
        Method::POST,
        &runs,
        "solo",
        Some("b"),
        Some(&submission("new")),
    )
    .await;
    assert_eq!(second.status, 201, "{}", second.text());
    let second = second.json();
    assert_eq!(nil_rows().await, 0);

    // The project-bound token lists and fetches both.
    let get = |path: String| {
        let srv = &srv;
        async move { call(srv, Method::GET, &path, "pat", None, None).await }
    };
    let page = get(runs.clone()).await;
    assert_eq!(page.status, 200, "{}", page.text());
    let listed: Vec<String> = page.json()["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_owned())
        .collect();
    for run in [&first, &second] {
        let id = run["id"].as_str().unwrap();
        assert!(listed.iter().any(|l| l == id), "{id} not in {listed:?}");
        let job = run["job_id"].as_str().unwrap();
        for path in [
            format!("/api/v1/runs/{id}"),
            format!("/api/v1/runs/{id}/config"),
            format!("/api/v1/runs/{id}/attempts"),
            format!("/api/v1/jobs/{job}"),
        ] {
            let res = get(path.clone()).await;
            assert_eq!(res.status, 200, "{path}: {}", res.text());
        }
        let tl = get(format!("/api/v1/runs/{id}/timeline")).await.json();
        assert_eq!(tl["items"][0]["type"], "job.created", "{tl}");
    }
    let jobs = get(format!("/api/v1/jobs?project={project}")).await.json();
    assert_eq!(jobs["items"].as_array().unwrap().len(), 2, "{jobs}");
    let mine = call(&srv, Method::GET, "/api/v1/jobs", "solo", None, None)
        .await
        .json();
    assert_eq!(mine["items"].as_array().unwrap().len(), 2, "{mine}");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn legacy_rows_move_keeping_experiments_lineage_and_events() {
    use mrl_server::seams::{Principal, PrincipalKind};
    let db = TestDb::new().await;
    let org = OrgId::new();
    // P and Q are linked by a parent run; U links to M, which the directory doesn't know.
    let [p, q, u, m] = [(); 4].map(|_| ProjectId::new());
    let person = |token: &str, id: &str, kind, org: Option<String>, role| TestUser {
        token: token.to_owned(),
        principal: Principal {
            id: id.to_owned(),
            kind,
            org,
        },
        role,
    };
    let people = vec![
        person(
            "solo",
            "usr_local",
            PrincipalKind::LocalOwner,
            None,
            Role::Owner,
        ),
        person(
            "pat",
            "tok_pat",
            PrincipalKind::Service,
            Some(org.to_string()),
            Role::Researcher,
        ),
    ];
    async fn post(
        srv: &TestServer,
        token: &str,
        path: String,
        key: Option<&str>,
        body: &Value,
    ) -> Value {
        let res = call(srv, Method::POST, &path, token, key, Some(body)).await;
        assert_eq!(res.status, 201, "{path}: {}", res.text());
        res.json()
    }
    let runs = |p: ProjectId| format!("/api/v1/projects/{p}/runs");
    let exps = |p: ProjectId| format!("/api/v1/projects/{p}/experiments");

    // Before the directory: the solo owner writes under the nil org, the token under its
    // org, so P gets a `default` and a `sweep` in each, with different descriptions.
    let old = runs_server_with(
        db.db(),
        accepting_chains(),
        &people,
        vec![],
        &Harness::default(),
    )
    .await;
    let solo_run = post(&old, "solo", runs(p), Some("a"), &submission("solo")).await;
    let pat_run = post(&old, "pat", runs(p), Some("b"), &submission("pat")).await;
    let local_sweep = post(
        &old,
        "solo",
        exps(p),
        None,
        &json!({"name": "sweep", "description": "local", "tags": ["x"]}),
    )
    .await;
    let org_sweep = post(
        &old,
        "pat",
        exps(p),
        None,
        &json!({"name": "sweep", "description": "org"}),
    )
    .await;
    let parent = post(&old, "solo", runs(q), Some("c"), &submission("parent")).await;
    let mut child = submission("child");
    child["parent_run_id"] = parent["id"].clone();
    let child = post(&old, "solo", runs(p), Some("d"), &child).await;
    let orphan_parent = post(
        &old,
        "solo",
        runs(m),
        Some("e"),
        &submission("orphan-parent"),
    )
    .await;
    let mut orphan = submission("orphan");
    orphan["parent_run_id"] = orphan_parent["id"].clone();
    post(&old, "solo", runs(u), Some("f"), &orphan).await;
    old.shutdown().await.unwrap();

    let harness = Harness::default();
    *harness.rules.lock().unwrap() = AuthRules::with_projects(&[(p, org), (q, org), (u, org)]);
    for start in ["first start", "second start"] {
        let srv = runs_server_with(db.db(), accepting_chains(), &people, vec![], &harness).await;
        // P and Q moved, events included; U and M (linked to a project nobody knows) stayed.
        let left: Vec<uuid::Uuid> = sqlx::query_scalar(
            "SELECT DISTINCT project_id FROM runs WHERE org_id = $1 ORDER BY project_id",
        )
        .bind(uuid::Uuid::nil())
        .fetch_all(db.pool())
        .await
        .unwrap();
        let mut want = vec![u.as_uuid(), m.as_uuid()];
        want.sort();
        assert_eq!(left, want, "{start}");
        let nil_events: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM outbox_events o JOIN jobs j ON o.aggregate_id = j.id \
              WHERE o.org_id = $1 AND j.project_id = ANY($2)",
        )
        .bind(uuid::Uuid::nil())
        .bind([p.as_uuid(), q.as_uuid()])
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(nil_events, 0, "{start}: job events still under the nil org");

        let get = |path: String| {
            let srv = &srv;
            async move {
                let res = call(srv, Method::GET, &path, "pat", None, None).await;
                assert_eq!(res.status, 200, "{path}: {}", res.text());
                res.json()
            }
        };
        // Every run is visible to the token and kept its experiment.
        let in_p = get(runs(p)).await;
        let listed = in_p["items"].as_array().unwrap();
        for run in [&solo_run, &pat_run, &child] {
            let got = listed.iter().find(|r| r["id"] == run["id"]);
            let got = got.unwrap_or_else(|| panic!("{start}: {} missing from P", run["id"]));
            assert_eq!(got["experiment_id"], run["experiment_id"], "{start}");
        }
        assert_ne!(solo_run["experiment_id"], pat_run["experiment_id"]);
        // Both `sweep`s keep their ids and metadata; the local one has a fixed new name.
        let local = get(format!(
            "/api/v1/experiments/{}",
            local_sweep["id"].as_str().unwrap()
        ))
        .await;
        let suffix = local_sweep["id"].as_str().unwrap();
        assert!(
            local["name"].as_str().unwrap().starts_with("sweep (local "),
            "{start}: {local}"
        );
        assert_eq!(
            (&local["description"], &local["tags"]),
            (&json!("local"), &json!(["x"])),
            "{start}"
        );
        let theirs = get(format!(
            "/api/v1/experiments/{}",
            org_sweep["id"].as_str().unwrap()
        ))
        .await;
        assert_eq!(
            (&theirs["name"], &theirs["description"]),
            (&json!("sweep"), &json!("org")),
            "{start}"
        );
        let names: Vec<Value> = get(exps(p)).await["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].clone())
            .collect();
        assert_eq!(names.len(), 4, "{start}: {names:?} ({suffix})");
        // Lineage survives.
        let got = get(format!("/api/v1/runs/{}", child["id"].as_str().unwrap())).await;
        assert_eq!(got["parent_run_id"], parent["id"], "{start}");
        let tl = get(format!(
            "/api/v1/runs/{}/timeline",
            child["id"].as_str().unwrap()
        ))
        .await;
        assert_eq!(tl["items"][0]["type"], "job.created", "{start}");
        srv.shutdown().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn legacy_runs_move_when_the_org_already_used_their_submit_key() {
    use mrl_server::seams::{Principal, PrincipalKind};
    let db = TestDb::new().await;
    let org = OrgId::new();
    let [p, q] = [(); 2].map(|_| ProjectId::new());
    let solo = vec![TestUser {
        token: "solo".into(),
        principal: Principal {
            id: "usr_local".into(),
            kind: PrincipalKind::LocalOwner,
            org: None,
        },
        role: Role::Owner,
    }];
    async fn submit(srv: &TestServer, prj: ProjectId, body: Value) -> Value {
        let path = format!("/api/v1/projects/{prj}/runs");
        let res = call(
            srv,
            Method::POST,
            &path,
            "solo",
            Some("nightly"),
            Some(&body),
        )
        .await;
        assert_eq!(res.status, 201, "{path}: {}", res.text());
        res.json()
    }
    // Before the directory: key `nightly` in P, under the nil org.
    let old = runs_server_with(
        db.db(),
        accepting_chains(),
        &solo,
        vec![],
        &Harness::default(),
    )
    .await;
    let local = submit(&old, p, submission("local")).await;
    old.shutdown().await.unwrap();
    // The directory knows only Q: P stays, and `nightly` is used again in the org.
    let harness = Harness::default();
    *harness.rules.lock().unwrap() = AuthRules::with_projects(&[(q, org)]);
    let srv = runs_server_with(db.db(), accepting_chains(), &solo, vec![], &harness).await;
    let body = submission("org");
    let theirs = submit(&srv, q, body.clone()).await;
    srv.shutdown().await.unwrap();
    // Now P is known too: its run moves although the org already has that key.
    *harness.rules.lock().unwrap() = AuthRules::with_projects(&[(p, org), (q, org)]);
    let srv = runs_server_with(db.db(), accepting_chains(), &solo, vec![], &harness).await;
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM runs WHERE org_id = $1")
        .bind(uuid::Uuid::nil())
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(left, 0, "the run in P stayed under the nil org");
    let path = format!("/api/v1/runs/{}", local["id"].as_str().unwrap());
    let res = call(&srv, Method::GET, &path, "solo", None, None).await;
    assert_eq!(res.status, 200, "{}", res.text());
    // The key still means the org's run.
    let again = submit(&srv, q, body).await;
    assert_eq!(again["id"], theirs["id"]);
    srv.shutdown().await.unwrap();
}

/// A job of a new run in `e`, moved to ALLOCATING.
async fn allocating(e: &Env, key: &str) -> JobRow {
    let run = e.submit("res", key, &submission(key)).await.json();
    let job = e.job_row(run["job_id"].as_str().unwrap()).await;
    let mut tx = e.db.db().begin().await.unwrap();
    let job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Place,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    job
}

async fn attempts_and_events(e: &Env, job: uuid::Uuid) -> (i64, i64) {
    sqlx::query_as(
        "SELECT (SELECT count(*) FROM job_attempts WHERE job_id = $1), \
                (SELECT count(*) FROM outbox_events WHERE aggregate_kind = 'attempt' \
                   AND aggregate_id IN (SELECT id FROM job_attempts WHERE job_id = $1))",
    )
    .bind(job)
    .fetch_one(e.db.pool())
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn stale_launches_add_nothing() {
    let e = env(true).await;
    let org = e.org.as_uuid();
    // Cancelled after the scheduler read it: the stale launch is refused and adds nothing,
    // even if the caller commits.
    let stale = allocating(&e, "cancelled").await;
    let mut tx = e.db.db().begin().await.unwrap();
    let fresh = JobTransitions::lock(&mut tx, org, stale.id)
        .await
        .unwrap()
        .unwrap();
    let done = JobTransitions::request_cancel(&mut tx, &fresh, "usr_res")
        .await
        .unwrap();
    assert_eq!(done.state, "CANCELLED");
    tx.commit().await.unwrap();
    let mut tx = e.db.db().begin().await.unwrap();
    let err = JobTransitions::launch(&mut tx, &stale, None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-CONFLICT-170");
    tx.commit().await.unwrap();
    assert_eq!(attempts_and_events(&e, stale.id).await, (0, 0));

    // Two launches from the same read: the second is a conflict, not a uniqueness error.
    let row = allocating(&e, "twice").await;
    let mut tx = e.db.db().begin().await.unwrap();
    JobTransitions::launch(&mut tx, &row, None).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = e.db.db().begin().await.unwrap();
    let err = JobTransitions::launch(&mut tx, &row, None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "MRL-CONFLICT-170");
    tx.commit().await.unwrap();
    assert_eq!(attempts_and_events(&e, row.id).await, (1, 1));
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn job_and_attempt_locks_never_deadlock() {
    let e = env(true).await;
    let org = e.org.as_uuid();
    let job = allocating(&e, "locks").await;
    let mut tx = e.db.db().begin().await.unwrap();
    let (_, att) = JobTransitions::launch(&mut tx, &job, None).await.unwrap();
    tx.commit().await.unwrap();
    // A locks the job, waits, then works on the attempt. B starts meanwhile from the attempt.
    let a = async {
        let mut tx = e.db.db().begin().await.unwrap();
        JobTransitions::lock(&mut tx, org, job.id)
            .await
            .unwrap()
            .unwrap();
        sqlx::query("SELECT pg_sleep(0.3)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let att = JobTransitions::lock_attempt(&mut tx, org, att.id)
            .await
            .unwrap()
            .unwrap();
        let r = JobTransitions::apply_attempt(
            &mut tx,
            &att,
            JobAttemptEvent::Acknowledge,
            AttemptChange::default(),
        )
        .await;
        tx.commit().await.unwrap();
        r
    };
    let b = async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let mut tx = e.db.db().begin().await.unwrap();
        let att = JobTransitions::lock_attempt(&mut tx, org, att.id)
            .await
            .unwrap()
            .unwrap();
        let r = JobTransitions::apply_attempt(
            &mut tx,
            &att,
            JobAttemptEvent::Kill,
            AttemptChange::default(),
        )
        .await;
        tx.commit().await.unwrap();
        r
    };
    let (ra, rb) = tokio::join!(a, b);
    assert_eq!(ra.unwrap().state, "LAUNCHING");
    assert_eq!(
        rb.unwrap().state,
        "KILLED",
        "B waited for A instead of deadlocking"
    );
}

/// A verifier that reads the database through `pool`, like a real one would.
struct DbVerifier(mrl_server::Db);

#[async_trait::async_trait]
impl mrl_server::modules::jobs::CompletionVerifier for DbVerifier {
    fn name(&self) -> &'static str {
        "db-verifier"
    }
    async fn verify(
        &self,
        _: &JobRow,
    ) -> Result<mrl_server::modules::jobs::Verification, mrl_server::seams::SeamError> {
        sqlx::query("SELECT 1")
            .execute(self.0.pool())
            .await
            .map_err(|e| mrl_server::seams::SeamError::new("verifier", e.to_string()))?;
        Ok(mrl_server::modules::jobs::Verification::Verified)
    }
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_database_verifier_works_on_one_connection() {
    let e = env(true).await;
    let org = e.org.as_uuid();
    let job = allocating(&e, "verify").await;
    let mut tx = e.db.db().begin().await.unwrap();
    let (job, _) = JobTransitions::launch(&mut tx, &job, None).await.unwrap();
    let job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Start,
        Owner::Jobs,
        JobChange::default(),
    )
    .await
    .unwrap();
    let job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Finish,
        Owner::Jobs,
        JobChange::default(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let small = e.db.replica_with(&mrl_server::config::DatabaseConfig {
        max_connections: 1,
        acquire_timeout_seconds: 3,
        ..mrl_testkit::pg::test_config()
    });
    let chains = Chains::new(
        vec![],
        vec![std::sync::Arc::new(DbVerifier(small.clone()))],
        vec![],
    );
    // Verify first, then write: the verifier never waits for the transaction's connection.
    let verdict = chains.verify(&job).await.unwrap();
    let mut tx = small.begin().await.unwrap();
    let row = JobTransitions::lock(&mut tx, org, job.id)
        .await
        .unwrap()
        .unwrap();
    let done = JobTransitions::verify_completion(&mut tx, &row, verdict)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(done.state, "COMPLETED");
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn submits_racing_renames_of_default_never_fail() {
    let e = env(true).await;
    for i in 0..10 {
        let key = format!("race-{i}");
        let body = submission(&key);
        let submit = e.submit("res", &key, &body);
        let rename = async {
            let exps = e
                .get(
                    "res",
                    &format!("/api/v1/projects/{}/experiments", e.project),
                )
                .await
                .json();
            let default = exps["items"]
                .as_array()
                .unwrap()
                .iter()
                .find(|x| x["name"] == "default")
                .cloned();
            if let Some(d) = default {
                let path = format!("/api/v1/experiments/{}", d["id"].as_str().unwrap());
                let body = json!({"name": format!("renamed-{i}")});
                let res = call(&e.srv, Method::PATCH, &path, "res", None, Some(&body)).await;
                assert!(res.status == 200 || res.status == 409, "{}", res.text());
            }
        };
        let (res, ()) = tokio::join!(submit, rename);
        assert_eq!(res.status, 201, "round {i}: {}", res.text());
    }
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_token_without_an_org_sees_its_jobs() {
    use mrl_server::seams::{Principal, PrincipalKind};
    let db = TestDb::new().await;
    let org = OrgId::new();
    let project = ProjectId::new();
    let people = vec![TestUser {
        token: "solo-pat".into(),
        principal: Principal {
            id: "tok_solo".into(),
            kind: PrincipalKind::Service,
            org: None,
        },
        role: Role::Owner,
    }];
    let harness = Harness::default();
    *harness.rules.lock().unwrap() = AuthRules::with_projects(&[(project, org)]);
    let srv = runs_server_with(db.db(), accepting_chains(), &people, vec![], &harness).await;
    let path = format!("/api/v1/projects/{project}/runs");
    let run = call(
        &srv,
        Method::POST,
        &path,
        "solo-pat",
        Some("k"),
        Some(&submission("x")),
    )
    .await;
    assert_eq!(run.status, 201, "{}", run.text());
    let job = run.json()["job_id"].as_str().unwrap().to_owned();
    let one = call(
        &srv,
        Method::GET,
        &format!("/api/v1/jobs/{job}"),
        "solo-pat",
        None,
        None,
    )
    .await;
    assert_eq!(one.status, 200, "{}", one.text());
    let all = call(&srv, Method::GET, "/api/v1/jobs", "solo-pat", None, None)
        .await
        .json();
    let ids: Vec<&str> = all["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|j| j["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, [job.as_str()], "{all}");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn nul_characters_are_field_errors() {
    let e = env(true).await;
    let mut named = submission("a\u{0}b");
    named["name"] = json!("a\u{0}b");
    let mut configured = submission("cfg");
    configured["config"] = json!({"note": "x\u{0}"});
    for (body, path) in [(named, "/name"), (configured, "/config/note")] {
        let res = e.submit("res", path, &body).await;
        assert_eq!(res.status, 422, "{}", res.text());
        assert_eq!(res.json()["errors"][0]["path"], path, "{}", res.text());
    }
    let exps = format!("/api/v1/projects/{}/experiments", e.project);
    let res = call(
        &e.srv,
        Method::POST,
        &exps,
        "res",
        None,
        Some(&json!({"name": "s", "tags": ["ok", "b\u{0}"]})),
    )
    .await;
    assert_eq!(
        (res.status, res.json()["errors"][0]["path"].clone()),
        (422, json!("/tags/1")),
        "{}",
        res.text()
    );
    let made = call(
        &e.srv,
        Method::POST,
        &exps,
        "res",
        None,
        Some(&json!({"name": "s"})),
    )
    .await
    .json();
    let path = format!("/api/v1/experiments/{}", made["id"].as_str().unwrap());
    let res = call(
        &e.srv,
        Method::PATCH,
        &path,
        "res",
        None,
        Some(&json!({"description": "d\u{0}"})),
    )
    .await;
    assert_eq!(
        (res.status, res.json()["errors"][0]["path"].clone()),
        (422, json!("/description")),
        "{}",
        res.text()
    );
    // Query-string filters are checked before they reach the database.
    let runs = format!("/api/v1/projects/{}/runs", e.project);
    for q in [
        "/api/v1/jobs?kind=%00",
        "/api/v1/jobs?kind=bogus",
        "/api/v1/jobs?state=%00",
        "/api/v1/jobs?project=%00",
        &format!("{runs}?state=%00"),
        &format!("{runs}?experiment=%00"),
    ] {
        let res = call(&e.srv, Method::GET, q, "res", None, None).await;
        assert_eq!(
            (res.status, res.json()["code"].clone()),
            (400, json!("MRL-VAL-006")),
            "{q}: {}",
            res.text()
        );
    }
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM runs")
        .fetch_one(e.db.pool())
        .await
        .unwrap();
    assert_eq!(runs, 0);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_failed_audit_write_rolls_the_change_back() {
    use std::sync::atomic::Ordering;
    let db = TestDb::new().await;
    let org = OrgId::new();
    let project = ProjectId::new();
    let harness = Harness::default();
    *harness.rules.lock().unwrap() = AuthRules::with_projects(&[(project, org)]);
    let srv = runs_server_with(
        db.db(),
        accepting_chains(),
        &users(org, OrgId::new()),
        vec![],
        &harness,
    )
    .await;
    let runs = format!("/api/v1/projects/{project}/runs");
    let exps = format!("/api/v1/projects/{project}/experiments");
    let run = call(
        &srv,
        Method::POST,
        &runs,
        "res",
        Some("ok"),
        Some(&submission("kept")),
    )
    .await
    .json();
    let exp = call(
        &srv,
        Method::POST,
        &exps,
        "res",
        None,
        Some(&json!({"name": "e"})),
    )
    .await
    .json();
    let count = |table: &'static str| {
        let db = &db;
        async move {
            let n: i64 =
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
                    .fetch_one(db.pool())
                    .await
                    .unwrap();
            n
        }
    };
    let before = (
        count("runs").await,
        count("experiments").await,
        count("outbox_events").await,
    );
    harness.audit.fail.store(true, Ordering::SeqCst);
    let job = run["job_id"].as_str().unwrap();
    let rid = run["id"].as_str().unwrap();
    let eid = exp["id"].as_str().unwrap();
    for (method, path, key, body) in [
        (
            Method::POST,
            runs.clone(),
            Some("new"),
            Some(submission("lost")),
        ),
        (
            Method::POST,
            exps.clone(),
            None,
            Some(json!({"name": "lost"})),
        ),
        (
            Method::PATCH,
            format!("/api/v1/experiments/{eid}"),
            None,
            Some(json!({"description": "lost"})),
        ),
        (
            Method::POST,
            format!("/api/v1/jobs/{job}:cancel"),
            None,
            None,
        ),
        (
            Method::POST,
            format!("/api/v1/runs/{rid}:cancel"),
            None,
            None,
        ),
        (
            Method::PATCH,
            format!("/api/v1/jobs/{job}"),
            None,
            Some(json!({"priority": 9})),
        ),
    ] {
        let res = call(&srv, method.clone(), &path, "maint", key, body.as_ref()).await;
        assert_eq!(res.status, 503, "{method} {path}: {}", res.text());
    }
    let after = (
        count("runs").await,
        count("experiments").await,
        count("outbox_events").await,
    );
    assert_eq!(after, before, "nothing was written");
    harness.audit.fail.store(false, Ordering::SeqCst);
    let get = |path: String| {
        let srv = &srv;
        async move {
            call(srv, Method::GET, &path, "res", None, None)
                .await
                .json()
        }
    };
    let job_now = get(format!("/api/v1/jobs/{job}")).await;
    assert_eq!(
        (&job_now["state"], &job_now["priority"]),
        (&json!("QUEUED"), &json!(0))
    );
    assert!(get(format!("/api/v1/experiments/{eid}")).await["description"].is_null());
    srv.shutdown().await.unwrap();
}

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

struct Env {
    db: TestDb,
    srv: TestServer,
    org: OrgId,
    project: ProjectId,
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
    let srv = runs_server(&db, chains, &users(org, OrgId::new()), vec![]).await;
    Env {
        db,
        srv,
        org,
        project: ProjectId::new(),
    }
}

impl Env {
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

    async fn timeline(&self, run: &str) -> Vec<String> {
        let res = self
            .get("viewer", &format!("/api/v1/runs/{run}/timeline"))
            .await;
        assert_eq!(res.status, 200, "{}", res.text());
        res.json()["items"]
            .as_array()
            .unwrap()
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
    let theirs = e.get("outsider", &e.runs_path()).await.json();
    assert_eq!(theirs["items"].as_array().unwrap().len(), 0);
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
    let tl = e.timeline(&rid).await;
    assert!(
        tl.ends_with(&[
            "job.cancel_requested".into(),
            "attempt.killed".into(),
            "job.cancelled".into()
        ]),
        "{tl:?}"
    );
    assert!(tl.contains(&"attempt.created".to_owned()));

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
    let failed = JobTransitions::verify_completion(&mut tx, &job, &none)
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
    let special = [JobEvent::Launch, JobEvent::Complete, JobEvent::Interrupt];
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
        let verified = JobTransitions::verify_completion(&mut tx, &row, &accepting_chains()).await;
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

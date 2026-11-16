//! Test helpers for runs and jobs: a role-based authorizer, an in-memory tenancy double
//! for CLI tests, and a team-mode server with the runs module on a [`TestDb`].
//!
//! ```text
//! let db = TestDb::new().await;
//! let srv = runs_server(&db, Chains::new(vec![], vec![], vec![]), &[("tok", alice())]).await;
//! let res = srv.post("/api/v1/projects/prj_…/runs", "tok", Some("key-1"), &body).await;
//! ```

#[cfg(test)]
mod tests;

use crate::pg::TestDb;
use crate::seams as ts;
use crate::server::{TestResponse, TestServer};
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::Path;
use axum::http::{Method, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use mrl_domain::ids::{DatasetVersionId, ModelVersionId, OrgId, ProjectId, WorkspaceId};
use mrl_domain::rbac::{allowed, Action, Role};
use mrl_server::config::{BucketConfig, Config, Mode};
use mrl_server::modules::jobs::{
    Chains, CompletionVerifier, JobRow, JobSpec, JobValidator, SchemaChecks, Verdict, Verification,
};
use mrl_server::modules::runs::RunsModule;
use mrl_server::seams::{Authorizer, Decision, Principal, RequestContext, Resource, SeamError};
use mrl_server::{AppState, ServerModule};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

/// One signed-in test user: token, principal and role.
#[derive(Debug, Clone)]
pub struct TestUser {
    /// Bearer token.
    pub token: String,
    /// Who it signs in as.
    pub principal: Principal,
    /// Role in their own org.
    pub role: Role,
}

/// A test user with `role` in `org`.
pub fn test_user(token: &str, id: &str, org: OrgId, role: Role) -> TestUser {
    TestUser {
        token: token.to_owned(),
        principal: ts::user(id, &org.to_string()),
        role,
    }
}

/// Authorizes like RBAC: other orgs are hidden (404); inside the org the role table
/// from `mrl_domain::rbac` decides (403 when refused).
pub struct RoleAuthorizer(HashMap<String, Role>);

impl RoleAuthorizer {
    /// Knows these users.
    pub fn new(users: &[TestUser]) -> Self {
        Self(
            users
                .iter()
                .map(|u| (u.principal.id.clone(), u.role))
                .collect(),
        )
    }
}

#[async_trait]
impl Authorizer for RoleAuthorizer {
    async fn authorize(
        &self,
        ctx: &RequestContext,
        action: &str,
        resource: &Resource,
    ) -> Result<Decision, SeamError> {
        if resource.org.is_some() && resource.org != ctx.principal.org {
            return Ok(Decision::Hide);
        }
        let Some(role) = self.0.get(&ctx.principal.id) else {
            return Ok(Decision::Hide);
        };
        let action: Option<Action> = serde_json::from_value(json!(action)).ok();
        Ok(match action {
            Some(a) if allowed(*role, a) => Decision::Allow,
            _ => Decision::Deny,
        })
    }
}

/// Team-mode config on loopback with a rate limit no test reaches.
pub fn team_config() -> Config {
    let mut c = Config::default();
    c.server.mode = Mode::Team;
    let any = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    c.server.listeners.api = any;
    c.server.listeners.grpc = any;
    c.server.listeners.internal = any;
    let wide = BucketConfig {
        per_second: 100_000,
        burst: 100_000,
    };
    c.server.rate_limit.principal = wide;
    c.server.rate_limit.org = wide;
    c
}

/// A team server with the runs module on `db`, these `chains`, these users and any extra
/// modules (like [`TenancyDouble`]).
pub async fn runs_server(
    db: &TestDb,
    chains: Chains,
    users: &[TestUser],
    extra: Vec<Box<dyn ServerModule>>,
) -> TestServer {
    let tokens: Vec<(&str, Principal)> = users
        .iter()
        .map(|u| (u.token.as_str(), u.principal.clone()))
        .collect();
    let seams = ts::team(&tokens).authorizer(RoleAuthorizer::new(users));
    let seams = ts::build(seams, Mode::Team);
    let mut modules: Vec<Box<dyn ServerModule>> =
        vec![Box::new(RunsModule::with_chains(db.db().clone(), chains))];
    modules.extend(extra);
    match TestServer::start(team_config(), seams, modules).await {
        Ok(s) => s,
        Err(e) => panic!("runs test server did not start: {}", e.explain()),
    }
}

/// Sends `method path` with a bearer token, an optional idempotency key and JSON body.
pub async fn call(
    srv: &TestServer,
    method: Method,
    path: &str,
    token: &str,
    key: Option<&str>,
    body: Option<&Value>,
) -> TestResponse {
    let auth = format!("Bearer {token}");
    let mut headers: Vec<(&str, &str)> = vec![("authorization", &auth)];
    if let Some(k) = key {
        headers.push(("idempotency-key", k));
    }
    let body = match body {
        Some(v) => {
            headers.push(("content-type", "application/json"));
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    srv.request(method, path, &headers, body).await
}

#[derive(Default)]
struct Tenancy {
    orgs: Vec<Value>,
    workspaces: Vec<Value>,
    projects: Vec<Value>,
}

/// An in-memory stand-in for the tenancy endpoints `mrl dev seed` and `mrl projects` call
/// (Doc 09 §6.3): `GET /me`, `GET/POST /orgs`, `GET/POST /orgs/{org}/workspaces`,
/// `GET/POST /workspaces/{ws}/projects`, `GET /projects/{prj}`. The real ones are P16's.
#[derive(Clone, Default)]
pub struct TenancyDouble(Arc<Mutex<Tenancy>>);

fn page(items: Vec<Value>) -> Json<Value> {
    Json(json!({ "items": items, "next_cursor": null }))
}

fn create(list: &mut Vec<Value>, item: Value) -> (StatusCode, Json<Value>) {
    list.push(item.clone());
    (StatusCode::CREATED, Json(item))
}

impl TenancyDouble {
    fn lock(&self) -> std::sync::MutexGuard<'_, Tenancy> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// How many orgs, workspaces and projects exist.
    pub fn counts(&self) -> (usize, usize, usize) {
        let t = self.lock();
        (t.orgs.len(), t.workspaces.len(), t.projects.len())
    }
}

impl ServerModule for TenancyDouble {
    fn name(&self) -> &'static str {
        "tenancy-double"
    }

    fn routes(&self) -> Router<AppState> {
        let me = |ctx: RequestContext| async move {
            Json(json!({ "id": ctx.principal.id, "org_id": ctx.principal.org }))
        };
        let (t1, t2, t3, t4, t5, t6, t7) = (
            self.clone(),
            self.clone(),
            self.clone(),
            self.clone(),
            self.clone(),
            self.clone(),
            self.clone(),
        );
        Router::new()
            .route("/me", get(me))
            .route(
                "/orgs",
                get(move || async move { page(t1.lock().orgs.clone()) }).post(
                    move |Json(b): Json<Value>| async move {
                        let item = json!({"id": OrgId::new().to_string(), "slug": b["slug"], "name": b["name"]});
                        create(&mut t2.lock().orgs, item)
                    },
                ),
            )
            .route(
                "/orgs/{org}/workspaces",
                get(move |Path(org): Path<String>| async move {
                    let t = t3.lock();
                    page(t.workspaces.iter().filter(|w| w["org_id"] == org.as_str()).cloned().collect())
                })
                .post(move |Path(org): Path<String>, Json(b): Json<Value>| async move {
                    let item = json!({"id": WorkspaceId::new().to_string(), "org_id": org, "slug": b["slug"], "name": b["name"]});
                    create(&mut t4.lock().workspaces, item)
                }),
            )
            .route(
                "/workspaces/{ws}/projects",
                get(move |Path(ws): Path<String>| async move {
                    let t = t5.lock();
                    page(t.projects.iter().filter(|p| p["workspace_id"] == ws.as_str()).cloned().collect())
                })
                .post(move |Path(ws): Path<String>, Json(b): Json<Value>| async move {
                    let item = json!({"id": ProjectId::new().to_string(), "workspace_id": ws, "slug": b["slug"], "name": b["name"]});
                    create(&mut t6.lock().projects, item)
                }),
            )
            .route(
                "/projects/{prj}",
                get(move |Path(prj): Path<String>| async move {
                    let t = t7.lock();
                    match t.projects.iter().find(|p| p["id"] == prj.as_str()) {
                        Some(p) => Ok(Json(p.clone())),
                        None => Err(StatusCode::NOT_FOUND),
                    }
                }),
            )
    }
}

/// A validator that vouches for every job, so submissions are QUEUED.
pub struct AcceptAll;

#[async_trait]
impl JobValidator for AcceptAll {
    fn name(&self) -> &'static str {
        "accept-all"
    }
    async fn validate(&self, _: &JobSpec) -> Result<Verdict, SeamError> {
        Ok(Verdict::Accept)
    }
}

/// A verifier that confirms every finished job.
pub struct VerifyAll;

#[async_trait]
impl CompletionVerifier for VerifyAll {
    fn name(&self) -> &'static str {
        "verify-all"
    }
    async fn verify(&self, _: &JobRow) -> Result<Verification, SeamError> {
        Ok(Verification::Verified)
    }
}

/// Chains that queue every job and complete every finished one.
pub fn accepting_chains() -> Chains {
    Chains::new(
        vec![Arc::new(SchemaChecks), Arc::new(AcceptAll)],
        vec![Arc::new(VerifyAll)],
        vec![],
    )
}

/// A valid submission body named `name`.
pub fn submission(name: &str) -> Value {
    json!({
        "name": name,
        "algorithm_id": "sft",
        "base_model_version_id": ModelVersionId::new().to_string(),
        "dataset_version_ids": [DatasetVersionId::new().to_string()],
        "config": {"epochs": 1},
    })
}

/// Random API and system sequences over runs and jobs, with the invariants every
/// sequence must keep: at most one live attempt per job, terminal states never change,
/// and a run always mirrors its job's state.
pub mod sequences {
    use super::*;
    use mrl_domain::failure::{FailureClass, FailureCode};
    use mrl_domain::state::{Job, JobAttemptEvent, JobEvent, JobState, Owner, StateMachine};
    use mrl_server::modules::jobs::{AttemptChange, JobChange, JobTransitions};
    use proptest::prelude::*;
    use uuid::Uuid;

    /// One step. Indexes pick among the jobs made so far (modulo their count).
    #[derive(Debug, Clone)]
    pub enum Op {
        /// `POST /projects/{prj}/runs`.
        Submit,
        /// `POST /runs/{run}:cancel`.
        CancelRun(usize),
        /// `POST /jobs/{job}:cancel`.
        CancelJob(usize),
        /// `PATCH /jobs/{job}`.
        Priority(usize, i32),
        /// Scheduler: QUEUED → ALLOCATING.
        Place(usize),
        /// Scheduler: ALLOCATING → PREPARING with a new attempt.
        Launch(usize),
        /// Scheduler: ALLOCATING → QUEUED.
        Requeue(usize),
        /// Scheduler: RETRY_WAIT → QUEUED.
        Retry(usize),
        /// Jobs: PREPARING → RUNNING.
        StartJob(usize),
        /// Jobs: RUNNING → FINALIZING.
        Finish(usize),
        /// Verifier chain on a FINALIZING job.
        Verify(usize),
        /// A failure of this class (index into `FailureClass::ALL`).
        Fail(usize, usize),
        /// An event on the job's current attempt.
        Attempt(usize, usize),
    }

    const ATTEMPT_EVENTS: [JobAttemptEvent; 6] = [
        JobAttemptEvent::Acknowledge,
        JobAttemptEvent::Start,
        JobAttemptEvent::ExitOk,
        JobAttemptEvent::ExitError,
        JobAttemptEvent::LoseLease,
        JobAttemptEvent::Kill,
    ];

    fn op() -> impl Strategy<Value = Op> {
        let i = 0usize..8;
        prop_oneof![
            3 => Just(Op::Submit),
            1 => i.clone().prop_map(Op::CancelRun),
            1 => i.clone().prop_map(Op::CancelJob),
            1 => (i.clone(), -5i32..5).prop_map(|(j, p)| Op::Priority(j, p)),
            2 => i.clone().prop_map(Op::Place),
            2 => i.clone().prop_map(Op::Launch),
            1 => i.clone().prop_map(Op::Requeue),
            1 => i.clone().prop_map(Op::Retry),
            2 => i.clone().prop_map(Op::StartJob),
            1 => i.clone().prop_map(Op::Finish),
            1 => i.clone().prop_map(Op::Verify),
            1 => (i.clone(), 0usize..FailureClass::ALL.len()).prop_map(|(j, c)| Op::Fail(j, c)),
            3 => (i, 0usize..ATTEMPT_EVENTS.len()).prop_map(|(j, e)| Op::Attempt(j, e)),
        ]
    }

    /// 1 to `max` random steps.
    pub fn ops(max: usize) -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(op(), 1..max)
    }

    /// The state one sequence runs in.
    pub struct World<'a> {
        /// Server under test.
        pub srv: &'a TestServer,
        /// Its database.
        pub db: &'a TestDb,
        /// Token of a maintainer in `org`.
        pub token: &'a str,
        /// The org.
        pub org: Uuid,
        /// The project runs go into.
        pub project: ProjectId,
        /// `(run id, job id)` of every submitted run.
        pub runs: Vec<(String, Uuid)>,
        /// Last seen state of every job.
        pub seen: HashMap<Uuid, String>,
    }

    impl World<'_> {
        fn pick(&self, i: usize) -> Option<(String, Uuid)> {
            (!self.runs.is_empty()).then(|| self.runs[i % self.runs.len()].clone())
        }

        /// Runs one step. Refused steps (illegal from the current state) are expected.
        pub async fn step(&mut self, op: &Op) {
            let jobs = self.pick(match op {
                Op::Submit => 0,
                Op::CancelRun(i)
                | Op::CancelJob(i)
                | Op::Priority(i, _)
                | Op::Place(i)
                | Op::Launch(i)
                | Op::Requeue(i)
                | Op::Retry(i)
                | Op::StartJob(i)
                | Op::Finish(i)
                | Op::Verify(i)
                | Op::Fail(i, _)
                | Op::Attempt(i, _) => *i,
            });
            match (op, jobs) {
                (Op::Submit, _) => {
                    let n = self.runs.len();
                    let path = format!("/api/v1/projects/{}/runs", self.project);
                    let key = format!("seq-{}-{n}", Uuid::new_v4());
                    let body = submission(&format!("run-{n}"));
                    let res = call(
                        self.srv,
                        Method::POST,
                        &path,
                        self.token,
                        Some(&key),
                        Some(&body),
                    )
                    .await;
                    assert_eq!(res.status, 201, "{}", res.text());
                    let v = res.json();
                    let run = v["id"].as_str().unwrap_or_default().to_owned();
                    let job: mrl_domain::ids::JobId = v["job_id"]
                        .as_str()
                        .unwrap_or_default()
                        .parse()
                        .unwrap_or_else(|e| panic!("job id: {e}"));
                    self.runs.push((run, job.as_uuid()));
                }
                (_, None) => {}
                (Op::CancelRun(_), Some((run, _))) => {
                    let res = call(
                        self.srv,
                        Method::POST,
                        &format!("/api/v1/runs/{run}:cancel"),
                        self.token,
                        None,
                        None,
                    )
                    .await;
                    assert!(matches!(res.status, 200 | 409), "{}", res.text());
                }
                (Op::CancelJob(_), Some((_, job))) => {
                    let id = mrl_domain::ids::JobId::from_uuid(job);
                    let res = call(
                        self.srv,
                        Method::POST,
                        &format!("/api/v1/jobs/{id}:cancel"),
                        self.token,
                        None,
                        None,
                    )
                    .await;
                    assert!(matches!(res.status, 200 | 409), "{}", res.text());
                }
                (Op::Priority(_, p), Some((_, job))) => {
                    let id = mrl_domain::ids::JobId::from_uuid(job);
                    let body = json!({ "priority": p });
                    let res = call(
                        self.srv,
                        Method::PATCH,
                        &format!("/api/v1/jobs/{id}"),
                        self.token,
                        None,
                        Some(&body),
                    )
                    .await;
                    assert!(matches!(res.status, 200 | 409), "{}", res.text());
                }
                (op, Some((_, job))) => self.system(op, job).await,
            }
        }

        async fn system(&self, op: &Op, job: Uuid) {
            let chains = accepting_chains();
            let mut tx = match self.db.db().begin().await {
                Ok(tx) => tx,
                Err(e) => panic!("begin: {e}"),
            };
            let Ok(Some(row)) = JobTransitions::lock(&mut tx, self.org, job).await else {
                panic!("job {job} vanished");
            };
            let none = JobChange::default();
            let result = match op {
                Op::Place(_) => {
                    JobTransitions::apply(&mut tx, &row, JobEvent::Place, Owner::Scheduler, none)
                        .await
                        .map(|_| ())
                }
                Op::Launch(_) => JobTransitions::launch(&mut tx, &row, None)
                    .await
                    .map(|_| ()),
                Op::Requeue(_) => {
                    JobTransitions::apply(&mut tx, &row, JobEvent::Requeue, Owner::Scheduler, none)
                        .await
                        .map(|_| ())
                }
                Op::Retry(_) => {
                    JobTransitions::apply(&mut tx, &row, JobEvent::Retry, Owner::Scheduler, none)
                        .await
                        .map(|_| ())
                }
                Op::StartJob(_) => {
                    JobTransitions::apply(&mut tx, &row, JobEvent::Start, Owner::Jobs, none)
                        .await
                        .map(|_| ())
                }
                Op::Finish(_) => {
                    JobTransitions::apply(&mut tx, &row, JobEvent::Finish, Owner::Jobs, none)
                        .await
                        .map(|_| ())
                }
                Op::Verify(_) => JobTransitions::verify_completion(&mut tx, &row, &chains)
                    .await
                    .map(|_| ()),
                Op::Fail(_, c) => {
                    let failure = FailureCode::class_only(FailureClass::ALL[*c]);
                    JobTransitions::fail_or_retry(&mut tx, &row, failure, "test failure")
                        .await
                        .map(|_| ())
                }
                Op::Attempt(_, e) => match row.current_attempt_id {
                    None => Ok(()),
                    Some(a) => match JobTransitions::lock_attempt(&mut tx, self.org, a).await {
                        Ok(Some(att)) => JobTransitions::apply_attempt(
                            &mut tx,
                            &att,
                            ATTEMPT_EVENTS[*e],
                            AttemptChange::default(),
                        )
                        .await
                        .map(|_| ()),
                        Ok(None) => panic!("attempt {a} vanished"),
                        Err(e) => Err(e),
                    },
                },
                Op::Submit | Op::CancelRun(_) | Op::CancelJob(_) | Op::Priority(..) => Ok(()),
            };
            match result {
                Ok(()) => {
                    if let Err(e) = tx.commit().await {
                        panic!("commit: {e}");
                    }
                }
                // Illegal steps are refused with MRL-STATE-170 and change nothing.
                Err(e) => assert_eq!(e.code(), "MRL-STATE-170", "{op:?}: {e}"),
            }
        }

        /// Checks every invariant against the database.
        pub async fn check(&mut self) {
            let pool = self.db.pool();
            let doubles: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM (SELECT job_id FROM job_attempts \
                   WHERE state IN ('PENDING','LAUNCHING','RUNNING') \
                   GROUP BY job_id HAVING count(*) > 1) d",
            )
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(doubles, 0, "a job has two live attempts");
            let rows: Vec<(Uuid, String, Option<String>)> = sqlx::query_as(
                "SELECT j.id, j.state, r.status_summary FROM jobs j LEFT JOIN runs r ON r.id = j.run_id",
            )
            .fetch_all(pool)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
            for (id, state, summary) in rows {
                assert_eq!(
                    summary.as_deref(),
                    Some(state.as_str()),
                    "run mirrors job {id}"
                );
                if let Some(before) = self.seen.get(&id) {
                    let terminal: JobState = before.parse().unwrap_or_else(|e| panic!("{e}"));
                    if Job::is_terminal(terminal) {
                        assert_eq!(&state, before, "terminal job {id} changed");
                    }
                }
                self.seen.insert(id, state);
            }
        }
    }
}

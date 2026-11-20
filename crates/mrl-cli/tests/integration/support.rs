//! Helpers for the runs and jobs tests: a role-based authorizer, an in-memory tenancy
//! double for the CLI, a team-mode server with the runs module on a [`TestDb`], and the
//! random-sequence model.

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::{Method, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use mrl_domain::ids::{DatasetVersionId, ModelVersionId, OrgId, ProjectId, WorkspaceId};
use mrl_domain::rbac::{allowed, Action, Role};
use mrl_server::config::{BucketConfig, Config, Mode};
use mrl_server::modules::jobs::{
    Chains, CompletionVerifier, JobRow, JobSpec, JobValidator, ProjectDirectory, Projects,
    SchemaChecks, Verdict, Verification,
};
use mrl_server::modules::runs::RunsModule;
use mrl_server::seams::{
    AuditEvent, AuditSink, Authorizer, Decision, Principal, PrincipalKind, RequestContext,
    Resource, SeamError,
};
use mrl_server::{AppState, Db, ServerModule};
use mrl_testkit::pg::TestDb;
use mrl_testkit::seams as ts;
use mrl_testkit::server::{TestResponse, TestServer};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
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

/// Changes a test can make to [`RoleAuthorizer`] while the server runs.
#[derive(Default)]
pub struct AuthRules {
    /// `(principal id, project id)` pairs the authorizer hides (404).
    pub hidden: HashSet<(String, String)>,
    /// The authorizer's backend is down: every call fails.
    pub down: bool,
    /// Like P16's authorizer, read the database on this pool for every decision.
    pub pool: Option<Db>,
    /// Which org owns each project (`prj_…` → `org_…`). Like P16's Rbac (A1 decision): a
    /// project that is missing here, or in another org than the one named, is hidden (404)
    /// for every principal. The server's project directory answers from this map too.
    /// `None` skips both (no directory), for servers whose projects come from
    /// [`TenancyDouble`] or that stand in for a build without P16.
    pub projects: Option<HashMap<String, String>>,
}

impl AuthRules {
    /// Rules that know these `(project, org)` pairs.
    pub fn with_projects(projects: &[(ProjectId, OrgId)]) -> Self {
        let map = projects
            .iter()
            .map(|(p, o)| (p.to_string(), o.to_string()))
            .collect();
        Self {
            projects: Some(map),
            ..Self::default()
        }
    }
}

/// Authorizes like RBAC: other orgs are hidden (404); inside the org the role table
/// from `mrl_domain::rbac` decides (403 when refused). [`AuthRules`] can hide projects
/// per user or take the authorizer down.
pub struct RoleAuthorizer {
    roles: HashMap<String, Role>,
    rules: Arc<Mutex<AuthRules>>,
}

impl RoleAuthorizer {
    /// Knows these users and follows `rules`.
    pub fn new(users: &[TestUser], rules: Arc<Mutex<AuthRules>>) -> Self {
        let roles = users
            .iter()
            .map(|u| (u.principal.id.clone(), u.role))
            .collect();
        Self { roles, rules }
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
        let pool = {
            let rules = self.rules.lock().unwrap();
            if rules.down {
                return Err(SeamError::new("authorizer", "the test backend is down"));
            }
            if let (Some(owners), Some(p)) = (&rules.projects, &resource.project) {
                if owners.get(p) != resource.org.as_ref() {
                    return Ok(Decision::Hide);
                }
            }
            if let Some(p) = &resource.project {
                if rules
                    .hidden
                    .contains(&(ctx.principal.id.clone(), p.clone()))
                {
                    return Ok(Decision::Hide);
                }
            }
            rules.pool.clone()
        };
        if let Some(db) = pool {
            sqlx::query("SELECT 1")
                .execute(db.pool())
                .await
                .map_err(|e| SeamError::new("authorizer", e.to_string()))?;
        }
        // Like P16: the solo owner may do everything once the project checks pass.
        if ctx.principal.kind == PrincipalKind::LocalOwner {
            return Ok(Decision::Allow);
        }
        // A principal bound to an org can't reach another one. One with no org (a token the
        // solo owner made without a project) has its role wherever it is asked.
        if ctx.principal.org.is_some()
            && resource.org.is_some()
            && resource.org != ctx.principal.org
        {
            return Ok(Decision::Hide);
        }
        let Some(role) = self.roles.get(&ctx.principal.id) else {
            return Ok(Decision::Hide);
        };
        let action: Option<Action> = serde_json::from_value(json!(action)).ok();
        Ok(match action {
            Some(a) if allowed(*role, a) => Decision::Allow,
            _ => Decision::Deny,
        })
    }
}

/// A project directory answering from [`AuthRules::projects`] (P16 registers the real one).
pub struct RulesDirectory(Arc<Mutex<AuthRules>>);

#[async_trait]
impl ProjectDirectory for RulesDirectory {
    async fn owner(&self, project: ProjectId) -> Result<Option<OrgId>, SeamError> {
        let rules = self.0.lock().unwrap();
        let owner = rules
            .projects
            .as_ref()
            .and_then(|m| m.get(&project.to_string()))
            .and_then(|o| o.parse().ok());
        Ok(owner)
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
    runs_server_with(db.db(), chains, users, extra, &Harness::default()).await
}

/// What a test can watch or change on a [`runs_server_with`] server.
#[derive(Clone, Default)]
pub struct Harness {
    /// The authorizer's rules.
    pub rules: Arc<Mutex<AuthRules>>,
    /// Every audit event the server records.
    pub audit: Arc<TestAudit>,
}

/// Keeps audit events in memory; `fail` makes every write fail (like a broken audit store).
#[derive(Default)]
pub struct TestAudit {
    events: Mutex<Vec<AuditEvent>>,
    /// Refuse every write.
    pub fail: std::sync::atomic::AtomicBool,
}

impl TestAudit {
    /// Events so far.
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait]
impl AuditSink for TestAudit {
    async fn record(&self, event: AuditEvent) -> Result<(), SeamError> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(SeamError::new("audit", "the test audit store is down"));
        }
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

/// [`runs_server`] with the runs module on `db` (any pool) and this `harness`.
pub async fn runs_server_with(
    db: &Db,
    chains: Chains,
    users: &[TestUser],
    extra: Vec<Box<dyn ServerModule>>,
    harness: &Harness,
) -> TestServer {
    let tokens: Vec<(&str, Principal)> = users
        .iter()
        .map(|u| (u.token.as_str(), u.principal.clone()))
        .collect();
    let authz = RoleAuthorizer::new(users, harness.rules.clone());
    let seams = ts::team(&tokens)
        .authorizer(authz)
        .audit_arc(harness.audit.clone());
    let seams = ts::build(seams, Mode::Team);
    let mut runs = RunsModule::with_chains(db.clone(), chains);
    if harness.rules.lock().unwrap().projects.is_some() {
        let directory = RulesDirectory(harness.rules.clone());
        runs = runs.with_projects(Projects::with(Arc::new(directory)));
    }
    let mut modules: Vec<Box<dyn ServerModule>> = vec![Box::new(runs)];
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
    page_size: usize,
    orgs: Vec<Value>,
    workspaces: Vec<Value>,
    projects: Vec<Value>,
}

/// An in-memory stand-in for the tenancy API `mrl dev seed` and `mrl projects` call:
/// P16's `GET /auth/me` and Doc 09 §6.3's `GET/POST /orgs`, `GET/POST
/// /orgs/{org}/workspaces`, `GET/POST /workspaces/{ws}/projects`, `GET /projects/{prj}`.
/// Lists are paged (`?cursor=`) with a settable page size; a duplicate slug gets 409.
#[derive(Clone)]
pub struct TenancyDouble(Arc<Mutex<Tenancy>>);

impl TenancyDouble {
    /// A double whose lists return `page_size` items per page.
    pub fn new(page_size: usize) -> Self {
        Self(Arc::new(Mutex::new(Tenancy {
            page_size: page_size.max(1),
            ..Tenancy::default()
        })))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Tenancy> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Adds an org directly (to put the seed's org on a later page).
    pub fn add_org(&self, slug: &str) {
        self.lock()
            .orgs
            .push(json!({"id": OrgId::new().to_string(), "slug": slug, "name": slug}));
    }

    /// How many orgs, workspaces and projects exist.
    pub fn counts(&self) -> (usize, usize, usize) {
        let t = self.lock();
        (t.orgs.len(), t.workspaces.len(), t.projects.len())
    }
}

#[derive(serde::Deserialize)]
struct Cursor {
    cursor: Option<String>,
}

fn page(items: &[Value], size: usize, q: &Cursor) -> Json<Value> {
    let start: usize = q
        .cursor
        .as_deref()
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let end = (start + size).min(items.len());
    let next = (end < items.len()).then(|| end.to_string());
    Json(json!({ "items": items.get(start..end).unwrap_or(&[]), "next_cursor": next }))
}

/// Adds `item` unless an item with its slug already exists in the same scope (`scope` key):
/// then 409 with a problem document, like a unique constraint would.
fn create(list: &mut Vec<Value>, item: Value, scope: &str) -> (StatusCode, Json<Value>) {
    let dup = list
        .iter()
        .any(|i| i["slug"] == item["slug"] && i.get(scope) == item.get(scope));
    if dup {
        let problem =
            json!({"code": "MRL-CONFLICT-001", "status": 409, "detail": "That slug is taken."});
        return (StatusCode::CONFLICT, Json(problem));
    }
    list.push(item.clone());
    (StatusCode::CREATED, Json(item))
}

impl ServerModule for TenancyDouble {
    fn name(&self) -> &'static str {
        "tenancy-double"
    }

    fn routes(&self) -> Router<AppState> {
        let me = |ctx: RequestContext| async move {
            Json(json!({ "user": { "id": ctx.principal.id }, "principal_kind": "user" }))
        };
        let t = || self.clone();
        let (t1, t2, t3, t4, t5, t6) = (t(), t(), t(), t(), t(), t());
        Router::new()
            .route("/auth/me", get(me))
            .route(
                "/orgs",
                get(move |Query(q): Query<Cursor>| async move {
                    let t = t1.lock();
                    page(&t.orgs, t.page_size, &q)
                })
                .post(move |Json(b): Json<Value>| async move {
                    let item = json!({"id": OrgId::new().to_string(), "slug": b["slug"], "name": b["name"]});
                    create(&mut t2.lock().orgs, item, "none")
                }),
            )
            .route(
                "/orgs/{org}/workspaces",
                get(move |Path(org): Path<String>, Query(q): Query<Cursor>| async move {
                    let t = t3.lock();
                    let mine: Vec<Value> =
                        t.workspaces.iter().filter(|w| w["org_id"] == org.as_str()).cloned().collect();
                    page(&mine, t.page_size, &q)
                })
                .post(move |Path(org): Path<String>, Json(b): Json<Value>| async move {
                    let item = json!({"id": WorkspaceId::new().to_string(), "org_id": org, "slug": b["slug"], "name": b["name"]});
                    create(&mut t4.lock().workspaces, item, "org_id")
                }),
            )
            .route(
                "/workspaces/{ws}/projects",
                get(move |Path(ws): Path<String>, Query(q): Query<Cursor>| async move {
                    let t = t5.lock();
                    let mine: Vec<Value> =
                        t.projects.iter().filter(|p| p["workspace_id"] == ws.as_str()).cloned().collect();
                    page(&mine, t.page_size, &q)
                })
                .post(move |Path(ws): Path<String>, Json(b): Json<Value>| async move {
                    let item = json!({"id": ProjectId::new().to_string(), "workspace_id": ws, "slug": b["slug"], "name": b["name"]});
                    create(&mut t6.lock().projects, item, "workspace_id")
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

/// Random API and system sequences over runs and jobs. Before each step the model works
/// out from P06's tables what the step must do (refused, or which new state), then checks
/// the database did exactly that. After each step it checks the invariants: at most one
/// live attempt per job, terminal states never change, a run mirrors its job's state.
pub mod sequences {
    use super::*;
    use mrl_domain::failure::{FailureClass, FailureCode};
    use mrl_domain::state::{
        Job, JobAttempt, JobAttemptEvent, JobAttemptState, JobEvent, JobState, Owner, StateMachine,
    };
    use mrl_server::modules::jobs::{
        AttemptChange, AttemptRow, JobChange, JobTransitions, RetryRules,
    };
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

    /// What a step must do, worked out from P06's tables before it runs.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Expect {
        /// Refused with MRL-STATE-170 (or 409 over HTTP); nothing changes.
        Refused,
        /// Nothing to act on (no attempt yet); nothing changes.
        Nothing,
        /// The job ends in this state.
        Job(JobState),
        /// The current attempt ends in this state; the job in `job` with `epoch`.
        Attempt {
            attempt: JobAttemptState,
            job: JobState,
            epoch: i64,
        },
        /// The job keeps its state and gets this priority.
        Priority(i32),
    }

    struct Snapshot {
        job: JobRow,
        attempt: Option<AttemptRow>,
        live: bool,
    }

    impl Snapshot {
        fn state(&self) -> JobState {
            self.job.state.parse().unwrap_or_else(|e| panic!("{e}"))
        }
    }

    fn is_live(s: JobAttemptState) -> bool {
        matches!(
            s,
            JobAttemptState::Pending | JobAttemptState::Launching | JobAttemptState::Running
        )
    }

    /// P06's step for `event` when `by` takes it, or refused.
    fn rule(s: JobState, event: JobEvent, by: Owner) -> Expect {
        match Job::transition(s, event) {
            Some(t) if t.owner == by => Expect::Job(t.to),
            _ => Expect::Refused,
        }
    }

    /// What `op` must do from `snap`.
    fn expect(op: &Op, snap: &Snapshot) -> Expect {
        let s = snap.state();
        match op {
            Op::Submit => Expect::Nothing,
            Op::CancelRun(_) | Op::CancelJob(_) => match s {
                JobState::Cancelling | JobState::Cancelled => Expect::Job(s),
                _ if Job::transition(s, JobEvent::RequestCancel).is_some() => {
                    Expect::Job(if snap.live {
                        JobState::Cancelling
                    } else {
                        JobState::Cancelled
                    })
                }
                _ => Expect::Refused,
            },
            Op::Priority(_, p) if !Job::is_terminal(s) => Expect::Priority(*p),
            Op::Priority(..) => Expect::Refused,
            Op::Place(_) => rule(s, JobEvent::Place, Owner::Scheduler),
            Op::Requeue(_) => rule(s, JobEvent::Requeue, Owner::Scheduler),
            Op::Retry(_) => rule(s, JobEvent::Retry, Owner::Scheduler),
            Op::StartJob(_) => rule(s, JobEvent::Start, Owner::Jobs),
            Op::Finish(_) => rule(s, JobEvent::Finish, Owner::Jobs),
            // A second live attempt is never allowed (index `one_live_attempt`).
            Op::Launch(_) if snap.live => Expect::Refused,
            Op::Launch(_) => rule(s, JobEvent::Launch, Owner::Scheduler),
            // The test chains verify every job.
            Op::Verify(_) => rule(s, JobEvent::Complete, Owner::ArtifactsVerifier),
            Op::Fail(_, c) => {
                let class = FailureClass::ALL[*c];
                let rules = RetryRules::from_json(&snap.job.retry_policy);
                let used = snap.job.retries_used[class.as_str()].as_u64().unwrap_or(0);
                let used = u32::try_from(used).unwrap_or(u32::MAX);
                let retry = class
                    .retry_policy_default()
                    .next_delay(used, rules.auto_tune);
                if retry.is_some() && Job::transition(s, JobEvent::Interrupt).is_some() {
                    Expect::Job(JobState::RetryWait)
                } else {
                    rule(s, JobEvent::Fail, Owner::Jobs)
                }
            }
            Op::Attempt(_, e) => {
                let Some(att) = &snap.attempt else {
                    return Expect::Nothing;
                };
                let event = ATTEMPT_EVENTS[*e];
                let from: JobAttemptState = att.state.parse().unwrap_or_else(|e| panic!("{e}"));
                let fenced = matches!(
                    event,
                    JobAttemptEvent::Acknowledge
                        | JobAttemptEvent::Start
                        | JobAttemptEvent::ExitOk
                        | JobAttemptEvent::ExitError
                );
                match JobAttempt::transition(from, event) {
                    Some(_) if fenced && att.epoch != snap.job.epoch => Expect::Refused,
                    Some(t) => Expect::Attempt {
                        attempt: t.to,
                        job: if s == JobState::Cancelling && !is_live(t.to) {
                            JobState::Cancelled
                        } else {
                            s
                        },
                        epoch: snap.job.epoch + i64::from(event == JobAttemptEvent::LoseLease),
                    },
                    None => Expect::Refused,
                }
            }
        }
    }

    impl World<'_> {
        fn pick(&self, i: usize) -> Option<(String, Uuid)> {
            (!self.runs.is_empty()).then(|| self.runs[i % self.runs.len()].clone())
        }

        async fn snapshot(&self, job: Uuid) -> Snapshot {
            let mut tx = self.db.db().begin().await.unwrap_or_else(|e| panic!("{e}"));
            let row = JobTransitions::lock(&mut tx, self.org, job)
                .await
                .unwrap_or_else(|e| panic!("{e}"))
                .unwrap_or_else(|| panic!("job {job} vanished"));
            let attempt = match row.current_attempt_id {
                Some(a) => JobTransitions::lock_attempt(&mut tx, self.org, a)
                    .await
                    .unwrap_or_else(|e| panic!("{e}")),
                None => None,
            };
            let live: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM job_attempts WHERE job_id = $1 \
                   AND state IN ('PENDING','LAUNCHING','RUNNING'))",
            )
            .bind(job)
            .fetch_one(&mut *tx)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
            Snapshot {
                job: row,
                attempt,
                live,
            }
        }

        /// Runs one step and checks it did exactly what P06's tables say.
        pub async fn step(&mut self, op: &Op) {
            let index = match op {
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
            };
            if let Op::Submit = op {
                return self.submit().await;
            }
            let Some((run, job)) = self.pick(index) else {
                return;
            };
            let before = self.snapshot(job).await;
            let want = expect(op, &before);
            let refused = match op {
                Op::CancelRun(_) | Op::CancelJob(_) | Op::Priority(..) => {
                    self.api(op, &run, job).await
                }
                _ => self.system(op, job).await,
            };
            let after = self.snapshot(job).await;
            let ctx = format!("{op:?} from {} → {want:?}", before.job.state);
            assert_eq!(
                refused,
                matches!(want, Expect::Refused),
                "{ctx}: refused = {refused}"
            );
            match want {
                Expect::Refused | Expect::Nothing => {
                    assert_eq!(after.job.version, before.job.version, "{ctx}: job changed");
                    let v = |s: &Snapshot| s.attempt.as_ref().map(|a| a.state_version);
                    assert_eq!(v(&after), v(&before), "{ctx}: attempt changed");
                }
                Expect::Job(state) => assert_eq!(after.state(), state, "{ctx}"),
                Expect::Priority(p) => {
                    assert_eq!(
                        (after.state(), after.job.priority),
                        (before.state(), p),
                        "{ctx}"
                    );
                }
                Expect::Attempt {
                    attempt,
                    job: state,
                    epoch,
                } => {
                    let got = after.attempt.as_ref().map(|a| a.state.as_str());
                    assert_eq!(got, Some(attempt.as_str()), "{ctx}");
                    assert_eq!((after.state(), after.job.epoch), (state, epoch), "{ctx}");
                }
            }
            if let (Op::Launch(_), Expect::Job(_)) = (op, &want) {
                let att = after
                    .attempt
                    .as_ref()
                    .unwrap_or_else(|| panic!("{ctx}: no attempt"));
                assert_eq!(
                    (att.state.as_str(), att.epoch),
                    ("PENDING", before.job.epoch + 1),
                    "{ctx}"
                );
            }
        }

        async fn submit(&mut self) {
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
            assert_eq!(v["state"], "QUEUED", "the test chains accept every run");
            let run = v["id"].as_str().unwrap_or_default().to_owned();
            let job: mrl_domain::ids::JobId = v["job_id"]
                .as_str()
                .unwrap_or_default()
                .parse()
                .unwrap_or_else(|e| panic!("job id: {e}"));
            self.runs.push((run, job.as_uuid()));
        }

        /// An API step; returns true if the server refused it (409 MRL-STATE-170).
        async fn api(&self, op: &Op, run: &str, job: Uuid) -> bool {
            let id = mrl_domain::ids::JobId::from_uuid(job);
            let res = match op {
                Op::CancelRun(_) => {
                    call(
                        self.srv,
                        Method::POST,
                        &format!("/api/v1/runs/{run}:cancel"),
                        self.token,
                        None,
                        None,
                    )
                    .await
                }
                Op::CancelJob(_) => {
                    call(
                        self.srv,
                        Method::POST,
                        &format!("/api/v1/jobs/{id}:cancel"),
                        self.token,
                        None,
                        None,
                    )
                    .await
                }
                Op::Priority(_, p) => {
                    let body = json!({ "priority": p });
                    call(
                        self.srv,
                        Method::PATCH,
                        &format!("/api/v1/jobs/{id}"),
                        self.token,
                        None,
                        Some(&body),
                    )
                    .await
                }
                other => panic!("{other:?} is not an API step"),
            };
            match res.status {
                200 => false,
                409 => {
                    assert_eq!(res.json()["code"], "MRL-STATE-170", "{}", res.text());
                    true
                }
                s => panic!("{op:?}: unexpected {s}: {}", res.text()),
            }
        }

        /// A scheduler/agent step through `JobTransitions`; returns true if it was refused.
        async fn system(&self, op: &Op, job: Uuid) -> bool {
            let chains = accepting_chains();
            let mut tx = self
                .db
                .db()
                .begin()
                .await
                .unwrap_or_else(|e| panic!("begin: {e}"));
            let row = JobTransitions::lock(&mut tx, self.org, job)
                .await
                .unwrap_or_else(|e| panic!("{e}"))
                .unwrap_or_else(|| panic!("job {job} vanished"));
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
                Op::Verify(_) => JobTransitions::verify_completion(
                    &mut tx,
                    &row,
                    chains.verify(&row).await.unwrap(),
                )
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
                other => panic!("{other:?} is not a system step"),
            };
            match result {
                Ok(()) => {
                    tx.commit().await.unwrap_or_else(|e| panic!("commit: {e}"));
                    false
                }
                Err(e) => {
                    assert_eq!(e.code(), "MRL-STATE-170", "{op:?}: {e}");
                    true
                }
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
            // Every job and attempt has events 0..=state_version (unique per aggregate, so
            // no gaps), and the last one names the state it is in now.
            type Events = (String, Uuid, String, i64, i64, Option<i64>, Option<String>);
            let events: Vec<Events> = sqlx::query_as(
                "SELECT a.kind, a.id, a.state, a.state_version, count(o.id), \
                   max(o.aggregate_seq), (array_agg(o.payload->>'to' ORDER BY o.aggregate_seq DESC))[1] \
                 FROM (SELECT 'job' AS kind, id, state, state_version FROM jobs WHERE org_id = $1 \
                       UNION ALL SELECT 'attempt', id, state, state_version FROM job_attempts \
                       WHERE org_id = $1) a \
                 LEFT JOIN outbox_events o ON o.aggregate_kind = a.kind AND o.aggregate_id = a.id \
                 GROUP BY a.kind, a.id, a.state, a.state_version",
            )
            .bind(self.org)
            .fetch_all(pool)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
            for (kind, id, state, version, count, last, to) in events {
                let what = format!("{kind} {id} in {state} at version {version}");
                assert_eq!(
                    (count, last),
                    (version + 1, Some(version)),
                    "{what}: events out of step"
                );
                assert_eq!(to.as_deref(), Some(state.as_str()), "{what}: last event");
            }
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

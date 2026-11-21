//! Runs: submit, list, read, config snapshot, attempts, timeline and cancel.

use super::experiments::{get_or_create, project_scope, DEFAULT_EXPERIMENT};
use crate::app::AppState;
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::http::idempotency::{IDEMPOTENCY_KEY, REPLAYED};
use crate::http::pagination::{Page, PageQuery};
use crate::modules::jobs::model::reason;
use crate::modules::jobs::scope::{self, resource};
use crate::modules::jobs::transitions::attempt_cols;
use crate::modules::jobs::{
    codes, Attempt, AttemptRow, Chains, JobChange, JobSpec, JobTransitions, JobsError, NewJob,
    Projects, ResolveError, RetryRules, Validation,
};
use crate::seams::RequestContext;
use crate::Db;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use mrl_domain::ids::{
    CheckpointId, DatasetVersionId, ExperimentId, JobId, ModelVersionId, ProjectId,
    RewardVersionId, RunId,
};
use mrl_domain::resources::{GpuConstraints, ResourceRequest, RoleRequest, WorkloadRole};
use mrl_domain::state::{JobEvent, Owner};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

/// Run kinds (other job kinds have no run).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum RunKind {
    /// Training.
    #[default]
    Train,
    /// Evaluation.
    Eval,
}

impl RunKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Train => "TRAIN",
            Self::Eval => "EVAL",
        }
    }
}

/// `POST /projects/{prj}/runs` body.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RunSubmission {
    /// 1–200 characters.
    pub name: String,
    /// `exp_…`; without it the run joins the project's `default` experiment.
    #[serde(default)]
    #[schema(value_type = Option<String>)]
    pub experiment_id: Option<ExperimentId>,
    /// `TRAIN` (default) or `EVAL`.
    #[serde(default)]
    pub kind: RunKind,
    /// The method, like `sft`, `dpo` or `grpo`.
    pub algorithm_id: String,
    /// The model version to start from, `mv_…`.
    #[schema(value_type = String)]
    pub base_model_version_id: ModelVersionId,
    /// The reward version, `rwdv_…`.
    #[serde(default)]
    #[schema(value_type = Option<String>)]
    pub reward_version_id: Option<RewardVersionId>,
    /// Dataset versions, `dsv_…`.
    #[serde(default)]
    #[schema(value_type = Vec<String>)]
    pub dataset_version_ids: Vec<DatasetVersionId>,
    /// The method's settings (a JSON object).
    #[serde(default = "empty_object")]
    #[schema(value_type = Object)]
    pub config: Value,
    /// Random seeds; the server picks `{"global": n}` when absent.
    #[serde(default)]
    #[schema(value_type = Option<Object>)]
    pub seeds: Option<Value>,
    /// What the job asks the scheduler for; default one trainer with one GPU.
    #[serde(default)]
    #[schema(value_type = Option<Object>)]
    pub resources: Option<ResourceRequest>,
    /// Higher runs first, −1000 to 1000 (default 0).
    #[serde(default)]
    pub priority: i32,
    /// Retry rules: `{auto_tune, budgets: {CLASS: n}}`.
    #[serde(default)]
    #[schema(value_type = Option<Object>)]
    pub retry: Option<RetryRules>,
    /// Longest allowed run time in seconds.
    #[serde(default)]
    pub max_runtime_seconds: Option<i64>,
    /// The run this one branches from, `run_…`.
    #[serde(default)]
    #[schema(value_type = Option<String>)]
    pub parent_run_id: Option<RunId>,
    /// The checkpoint this one starts from, `ckpt_…`.
    #[serde(default)]
    #[schema(value_type = Option<String>)]
    pub parent_checkpoint_id: Option<CheckpointId>,
}

fn empty_object() -> Value {
    json!({})
}

/// The resource request a submission without `resources` gets: one trainer, one GPU.
pub fn default_resources() -> ResourceRequest {
    ResourceRequest {
        roles: vec![RoleRequest {
            role: WorkloadRole::Trainer,
            replicas: 1,
            gpu_count: 1,
            gpu: GpuConstraints::default(),
            node_ids: Vec::new(),
            cpu_millis: 4_000,
            mem_bytes: 16 << 30,
        }],
    }
}

/// A run, as the API shows it. `state` mirrors its job.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct Run {
    /// `run_…`.
    pub id: String,
    /// `prj_…`.
    pub project_id: String,
    /// `exp_…`.
    pub experiment_id: String,
    /// The run's job, `job_…`.
    pub job_id: String,
    /// Name.
    pub name: String,
    /// `TRAIN` or `EVAL`.
    pub kind: String,
    /// The method, like `sft`.
    pub algorithm_id: String,
    /// The engine version picked by validation, once engines are registered.
    pub engine_version_id: Option<String>,
    /// `mv_…`.
    pub base_model_version_id: String,
    /// `rwdv_…`.
    pub reward_version_id: Option<String>,
    /// `run_…` this run branches from.
    pub parent_run_id: Option<String>,
    /// `ckpt_…` this run starts from.
    pub parent_checkpoint_id: Option<String>,
    /// Latest committed checkpoint, `ckpt_…`.
    pub latest_committed_checkpoint_id: Option<String>,
    /// Final checkpoint, `ckpt_…`.
    pub final_checkpoint_id: Option<String>,
    /// The job's state: `QUEUED`, `RUNNING`, `REJECTED`, ….
    pub state: String,
    /// Why: `{code, message, problems?}`; for REJECTED, every problem with a hint.
    #[schema(value_type = Option<Object>)]
    pub state_reason: Option<Value>,
    /// Failure class of the last failure.
    pub failure_class: Option<String>,
    /// Submitter's principal id.
    pub created_by: String,
    /// Created.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
}

macro_rules! run_cols {
    () => {
        "r.id, r.org_id, r.project_id, r.experiment_id, j.id AS job_id, r.name, r.kind, \
         r.algorithm_id, r.engine_version_id, r.base_model_version_id, r.reward_version_id, \
         r.parent_run_id, r.parent_checkpoint_id, r.latest_committed_checkpoint_id, \
         r.final_checkpoint_id, j.state, j.state_reason, j.failure_class, r.created_by, \
         r.created_at, r.submit_fingerprint"
    };
}

/// A run joined with its job.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct RunRow {
    /// Run id.
    pub id: Uuid,
    /// Org.
    pub org_id: Uuid,
    /// Project.
    pub project_id: Uuid,
    /// Experiment.
    pub experiment_id: Uuid,
    /// The run's job.
    pub job_id: Uuid,
    /// Name.
    pub name: String,
    /// `TRAIN` or `EVAL`.
    pub kind: String,
    /// Method.
    pub algorithm_id: String,
    /// Engine version.
    pub engine_version_id: Option<Uuid>,
    /// Base model version.
    pub base_model_version_id: Uuid,
    /// Reward version.
    pub reward_version_id: Option<Uuid>,
    /// Parent run.
    pub parent_run_id: Option<Uuid>,
    /// Parent checkpoint.
    pub parent_checkpoint_id: Option<Uuid>,
    /// Latest committed checkpoint.
    pub latest_committed_checkpoint_id: Option<Uuid>,
    /// Final checkpoint.
    pub final_checkpoint_id: Option<Uuid>,
    /// Job state.
    pub state: String,
    /// Job state reason.
    pub state_reason: Option<Value>,
    /// Job failure class.
    pub failure_class: Option<String>,
    /// Submitter.
    pub created_by: String,
    /// Created.
    pub created_at: OffsetDateTime,
    /// SHA-256 of the submit request.
    pub submit_fingerprint: Vec<u8>,
}

impl RunRow {
    /// The API view.
    pub fn view(&self) -> Run {
        let ckpt = |c: Option<Uuid>| c.map(|c| CheckpointId::from_uuid(c).to_string());
        Run {
            id: RunId::from_uuid(self.id).to_string(),
            project_id: ProjectId::from_uuid(self.project_id).to_string(),
            experiment_id: ExperimentId::from_uuid(self.experiment_id).to_string(),
            job_id: JobId::from_uuid(self.job_id).to_string(),
            name: self.name.clone(),
            kind: self.kind.clone(),
            algorithm_id: self.algorithm_id.clone(),
            engine_version_id: self.engine_version_id.map(|e| e.to_string()),
            base_model_version_id: ModelVersionId::from_uuid(self.base_model_version_id)
                .to_string(),
            reward_version_id: self
                .reward_version_id
                .map(|r| RewardVersionId::from_uuid(r).to_string()),
            parent_run_id: self.parent_run_id.map(|r| RunId::from_uuid(r).to_string()),
            parent_checkpoint_id: ckpt(self.parent_checkpoint_id),
            latest_committed_checkpoint_id: ckpt(self.latest_committed_checkpoint_id),
            final_checkpoint_id: ckpt(self.final_checkpoint_id),
            state: self.state.clone(),
            state_reason: self.state_reason.clone(),
            failure_class: self.failure_class.clone(),
            created_by: self.created_by.clone(),
            created_at: self.created_at,
        }
    }

    fn resource(&self) -> crate::seams::Resource {
        resource(
            "run",
            Some(RunId::from_uuid(self.id).to_string()),
            self.org_id,
            self.project_id,
        )
    }
}

/// The immutable config snapshot of a run.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RunConfig {
    /// `run_…`.
    pub run_id: String,
    /// What was submitted (the whole request body).
    #[schema(value_type = Object)]
    pub submitted: Value,
    /// The config after every resolver: what the engine receives.
    #[schema(value_type = Object)]
    pub resolved: Value,
    /// Hex SHA-256 of `resolved` (compact JSON, sorted keys).
    pub resolved_sha256: String,
    /// Dataset versions, `dsv_…`.
    pub dataset_version_ids: Vec<String>,
    /// Random seeds.
    #[schema(value_type = Object)]
    pub seeds: Value,
    /// Image digest or lock hash, once pinned.
    pub runtime_ref: Option<String>,
    /// Created.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
}

/// One timeline entry: an outbox event of the run's job or one of its attempts.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct TimelineEntry {
    /// Event type, like `job.queued`.
    pub r#type: String,
    /// `job` or `attempt`.
    pub aggregate_kind: String,
    /// `job_…` or `att_…`.
    pub aggregate_id: String,
    /// The aggregate's version after the change.
    pub aggregate_seq: i64,
    /// The event body (`from`, `to`, `reason`, …).
    #[schema(value_type = Object)]
    pub payload: Value,
    /// When it happened.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub at: OffsetDateTime,
}

/// Filters for `GET /projects/{prj}/runs`.
#[derive(Debug, Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct RunFilter {
    /// Only runs in this experiment (`exp_…`).
    pub experiment: Option<String>,
    /// Only runs whose job is in this state, like `REJECTED`.
    pub state: Option<String>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn bad_body(detail: impl Into<String>) -> JobsError {
    ApiError::new(ErrorCode::BadBody, detail).into()
}

fn field(path: &str, message: &str, hint: &str) -> FieldError {
    FieldError::new(path, ErrorCode::FieldValue, message, hint)
}

fn check_submission(s: &RunSubmission) -> Result<(), JobsError> {
    let mut errors = Vec::new();
    let name = s.name.trim().chars().count();
    if name == 0 || name > 200 {
        errors.push(field(
            "/name",
            "The name must have 1 to 200 characters.",
            "Use a short name like `sft-baseline`.",
        ));
    }
    let algo_ok = (1..=64).contains(&s.algorithm_id.len())
        && s.algorithm_id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_.-".contains(&b));
    if !algo_ok {
        errors.push(field(
            "/algorithm_id",
            "The method id must be 1 to 64 lowercase letters, digits, `_`, `.` or `-`.",
            "Use a method id like `sft`, `dpo` or `grpo`.",
        ));
    }
    if !(-1000..=1000).contains(&s.priority) {
        errors.push(field(
            "/priority",
            "priority must be from -1000 to 1000.",
            "Leave it out to use 0.",
        ));
    }
    if s.max_runtime_seconds.is_some_and(|m| m <= 0) {
        errors.push(field(
            "/max_runtime_seconds",
            "The time limit must be more than 0 seconds.",
            "Leave it out for no limit.",
        ));
    }
    if s.seeds.as_ref().is_some_and(|v| !v.is_object()) {
        errors.push(field(
            "/seeds",
            "seeds must be a JSON object, like {\"global\": 42}.",
            "Leave it out to let the server pick one.",
        ));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors).into())
    }
}

/// Fetches run `id` with its job. Callers authorize it: the authorizer decides who may see
/// it, so other orgs get 404.
pub async fn find_run(db: &Db, id: Uuid) -> Result<Option<RunRow>, JobsError> {
    Ok(sqlx::query_as::<_, RunRow>(concat!(
        "SELECT ",
        run_cols!(),
        " FROM runs r JOIN jobs j ON j.run_id = r.id WHERE r.id = $1"
    ))
    .bind(id)
    .fetch_optional(db.pool())
    .await?)
}

async fn find_by_key(
    db: &Db,
    org: Uuid,
    created_by: &str,
    key: &str,
    fingerprint: &[u8],
) -> Result<Option<RunRow>, JobsError> {
    Ok(sqlx::query_as::<_, RunRow>(concat!(
        "SELECT ",
        run_cols!(),
        " FROM runs r JOIN jobs j ON j.run_id = r.id \
         WHERE r.org_id = $1 AND r.created_by = $2 AND (r.submit_key = $3 \
           OR (r.legacy_submit_key = $3 AND r.submit_fingerprint = $4)) \
         ORDER BY (r.submit_fingerprint = $4) DESC, (r.submit_key = $3) DESC LIMIT 1"
    ))
    .bind(org)
    .bind(created_by)
    .bind(key)
    .bind(fingerprint)
    .fetch_optional(db.pool())
    .await?)
}

/// Reads run `run` for `ctx` and checks `runs.read`.
async fn read_run(
    state: &AppState,
    db: &Db,
    ctx: &RequestContext,
    run: &str,
) -> Result<RunRow, JobsError> {
    let id: RunId = scope::parse_id(run, "run")?;
    let row = find_run(db, id.as_uuid())
        .await?
        .ok_or_else(|| JobsError::not_found("run"))?;
    scope::authorize(state, ctx, "runs.read", &row.resource()).await?;
    Ok(row)
}

fn created(row: &RunRow) -> Response {
    let location = format!("/api/v1/runs/{}", RunId::from_uuid(row.id));
    let mut res = (StatusCode::CREATED, Json(row.view())).into_response();
    if let Ok(v) = HeaderValue::from_str(&location) {
        res.headers_mut().insert(header::LOCATION, v);
    }
    res
}

/// The same principal sent the same key again: the same request gets its run back,
/// another request is refused.
fn replay(row: &RunRow, fingerprint: &[u8]) -> Result<Response, JobsError> {
    if row.submit_fingerprint != fingerprint {
        return Err(ApiError::new(
            ErrorCode::IdempotencyMismatch,
            "This key was already used to submit a different run.",
        )
        .into());
    }
    let mut response = created(row);
    response
        .headers_mut()
        .insert(REPLAYED, HeaderValue::from_static("true"));
    Ok(response)
}

/// `POST /projects/{prj}/runs`. Needs `Idempotency-Key`.
///
/// Creates the run, its config snapshot and its job, then validates: the answer is the
/// run in QUEUED, or in REJECTED with every problem and a hint in `state_reason`.
#[utoipa::path(post, path = "/projects/{prj}/runs", operation_id = "submit_run", tag = "runs",
    params(("prj" = String, Path, description = "Project id, `prj_…`."),
           ("Idempotency-Key" = String, Header, description = "A new random value per run; a retry with the same key returns the same run.")),
    request_body = RunSubmission,
    responses((status = 201, description = "The run, QUEUED or REJECTED.", body = Run),
              (status = 400, description = "No `Idempotency-Key` (MRL-IDEM-170) or an unreadable body.", body = crate::error::Problem),
              (status = 422, description = "Fields need a change, or the key was used for another run.", body = crate::error::Problem)))]
// Axum extractors, one per input.
#[allow(clippy::too_many_arguments)]
pub async fn submit(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    Extension(chains): Extension<Chains>,
    Extension(projects): Extension<Projects>,
    ctx: RequestContext,
    Path(prj): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, JobsError> {
    let key = headers
        .get(&IDEMPOTENCY_KEY)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| {
            JobsError::domain(
                codes::SUBMIT_KEY_MISSING,
                StatusCode::BAD_REQUEST,
                "Send an Idempotency-Key header with a new random value for this run.",
            )
        })?;
    let (org, project) = project_scope(&state, &ctx, &projects, &prj, "runs.submit").await?;
    let submitted: Value = serde_json::from_slice(&body)
        .map_err(|e| bad_body(format!("The body is not JSON: {e}.")))?;
    scope::no_nul(&submitted)?;
    let sub: RunSubmission = serde_json::from_value(submitted.clone())
        .map_err(|e| bad_body(format!("The run could not be read: {e}.")))?;
    check_submission(&sub)?;
    let fingerprint = Sha256::new()
        .chain_update(prj.as_bytes())
        .chain_update(b"\n")
        .chain_update(&body)
        .finalize()
        .to_vec();
    let who = ctx.principal.id.clone();
    if let Some(existing) = find_by_key(&db, org, &who, &key, &fingerprint).await? {
        return replay(&existing, &fingerprint);
    }

    let resolved = chains.resolve(&sub.config).await.map_err(|e| match e {
        ResolveError::Invalid(errors) => JobsError::Api(ApiError::validation(errors)),
        ResolveError::Seam(s) => JobsError::Seam(s),
    })?;
    let resources = sub.resources.clone().unwrap_or_else(default_resources);
    let spec = JobSpec {
        org_id: org,
        project_id: project,
        kind: sub.kind.as_str().to_owned(),
        algorithm_id: sub.algorithm_id.clone(),
        base_model_version_id: sub.base_model_version_id.as_uuid(),
        reward_version_id: sub.reward_version_id.map(|r| r.as_uuid()),
        dataset_version_ids: sub
            .dataset_version_ids
            .iter()
            .map(|d| d.as_uuid())
            .collect(),
        resolved: resolved.clone(),
        resources: resources.clone(),
        max_runtime_seconds: sub.max_runtime_seconds,
    };
    let verdict = chains.validate(&spec).await?;
    let seeds = sub.seeds.clone().unwrap_or_else(|| {
        let n = u32::from_be_bytes(
            state.seams.ids.new_uuid().as_bytes()[12..16]
                .try_into()
                .unwrap_or([0; 4]),
        );
        json!({ "global": n })
    });
    let resolved_sha256 =
        Sha256::digest(serde_json::to_vec(&resolved).unwrap_or_default()).to_vec();

    // The parent's project never changes, so it is authorized before the write transaction
    // (the authorizer may need a pool connection). A parent the submitter can't read
    // answers exactly like a missing one.
    if let Some(parent) = sub.parent_run_id {
        let parent_project: Option<Uuid> =
            sqlx::query_scalar("SELECT project_id FROM runs WHERE id = $1 AND org_id = $2")
                .bind(parent.as_uuid())
                .bind(org)
                .fetch_optional(db.pool())
                .await?;
        let readable = match parent_project {
            Some(p) => {
                let res = resource("run", Some(parent.to_string()), org, p);
                scope::allowed(&state, &ctx, "runs.read", &res).await?
            }
            None => false,
        };
        if !readable {
            return Err(JobsError::not_found("parent run"));
        }
    }
    let mut tx = db.begin().await?;
    let experiment = match sub.experiment_id {
        Some(e) => sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM experiments WHERE id = $1 AND org_id = $2 AND project_id = $3",
        )
        .bind(e.as_uuid())
        .bind(org)
        .bind(project)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| JobsError::not_found("experiment"))?,
        None => get_or_create(&mut tx, org, project, DEFAULT_EXPERIMENT, &who).await?,
    };
    if let Some(parent) = sub.parent_run_id {
        let found: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM runs WHERE id = $1 AND org_id = $2")
                .bind(parent.as_uuid())
                .bind(org)
                .fetch_optional(&mut *tx)
                .await?;
        found.ok_or_else(|| JobsError::not_found("parent run"))?;
    }
    let run_id = Uuid::now_v7();
    let inserted = sqlx::query(
        "INSERT INTO runs (id, org_id, project_id, experiment_id, name, kind, algorithm_id, \
           base_model_version_id, reward_version_id, parent_run_id, parent_checkpoint_id, \
           submit_key, submit_fingerprint, created_by) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
    )
    .bind(run_id)
    .bind(org)
    .bind(project)
    .bind(experiment)
    .bind(sub.name.trim())
    .bind(sub.kind.as_str())
    .bind(&sub.algorithm_id)
    .bind(sub.base_model_version_id.as_uuid())
    .bind(sub.reward_version_id.map(|r| r.as_uuid()))
    .bind(sub.parent_run_id.map(|r| r.as_uuid()))
    .bind(sub.parent_checkpoint_id.map(|c| c.as_uuid()))
    .bind(&key)
    .bind(&fingerprint)
    .bind(&who)
    .execute(&mut *tx)
    .await
    .map_err(JobsError::from);
    if let Err(e) = inserted {
        if e.is_unique_violation("runs_org_id_created_by_submit_key_key") {
            // A concurrent request with this key won; answer with its run.
            drop(tx);
            let existing = find_by_key(&db, org, &who, &key, &fingerprint)
                .await?
                .ok_or_else(|| JobsError::changed("run"))?;
            return replay(&existing, &fingerprint);
        }
        return Err(e);
    }
    sqlx::query(
        "INSERT INTO run_configs (run_id, org_id, submitted, resolved, resolved_sha256, \
           dataset_version_ids, seeds) VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(run_id)
    .bind(org)
    .bind(&submitted)
    .bind(&resolved)
    .bind(&resolved_sha256)
    .bind(&spec.dataset_version_ids)
    .bind(&seeds)
    .execute(&mut *tx)
    .await?;
    let job = JobTransitions::create(
        &mut tx,
        &NewJob {
            id: Uuid::now_v7(),
            org_id: org,
            project_id: project,
            kind: sub.kind.as_str().to_owned(),
            run_id: Some(run_id),
            subject_ref: None,
            priority: sub.priority,
            resource_request: serde_json::to_value(&resources).unwrap_or_else(|_| json!({})),
            retry_policy: serde_json::to_value(sub.retry.clone().unwrap_or_default())
                .unwrap_or_else(|_| json!({})),
            max_runtime_seconds: sub.max_runtime_seconds,
            created_by: who.clone(),
        },
    )
    .await?;
    let job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Submit,
        Owner::Jobs,
        JobChange::default(),
    )
    .await?;
    match verdict {
        Validation::Accepted => {
            JobTransitions::apply(
                &mut tx,
                &job,
                JobEvent::Accept,
                Owner::Jobs,
                JobChange::default(),
            )
            .await?
        }
        Validation::Rejected(problems) => {
            let change = JobChange {
                reason: Some(reason(&problems)),
                failure: None,
            };
            JobTransitions::apply(&mut tx, &job, JobEvent::Reject, Owner::Jobs, change).await?
        }
    };
    let target = RunId::from_uuid(run_id).to_string();
    scope::audit(&mut tx, &state, &ctx, (org, project), "runs.submit", target).await?;
    tx.commit().await?;
    let row = find_run(&db, run_id)
        .await?
        .ok_or_else(|| JobsError::not_found("run"))?;
    Ok(created(&row))
}

/// `GET /projects/{prj}/runs`
#[utoipa::path(get, path = "/projects/{prj}/runs", operation_id = "list_runs", tag = "runs",
    params(("prj" = String, Path, description = "Project id, `prj_…`."), PageQuery, RunFilter),
    responses((status = 200, description = "A page of runs, newest first.", body = Page<Run>)))]
pub async fn list(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    Extension(projects): Extension<Projects>,
    ctx: RequestContext,
    Path(prj): Path<String>,
    Query(q): Query<PageQuery>,
    Query(f): Query<RunFilter>,
) -> Result<Json<Page<Run>>, JobsError> {
    let (org, project) = project_scope(&state, &ctx, &projects, &prj, "runs.read").await?;
    let page = q.resolve()?;
    let after = scope::after::<RunId>(&page, |r| r.as_uuid())?;
    let experiment = f
        .experiment
        .as_deref()
        .map(|e| {
            e.parse::<ExperimentId>().map(|e| e.as_uuid()).map_err(|_| {
                ApiError::new(
                    ErrorCode::Filter,
                    format!("experiment `{e}` is not an experiment id."),
                )
            })
        })
        .transpose()?;
    if let Some(s) = &f.state {
        s.parse::<mrl_domain::state::JobState>().map_err(|_| {
            ApiError::new(
                ErrorCode::Filter,
                format!("state `{s}` is not a job state."),
            )
        })?;
    }
    let rows = sqlx::query_as::<_, RunRow>(concat!(
        "SELECT ",
        run_cols!(),
        " FROM runs r JOIN jobs j ON j.run_id = r.id \
         WHERE r.org_id = $1 AND r.project_id = $2 \
           AND ($3::uuid IS NULL OR r.experiment_id = $3) \
           AND ($4::text IS NULL OR j.state = $4) \
           AND ($5::uuid IS NULL OR r.id < $5) ORDER BY r.id DESC LIMIT $6"
    ))
    .bind(org)
    .bind(project)
    .bind(experiment)
    .bind(&f.state)
    .bind(after)
    .bind(i64::try_from(page.fetch()).unwrap_or(i64::MAX))
    .fetch_all(db.pool())
    .await?;
    let (rows, next_cursor) = scope::cut(rows, &page, |r| RunId::from_uuid(r.id).to_string());
    Ok(Json(Page {
        items: rows.iter().map(RunRow::view).collect(),
        next_cursor,
    }))
}

/// `GET /runs/{run}`
#[utoipa::path(get, path = "/runs/{run}", operation_id = "get_run", tag = "runs",
    params(("run" = String, Path, description = "Run id, `run_…`.")),
    responses((status = 200, description = "The run.", body = Run),
              (status = 404, description = "No such run.", body = crate::error::Problem)))]
pub async fn get(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Path(run): Path<String>,
) -> Result<Json<Run>, JobsError> {
    Ok(Json(read_run(&state, &db, &ctx, &run).await?.view()))
}

/// `POST /runs/{run}:cancel` (axum sees `/runs/{run}` with the action in the segment).
#[utoipa::path(post, path = "/runs/{run}:cancel", operation_id = "cancel_run", tag = "runs",
    params(("run" = String, Path, description = "Run id, `run_…`.")),
    responses((status = 200, description = "The run, now CANCELLING or CANCELLED.", body = Run),
              (status = 409, description = "The run already finished (MRL-STATE-170).", body = crate::error::Problem)))]
pub async fn action(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Path(segment): Path<String>,
) -> Result<Json<Run>, JobsError> {
    let Some(run) = segment.strip_suffix(":cancel") else {
        return Err(ApiError::new(ErrorCode::RouteNotFound, "No endpoint has this path.").into());
    };
    let row = read_run(&state, &db, &ctx, run).await?;
    let job = JobId::from_uuid(row.job_id).to_string();
    crate::modules::jobs::api::cancel(&state, &db, &ctx, &job).await?;
    let row = find_run(&db, row.id)
        .await?
        .ok_or_else(|| JobsError::not_found("run"))?;
    Ok(Json(row.view()))
}

/// `GET /runs/{run}/config`
#[utoipa::path(get, path = "/runs/{run}/config", operation_id = "get_run_config", tag = "runs",
    params(("run" = String, Path, description = "Run id, `run_…`.")),
    responses((status = 200, description = "The immutable config snapshot.", body = RunConfig)))]
pub async fn config(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Path(run): Path<String>,
) -> Result<Response, JobsError> {
    let row = read_run(&state, &db, &ctx, &run).await?;
    let (mut cfg, sha, datasets) = {
        let r = sqlx::query_as::<_, (Value, Value, Vec<u8>, Vec<Uuid>, Value, Option<String>, OffsetDateTime)>(
            "SELECT submitted, resolved, resolved_sha256, dataset_version_ids, seeds, runtime_ref, \
               created_at FROM run_configs WHERE run_id = $1 AND org_id = $2",
        )
        .bind(row.id)
        .bind(row.org_id)
        .fetch_optional(db.pool())
        .await?
        .ok_or_else(|| JobsError::not_found("run config"))?;
        (
            RunConfig {
                run_id: String::new(),
                submitted: r.0,
                resolved: r.1,
                resolved_sha256: String::new(),
                dataset_version_ids: Vec::new(),
                seeds: r.4,
                runtime_ref: r.5,
                created_at: r.6,
            },
            r.2,
            r.3,
        )
    };
    cfg.run_id = RunId::from_uuid(row.id).to_string();
    cfg.resolved_sha256 = hex(&sha);
    cfg.dataset_version_ids = datasets
        .into_iter()
        .map(|d| DatasetVersionId::from_uuid(d).to_string())
        .collect();
    // The snapshot never changes, so its hash is a strong ETag.
    let tag = cfg.resolved_sha256.clone();
    Ok(with_etag_text(&tag, cfg))
}

fn with_etag_text<T: Serialize>(tag: &str, body: T) -> Response {
    let mut res = Json(body).into_response();
    if let Ok(v) = HeaderValue::from_str(&format!("\"{tag}\"")) {
        res.headers_mut().insert(header::ETAG, v);
    }
    res
}

/// `GET /runs/{run}/attempts`
#[utoipa::path(get, path = "/runs/{run}/attempts", operation_id = "list_run_attempts", tag = "runs",
    params(("run" = String, Path, description = "Run id, `run_…`."), PageQuery),
    responses((status = 200, description = "The run's attempts, oldest first.", body = Page<Attempt>)))]
pub async fn attempts(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Path(run): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<Attempt>>, JobsError> {
    let row = read_run(&state, &db, &ctx, &run).await?;
    let page = q.resolve()?;
    let after: Option<i32> = page
        .after
        .as_deref()
        .map(|c| {
            c.parse().map_err(|_| {
                JobsError::from(ApiError::new(
                    ErrorCode::Pagination,
                    "cursor must be the next_cursor value from an earlier page, unchanged.",
                ))
            })
        })
        .transpose()?;
    let rows = sqlx::query_as::<_, AttemptRow>(concat!(
        "SELECT ",
        attempt_cols!(),
        " FROM job_attempts WHERE job_id = $1 AND org_id = $2 \
           AND ($3::int IS NULL OR attempt_no > $3) ORDER BY attempt_no LIMIT $4"
    ))
    .bind(row.job_id)
    .bind(row.org_id)
    .bind(after)
    .bind(i64::try_from(page.fetch()).unwrap_or(i64::MAX))
    .fetch_all(db.pool())
    .await?;
    let (rows, next_cursor) = scope::cut(rows, &page, |a| a.attempt_no.to_string());
    Ok(Json(Page {
        items: rows.iter().map(AttemptRow::view).collect(),
        next_cursor,
    }))
}

/// `GET /runs/{run}/timeline`: the outbox events of the run's job and attempts, in order.
#[utoipa::path(get, path = "/runs/{run}/timeline", operation_id = "get_run_timeline", tag = "runs",
    params(("run" = String, Path, description = "Run id, `run_…`."), PageQuery),
    responses((status = 200, description = "State changes, oldest first.", body = Page<TimelineEntry>)))]
pub async fn timeline(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Path(run): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<TimelineEntry>>, JobsError> {
    let row = read_run(&state, &db, &ctx, &run).await?;
    let page = q.resolve()?;
    let after: Option<i64> = page
        .after
        .as_deref()
        .map(|c| {
            c.parse().map_err(|_| {
                JobsError::from(ApiError::new(
                    ErrorCode::Pagination,
                    "cursor must be the next_cursor value from an earlier page, unchanged.",
                ))
            })
        })
        .transpose()?;
    type Event = (i64, String, String, Uuid, i64, Value, OffsetDateTime);
    let rows = sqlx::query_as::<_, Event>(
        "SELECT id, type, aggregate_kind, aggregate_id, aggregate_seq, payload, created_at \
         FROM outbox_events WHERE ($1::bigint IS NULL OR id > $1) AND ( \
           (aggregate_kind = 'job' AND aggregate_id = $2) OR \
           (aggregate_kind = 'attempt' AND aggregate_id IN \
              (SELECT id FROM job_attempts WHERE job_id = $2))) \
         ORDER BY id LIMIT $3",
    )
    .bind(after)
    .bind(row.job_id)
    .bind(i64::try_from(page.fetch()).unwrap_or(i64::MAX))
    .fetch_all(db.pool())
    .await?;
    let (rows, next_cursor) = scope::cut(rows, &page, |e| e.0.to_string());
    let items = rows
        .into_iter()
        .map(|(_, kind, agg, id, seq, payload, at)| TimelineEntry {
            aggregate_id: if agg == "job" {
                JobId::from_uuid(id).to_string()
            } else {
                mrl_domain::ids::AttemptId::from_uuid(id).to_string()
            },
            r#type: kind,
            aggregate_kind: agg,
            aggregate_seq: seq,
            payload,
            at,
        })
        .collect();
    Ok(Json(Page { items, next_cursor }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub(v: Value) -> RunSubmission {
        serde_json::from_value(v).unwrap()
    }

    fn minimal() -> Value {
        json!({
            "name": "first",
            "algorithm_id": "sft",
            "base_model_version_id": ModelVersionId::new().to_string(),
        })
    }

    #[test]
    fn minimal_submission_gets_defaults() {
        let s = sub(minimal());
        assert_eq!(s.kind, RunKind::Train);
        assert_eq!(s.config, json!({}));
        assert!(s.resources.is_none() && s.seeds.is_none());
        check_submission(&s).unwrap();
        default_resources().validate().unwrap();
    }

    #[test]
    fn unknown_fields_and_bad_ids_are_refused() {
        let mut v = minimal();
        v["bogus"] = json!(1);
        assert!(serde_json::from_value::<RunSubmission>(v).is_err());
        let mut v = minimal();
        v["base_model_version_id"] = json!("run_123");
        assert!(serde_json::from_value::<RunSubmission>(v).is_err());
    }

    #[test]
    fn field_checks_name_every_problem() {
        let mut v = minimal();
        v["name"] = json!(" ");
        v["algorithm_id"] = json!("SFT!");
        v["priority"] = json!(5000);
        v["max_runtime_seconds"] = json!(0);
        v["seeds"] = json!([1]);
        let JobsError::Api(e) = check_submission(&sub(v)).unwrap_err() else {
            panic!("expected a 422");
        };
        let paths: Vec<_> = e.errors.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(
            paths,
            [
                "/name",
                "/algorithm_id",
                "/priority",
                "/max_runtime_seconds",
                "/seeds"
            ]
        );
    }

    #[test]
    fn hex_is_lowercase_pairs() {
        assert_eq!(hex(&[0, 15, 255]), "000fff");
    }
}

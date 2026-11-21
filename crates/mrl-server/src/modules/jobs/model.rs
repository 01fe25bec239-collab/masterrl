//! Job and attempt rows as stored, and the JSON views the API returns.

use super::error::JobsError;
use mrl_domain::ids::{AttemptId, JobId, PoolId, ProjectId, RunId};
use mrl_domain::state::{JobAttemptState, JobState};
use serde::Serialize;
use time::OffsetDateTime;
use uuid::Uuid;

/// Every job kind (the `jobs.kind` check in migration 17004).
pub const JOB_KINDS: [&str; 10] = [
    "TRAIN",
    "EVAL",
    "DATA_PREP",
    "IMPORT",
    "REWARD_PREVIEW",
    "MERGE_ADAPTER",
    "CONVERT",
    "QUANTIZE",
    "EXPORT",
    "GENERATE",
];

/// One `jobs` row.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct JobRow {
    /// Job id.
    pub id: Uuid,
    /// Owning org.
    pub org_id: Uuid,
    /// Owning project.
    pub project_id: Uuid,
    /// `TRAIN`, `EVAL`, `DATA_PREP`, ….
    pub kind: String,
    /// The run, for TRAIN and EVAL jobs.
    pub run_id: Option<Uuid>,
    /// What a non-run job works on.
    pub subject_ref: Option<serde_json::Value>,
    /// Current state (wire name).
    pub state: String,
    /// Bumped by every state change; the compare-and-set token.
    pub state_version: i64,
    /// Bumped by every change; the ETag.
    pub version: i64,
    /// Why the job is in this state: `{code, message, problems?}`.
    pub state_reason: Option<serde_json::Value>,
    /// Failure class (or `CLASS/SUB`) of the last failure.
    pub failure_class: Option<String>,
    /// Fencing epoch, bumped for every new attempt and lost lease.
    pub epoch: i64,
    /// Higher runs first.
    pub priority: i32,
    /// Pool, once placed.
    pub pool_id: Option<Uuid>,
    /// A `ResourceRequest`.
    pub resource_request: serde_json::Value,
    /// Retry rules: `{auto_tune, budgets: {CLASS: n}}`.
    pub retry_policy: serde_json::Value,
    /// Retries used per failure class.
    pub retries_used: serde_json::Value,
    /// The latest attempt.
    pub current_attempt_id: Option<Uuid>,
    /// Last time it entered QUEUED.
    pub queued_at: Option<OffsetDateTime>,
    /// First time it entered RUNNING.
    pub started_at: Option<OffsetDateTime>,
    /// When it reached a final state.
    pub finished_at: Option<OffsetDateTime>,
    /// Earliest retry time while in RETRY_WAIT.
    pub next_eligible_at: Option<OffsetDateTime>,
    /// Longest allowed run time.
    pub max_runtime_seconds: Option<i64>,
    /// Principal id of the submitter.
    pub created_by: String,
    /// Created.
    pub created_at: OffsetDateTime,
    /// Last changed.
    pub updated_at: OffsetDateTime,
}

impl JobRow {
    /// The state as the domain enum.
    pub fn job_state(&self) -> Result<JobState, JobsError> {
        self.state
            .parse()
            .map_err(|e: mrl_domain::state::UnknownState| {
                JobsError::illegal(format!("The job has a state the server doesn't know: {e}."))
            })
    }

    /// The API view.
    pub fn view(&self) -> Job {
        Job {
            id: JobId::from_uuid(self.id).to_string(),
            project_id: ProjectId::from_uuid(self.project_id).to_string(),
            run_id: self.run_id.map(|r| RunId::from_uuid(r).to_string()),
            kind: self.kind.clone(),
            state: self.state.clone(),
            state_version: self.state_version,
            state_reason: self.state_reason.clone(),
            failure_class: self.failure_class.clone(),
            epoch: self.epoch,
            priority: self.priority,
            pool_id: self.pool_id.map(|p| PoolId::from_uuid(p).to_string()),
            resource_request: self.resource_request.clone(),
            retry_policy: self.retry_policy.clone(),
            retries_used: self.retries_used.clone(),
            current_attempt_id: self
                .current_attempt_id
                .map(|a| AttemptId::from_uuid(a).to_string()),
            queued_at: self.queued_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            next_eligible_at: self.next_eligible_at,
            max_runtime_seconds: self.max_runtime_seconds,
            created_by: self.created_by.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            version: self.version,
        }
    }
}

/// One `job_attempts` row.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct AttemptRow {
    /// Attempt id.
    pub id: Uuid,
    /// Owning org.
    pub org_id: Uuid,
    /// The job.
    pub job_id: Uuid,
    /// 1, 2, 3, … per job.
    pub attempt_no: i32,
    /// The job's epoch when this attempt was created.
    pub epoch: i64,
    /// Current state (wire name).
    pub state: String,
    /// Bumped by every state change; the compare-and-set token.
    pub state_version: i64,
    /// Checkpoint to resume from.
    pub resume_from_checkpoint_id: Option<Uuid>,
    /// Lease renewed by agent heartbeats.
    pub lease_expires_at: Option<OffsetDateTime>,
    /// The worker's exit report.
    pub exit_report: Option<serde_json::Value>,
    /// Failure class of an error exit.
    pub failure_class: Option<String>,
    /// Last training step reported.
    pub last_step: Option<i64>,
    /// When it started running.
    pub started_at: Option<OffsetDateTime>,
    /// When it ended.
    pub ended_at: Option<OffsetDateTime>,
    /// Created.
    pub created_at: OffsetDateTime,
}

impl AttemptRow {
    /// The state as the domain enum.
    pub fn attempt_state(&self) -> Result<JobAttemptState, JobsError> {
        self.state
            .parse()
            .map_err(|e: mrl_domain::state::UnknownState| {
                JobsError::illegal(format!(
                    "The attempt has a state the server doesn't know: {e}."
                ))
            })
    }

    /// The API view.
    pub fn view(&self) -> Attempt {
        Attempt {
            id: AttemptId::from_uuid(self.id).to_string(),
            job_id: JobId::from_uuid(self.job_id).to_string(),
            attempt_no: self.attempt_no,
            epoch: self.epoch,
            state: self.state.clone(),
            failure_class: self.failure_class.clone(),
            last_step: self.last_step,
            started_at: self.started_at,
            ended_at: self.ended_at,
            created_at: self.created_at,
        }
    }
}

/// A job, as the API shows it.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct Job {
    /// `job_…`.
    pub id: String,
    /// `prj_…`.
    pub project_id: String,
    /// `run_…`, for TRAIN and EVAL jobs.
    pub run_id: Option<String>,
    /// `TRAIN`, `EVAL`, `DATA_PREP`, ….
    pub kind: String,
    /// `CREATED`, `QUEUED`, `RUNNING`, `REJECTED`, ….
    pub state: String,
    /// Bumped by every state change.
    pub state_version: i64,
    /// Why the job is in this state: `{code, message, problems?}`.
    #[schema(value_type = Option<Object>)]
    pub state_reason: Option<serde_json::Value>,
    /// Failure class of the last failure, like `OOM/OOM_GPU`.
    pub failure_class: Option<String>,
    /// Fencing epoch.
    pub epoch: i64,
    /// Higher runs first.
    pub priority: i32,
    /// `pool_…`, once placed.
    pub pool_id: Option<String>,
    /// What the job asks the scheduler for.
    #[schema(value_type = Object)]
    pub resource_request: serde_json::Value,
    /// Retry rules: `{auto_tune, budgets}`.
    #[schema(value_type = Object)]
    pub retry_policy: serde_json::Value,
    /// Retries used per failure class.
    #[schema(value_type = Object)]
    pub retries_used: serde_json::Value,
    /// `att_…`, the latest attempt.
    pub current_attempt_id: Option<String>,
    /// Last time it entered QUEUED.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub queued_at: Option<OffsetDateTime>,
    /// First time it entered RUNNING.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub started_at: Option<OffsetDateTime>,
    /// When it reached a final state.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub finished_at: Option<OffsetDateTime>,
    /// Earliest retry time while in RETRY_WAIT.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub next_eligible_at: Option<OffsetDateTime>,
    /// Longest allowed run time.
    pub max_runtime_seconds: Option<i64>,
    /// Principal id of the submitter.
    pub created_by: String,
    /// Created.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
    /// Last changed.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub updated_at: OffsetDateTime,
    /// Bumped by every change; also the `ETag`.
    pub version: i64,
}

/// One attempt at running a job.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct Attempt {
    /// `att_…`.
    pub id: String,
    /// `job_…`.
    pub job_id: String,
    /// 1, 2, 3, … per job.
    pub attempt_no: i32,
    /// The job's epoch when this attempt was created.
    pub epoch: i64,
    /// `PENDING`, `LAUNCHING`, `RUNNING`, `EXITED_OK`, `EXITED_ERROR`, `LOST`, `KILLED`.
    pub state: String,
    /// Failure class of an error exit.
    pub failure_class: Option<String>,
    /// Last training step reported.
    pub last_step: Option<i64>,
    /// When it started running.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub started_at: Option<OffsetDateTime>,
    /// When it ended.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub ended_at: Option<OffsetDateTime>,
    /// Created.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
}

/// Why a job could not be queued, or a step failed: one problem.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct Rejection {
    /// Error code, like `MRL-CAP-001`.
    pub code: String,
    /// JSON Pointer into the submission, like `/algorithm_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// What is wrong, in plain words.
    pub message: String,
    /// What to do, from the guidance catalog.
    pub hint: String,
}

impl Rejection {
    /// A rejection with `code`; the hint comes from the guidance catalog.
    pub fn new(
        code: mrl_domain::errors::ErrorCode,
        path: Option<&str>,
        message: impl Into<String>,
    ) -> Self {
        let code = code.to_string();
        Self {
            hint: super::error::hint_for(&code),
            code,
            path: path.map(str::to_owned),
            message: message.into(),
        }
    }

    /// A rejection with a failure code (like `VERIFICATION_FAILED`) instead of an error code.
    pub fn failure(code: mrl_domain::failure::FailureCode, message: impl Into<String>) -> Self {
        let code = code.to_string();
        Self {
            hint: super::error::hint_for(&code),
            code,
            path: None,
            message: message.into(),
        }
    }
}

/// `state_reason` for a set of problems: the first one's code and message plus the full list.
pub fn reason(problems: &[Rejection]) -> serde_json::Value {
    let first = problems.first();
    serde_json::json!({
        "code": first.map(|p| p.code.as_str()),
        "message": first.map(|p| p.message.as_str()),
        "problems": problems,
    })
}

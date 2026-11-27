//! `JobTransitions`: the only code that changes `jobs.state` and `job_attempts.state`.
//!
//! Every change checks P06's transition table (step and owner), updates the row with
//! compare-and-set on `state_version`, mirrors the state onto the run, and writes the
//! outbox event, all in the caller's transaction. The outbox rows are the timeline.
//! A lost compare-and-set is reported (MRL-CONFLICT-170), never retried here.

use super::chains::{Completion, CompletionOutcome};
use super::error::JobsError;
use super::model::{reason, AttemptRow, JobRow};
use crate::db::Tx;
use crate::error::{ApiError, ErrorCode};
use crate::modules::events::{NewEvent, OutboxWriter};
use mrl_domain::failure::{FailureClass, FailureCode, RetryBudget};
use mrl_domain::ids::{JobId, RunId};
use mrl_domain::state::{
    Job, JobAttempt, JobAttemptEvent, JobAttemptState, JobEvent, JobState, Owner, StateMachine,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

macro_rules! job_cols {
    () => {
        "id, org_id, project_id, kind, run_id, subject_ref, state, state_version, version, \
         state_reason, failure_class, epoch, priority, pool_id, resource_request, retry_policy, \
         retries_used, current_attempt_id, queued_at, started_at, finished_at, next_eligible_at, \
         max_runtime_seconds, created_by, created_at, updated_at"
    };
}
pub(crate) use job_cols;

macro_rules! attempt_cols {
    () => {
        "id, org_id, job_id, attempt_no, epoch, state, state_version, resume_from_checkpoint_id, \
         lease_expires_at, exit_report, failure_class, last_step, started_at, ended_at, created_at"
    };
}
pub(crate) use attempt_cols;

/// A job to create in CREATED.
#[derive(Debug, Clone, PartialEq)]
pub struct NewJob {
    /// New job id.
    pub id: Uuid,
    /// Owning org.
    pub org_id: Uuid,
    /// Owning project.
    pub project_id: Uuid,
    /// `TRAIN`, `EVAL`, ….
    pub kind: String,
    /// The run, for TRAIN and EVAL.
    pub run_id: Option<Uuid>,
    /// What a non-run job works on.
    pub subject_ref: Option<Value>,
    /// Higher runs first.
    pub priority: i32,
    /// A `ResourceRequest` as JSON.
    pub resource_request: Value,
    /// Retry rules, see [`RetryRules`].
    pub retry_policy: Value,
    /// Longest allowed run time.
    pub max_runtime_seconds: Option<i64>,
    /// Submitter's principal id.
    pub created_by: String,
}

/// Extra facts a job transition records.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct JobChange {
    /// New `state_reason` (`None` clears it: each state explains itself).
    pub reason: Option<Value>,
    /// Failure to record in `failure_class`.
    pub failure: Option<FailureCode>,
}

/// Extra facts an attempt transition records.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AttemptChange {
    /// Failure class of an error exit.
    pub failure: Option<FailureCode>,
    /// The worker's exit report.
    pub exit_report: Option<Value>,
    /// Last training step reported.
    pub last_step: Option<i64>,
}

/// Per-job retry rules (`jobs.retry_policy`): P06's defaults with optional per-class budgets.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryRules {
    /// Allow automatic setting changes, which makes `OOM` retryable.
    #[serde(default)]
    pub auto_tune: bool,
    /// Retry budget per failure class name, replacing the default budget.
    #[serde(default)]
    pub budgets: BTreeMap<String, u32>,
}

impl RetryRules {
    /// Parses stored rules; unreadable rules fall back to the defaults.
    pub fn from_json(v: &Value) -> Self {
        serde_json::from_value(v.clone()).unwrap_or_default()
    }
}

/// See the module docs.
pub struct JobTransitions;

fn internal(e: serde_json::Error) -> JobsError {
    tracing::error!(error = %e, "could not encode a job event");
    JobsError::Api(ApiError::new(
        ErrorCode::Internal,
        "The server hit an internal problem.",
    ))
}

fn is_live(s: JobAttemptState) -> bool {
    matches!(
        s,
        JobAttemptState::Pending | JobAttemptState::Launching | JobAttemptState::Running
    )
}

impl JobTransitions {
    /// Reads a job and locks it for this transaction.
    pub async fn lock(tx: &mut Tx, org: Uuid, id: Uuid) -> Result<Option<JobRow>, JobsError> {
        Ok(sqlx::query_as::<_, JobRow>(concat!(
            "SELECT ",
            job_cols!(),
            " FROM jobs WHERE id = $1 AND org_id = $2 FOR UPDATE"
        ))
        .bind(id)
        .bind(org)
        .fetch_optional(&mut **tx)
        .await?)
    }

    /// Reads an attempt and locks it for this transaction. Its job is locked first: every
    /// transition locks the job before the attempt, so two transactions can't deadlock.
    pub async fn lock_attempt(
        tx: &mut Tx,
        org: Uuid,
        id: Uuid,
    ) -> Result<Option<AttemptRow>, JobsError> {
        let job: Option<Uuid> =
            sqlx::query_scalar("SELECT job_id FROM job_attempts WHERE id = $1 AND org_id = $2")
                .bind(id)
                .bind(org)
                .fetch_optional(&mut **tx)
                .await?;
        let Some(job) = job else {
            return Ok(None);
        };
        sqlx::query("SELECT 1 FROM jobs WHERE id = $1 AND org_id = $2 FOR UPDATE")
            .bind(job)
            .bind(org)
            .execute(&mut **tx)
            .await?;
        Ok(sqlx::query_as::<_, AttemptRow>(concat!(
            "SELECT ",
            attempt_cols!(),
            " FROM job_attempts WHERE id = $1 AND org_id = $2 FOR UPDATE"
        ))
        .bind(id)
        .bind(org)
        .fetch_optional(&mut **tx)
        .await?)
    }

    /// Inserts a job in CREATED and writes `job.created` (sequence 0).
    pub async fn create(tx: &mut Tx, job: &NewJob) -> Result<JobRow, JobsError> {
        let row = sqlx::query_as::<_, JobRow>(concat!(
            "INSERT INTO jobs (id, org_id, project_id, kind, run_id, subject_ref, state, priority, \
               resource_request, retry_policy, max_runtime_seconds, created_by) \
             VALUES ($1, $2, $3, $4, $5, $6, 'CREATED', $7, $8, $9, $10, $11) RETURNING ",
            job_cols!()
        ))
        .bind(job.id)
        .bind(job.org_id)
        .bind(job.project_id)
        .bind(&job.kind)
        .bind(job.run_id)
        .bind(&job.subject_ref)
        .bind(job.priority)
        .bind(&job.resource_request)
        .bind(&job.retry_policy)
        .bind(job.max_runtime_seconds)
        .bind(&job.created_by)
        .fetch_one(&mut **tx)
        .await?;
        let payload = json!({
            "to": JobState::Created.as_str(),
            "kind": row.kind,
            "run_id": row.run_id.map(|r| RunId::from_uuid(r).to_string()),
            "by": Owner::Jobs.as_str(),
        });
        let event = NewEvent::new(Job::CREATED_EVENT, "job", row.id, 0, payload)
            .map_err(internal)?
            .org(row.org_id);
        OutboxWriter::write(tx, &event).await?;
        Self::mirror(tx, &row).await?;
        Ok(row)
    }

    /// Applies `event` to the job `row` was read as, on behalf of `by`.
    ///
    /// Starting an attempt goes through [`Self::launch`], completion through
    /// [`Self::verify_completion`], retries through [`Self::fail_or_retry`] and cancelling
    /// through [`Self::request_cancel`] (or an attempt ending), because those steps check or
    /// change more than the state.
    pub async fn apply(
        tx: &mut Tx,
        row: &JobRow,
        event: JobEvent,
        by: Owner,
        change: JobChange,
    ) -> Result<JobRow, JobsError> {
        let special = match event {
            JobEvent::Launch => Some("JobTransitions::launch"),
            JobEvent::Complete => Some("JobTransitions::verify_completion"),
            JobEvent::Interrupt => Some("JobTransitions::fail_or_retry"),
            JobEvent::RequestCancel | JobEvent::ConfirmCancel => {
                Some("JobTransitions::request_cancel")
            }
            _ => None,
        };
        if let Some(path) = special {
            return Err(JobsError::illegal(format!(
                "The step `{}` must go through {path}.",
                event.event_type()
            )));
        }
        Self::step(tx, row, event, by, change, Extra::keep(row)).await
    }

    /// ALLOCATING → PREPARING: bumps the epoch and creates the next attempt in PENDING.
    /// The job is locked and `row` checked against it (version and epoch) before the
    /// attempt is inserted, so a stale row adds nothing.
    pub async fn launch(
        tx: &mut Tx,
        row: &JobRow,
        resume_from_checkpoint: Option<Uuid>,
    ) -> Result<(JobRow, AttemptRow), JobsError> {
        let current: Option<(i64, i64)> = sqlx::query_as(
            "SELECT state_version, epoch FROM jobs WHERE id = $1 AND org_id = $2 FOR UPDATE",
        )
        .bind(row.id)
        .bind(row.org_id)
        .fetch_optional(&mut **tx)
        .await?;
        if current != Some((row.state_version, row.epoch)) {
            return Err(JobsError::changed("job"));
        }
        Job::can_transition(row.job_state()?, JobEvent::Launch)?;
        let epoch = row.epoch + 1;
        let attempt = sqlx::query_as::<_, AttemptRow>(concat!(
            "INSERT INTO job_attempts (id, org_id, job_id, attempt_no, epoch, state, \
               resume_from_checkpoint_id) \
             SELECT $1, $2, $3, COALESCE(MAX(attempt_no), 0) + 1, $4, 'PENDING', $5 \
               FROM job_attempts WHERE job_id = $3 RETURNING ",
            attempt_cols!()
        ))
        .bind(Uuid::now_v7())
        .bind(row.org_id)
        .bind(row.id)
        .bind(epoch)
        .bind(resume_from_checkpoint)
        .fetch_one(&mut **tx)
        .await
        .map_err(JobsError::from)
        .map_err(|e| {
            if e.is_unique_violation("one_live_attempt") {
                JobsError::illegal("This job already has an attempt that is still running.")
            } else {
                e
            }
        })?;
        let extra = Extra {
            epoch,
            current_attempt_id: Some(attempt.id),
            ..Extra::keep(row)
        };
        let job = Self::step(
            tx,
            row,
            JobEvent::Launch,
            Owner::Scheduler,
            JobChange::default(),
            extra,
        )
        .await?;
        let payload = json!({
            "to": attempt.state,
            "job_id": JobId::from_uuid(row.id).to_string(),
            "attempt_no": attempt.attempt_no,
            "epoch": epoch,
        });
        let event = NewEvent::new(JobAttempt::CREATED_EVENT, "attempt", attempt.id, 0, payload)
            .map_err(internal)?
            .org(row.org_id);
        OutboxWriter::write(tx, &event).await?;
        Ok((job, attempt))
    }

    /// After a failure: RETRY_WAIT with backoff when the job's retry rules allow another
    /// try from this state, otherwise FAILED.
    pub async fn fail_or_retry(
        tx: &mut Tx,
        row: &JobRow,
        failure: FailureCode,
        message: &str,
    ) -> Result<JobRow, JobsError> {
        let rules = RetryRules::from_json(&row.retry_policy);
        let class = failure.class();
        let mut policy = class.retry_policy_default();
        if let Some(n) = rules.budgets.get(class.as_str()) {
            policy.budget = RetryBudget::Limited(*n);
        }
        let mut used: BTreeMap<String, u32> =
            serde_json::from_value(row.retries_used.clone()).unwrap_or_default();
        let count = used.get(class.as_str()).copied().unwrap_or(0);
        let change = JobChange {
            reason: Some(json!({ "code": failure.to_string(), "message": message })),
            failure: Some(failure),
        };
        let can_wait = Job::transition(row.job_state()?, JobEvent::Interrupt).is_some();
        match policy.next_delay(count, rules.auto_tune) {
            Some(delay) if can_wait => {
                if policy.consumes_budget {
                    used.insert(class.as_str().to_owned(), count + 1);
                }
                let extra = Extra {
                    retries_used: serde_json::to_value(&used).map_err(internal)?,
                    retry_in: Some(delay.as_secs_f64()),
                    ..Extra::keep(row)
                };
                Self::step(tx, row, JobEvent::Interrupt, Owner::Jobs, change, extra).await
            }
            _ => Self::apply(tx, row, JobEvent::Fail, Owner::Jobs, change).await,
        }
    }

    /// FINALIZING → COMPLETED, FAILED (VERIFICATION_FAILED) or RETRY_WAIT, as `verdict`
    /// says. Get `verdict` from [`super::chains::Chains::verify`] **before** opening the write
    /// transaction (a verifier may need a pool connection); the compare-and-set here
    /// refuses a row that changed meanwhile (MRL-CONFLICT-170).
    pub async fn verify_completion(
        tx: &mut Tx,
        row: &JobRow,
        verdict: Completion,
    ) -> Result<JobRow, JobsError> {
        Job::can_transition(row.job_state()?, JobEvent::Complete)?;
        let (outcome, writes) = verdict
            .for_job(row)
            .ok_or_else(|| JobsError::changed("job"))?;
        let done = match outcome {
            CompletionOutcome::Verified => {
                let extra = Extra::keep(row);
                let change = JobChange::default();
                let by = Owner::ArtifactsVerifier;
                Self::step(tx, row, JobEvent::Complete, by, change, extra).await
            }
            CompletionOutcome::Failed(problem) => {
                let change = JobChange {
                    reason: Some(reason(std::slice::from_ref(&problem))),
                    failure: Some(FailureCode::class_only(FailureClass::VerificationFailed)),
                };
                Self::apply(tx, row, JobEvent::Fail, Owner::Jobs, change).await
            }
            CompletionOutcome::RetryLater(why) => {
                let failure = FailureCode::class_only(FailureClass::InfraTransient);
                Self::fail_or_retry(tx, row, failure, &why).await
            }
        }?;
        for write in writes {
            write.commit(tx).await?;
        }
        Ok(done)
    }

    /// Cancel (Doc 09 §4.1): → CANCELLING, then → CANCELLED at once when no attempt is live.
    /// Asking again while CANCELLING or after CANCELLED changes nothing.
    pub async fn request_cancel(
        tx: &mut Tx,
        row: &JobRow,
        actor: &str,
    ) -> Result<JobRow, JobsError> {
        let state = row.job_state()?;
        if matches!(state, JobState::Cancelling | JobState::Cancelled) {
            return Ok(row.clone());
        }
        let asked = JobChange {
            reason: Some(json!({ "message": "Cancel requested.", "actor": actor })),
            failure: None,
        };
        let extra = Extra::keep(row);
        let row = Self::step(tx, row, JobEvent::RequestCancel, Owner::Jobs, asked, extra).await?;
        Self::confirm_cancel_if_idle(tx, &row).await
    }

    async fn confirm_cancel_if_idle(tx: &mut Tx, row: &JobRow) -> Result<JobRow, JobsError> {
        if row.job_state()? != JobState::Cancelling {
            return Ok(row.clone());
        }
        let live: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM job_attempts WHERE job_id = $1 \
               AND state IN ('PENDING','LAUNCHING','RUNNING'))",
        )
        .bind(row.id)
        .fetch_one(&mut **tx)
        .await?;
        if live {
            return Ok(row.clone());
        }
        let done = JobChange {
            reason: row.state_reason.clone(),
            failure: Some(FailureCode::class_only(FailureClass::CancelledByUser)),
        };
        let extra = Extra::keep(row);
        Self::step(tx, row, JobEvent::ConfirmCancel, Owner::Jobs, done, extra).await
    }

    /// Applies `event` to the attempt `attempt` was read as.
    ///
    /// Steps of a live run (acknowledge, start, exit) need the attempt's epoch to be the
    /// job's (older attempts are fenced). A lost lease bumps the job's epoch. When the last
    /// live attempt of a CANCELLING job ends, the job becomes CANCELLED.
    pub async fn apply_attempt(
        tx: &mut Tx,
        attempt: &AttemptRow,
        event: JobAttemptEvent,
        change: AttemptChange,
    ) -> Result<AttemptRow, JobsError> {
        let to = JobAttempt::can_transition(attempt.attempt_state()?, event)?;
        let job = Self::lock(tx, attempt.org_id, attempt.job_id)
            .await?
            .ok_or_else(|| JobsError::not_found("job"))?;
        let fenced = matches!(
            event,
            JobAttemptEvent::Acknowledge
                | JobAttemptEvent::Start
                | JobAttemptEvent::ExitOk
                | JobAttemptEvent::ExitError
        );
        if fenced && attempt.epoch != job.epoch {
            return Err(JobsError::illegal(format!(
                "This attempt belongs to epoch {} but the job is at epoch {}, so its reports are ignored.",
                attempt.epoch, job.epoch
            )));
        }
        let failure = change.failure.map(|f| f.to_string());
        let next = sqlx::query_as::<_, AttemptRow>(concat!(
            "UPDATE job_attempts SET state = $3, state_version = state_version + 1, \
               failure_class = COALESCE($4, failure_class), \
               exit_report = COALESCE($5, exit_report), last_step = COALESCE($6, last_step), \
               started_at = CASE WHEN $3 = 'RUNNING' THEN now() ELSE started_at END, \
               ended_at = CASE WHEN $3 IN ('EXITED_OK','EXITED_ERROR','LOST','KILLED') \
                 THEN now() ELSE ended_at END \
             WHERE id = $1 AND org_id = $2 AND state_version = $7 RETURNING ",
            attempt_cols!()
        ))
        .bind(attempt.id)
        .bind(attempt.org_id)
        .bind(to.as_str())
        .bind(&failure)
        .bind(&change.exit_report)
        .bind(change.last_step)
        .bind(attempt.state_version)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| JobsError::changed("attempt"))?;
        let payload = json!({
            "from": attempt.state,
            "to": next.state,
            "job_id": JobId::from_uuid(attempt.job_id).to_string(),
            "epoch": attempt.epoch,
            "failure_class": failure,
        });
        let e = NewEvent::new(
            event.event_type(),
            "attempt",
            next.id,
            next.state_version,
            payload,
        )
        .map_err(internal)?
        .org(next.org_id);
        OutboxWriter::write(tx, &e).await?;
        let job = if event == JobAttemptEvent::LoseLease {
            sqlx::query_as::<_, JobRow>(concat!(
                "UPDATE jobs SET epoch = epoch + 1, version = version + 1, updated_at = now() \
                 WHERE id = $1 AND org_id = $2 RETURNING ",
                job_cols!()
            ))
            .bind(job.id)
            .bind(job.org_id)
            .fetch_one(&mut **tx)
            .await?
        } else {
            job
        };
        if !is_live(to) {
            Self::confirm_cancel_if_idle(tx, &job).await?;
        }
        Ok(next)
    }

    /// The checked, compare-and-set write behind every job transition. The CAS also checks
    /// the epoch, because a lost lease bumps it without a state change: a row read before
    /// that must not write the old epoch back.
    async fn step(
        tx: &mut Tx,
        row: &JobRow,
        event: JobEvent,
        by: Owner,
        change: JobChange,
        extra: Extra,
    ) -> Result<JobRow, JobsError> {
        let from = row.job_state()?;
        let t = Job::transition(from, event).ok_or_else(|| {
            // `transition` found no row, so this always yields the reason.
            match Job::can_transition(from, event) {
                Err(e) => JobsError::from(e),
                Ok(_) => JobsError::illegal("This step is not allowed."),
            }
        })?;
        if t.owner != by {
            return Err(JobsError::illegal(format!(
                "Only the {} may take the step `{}`.",
                t.owner.as_str().replace('_', " "),
                event.event_type()
            )));
        }
        let failure = change
            .failure
            .map(|f| f.to_string())
            .or_else(|| row.failure_class.clone());
        let next = sqlx::query_as::<_, JobRow>(concat!(
            "UPDATE jobs SET state = $3, state_version = state_version + 1, \
               version = version + 1, updated_at = now(), state_reason = $4, \
               failure_class = $5, retries_used = $6, epoch = $7, current_attempt_id = $8, \
               next_eligible_at = CASE WHEN $3 = 'RETRY_WAIT' \
                 THEN now() + make_interval(secs => $9) END, \
               queued_at = CASE WHEN $3 = 'QUEUED' THEN now() ELSE queued_at END, \
               started_at = CASE WHEN $3 = 'RUNNING' THEN COALESCE(started_at, now()) \
                 ELSE started_at END, \
               finished_at = CASE WHEN $3 IN ('COMPLETED','FAILED','CANCELLED','REJECTED') \
                 THEN now() ELSE finished_at END \
             WHERE id = $1 AND org_id = $2 AND state_version = $10 AND epoch = $11 RETURNING ",
            job_cols!()
        ))
        .bind(row.id)
        .bind(row.org_id)
        .bind(t.to.as_str())
        .bind(&change.reason)
        .bind(&failure)
        .bind(&extra.retries_used)
        .bind(extra.epoch)
        .bind(extra.current_attempt_id)
        .bind(extra.retry_in)
        .bind(row.state_version)
        .bind(row.epoch)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| JobsError::changed("job"))?;
        let payload = json!({
            "from": from.as_str(),
            "to": t.to.as_str(),
            "by": by.as_str(),
            "run_id": next.run_id.map(|r| RunId::from_uuid(r).to_string()),
            "reason": next.state_reason,
            "failure_class": change.failure.map(|f| f.to_string()),
            "epoch": next.epoch,
        });
        let e = NewEvent::new(
            event.event_type(),
            "job",
            next.id,
            next.state_version,
            payload,
        )
        .map_err(internal)?
        .org(next.org_id);
        OutboxWriter::write(tx, &e).await?;
        Self::mirror(tx, &next).await?;
        Ok(next)
    }

    /// Copies the job's state onto its run (`runs.status_summary`).
    async fn mirror(tx: &mut Tx, job: &JobRow) -> Result<(), JobsError> {
        if let Some(run) = job.run_id {
            sqlx::query("UPDATE runs SET status_summary = $1 WHERE id = $2 AND org_id = $3")
                .bind(&job.state)
                .bind(run)
                .bind(job.org_id)
                .execute(&mut **tx)
                .await?;
        }
        Ok(())
    }
}

/// Columns a transition may change besides state and reason.
struct Extra {
    retries_used: Value,
    epoch: i64,
    current_attempt_id: Option<Uuid>,
    retry_in: Option<f64>,
}

impl Extra {
    fn keep(row: &JobRow) -> Self {
        Self {
            retries_used: row.retries_used.clone(),
            epoch: row.epoch,
            current_attempt_id: row.current_attempt_id,
            retry_in: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_rules_default_and_parse() {
        assert_eq!(RetryRules::from_json(&json!({})), RetryRules::default());
        let r = RetryRules::from_json(&json!({"auto_tune": true, "budgets": {"NODE_LOST": 1}}));
        assert!(r.auto_tune);
        assert_eq!(r.budgets.get("NODE_LOST"), Some(&1));
        // Unknown keys are not silently half-read.
        assert_eq!(
            RetryRules::from_json(&json!({"bogus": 1})),
            RetryRules::default()
        );
    }

    /// The submit and cancel paths only take steps P06 allows the jobs module to take.
    #[test]
    fn jobs_module_owns_the_steps_it_takes() {
        use JobEvent::*;
        use JobState::*;
        let steps = [
            (Created, Submit),
            (Validating, Accept),
            (Validating, Reject),
            (Queued, RequestCancel),
            (Running, RequestCancel),
            (Cancelling, ConfirmCancel),
            (Running, Fail),
        ];
        for (from, event) in steps {
            let t = Job::transition(from, event).unwrap_or_else(|| panic!("{from} {event:?}"));
            assert_eq!(t.owner, Owner::Jobs, "{from} {event:?}");
        }
        let launch = Job::transition(Allocating, Launch).unwrap();
        assert_eq!(launch.owner, Owner::Scheduler);
        let complete = Job::transition(Finalizing, Complete).unwrap();
        assert_eq!(complete.owner, Owner::ArtifactsVerifier);
        // Created and Validating can't be cancelled; terminal states absorb everything.
        assert!(Job::transition(Created, RequestCancel).is_none());
        for terminal in Job::TERMINAL {
            for e in Job::EVENTS {
                assert!(Job::transition(*terminal, *e).is_none());
            }
        }
        for terminal in JobAttempt::TERMINAL {
            assert!(!is_live(*terminal));
        }
    }
}

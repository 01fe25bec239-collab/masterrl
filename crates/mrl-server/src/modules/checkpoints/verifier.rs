//! The FINALIZING checks (Doc 11 §11, P8) and the pass that applies them.

use super::service::{
    ckpt_cols, lock_run, metrics, system_audit, CheckpointRow, CheckpointService,
};
use crate::db::Db;
use crate::modules::jobs::chains::{CompletionVerifier, Verification};
use crate::modules::jobs::transitions::job_cols;
use crate::modules::jobs::{Chains, JobRow, JobTransitions, JobsError, Rejection};
use crate::seams::SeamError;
use async_trait::async_trait;
use mrl_domain::failure::{FailureClass, FailureCode};
use mrl_domain::ids::CheckpointId;
use serde_json::Value;
use sqlx::PgConnection;
use std::collections::BTreeMap;
use uuid::Uuid;

/// A TRAIN job's results are verified when:
/// 1. the attempt committed a final checkpoint, or exited with its committed resume checkpoint at the step budget;
/// 2. every metric the run config names in `metrics.required` was reported with it;
/// 3. its step equals the planned steps (`common.max_steps`, when set), unless the engine
///    recorded an early stop (`early_stop ≥ 1` in the checkpoint's metrics).
///
/// Anything else fails the job with VERIFICATION_FAILED. Other job kinds: abstains.
pub struct FinalCheckpointVerifier {
    db: Db,
}

impl FinalCheckpointVerifier {
    /// The verifier over this database.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

fn failed(message: String) -> Verification {
    Verification::Failed(Rejection::failure(
        FailureCode::class_only(FailureClass::VerificationFailed),
        message,
    ))
}

/// The verdict for a run's final checkpoint (`None`: there is none) and its resolved config.
pub fn judge(last: Option<&CheckpointRow>, resolved: &Value) -> Verification {
    let reported = last.map(metrics).unwrap_or_default();
    judge_report(last, resolved, &reported, false)
}

fn judge_report(
    last: Option<&CheckpointRow>,
    resolved: &Value,
    reported: &BTreeMap<String, f64>,
    resumed: bool,
) -> Verification {
    let Some(ckpt) = last.filter(|c| c.state == "COMMITTED" && (c.is_final || resumed)) else {
        return failed(
            "The run finished without a committed final checkpoint, so its result can't be confirmed."
                .into(),
        );
    };
    let missing: Vec<&str> = resolved
        .pointer("/metrics/required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|name| !reported.contains_key(*name))
        .collect();
    if !missing.is_empty() {
        return failed(format!(
            "The run finished without these required metrics: {}.",
            missing.join(", ")
        ));
    }
    let planned = planned_steps(resolved);
    let early = reported.get("early_stop").is_some_and(|v| *v >= 1.0);
    if let Some(planned) = planned {
        if ckpt.global_step != planned && !early && !resumed {
            return failed(format!(
                "The run stopped at step {} of {planned} without recording an early stop.",
                ckpt.global_step
            ));
        }
    }
    Verification::Verified
}

fn planned_steps(resolved: &Value) -> Option<i64> {
    resolved
        .pointer("/common/max_steps")
        .and_then(Value::as_i64)
}

#[derive(PartialEq, sqlx::FromRow)]
struct ExitEvidence {
    last_step: Option<i64>,
    exit_report: Option<Value>,
    resume_from_checkpoint_id: Option<Uuid>,
}

#[derive(PartialEq)]
struct FinalEvidence {
    checkpoint: Option<CheckpointRow>,
    exit: Option<ExitEvidence>,
    resolved: Value,
    resumed: bool,
}

async fn final_evidence(
    conn: &mut PgConnection,
    run: Uuid,
    job: &JobRow,
) -> Result<FinalEvidence, sqlx::Error> {
    let exit = sqlx::query_as::<_, ExitEvidence>(
        "SELECT last_step, exit_report, resume_from_checkpoint_id FROM job_attempts
         WHERE id = $1 AND job_id = $2 AND epoch = $3 AND state = 'EXITED_OK'",
    )
    .bind(job.current_attempt_id)
    .bind(job.id)
    .bind(job.epoch)
    .fetch_optional(&mut *conn)
    .await?;
    let resolved = sqlx::query_scalar("SELECT resolved FROM run_configs WHERE run_id = $1")
        .bind(run)
        .fetch_optional(&mut *conn)
        .await?
        .unwrap_or(Value::Null);
    let mut checkpoint = sqlx::query_as::<_, CheckpointRow>(concat!(
        "SELECT ",
        ckpt_cols!(),
        " FROM checkpoints WHERE id = (SELECT final_checkpoint_id FROM runs WHERE id = $1)
           AND attempt_id = $2 AND attempt_epoch = $3"
    ))
    .bind(run)
    .bind(job.current_attempt_id)
    .bind(job.epoch)
    .fetch_optional(&mut *conn)
    .await?;
    let mut resumed = false;
    if checkpoint.is_none() {
        if let (Some(budget), Some(exit)) =
            (planned_steps(&resolved).filter(|step| *step >= 0), &exit)
        {
            let reported = exit
                .exit_report
                .as_ref()
                .and_then(|report| report["last_committed_checkpoint_id"].as_str())
                .and_then(|id| id.parse::<CheckpointId>().ok())
                .map(|id| id.as_uuid());
            if let Some(id) = reported.filter(|id| Some(*id) == exit.resume_from_checkpoint_id) {
                checkpoint = sqlx::query_as::<_, CheckpointRow>(concat!(
                    "SELECT ",
                    ckpt_cols!(),
                    " FROM checkpoints WHERE id = $1 AND run_id = $2 AND job_id = $3
                       AND state = 'COMMITTED' AND global_step >= $4"
                ))
                .bind(id)
                .bind(run)
                .bind(job.id)
                .bind(budget)
                .fetch_optional(&mut *conn)
                .await?;
                resumed = checkpoint.is_some();
            }
        }
    }
    Ok(FinalEvidence {
        checkpoint,
        exit,
        resolved,
        resumed,
    })
}

const FINAL_SNAPSHOT: &str =
    "SELECT r.final_checkpoint_id, c.state_version, c.manifest_sha256 FROM runs r
     LEFT JOIN checkpoints c ON c.id = r.final_checkpoint_id WHERE r.id = $1";

const OPEN_FINAL: &str =
    "SELECT EXISTS (SELECT 1 FROM checkpoints WHERE attempt_id = $1 AND attempt_epoch = $2
     AND is_final AND state IN ('WRITING','UPLOADED','VERIFYING'))";

#[async_trait]
impl CompletionVerifier for FinalCheckpointVerifier {
    fn name(&self) -> &'static str {
        "final-checkpoint"
    }

    async fn verify(&self, job: &JobRow) -> Result<Verification, SeamError> {
        let Some(run) = job.run_id.filter(|_| job.kind == "TRAIN") else {
            return Ok(Verification::Abstain);
        };
        let db = |e: sqlx::Error| SeamError::from(crate::db::DbError::from(e));
        let Some(attempt) = job.current_attempt_id else {
            return Ok(failed("The run has no finished attempt to verify.".into()));
        };
        if sqlx::query_scalar::<_, bool>(OPEN_FINAL)
            .bind(attempt)
            .bind(job.epoch)
            .fetch_one(self.db.pool())
            .await
            .map_err(db)?
        {
            // RetryLater would transition the job out of FINALIZING in P17.
            return Err(SeamError::new(
                "checkpoints",
                "The final checkpoint is still being saved.",
            ));
        }
        let mut conn = self.db.pool().acquire().await.map_err(db)?;
        let evidence = final_evidence(&mut conn, run, job).await.map_err(db)?;
        let Some(checkpoint) = evidence.checkpoint.as_ref() else {
            return Ok(judge(None, &evidence.resolved));
        };
        let Some(exit) = evidence.exit else {
            return Ok(failed("The run has no finished attempt to verify.".into()));
        };
        if !evidence.resumed {
            let Some(step) = exit.last_step else {
                return Ok(failed(
                    "The finished attempt did not report its final step.".into(),
                ));
            };
            if step != checkpoint.global_step {
                return Ok(failed(format!(
                    "The attempt finished at step {step}, but its final checkpoint is at step {}.",
                    checkpoint.global_step,
                )));
            }
        }
        // A budget-exhausted resume emits no training metrics of its own.
        let report = exit
            .exit_report
            .as_ref()
            .and_then(|r| r["final_metrics"].as_object());
        let mut reported = if evidence.resumed || report.is_none() {
            metrics(checkpoint)
        } else {
            BTreeMap::new()
        };
        if let Some(values) = report {
            reported.extend(values.iter().filter_map(|(name, value)| {
                let value = value.as_f64()?;
                value.is_finite().then(|| (name.clone(), value))
            }));
        }
        Ok(judge_report(
            Some(checkpoint),
            &evidence.resolved,
            &reported,
            evidence.resumed,
        ))
    }
}

/// Decides every FINALIZING job (oldest first, up to 50 per pass): the chain runs with no
/// transaction open, then `verify_completion` applies the verdict under the job lock (a job
/// that changed meanwhile is left for the next pass).
pub async fn finalize(service: &CheckpointService, chains: &Chains) -> Result<(), JobsError> {
    let mut after = Uuid::nil();
    loop {
        let jobs = sqlx::query_as::<_, JobRow>(concat!(
            "SELECT ",
            job_cols!(),
            " FROM jobs WHERE state = 'FINALIZING' AND id > $1 ORDER BY id LIMIT 50"
        ))
        .bind(after)
        .fetch_all(service.db().pool())
        .await?;
        let Some(last) = jobs.last() else {
            break;
        };
        after = last.id;
        for job in jobs {
            if let Err(e) = finalize_one(service, chains, &job).await {
                tracing::warn!(code = e.code(), job = %job.id, "job completion was not decided: {e}");
            }
        }
    }

    Ok(())
}

async fn finalize_one(
    service: &CheckpointService,
    chains: &Chains,
    job: &JobRow,
) -> Result<(), JobsError> {
    let pool = service.db().pool();
    type Snapshot = (Option<Uuid>, Option<i64>, Option<Vec<u8>>);
    let snapshot: Option<Snapshot> = match job.run_id {
        Some(run) => {
            sqlx::query_as(FINAL_SNAPSHOT)
                .bind(run)
                .fetch_optional(pool)
                .await?
        }
        None => None,
    };
    if job.kind == "TRAIN"
        && sqlx::query_scalar::<_, bool>(OPEN_FINAL)
            .bind(job.current_attempt_id)
            .bind(job.epoch)
            .fetch_one(pool)
            .await?
    {
        return Ok(());
    }
    let evidence = match job.run_id.filter(|_| job.kind == "TRAIN") {
        Some(run) => {
            let mut conn = pool.acquire().await?;
            Some(final_evidence(&mut conn, run, job).await?)
        }
        None => None,
    };
    let verdict = chains.verify(job).await?;
    let mut tx = service.db().begin().await?;
    let Some(locked) = JobTransitions::lock(&mut tx, job.org_id, job.id).await? else {
        return Ok(());
    };
    if let Some(run) = job.run_id {
        lock_run(&mut tx, run).await?;
        let current = sqlx::query_as(FINAL_SNAPSHOT)
            .bind(run)
            .fetch_optional(&mut *tx)
            .await?;
        let pending: bool = sqlx::query_scalar(OPEN_FINAL)
            .bind(job.current_attempt_id)
            .bind(job.epoch)
            .fetch_one(&mut *tx)
            .await?;
        if current != snapshot || (job.kind == "TRAIN" && pending) {
            return Ok(());
        }
        if let Some(evidence) = evidence.as_ref() {
            if final_evidence(&mut tx, run, &locked).await? != *evidence {
                return Ok(());
            }
        }
    }
    match JobTransitions::verify_completion(&mut tx, &locked, verdict).await {
        Ok(done) => {
            if done.state == "COMPLETED" {
                if let Some(checkpoint) = evidence.filter(|e| e.resumed).and_then(|e| e.checkpoint)
                {
                    sqlx::query(
                        "UPDATE checkpoints SET is_final = false
                         WHERE run_id = $1 AND id <> $2 AND state = 'COMMITTED' AND is_final",
                    )
                    .bind(checkpoint.run_id)
                    .bind(checkpoint.id)
                    .execute(&mut *tx)
                    .await?;
                    sqlx::query("UPDATE checkpoints SET is_final = true WHERE id = $1")
                        .bind(checkpoint.id)
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query("UPDATE runs SET final_checkpoint_id = $2 WHERE id = $1")
                        .bind(checkpoint.run_id)
                        .bind(checkpoint.id)
                        .execute(&mut *tx)
                        .await?;
                    system_audit(
                        &mut tx,
                        service.db(),
                        &checkpoint,
                        "checkpoints.finalize_resume",
                    )
                    .await?;
                }
            }
            tx.commit().await.map_err(crate::db::DbError::from)?;
        }
        Err(e) if e.code() == "MRL-CONFLICT-170" || e.code() == "MRL-STATE-170" => {}
        Err(e) => return Err(e),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    fn ckpt(step: i64, metrics: Value) -> CheckpointRow {
        let now = time::OffsetDateTime::UNIX_EPOCH;
        CheckpointRow {
            id: Uuid::nil(),
            org_id: Uuid::nil(),
            storage_org: "local".into(),
            run_id: Uuid::nil(),
            job_id: Uuid::nil(),
            attempt_id: Uuid::nil(),
            attempt_epoch: 1,
            global_step: step,
            policy_version: None,
            kind: "full_resume".into(),
            layout: "fake-v1".into(),
            is_final: true,
            state: "COMMITTED".into(),
            state_version: 3,
            verification_level: Some("manifest_head".into()),
            manifest_key: None,
            manifest_sha256: None,
            total_bytes: None,
            file_count: None,
            metrics_at_save: Some(metrics),
            corrupt_reason: None,
            commit_requested_at: None,
            upload_command_id: None,
            tickets_expire_at: None,
            node_id: None,
            created_at: now,
            committed_at: None,
            verified_at: None,
        }
    }

    fn is_failed(v: &Verification, needle: &str) -> bool {
        matches!(v, Verification::Failed(r) if r.code == "VERIFICATION_FAILED" && r.message.contains(needle))
    }

    #[test]
    fn final_checkpoint_metrics_and_steps_decide() {
        let cfg = json!({"metrics": {"required": ["train/loss"]}, "common": {"max_steps": 10}});
        assert!(is_failed(&judge(None, &cfg), "final checkpoint"));
        let mut not_final = ckpt(10, json!({"train/loss": 1.0}));
        not_final.is_final = false;
        assert!(is_failed(
            &judge(Some(&not_final), &cfg),
            "final checkpoint"
        ));
        let mut corrupt = ckpt(10, json!({"train/loss": 1.0}));
        corrupt.state = "CORRUPT".into();
        assert!(is_failed(&judge(Some(&corrupt), &cfg), "final checkpoint"));
        assert!(is_failed(
            &judge(Some(&ckpt(10, json!({}))), &cfg),
            "train/loss"
        ));
        assert!(is_failed(
            &judge(Some(&ckpt(7, json!({"train/loss": 1.0}))), &cfg),
            "step 7 of 10"
        ));
        let early = ckpt(7, json!({"train/loss": 1.0, "early_stop": 1.0}));
        assert_eq!(judge(Some(&early), &cfg), Verification::Verified);
        assert_eq!(
            judge(Some(&ckpt(10, json!({"train/loss": 0.5}))), &cfg),
            Verification::Verified
        );
        assert_eq!(
            judge(Some(&ckpt(3, json!({}))), &Value::Null),
            Verification::Verified
        );
    }
}

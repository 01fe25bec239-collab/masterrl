//! The FINALIZING checks (Doc 11 §11, P8) and the pass that applies them.

use super::service::{ckpt_cols, metrics, CheckpointRow, CheckpointService};
use crate::db::Db;
use crate::modules::jobs::chains::{CompletionVerifier, Verification};
use crate::modules::jobs::transitions::job_cols;
use crate::modules::jobs::{Chains, JobRow, JobTransitions, JobsError, Rejection};
use crate::seams::SeamError;
use async_trait::async_trait;
use mrl_domain::failure::{FailureClass, FailureCode};
use serde_json::Value;

/// A TRAIN job's results are verified when:
/// 1. the run's final checkpoint is COMMITTED with `is_final`;
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
    let Some(ckpt) = last.filter(|c| c.state == "COMMITTED" && c.is_final) else {
        return failed(
            "The run finished without a committed final checkpoint, so its result can't be confirmed."
                .into(),
        );
    };
    let reported = metrics(ckpt);
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
    let planned = resolved
        .pointer("/common/max_steps")
        .and_then(Value::as_i64);
    let early = reported.get("early_stop").is_some_and(|v| *v >= 1.0);
    if let Some(planned) = planned {
        if ckpt.global_step != planned && !early {
            return failed(format!(
                "The run stopped at step {} of {planned} without recording an early stop.",
                ckpt.global_step
            ));
        }
    }
    Verification::Verified
}

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
        let last = sqlx::query_as::<_, CheckpointRow>(concat!(
            "SELECT ",
            ckpt_cols!(),
            " FROM checkpoints WHERE id = (SELECT final_checkpoint_id FROM runs WHERE id = $1)"
        ))
        .bind(run)
        .fetch_optional(self.db.pool())
        .await
        .map_err(db)?;
        let resolved: Value =
            sqlx::query_scalar("SELECT resolved FROM run_configs WHERE run_id = $1")
                .bind(run)
                .fetch_optional(self.db.pool())
                .await
                .map_err(db)?
                .unwrap_or(Value::Null);
        Ok(judge(last.as_ref(), &resolved))
    }
}

/// Decides every FINALIZING job (oldest first, up to 50 per pass): the chain runs with no
/// transaction open, then `verify_completion` applies the verdict under the job lock (a job
/// that changed meanwhile is left for the next pass).
pub async fn finalize(service: &CheckpointService, chains: &Chains) -> Result<(), JobsError> {
    let jobs = sqlx::query_as::<_, JobRow>(concat!(
        "SELECT ",
        job_cols!(),
        " FROM jobs WHERE state = 'FINALIZING' ORDER BY updated_at LIMIT 50"
    ))
    .fetch_all(service.db().pool())
    .await?;
    for job in jobs {
        let verdict = chains.verify(&job).await?;
        let mut tx = service.db().begin().await?;
        let Some(locked) = JobTransitions::lock(&mut tx, job.org_id, job.id).await? else {
            continue;
        };
        match JobTransitions::verify_completion(&mut tx, &locked, verdict).await {
            Ok(_) => tx.commit().await.map_err(crate::db::DbError::from)?,
            Err(e) if e.code() == "MRL-CONFLICT-170" || e.code() == "MRL-STATE-170" => {}
            Err(e) => return Err(e),
        }
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

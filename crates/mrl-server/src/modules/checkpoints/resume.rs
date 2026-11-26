//! Launch contributor: an attempt's pinned resume checkpoint, staged for the worker as the
//! `resume_from` folder input (one input file per checkpoint file, at `resume_from/<path>`;
//! the agent hands the worker the folder).

use crate::db::Db;
use crate::modules::artifacts::InputFile;
use crate::modules::jobs::{AttemptRow, JobRow};
use crate::modules::scheduler::pipeline::{LaunchContributor, LaunchError, LaunchPlan, Placement};
use mrl_domain::failure::FailureClass;
use sqlx::Row;

/// Name of the input the worker reads as `JobContext.resume_from`.
pub const RESUME_INPUT: &str = "resume_from";

/// See the module docs.
pub struct ResumeInputs {
    db: Db,
}

impl ResumeInputs {
    /// Reads checkpoint files from `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl LaunchContributor for ResumeInputs {
    fn name(&self) -> &'static str {
        "checkpoint_resume"
    }

    async fn contribute(
        &self,
        _job: &JobRow,
        attempt: &AttemptRow,
        _placement: &Placement,
        plan: &mut LaunchPlan,
    ) -> Result<(), LaunchError> {
        let Some(checkpoint) = attempt.resume_from_checkpoint_id else {
            return Ok(());
        };
        let rows = sqlx::query(
            "SELECT f.path, f.object_key, f.sha256, f.size_bytes
               FROM checkpoint_files f JOIN checkpoints c ON c.id = f.checkpoint_id
              WHERE c.id = $1 AND c.org_id = $2 AND c.state = 'COMMITTED'
              ORDER BY f.path",
        )
        .bind(checkpoint)
        .bind(attempt.org_id)
        .fetch_all(self.db.pool())
        .await
        .map_err(|e| LaunchError::Transient(format!("the database did not answer: {e}")))?;
        if rows.is_empty() {
            // Pinned COMMITTED at the retry; found corrupt or removed since. The next retry
            // picks the latest checkpoint still committed.
            return Err(LaunchError::Fail(
                FailureClass::InfraTransient,
                "The checkpoint this attempt resumes from is no longer available.".into(),
            ));
        }
        for row in rows {
            let get = |e: sqlx::Error| LaunchError::Transient(format!("checkpoint file: {e}"));
            let path: String = row.try_get("path").map_err(get)?;
            let sha: Vec<u8> = row.try_get("sha256").map_err(get)?;
            let size: i64 = row.try_get("size_bytes").map_err(get)?;
            plan.inputs.push(InputFile {
                name: RESUME_INPUT.into(),
                key: row.try_get("object_key").map_err(get)?,
                sha256: sha.iter().map(|b| format!("{b:02x}")).collect(),
                size: u64::try_from(size).unwrap_or_default(),
                local_path: format!("{RESUME_INPUT}/{path}"),
            });
        }
        Ok(())
    }
}

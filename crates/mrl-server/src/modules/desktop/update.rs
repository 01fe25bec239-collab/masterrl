//! `POST /desktop/update/prepare`: before the app installs an update and restarts, every
//! running job saves an emergency checkpoint (P23 `CheckpointAttempt`) and the bundled
//! database is backed up (P11).
//!
//! Agents and training keep running through the update (they are user services), so a refused
//! or slow checkpoint does not block it: the answer lists what happened and `ready` is false
//! only when the backup failed. The app asks the person before going on in that case.

use super::{api, backups, Desktop};
use crate::app::AppState;
use crate::modules::checkpoints::service::{CheckpointService, EmergencyOutcome, Secrets};
use crate::modules::jobs::JobsError;
use crate::seams::RequestContext;
use async_trait::async_trait;
use axum::extract::State;
use axum::{Extension, Json};
use mrl_domain::ids::{CheckpointId, JobId};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Asks a running job for a checkpoint now.
#[async_trait]
pub trait EmergencyCheckpoints: Send + Sync {
    /// Waits until a checkpoint of `job` commits, the agent refuses, or `deadline` passes.
    async fn checkpoint(
        &self,
        job: Uuid,
        reason: &str,
        deadline: Duration,
    ) -> Result<EmergencyOutcome, String>;
}

/// [`EmergencyCheckpoints`] through P23's checkpoint service.
pub struct ServiceCheckpoints {
    service: CheckpointService,
}

impl ServiceCheckpoints {
    /// A service with its own (stateless) handles on storage and command sending, like the
    /// checkpoints module's.
    pub fn new(db: crate::db::Db, config: &crate::Config) -> Self {
        let store = crate::modules::artifacts::ArtifactsModule::new(db.clone(), config).store();
        let gateway = crate::modules::nodes::NodesModule::from_config(db.clone(), config).gateway();
        Self {
            service: CheckpointService::new(
                db,
                store,
                Secrets::keychain(config),
                &gateway,
                config.checkpoints.clone(),
            ),
        }
    }
}

#[async_trait]
impl EmergencyCheckpoints for ServiceCheckpoints {
    async fn checkpoint(
        &self,
        job: Uuid,
        reason: &str,
        deadline: Duration,
    ) -> Result<EmergencyOutcome, String> {
        self.service
            .emergency_checkpoint(job, reason, deadline)
            .await
            .map_err(|e| e.to_string())
    }
}

/// How one job's emergency checkpoint went.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct DesktopUpdateCheckpoint {
    /// `job_…`.
    pub job_id: String,
    /// The run's name, or the job kind.
    pub name: String,
    /// `committed`, `refused`, `timed_out` or `failed`.
    pub outcome: String,
    /// `ckpt_…` when committed.
    pub checkpoint_id: Option<String>,
    /// Why, in plain words, when not committed.
    pub detail: Option<String>,
}

/// Whether the app may install the update now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct DesktopUpdateReadiness {
    /// The backup is made (or there is no bundled database): go ahead.
    pub ready: bool,
    /// One entry per running job.
    pub checkpoints: Vec<DesktopUpdateCheckpoint>,
    /// The backup's file name.
    pub backup: Option<String>,
    /// Why the backup failed.
    pub backup_error: Option<String>,
}

fn outcome(
    job: Uuid,
    name: String,
    r: Result<EmergencyOutcome, String>,
) -> DesktopUpdateCheckpoint {
    let (outcome, checkpoint_id, detail) = match r {
        Ok(EmergencyOutcome::Committed(c)) => (
            "committed",
            Some(CheckpointId::from_uuid(c).to_string()),
            None,
        ),
        Ok(EmergencyOutcome::Refused(why)) => ("refused", None, Some(why)),
        Ok(EmergencyOutcome::TimedOut) => (
            "timed_out",
            None,
            Some("No checkpoint was saved in time; training goes on from its last one.".into()),
        ),
        Err(why) => ("failed", None, Some(why)),
    };
    DesktopUpdateCheckpoint {
        job_id: JobId::from_uuid(job).to_string(),
        name,
        outcome: outcome.to_owned(),
        checkpoint_id,
        detail,
    }
}

#[utoipa::path(post, path = "/desktop/update/prepare", tag = "desktop", operation_id = "desktop_prepare_update",
    responses((status = 200, body = DesktopUpdateReadiness),
        (status = 409, description = "Busy (MRL-CONFLICT-300) or a team core (MRL-STATE-300)")))]
/// Saves emergency checkpoints of running jobs and backs the database up, before an update.
pub async fn prepare_update(
    State(state): State<AppState>,
    Extension(desktop): Extension<Arc<Desktop>>,
    ctx: RequestContext,
) -> Result<Json<DesktopUpdateReadiness>, JobsError> {
    api::guard(&state, &ctx).await?;
    let _one = backups::exclusive(&desktop)?;
    let jobs: Vec<(Uuid, Option<String>, String)> = sqlx::query_as(
        "SELECT j.id, r.name, j.kind FROM jobs j LEFT JOIN runs r ON r.id = j.run_id
          WHERE j.state = 'RUNNING' ORDER BY j.id",
    )
    .fetch_all(desktop.db.pool())
    .await?;
    let deadline = Duration::from_secs(desktop.config.update_checkpoint_seconds);
    let checkpoints = futures_util::future::join_all(jobs.into_iter().map(|(job, run, kind)| {
        let desktop = desktop.clone();
        async move {
            let r = desktop
                .checkpoints
                .checkpoint(job, "app update", deadline)
                .await;
            let name = run.unwrap_or_else(|| format!("{} job", kind.to_lowercase()));
            outcome(job, name, r)
        }
    }))
    .await;
    let (backup, backup_error) = match desktop.db.backups() {
        None => (None, None),
        Some(b) => match b.create("before-update").await {
            Ok(file) => (Some(backups::describe(&file).name), None),
            Err(e) => (None, Some(e.to_string())),
        },
    };
    let ready = backup_error.is_none();
    let mut tx = desktop.db.begin().await?;
    api::audit(
        &mut tx,
        &state,
        &ctx,
        "desktop.prepare_update",
        backup.as_deref().unwrap_or("no-backup"),
    )
    .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(Json(DesktopUpdateReadiness {
        ready,
        checkpoints,
        backup,
        backup_error,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_outcome_reads_plainly() {
        let j = Uuid::now_v7();
        let c = outcome(
            j,
            "a".into(),
            Ok(EmergencyOutcome::Committed(Uuid::now_v7())),
        );
        assert_eq!(c.outcome, "committed");
        assert!(c.checkpoint_id.is_some_and(|id| id.starts_with("ckpt_")));
        let t = outcome(j, "a".into(), Ok(EmergencyOutcome::TimedOut));
        assert_eq!(
            (t.outcome.as_str(), t.detail.is_some()),
            ("timed_out", true)
        );
        let r = outcome(j, "a".into(), Ok(EmergencyOutcome::Refused("busy".into())));
        assert_eq!(r.detail.as_deref(), Some("busy"));
        let f = outcome(j, "a".into(), Err("db".into()));
        assert_eq!(f.outcome, "failed");
    }
}

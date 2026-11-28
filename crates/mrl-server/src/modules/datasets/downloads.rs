//! Keep direct downloads of Dataset Studio job outputs under dataset export policy.

use super::api::{dataset_resource, EXPORT_REFUSED_DETAIL};
use crate::app::AppState;
use crate::db::Db;
use crate::modules::artifacts::{not_found, Artifact, DownloadGuard, Error};
use crate::seams::RequestContext;
use axum::http::StatusCode;
use mrl_domain::ids::{JobId, ProjectId};
use uuid::Uuid;

/// Checks Dataset Studio output artifacts, including outputs still being verified.
pub struct DatasetDownloadGuard(Db);

impl DatasetDownloadGuard {
    /// Reads current dataset permissions and sensitivity from `db`.
    pub fn new(db: Db) -> Self {
        Self(db)
    }
}

#[async_trait::async_trait]
impl DownloadGuard for DatasetDownloadGuard {
    async fn check(
        &self,
        state: &AppState,
        ctx: &RequestContext,
        _org: &str,
        artifact: &Artifact,
    ) -> Result<(), Error> {
        // INT-6 can commit the artifact before it updates its receipt. Its job-attempt
        // name and system actor still identify who owns that output.
        let job = (artifact.created_by == "system:builtin")
            .then(|| artifact.name.split_once('-'))
            .flatten()
            .and_then(|(job, _)| job.parse::<JobId>().ok())
            .map(|id| id.as_uuid());
        let datasets: Vec<(Uuid, Uuid, Uuid, bool, bool)> = sqlx::query_as(
            "WITH versions AS (
                 SELECT id FROM dataset_versions WHERE artifact_id=$1
                 UNION
                 SELECT version_id FROM dataset_work WHERE job_id=$2
                 UNION
                 SELECT w.version_id FROM builtin_jobs b JOIN dataset_work w ON w.job_id=b.job_id
                 WHERE b.artifact_id=$1
             )
             SELECT DISTINCT d.id, d.org_id, d.project_id, dataset_version_is_restricted(v.id),
                    d.deleted_at IS NOT NULL
             FROM versions ids JOIN dataset_versions v ON v.id=ids.id
             JOIN datasets d ON d.id=v.dataset_id AND d.org_id=v.org_id",
        )
        .bind(artifact.uuid())
        .bind(job)
        .fetch_all(self.0.pool())
        .await?;
        for (id, org, project, restricted, deleted) in datasets {
            if deleted {
                return Err(not_found("artifact"));
            }
            let ctx = ctx
                .clone()
                .with_project(ProjectId::from_uuid(project).to_string());
            state
                .authorize(&ctx, "datasets.export", &dataset_resource(org, project, id))
                .await?;
            if restricted {
                return Err(Error::coded(
                    "MRL-PERM-270",
                    StatusCode::FORBIDDEN,
                    EXPORT_REFUSED_DETAIL,
                ));
            }
        }
        Ok(())
    }
}

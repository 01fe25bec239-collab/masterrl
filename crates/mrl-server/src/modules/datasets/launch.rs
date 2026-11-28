//! Launch contributor: each dataset version a run or settings check names is staged as the folder input
//! `dataset_<n>` (n = its place in `dataset_version_ids`): one input file per
//! shard at `dataset_<n>/shards/…` plus `dataset_<n>/dataset.json`. The agent checks every
//! file's sha256; engines read the folder with `mrl_runtime.data.adapter.DatasetAdapter`.

use super::store::{hex, SUMMARY};
use crate::db::Db;
use crate::modules::artifacts::InputFile;
use crate::modules::jobs::{AttemptRow, JobRow};
use crate::modules::scheduler::pipeline::{LaunchContributor, LaunchError, LaunchPlan, Placement};
use mrl_domain::failure::FailureClass;
use mrl_domain::ids::DatasetVersionId;
use uuid::Uuid;

/// The staged input name of the run's `index`-th dataset version.
pub fn input_name(index: usize) -> String {
    format!("dataset_{index}")
}

/// See the module docs.
pub struct DatasetInputs {
    db: Db,
}

impl DatasetInputs {
    /// Reads versions from `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[derive(sqlx::FromRow)]
struct Staged {
    path: String,
    object_key: String,
    sha256: Vec<u8>,
    size_bytes: i64,
}

#[derive(serde::Deserialize)]
struct ProbeInputs {
    dataset_version_ids: Vec<DatasetVersionId>,
}

#[async_trait::async_trait]
impl LaunchContributor for DatasetInputs {
    fn name(&self) -> &'static str {
        "dataset_inputs"
    }

    async fn contribute(
        &self,
        job: &JobRow,
        _attempt: &AttemptRow,
        _placement: &Placement,
        plan: &mut LaunchPlan,
    ) -> Result<(), LaunchError> {
        if matches!(job.kind.as_str(), "IMPORT" | "DATA_PREP") {
            let allowed: Option<bool> = sqlx::query_scalar(
                "SELECT d.sensitivity <> 'restricted-pii' AND d.deleted_at IS NULL
                 FROM dataset_work w JOIN dataset_versions v ON v.id=w.version_id
                 JOIN datasets d ON d.id=v.dataset_id WHERE w.job_id=$1 AND w.purpose IN ('jsonl','hub_push')",
            ).bind(job.id).fetch_optional(self.db.pool()).await.map_err(|e| LaunchError::Transient(e.to_string()))?;
            if allowed == Some(false) {
                return Err(LaunchError::Fail(
                    FailureClass::UserConfig,
                    "The dataset is deleted or marked restricted personal data.".into(),
                ));
            }
        }
        let transient =
            |e: sqlx::Error| LaunchError::Transient(format!("the database did not answer: {e}"));
        let ids = if let Some(run) = job.run_id {
            sqlx::query_scalar::<_, Vec<Uuid>>(
                "SELECT dataset_version_ids FROM run_configs WHERE run_id = $1 AND org_id = $2",
            )
            .bind(run)
            .bind(job.org_id)
            .fetch_optional(self.db.pool())
            .await
            .map_err(transient)?
            .unwrap_or_default()
        } else if job.kind == "TRAIN"
            && job
                .subject_ref
                .as_ref()
                .is_some_and(|subject| subject["kind"] == "run_probe")
        {
            let probe: ProbeInputs =
                serde_json::from_str(&plan.launch.job_spec_json).map_err(|_| {
                    LaunchError::Fail(
                        FailureClass::HarnessError,
                        "The settings check is missing a valid list of dataset versions.".into(),
                    )
                })?;
            probe
                .dataset_version_ids
                .iter()
                .map(DatasetVersionId::as_uuid)
                .collect()
        } else {
            return Ok(());
        };
        for (n, id) in ids.into_iter().enumerate() {
            let ready: Option<bool> = sqlx::query_scalar(
                "SELECT v.state = 'READY' FROM dataset_versions v JOIN datasets d ON d.id = v.dataset_id \
                  WHERE v.id = $1 AND v.org_id = $2 AND d.project_id = $3 AND d.deleted_at IS NULL",
            )
            .bind(id)
            .bind(job.org_id)
            .bind(job.project_id)
            .fetch_optional(self.db.pool())
            .await
            .map_err(transient)?;
            if ready != Some(true) {
                return Err(LaunchError::Fail(
                    FailureClass::UserConfig,
                    format!(
                        "The dataset version {} is no longer ready in this project, so the job can't use it.",
                        DatasetVersionId::from_uuid(id)
                    ),
                ));
            }
            let files: Vec<Staged> = sqlx::query_as(
                "SELECT path, object_key, sha256, size_bytes FROM dataset_files \
                  WHERE dataset_version_id = $1 AND (role = 'shard' OR path = $2) ORDER BY path",
            )
            .bind(id)
            .bind(SUMMARY)
            .fetch_all(self.db.pool())
            .await
            .map_err(transient)?;
            let name = input_name(n);
            for f in files {
                plan.inputs.push(InputFile {
                    local_path: format!("{name}/{}", f.path),
                    name: name.clone(),
                    key: f.object_key,
                    sha256: hex(&f.sha256),
                    size: u64::try_from(f.size_bytes).unwrap_or_default(),
                });
            }
        }
        Ok(())
    }
}

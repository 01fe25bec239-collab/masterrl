//! Launch contributor: a run's model version, staged on its machine as the folder input
//! `model` (one input file per version file, at `model/<path>`). The agent checks every
//! file's size and sha256 as it stages it, so a changed blob fails the attempt; the job spec
//! gets the version's weights hash as `base_model_sha256` (the worker refuses a run without).

use super::store::{self, hex};
use crate::db::Db;
use crate::modules::artifacts::InputFile;
use crate::modules::jobs::{AttemptRow, JobRow};
use crate::modules::scheduler::pipeline::{LaunchContributor, LaunchError, LaunchPlan, Placement};
use mrl_domain::failure::FailureClass;
use mrl_domain::ids::ModelVersionId;
use serde_json::Value;

/// Name of the input the worker reads as its model folder.
pub const MODEL_INPUT: &str = "model";

/// See the module docs.
pub struct ModelInputs {
    db: Db,
}

impl ModelInputs {
    /// Reads model versions from `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

fn gone(why: &str) -> LaunchError {
    LaunchError::Fail(FailureClass::UserConfig, why.to_owned())
}

#[async_trait::async_trait]
impl LaunchContributor for ModelInputs {
    fn name(&self) -> &'static str {
        "model_inputs"
    }

    async fn contribute(
        &self,
        job: &JobRow,
        _attempt: &AttemptRow,
        _placement: &Placement,
        plan: &mut LaunchPlan,
    ) -> Result<(), LaunchError> {
        // Built-in jobs carry their own files; only runs train on a model version.
        if job.run_id.is_none() {
            return Ok(());
        }
        let mut spec: Value = serde_json::from_str(&plan.launch.job_spec_json).map_err(|e| {
            LaunchError::Fail(
                FailureClass::UserConfig,
                format!("The job spec does not read: {e}"),
            )
        })?;
        let Some(mv) = spec
            .get("base_model_version_id")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<ModelVersionId>().ok())
        else {
            return Ok(());
        };
        let db = |e: crate::db::DbError| {
            LaunchError::Transient(format!("the database did not answer: {e}"))
        };
        let version = super::gates::ready_in(&self.db, job.org_id, job.project_id, mv.as_uuid())
            .await
            .map_err(db)?
            .ok_or_else(|| {
                gone("The model version of this run is not ready or no longer available.")
            })?;
        if !version.code_approved() {
            return Err(gone(
                "The model version of this run ships its own code that is not approved for this revision.",
            ));
        }
        let weights = version
            .weights_sha256
            .as_deref()
            .ok_or_else(|| gone("The model version of this run has no weights."))?;
        let files = store::files(&self.db, version.id).await.map_err(db)?;
        for f in files {
            plan.inputs.push(InputFile {
                name: MODEL_INPUT.into(),
                key: f.object_key,
                sha256: hex(&f.sha256),
                size: u64::try_from(f.size_bytes).unwrap_or_default(),
                local_path: format!("{MODEL_INPUT}/{}", f.path),
            });
        }
        spec["base_model_sha256"] = Value::from(hex(weights));
        plan.launch.job_spec_json = spec.to_string();
        Ok(())
    }
}

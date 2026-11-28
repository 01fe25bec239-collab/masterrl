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

const ADAPTER_INPUT: &str = "model_adapter";

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
        let mut inputs = vec![(MODEL_INPUT, &version)];
        let base = if version.kind.as_deref() == Some("adapter") {
            let base = super::gates::adapter_base(&self.db, &version)
                .await
                .map_err(db)?
                .filter(super::store::VersionRow::code_approved)
                .ok_or_else(|| gone("The adapter's original base model is unavailable or requires code approval."))?;
            Some(base)
        } else {
            None
        };
        if let Some(base) = &base {
            if plan.launch.engine_name != "trl" {
                return Err(gone(
                    "This engine cannot continue training a staged adapter.",
                ));
            }
            inputs = vec![(MODEL_INPUT, base), (ADAPTER_INPUT, &version)];
        }
        let weights = base
            .as_ref()
            .unwrap_or(&version)
            .weights_sha256
            .as_deref()
            .ok_or_else(|| gone("The model version of this run has no weights."))?;
        for (name, v) in inputs {
            for f in store::files(&self.db, v.id).await.map_err(db)? {
                plan.inputs.push(InputFile {
                    name: name.into(),
                    key: f.object_key,
                    sha256: hex(&f.sha256),
                    size: u64::try_from(f.size_bytes).unwrap_or_default(),
                    local_path: format!("{name}/{}", f.path),
                });
            }
        }
        spec["base_model_sha256"] = Value::from(hex(weights));
        plan.launch.job_spec_json = spec.to_string();
        Ok(())
    }
}

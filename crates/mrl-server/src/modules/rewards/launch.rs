//! Launch contributor: a run's pinned reward version, handed to the attempt.
//!
//! For a job of a run with `reward_version_id`:
//! - the version must be PUBLISHED (or DEPRECATED: deprecation stops new runs, not the
//!   attempts of runs made before it); a DRAFT, TESTING or FAILED_TESTS version fails the
//!   attempt (USER_CONFIG) with a plain reason, because nothing proved it in the sandbox;
//! - `job_spec_json.reward` gets the version id, its content hash, the bundle hash the
//!   sandbox reported in the passing test run (`exec_sha256`, which the harness's
//!   `RewardClient` compares with Describe), the sandbox tier and the policy;
//! - every file of the code bundle is staged at `reward/code/<path>` (input
//!   `reward.code/<path>`), so the agent can lay out the bundle read-only for the reward
//!   sandbox (see the P29 handoff for the bundle format).

use super::store;
use crate::db::Db;
use crate::modules::artifacts::{ArtifactStore, InputFile};
use crate::modules::jobs::{AttemptRow, JobRow};
use crate::modules::scheduler::pipeline::{LaunchContributor, LaunchError, LaunchPlan, Placement};
use mrl_domain::failure::FailureClass;
use mrl_domain::ids::RewardVersionId;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

/// Where code files are staged, relative to the attempt's staging folder.
pub const CODE_DIR: &str = "reward/code";
/// Input name prefix of code files.
pub const CODE_INPUT: &str = "reward.code";

/// See the module docs.
pub struct RewardInputs {
    db: Db,
    store: Arc<ArtifactStore>,
}

impl RewardInputs {
    /// The contributor for this build (its own handle on object storage, like P23's).
    pub fn new(db: Db, config: &crate::Config) -> Self {
        let store = crate::modules::artifacts::ArtifactsModule::new(db.clone(), config).store();
        Self { db, store }
    }

    /// With an explicit store (tests).
    pub fn with_store(db: Db, store: Arc<ArtifactStore>) -> Self {
        Self { db, store }
    }
}

fn transient(e: impl std::fmt::Display) -> LaunchError {
    LaunchError::Transient(format!("the reward records could not be read: {e}"))
}

/// The `reward` object for the job spec.
pub fn spec_entry(v: &store::VersionRow) -> Value {
    json!({
        "version_id": RewardVersionId::from_uuid(v.id).to_string(),
        "content_sha256": v.content_sha256,
        "exec_sha256": v.exec_sha256,
        "sandbox_tier": v.sandbox_tier,
        "policy": v.policy,
        "code_dir": v.code_artifact_id.map(|_| CODE_DIR),
    })
}

#[async_trait::async_trait]
impl LaunchContributor for RewardInputs {
    fn name(&self) -> &'static str {
        "reward_inputs"
    }

    async fn contribute(
        &self,
        job: &JobRow,
        _attempt: &AttemptRow,
        _placement: &Placement,
        plan: &mut LaunchPlan,
    ) -> Result<(), LaunchError> {
        let Some(run) = job.run_id else {
            return Ok(());
        };
        let pinned: Option<Option<Uuid>> =
            sqlx::query_scalar("SELECT reward_version_id FROM runs WHERE id = $1 AND org_id = $2")
                .bind(run)
                .bind(job.org_id)
                .fetch_optional(self.db.pool())
                .await
                .map_err(transient)?;
        let Some(version) = pinned.flatten() else {
            return Ok(());
        };
        let v = store::version(&self.db, version)
            .await
            .map_err(transient)?
            .filter(|v| v.org_id == job.org_id)
            .ok_or_else(|| {
                LaunchError::Fail(
                    FailureClass::UserConfig,
                    "The run's reward version doesn't exist any more.".into(),
                )
            })?;
        if !matches!(v.state.as_str(), "PUBLISHED" | "DEPRECATED") {
            return Err(LaunchError::Fail(
                FailureClass::UserConfig,
                format!(
                    "The run's reward version is {}. A run can use a reward version only after its tests passed and it was published.",
                    v.state
                ),
            ));
        }
        let mut spec: Value = if plan.launch.job_spec_json.is_empty() {
            json!({})
        } else {
            serde_json::from_str(&plan.launch.job_spec_json).map_err(|e| {
                LaunchError::Fail(
                    FailureClass::HarnessError,
                    format!("The job spec could not be read: {e}"),
                )
            })?
        };
        spec["reward"] = spec_entry(&v);
        plan.launch.job_spec_json = spec.to_string();
        let Some(artifact) = v.code_artifact_id else {
            return Ok(());
        };
        let org = crate::modules::jobs::scope::org_text(job.org_id)
            .unwrap_or_else(|| crate::modules::artifacts::LOCAL_ORG.to_owned());
        let found = self
            .store
            .get(&org, artifact)
            .await
            .map_err(|e| LaunchError::Transient(format!("storage did not answer: {e}")))?
            .filter(|a| a.state == "COMMITTED")
            .ok_or_else(|| {
                LaunchError::Fail(
                    FailureClass::UserConfig,
                    "The reward's code bundle is no longer stored.".into(),
                )
            })?;
        let files = v.code_files.as_array().cloned().unwrap_or_default();
        for f in files {
            let path = f.get("path").and_then(Value::as_str).unwrap_or_default();
            let (key, file) = self
                .store
                .download_source(&org, &found, path)
                .await
                .map_err(|e| LaunchError::Transient(format!("storage did not answer: {e}")))?;
            plan.inputs.push(InputFile {
                name: format!("{CODE_INPUT}/{path}"),
                key,
                sha256: file.sha256,
                size: file.size,
                local_path: format!("{CODE_DIR}/{path}"),
            });
        }
        Ok(())
    }
}

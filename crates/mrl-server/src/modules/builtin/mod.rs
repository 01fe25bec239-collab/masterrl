//! Built-in jobs end to end (A1, INT-6): non-run jobs that run a built-in task
//! (python/mrl-runtime/src/mrl_runtime/builtin/) in the sandbox its job kind names in
//! mrl-builtin.yaml.
//!
//! 1. [`submit`] creates the job (QUEUED) and its `builtin_jobs` row.
//! 2. [`launch::BuiltinLaunch`] (launch chain) adds the task, inputs, sandbox, built-in runtime
//!    and staged files; the scheduler's transfer step delivers the job's [`grants`].
//! 3. The worker sends `OutputsStaged` (its artifact manifest); [`sink::OutputsSink`] issues PUT
//!    tickets under `jobs/{job}/outputs/attempt-{a}/` and sends `UploadArtifacts`. The worker
//!    exits once the agent says the upload finished.
//! 4. The module's background pass ([`sink::register_pass`]) registers the uploaded files as
//!    one P19 artifact and records it on the job's row.
//! 5. [`OutputsVerifier`] (completion chain) holds a FINALIZING built-in job until then (or
//!    fails it); after that the parts' own verifiers read [`outputs`].
//!
//! Register with `Box::new(crate::modules::builtin::BuiltinModule::new(db.clone(), config)),`.

pub mod launch;
pub mod sink;

use crate::app::{ServerModule, Task};
use crate::config::Role;
use crate::db::{Db, LeaderElection, Tx};
use crate::modules::artifacts::InputFile;
use crate::modules::jobs::{
    CompletionVerifier, JobChange, JobRow, JobTransitions, JobsError, NewJob, Rejection,
    Verification,
};
use crate::modules::nodes::lanes::LaneRouter;
use crate::seams::SeamError;
use mrl_domain::failure::{FailureClass, FailureCode};
use mrl_domain::ids::{NodeId, SecretId};
use mrl_domain::resources::{GpuConstraints, ResourceRequest, RoleRequest, WorkloadRole};
use mrl_domain::state::{JobEvent, Owner};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use uuid::Uuid;

/// The built-in manifest the server embeds (`just gen` writes it from mrl-builtin.yaml).
pub fn manifest() -> &'static Value {
    static M: OnceLock<Value> = OnceLock::new();
    M.get_or_init(|| {
        serde_json::from_str::<Value>(include_str!(
            "../../../engines/first_party/mrl-builtin.json"
        ))
        .map(|b| b["manifest"].clone())
        .unwrap_or(Value::Null)
    })
}

/// The sandbox profile a built-in job kind runs in; `None`: not a built-in job kind.
pub fn sandbox(kind: &str) -> Option<&'static str> {
    manifest()["spec"]["jobKinds"]
        .as_array()?
        .iter()
        .find(|k| k["kind"] == kind)?["sandbox"]
        .as_str()
}

/// A secret handed to a built-in task as a credential (the environment variable for `name`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltinGrant {
    /// The secret.
    pub secret: SecretId,
    /// The use it must allow, like `hub_import`.
    pub allowed_use: String,
    /// Credential name, like `HF_TOKEN`.
    pub name: String,
}

/// A built-in job to create.
#[derive(Debug, Clone)]
pub struct BuiltinJob {
    /// Owning org.
    pub org_id: Uuid,
    /// Owning project.
    pub project_id: Uuid,
    /// A job kind from mrl-builtin.yaml, like `IMPORT`.
    pub kind: String,
    /// The built-in task, like `import_model`.
    pub task: String,
    /// The task's inputs (no secrets: those go in `grants`).
    pub inputs: Value,
    /// Stored files to stage. Several with the same `name` make a folder input.
    pub files: Vec<InputFile>,
    /// What the job works on (the job's `subject_ref`), like `{"model_version_id": "mv_…"}`.
    pub subject: Value,
    /// Submitter's principal id.
    pub created_by: String,
    /// Secrets delivered as credentials.
    pub grants: Vec<BuiltinGrant>,
    /// Run only on this machine (its inputs exist there only).
    pub node_id: Option<Uuid>,
}

/// A built-in job's registered outputs.
#[derive(Debug, Clone, PartialEq)]
pub struct JobOutputs {
    /// The P19 artifact holding every output file.
    pub artifact_id: Uuid,
    /// Its `artifact-manifest/v1` document.
    pub manifest: Value,
}

fn seam(detail: impl Into<String>) -> JobsError {
    JobsError::Seam(SeamError::new("builtin", detail.into()))
}

/// Creates a built-in job, QUEUED, in `tx`. The caller has checked the request (permission,
/// inputs); the job runs CPU-only on one machine (`node_id`'s when given).
pub async fn submit(tx: &mut Tx, job: BuiltinJob) -> Result<JobRow, JobsError> {
    if sandbox(&job.kind).is_none() {
        return Err(seam(format!(
            "{} is not a job kind in mrl-builtin.yaml",
            job.kind
        )));
    }
    let request = ResourceRequest {
        roles: vec![RoleRequest {
            role: WorkloadRole::Service,
            replicas: 1,
            gpu_count: 0,
            gpu: GpuConstraints::default(),
            node_ids: job.node_id.map(NodeId::from_uuid).into_iter().collect(),
            cpu_millis: 1000,
            mem_bytes: 2 << 30,
        }],
    };
    let row = JobTransitions::create(
        tx,
        &NewJob {
            id: Uuid::now_v7(),
            org_id: job.org_id,
            project_id: job.project_id,
            kind: job.kind,
            run_id: None,
            subject_ref: Some(job.subject),
            priority: 0,
            resource_request: serde_json::to_value(&request).map_err(|e| seam(e.to_string()))?,
            retry_policy: json!({}),
            max_runtime_seconds: None,
            created_by: job.created_by,
        },
    )
    .await?;
    sqlx::query(
        "INSERT INTO builtin_jobs (job_id, task, inputs, files, grants) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(row.id)
    .bind(&job.task)
    .bind(&job.inputs)
    .bind(sqlx::types::Json(&job.files))
    .bind(sqlx::types::Json(&job.grants))
    .execute(&mut **tx)
    .await?;
    let row = JobTransitions::apply(
        tx,
        &row,
        JobEvent::Submit,
        Owner::Jobs,
        JobChange::default(),
    )
    .await?;
    JobTransitions::apply(
        tx,
        &row,
        JobEvent::Accept,
        Owner::Jobs,
        JobChange::default(),
    )
    .await
}

/// The registered outputs of built-in job `job_id` in `org_id`; `None` until they are
/// registered (or for any other job).
pub async fn outputs(
    db: &Db,
    org_id: Uuid,
    job_id: Uuid,
) -> Result<Option<JobOutputs>, sqlx::Error> {
    let row: Option<(Uuid, Value)> = sqlx::query_as(
        "SELECT a.id, a.manifest FROM builtin_jobs b
         JOIN jobs j ON j.id = b.job_id
         JOIN artifacts a ON a.id = b.artifact_id
         WHERE b.job_id = $1 AND j.org_id = $2",
    )
    .bind(job_id)
    .bind(org_id)
    .fetch_optional(db.pool())
    .await?;
    Ok(row.map(|(artifact_id, manifest)| JobOutputs {
        artifact_id,
        manifest,
    }))
}

/// The secrets built-in job `job_id` gets as credentials (empty for any other job).
pub async fn grants(db: &Db, job_id: Uuid) -> Result<Vec<BuiltinGrant>, sqlx::Error> {
    let row: Option<sqlx::types::Json<Vec<BuiltinGrant>>> =
        sqlx::query_scalar("SELECT grants FROM builtin_jobs WHERE job_id = $1")
            .bind(job_id)
            .fetch_optional(db.pool())
            .await?;
    Ok(row.map(|g| g.0).unwrap_or_default())
}

/// Completion chain: a FINALIZING built-in job waits until its outputs are registered and
/// fails if they can't be. It vouches for nothing (abstains once they are): the part that
/// owns the job kind checks what they contain.
pub struct OutputsVerifier {
    db: Db,
}

impl OutputsVerifier {
    /// Reads `builtin_jobs` from `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl CompletionVerifier for OutputsVerifier {
    fn name(&self) -> &'static str {
        "builtin_outputs"
    }

    async fn verify(&self, job: &JobRow) -> Result<Verification, SeamError> {
        let row: Option<(Option<Uuid>, Option<Uuid>, Option<String>)> = sqlx::query_as(
            "SELECT upload_attempt_id, artifact_id, failure FROM builtin_jobs WHERE job_id = $1",
        )
        .bind(job.id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|e| SeamError::new("builtin", e.to_string()))?;
        let failed = |why: &str| {
            Verification::Failed(Rejection::failure(
                FailureCode::class_only(FailureClass::VerificationFailed),
                format!("The job's results could not be saved: {why}."),
            ))
        };
        Ok(match row {
            None => Verification::Abstain,
            // The worker exits only after its outputs were taken, so the finished attempt's
            // outputs are known by now.
            Some((attempt, _, _)) if attempt.is_none() || attempt != job.current_attempt_id => {
                failed("the job sent none")
            }
            Some((_, Some(_), _)) => Verification::Abstain,
            Some((_, None, Some(why))) => failed(&why),
            Some((_, None, None)) => {
                return Err(SeamError::new(
                    "builtin",
                    "the job's results are still being saved",
                ));
            }
        })
    }
}

/// The module: the CONTROL sink for job outputs and the pass that registers them.
pub struct BuiltinModule {
    pass: Arc<sink::Outputs>,
}

impl BuiltinModule {
    /// Its own handles on storage, the secret store and command sending; the sink goes on the
    /// process-wide lane router.
    pub fn new(db: Db, config: &crate::Config) -> Self {
        let outputs = Arc::new(sink::Outputs::from_config(db, config));
        LaneRouter::global().register(Arc::new(sink::OutputsSink::new(outputs.clone())));
        Self { pass: outputs }
    }

    /// The module with explicit parts (tests). Registers its sink on `lanes`.
    pub fn with_parts(outputs: Arc<sink::Outputs>, lanes: &LaneRouter) -> Self {
        lanes.register(Arc::new(sink::OutputsSink::new(outputs.clone())));
        Self { pass: outputs }
    }
}

impl ServerModule for BuiltinModule {
    fn name(&self) -> &'static str {
        "builtin"
    }

    fn background_tasks(&self) -> Vec<Task> {
        let pass = self.pass.clone();
        vec![Task::new(
            "builtin.outputs",
            Role::Background,
            move |cancel| async move {
                let election =
                    LeaderElection::new(pass.db().clone(), "builtin", Duration::from_secs(30));
                election
                    .run(cancel, |lead| {
                        let pass = pass.clone();
                        async move {
                            loop {
                                sink::register_pass(&pass).await;
                                tokio::select! {
                                    () = lead.cancelled() => return Ok(()),
                                    () = tokio::time::sleep(Duration::from_secs(2)) => {}
                                }
                            }
                        }
                    })
                    .await;
                Ok(())
            },
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_embedded_manifest_names_each_kinds_sandbox_and_a_runtime() {
        assert_eq!(sandbox("DATA_PREP"), Some("data_prep"));
        assert_eq!(sandbox("IMPORT"), Some("fetch"));
        assert_eq!(sandbox("TRAIN"), None);
        assert!(launch::runtime_ref().is_some_and(|r| r.starts_with("uv:sha256:")));
    }
}

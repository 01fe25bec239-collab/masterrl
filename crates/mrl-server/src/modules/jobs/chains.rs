//! The three extension chains later parts plug into.
//!
//! - [`JobValidator`]s decide whether a submitted job may be queued (P24 adds the engine
//!   capability check). A job is queued only when no member rejects it and at least one
//!   member vouches for it; with none vouching it is REJECTED with MRL-CAP-001.
//! - [`CompletionVerifier`]s decide whether a FINALIZING job may become COMPLETED (P23 adds
//!   checkpoint and metric checks). With none verifying it, it FAILS with VERIFICATION_FAILED.
//! - [`SubmissionResolver`]s turn the submitted config into the resolved one (P28 adds
//!   presets and defaults). With none registered the config is kept as submitted.
//!
//! Each part adds one line inside its block in [`Chains::registered`], like
//! `Arc::new(crate::modules::engines::CapabilityValidator::new(db.clone())),`.

use super::error::codes;
use super::model::{JobRow, Rejection};
use crate::db::Db;
use crate::error::FieldError;
use crate::seams::SeamError;
use async_trait::async_trait;
use mrl_domain::failure::{FailureClass, FailureCode};
use mrl_domain::resources::ResourceRequest;
use std::sync::Arc;
use uuid::Uuid;

/// What a validator sees: the job as it would be queued.
#[derive(Debug, Clone, PartialEq)]
pub struct JobSpec {
    /// Owning org.
    pub org_id: Uuid,
    /// Owning project.
    pub project_id: Uuid,
    /// `TRAIN` or `EVAL` for runs.
    pub kind: String,
    /// The method, like `sft`.
    pub algorithm_id: String,
    /// The model version to start from.
    pub base_model_version_id: Uuid,
    /// The reward version, if any.
    pub reward_version_id: Option<Uuid>,
    /// Dataset versions.
    pub dataset_version_ids: Vec<Uuid>,
    /// The resolved config (after every [`SubmissionResolver`]).
    pub resolved: serde_json::Value,
    /// What the job asks the scheduler for.
    pub resources: ResourceRequest,
    /// Longest allowed run time.
    pub max_runtime_seconds: Option<i64>,
}

/// A validator's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing to say about this job (not this validator's engine or method).
    Abstain,
    /// Checked and runnable: this validator vouches for the job.
    Accept,
    /// Checked and runnable on this engine version (`engine_versions.id`); the run records it.
    AcceptEngine(Uuid),
    /// Problems found; the job is REJECTED with them.
    Reject(Vec<Rejection>),
}

/// Decides whether a job may be queued.
#[async_trait]
pub trait JobValidator: Send + Sync {
    /// Short name for logs, like `schema`.
    fn name(&self) -> &'static str;
    /// Checks `spec`. `Err` fails the submit request (503) and changes nothing.
    async fn validate(&self, spec: &JobSpec) -> Result<Verdict, SeamError>;
}

/// A verifier's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verification {
    /// Nothing to say about this job.
    Abstain,
    /// The results are complete and sound.
    Verified,
    /// The results are not acceptable; the job FAILS with VERIFICATION_FAILED.
    Failed(Rejection),
    /// Checking failed for a passing reason (like storage being down); retry later.
    RetryLater(String),
}

/// Decides whether a FINALIZING job may become COMPLETED.
#[async_trait]
pub trait CompletionVerifier: Send + Sync {
    /// Short name for logs.
    fn name(&self) -> &'static str;
    /// Checks the finished `job`.
    async fn verify(&self, job: &JobRow) -> Result<Verification, SeamError>;
}

/// Why a resolver refused a submission.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResolveError {
    /// The submission names something the resolver can't use (422).
    #[error("{} field(s) need a change", .0.len())]
    Invalid(Vec<FieldError>),
    /// The resolver's backend failed (503).
    #[error(transparent)]
    Seam(#[from] SeamError),
}

/// Turns the submitted config into the resolved one.
#[async_trait]
pub trait SubmissionResolver: Send + Sync {
    /// Short name for logs.
    fn name(&self) -> &'static str;
    /// Returns the config after this step; `resolved` is the output of the previous member.
    async fn resolve(
        &self,
        submitted: &serde_json::Value,
        resolved: serde_json::Value,
    ) -> Result<serde_json::Value, ResolveError>;
}

/// The default validator: checks the submission's own rules. It never vouches for a job.
pub struct SchemaChecks;

#[async_trait]
impl JobValidator for SchemaChecks {
    fn name(&self) -> &'static str {
        "schema"
    }

    async fn validate(&self, spec: &JobSpec) -> Result<Verdict, SeamError> {
        let mut problems = Vec::new();
        if let Err(e) = spec.resources.validate() {
            problems.push(Rejection::new(
                codes::FIELD_VALUE,
                Some("/resources"),
                format!("The resource request can't be placed: {e}."),
            ));
        }
        if spec.kind == "TRAIN" && spec.dataset_version_ids.is_empty() {
            problems.push(Rejection::new(
                codes::FIELD_MISSING,
                Some("/dataset_version_ids"),
                "A training run needs at least one dataset version.",
            ));
        }
        if !spec.resolved.is_object() {
            problems.push(Rejection::new(
                codes::FIELD_VALUE,
                Some("/config"),
                "The run config must be a JSON object.",
            ));
        }
        Ok(if problems.is_empty() {
            Verdict::Abstain
        } else {
            Verdict::Reject(problems)
        })
    }
}

/// The outcome of the validator chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Validation {
    /// Queue the job, on the engine version a validator picked (if any).
    Accepted(Option<Uuid>),
    /// Reject it with these problems (never empty).
    Rejected(Vec<Rejection>),
}

/// A verifier decision tied to the job snapshot the chain inspected.
#[derive(Debug, Clone)]
pub struct Completion {
    snapshot: (Uuid, Uuid, i64, i64),
    outcome: CompletionOutcome,
}

impl Completion {
    fn new(job: &JobRow, outcome: CompletionOutcome) -> Self {
        Self {
            snapshot: (job.org_id, job.id, job.state_version, job.epoch),
            outcome,
        }
    }

    pub(super) fn for_job(self, job: &JobRow) -> Option<CompletionOutcome> {
        (self.snapshot == (job.org_id, job.id, job.state_version, job.epoch))
            .then_some(self.outcome)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CompletionOutcome {
    /// Complete the job.
    Verified,
    /// Fail it with VERIFICATION_FAILED.
    Failed(Rejection),
    /// Retry later (INFRA_TRANSIENT).
    RetryLater(String),
}

/// Every registered chain member, in order. Cheap to clone.
#[derive(Clone)]
pub struct Chains {
    validators: Arc<[Arc<dyn JobValidator>]>,
    verifiers: Arc<[Arc<dyn CompletionVerifier>]>,
    resolvers: Arc<[Arc<dyn SubmissionResolver>]>,
}

impl Chains {
    /// The members this build registers. Each part adds one line inside its block.
    pub fn registered(db: &Db) -> Self {
        let _ = db;
        Self::new(
            vec![
                Arc::new(SchemaChecks),
                // <mrl:job-validators> Validators go right below this line.
                Arc::new(crate::modules::engines::CapabilityValidator::new(
                    db.clone(),
                )),
                // <mrl:job-validators P26> P26 validators go right below this line.
                Arc::new(crate::modules::models::gates::ModelGates::new(db.clone())),
                // <mrl:job-validators P28> P28 validators go right below this line.
            ],
            vec![
                // <mrl:completion-verifiers> Verifiers go right below this line.
                Arc::new(crate::modules::builtin::OutputsVerifier::new(db.clone())),
                Arc::new(crate::modules::checkpoints::FinalCheckpointVerifier::new(
                    db.clone(),
                )),
                // <mrl:completion-verifiers P26> P26 verifiers go right below this line.
                Arc::new(crate::modules::models::outputs::ModelOutputs::new(
                    db.clone(),
                )),
                // <mrl:completion-verifiers P27> P27 verifiers go right below this line.
                // <mrl:completion-verifiers P28> P28 verifiers go right below this line.
                // <mrl:completion-verifiers P29> P29 verifiers go right below this line.
            ],
            vec![
                // <mrl:submission-resolvers> Resolvers go right below this line.
            ],
        )
    }

    /// Chains with exactly these members (tests use this).
    pub fn new(
        validators: Vec<Arc<dyn JobValidator>>,
        verifiers: Vec<Arc<dyn CompletionVerifier>>,
        resolvers: Vec<Arc<dyn SubmissionResolver>>,
    ) -> Self {
        Self {
            validators: validators.into(),
            verifiers: verifiers.into(),
            resolvers: resolvers.into(),
        }
    }

    /// Runs every validator. Rejections from all members are collected.
    pub async fn validate(&self, spec: &JobSpec) -> Result<Validation, SeamError> {
        let mut problems = Vec::new();
        let mut vouched = false;
        let mut engine = None;
        for v in self.validators.iter() {
            match v.validate(spec).await? {
                Verdict::Abstain => {}
                Verdict::Accept => vouched = true,
                Verdict::AcceptEngine(id) => {
                    if engine.is_some_and(|e| e != id) {
                        return Err(SeamError::new(
                            "jobs",
                            "Two validators picked different engine versions for this job.",
                        ));
                    }
                    vouched = true;
                    engine = Some(id);
                }
                Verdict::Reject(p) => problems.extend(p),
            }
        }
        if problems.is_empty() && !vouched {
            problems.push(Rejection::new(
                codes::NO_ENGINE,
                Some("/algorithm_id"),
                format!(
                    "No engine is registered for the method `{}` yet, so the app can't check that this run can work.",
                    spec.algorithm_id
                ),
            ));
        }
        Ok(if problems.is_empty() {
            Validation::Accepted(engine)
        } else {
            Validation::Rejected(problems)
        })
    }

    /// Runs every verifier: any failure fails, else any retry retries, else one verification completes.
    pub async fn verify(&self, job: &JobRow) -> Result<Completion, SeamError> {
        let mut retry = None;
        let mut verified = false;
        for v in self.verifiers.iter() {
            match v.verify(job).await? {
                Verification::Abstain => {}
                Verification::Verified => verified = true,
                Verification::Failed(r) => {
                    return Ok(Completion::new(job, CompletionOutcome::Failed(r)))
                }
                Verification::RetryLater(why) => retry = Some(why),
            }
        }
        let outcome = match (retry, verified) {
            (Some(why), _) => CompletionOutcome::RetryLater(why),
            (None, true) => CompletionOutcome::Verified,
            (None, false) => CompletionOutcome::Failed(Rejection::failure(
                FailureCode::class_only(FailureClass::VerificationFailed),
                "No checker is registered to confirm this job's results, so it can't be marked complete.",
            )),
        };
        Ok(Completion::new(job, outcome))
    }

    /// Runs every resolver in order, starting from the submitted config.
    pub async fn resolve(
        &self,
        submitted: &serde_json::Value,
    ) -> Result<serde_json::Value, ResolveError> {
        let mut resolved = submitted.clone();
        for r in self.resolvers.iter() {
            resolved = r.resolve(submitted, resolved).await?;
        }
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_domain::resources::{GpuConstraints, RoleRequest, WorkloadRole};

    fn spec() -> JobSpec {
        JobSpec {
            org_id: Uuid::nil(),
            project_id: Uuid::nil(),
            kind: "TRAIN".into(),
            algorithm_id: "sft".into(),
            base_model_version_id: Uuid::nil(),
            reward_version_id: None,
            dataset_version_ids: vec![Uuid::nil()],
            resolved: serde_json::json!({}),
            resources: ResourceRequest {
                roles: vec![RoleRequest {
                    role: WorkloadRole::Trainer,
                    replicas: 1,
                    gpu_count: 1,
                    gpu: GpuConstraints::default(),
                    node_ids: vec![],
                    cpu_millis: 1000,
                    mem_bytes: 1 << 30,
                }],
            },
            max_runtime_seconds: None,
        }
    }

    struct Fixed(Verdict);
    #[async_trait]
    impl JobValidator for Fixed {
        fn name(&self) -> &'static str {
            "fixed"
        }
        async fn validate(&self, _: &JobSpec) -> Result<Verdict, SeamError> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn default_chain_rejects_with_no_engine() {
        let chains = Chains::new(vec![Arc::new(SchemaChecks)], vec![], vec![]);
        let Validation::Rejected(p) = chains.validate(&spec()).await.unwrap() else {
            panic!("expected a rejection");
        };
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].code, "MRL-CAP-001");
        assert_eq!(p[0].path.as_deref(), Some("/algorithm_id"));
        assert!(p[0].message.contains("`sft`"));
        assert!(p[0].hint.contains("Pick another method"), "{}", p[0].hint);
    }

    #[tokio::test]
    async fn schema_problems_are_listed_and_no_engine_is_not_added() {
        let chains = Chains::new(vec![Arc::new(SchemaChecks)], vec![], vec![]);
        let mut s = spec();
        s.dataset_version_ids.clear();
        s.resources.roles.clear();
        s.resolved = serde_json::json!([1]);
        let Validation::Rejected(p) = chains.validate(&s).await.unwrap() else {
            panic!("expected a rejection");
        };
        let paths: Vec<_> = p.iter().filter_map(|r| r.path.as_deref()).collect();
        assert_eq!(paths, ["/resources", "/dataset_version_ids", "/config"]);
    }

    #[tokio::test]
    async fn an_accepting_member_queues_unless_another_rejects() {
        let accept: Arc<dyn JobValidator> = Arc::new(Fixed(Verdict::Accept));
        let chains = Chains::new(vec![Arc::new(SchemaChecks), accept.clone()], vec![], vec![]);
        assert_eq!(
            chains.validate(&spec()).await.unwrap(),
            Validation::Accepted(None)
        );
        let reject = Arc::new(Fixed(Verdict::Reject(vec![Rejection::new(
            codes::NO_ENGINE,
            None,
            "no",
        )])));
        let chains = Chains::new(vec![accept, reject], vec![], vec![]);
        assert!(matches!(
            chains.validate(&spec()).await.unwrap(),
            Validation::Rejected(_)
        ));
    }

    #[tokio::test]
    async fn the_picked_engine_is_kept_and_two_picks_must_agree() {
        let (a, b) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let pick = |id| -> Arc<dyn JobValidator> { Arc::new(Fixed(Verdict::AcceptEngine(id))) };
        let accept: Arc<dyn JobValidator> = Arc::new(Fixed(Verdict::Accept));
        let chains = Chains::new(vec![accept, pick(a), pick(a)], vec![], vec![]);
        assert_eq!(
            chains.validate(&spec()).await.unwrap(),
            Validation::Accepted(Some(a))
        );
        let chains = Chains::new(vec![pick(a), pick(b)], vec![], vec![]);
        assert!(chains.validate(&spec()).await.is_err());
    }

    struct Says(Verification);
    #[async_trait]
    impl CompletionVerifier for Says {
        fn name(&self) -> &'static str {
            "says"
        }
        async fn verify(&self, _: &JobRow) -> Result<Verification, SeamError> {
            Ok(self.0.clone())
        }
    }

    fn job() -> JobRow {
        let now = time::OffsetDateTime::UNIX_EPOCH;
        JobRow {
            id: Uuid::nil(),
            org_id: Uuid::nil(),
            project_id: Uuid::nil(),
            kind: "TRAIN".into(),
            run_id: Some(Uuid::nil()),
            subject_ref: None,
            state: "FINALIZING".into(),
            state_version: 1,
            version: 1,
            state_reason: None,
            failure_class: None,
            epoch: 1,
            priority: 0,
            pool_id: None,
            resource_request: serde_json::json!({}),
            retry_policy: serde_json::json!({}),
            retries_used: serde_json::json!({}),
            current_attempt_id: None,
            queued_at: None,
            started_at: None,
            finished_at: None,
            next_eligible_at: None,
            max_runtime_seconds: None,
            created_by: "u".into(),
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn verifier_chain_rules() {
        let none = Chains::new(vec![], vec![], vec![]);
        let CompletionOutcome::Failed(r) = none.verify(&job()).await.unwrap().outcome else {
            panic!("no verifier must fail");
        };
        assert_eq!(r.code, "VERIFICATION_FAILED");
        let ok: Arc<dyn CompletionVerifier> = Arc::new(Says(Verification::Verified));
        let retry: Arc<dyn CompletionVerifier> =
            Arc::new(Says(Verification::RetryLater("storage down".into())));
        let fail: Arc<dyn CompletionVerifier> = Arc::new(Says(Verification::Failed(
            Rejection::new(codes::FIELD_VALUE, None, "bad"),
        )));
        let c = |v: Vec<Arc<dyn CompletionVerifier>>| Chains::new(vec![], v, vec![]);
        assert_eq!(
            c(vec![ok.clone()]).verify(&job()).await.unwrap().outcome,
            CompletionOutcome::Verified
        );
        assert_eq!(
            c(vec![ok.clone(), retry.clone()])
                .verify(&job())
                .await
                .unwrap()
                .outcome,
            CompletionOutcome::RetryLater("storage down".into())
        );
        assert!(matches!(
            c(vec![retry, fail, ok])
                .verify(&job())
                .await
                .unwrap()
                .outcome,
            CompletionOutcome::Failed(_)
        ));
    }

    struct AddKey;
    #[async_trait]
    impl SubmissionResolver for AddKey {
        fn name(&self) -> &'static str {
            "add-key"
        }
        async fn resolve(
            &self,
            _: &serde_json::Value,
            mut resolved: serde_json::Value,
        ) -> Result<serde_json::Value, ResolveError> {
            resolved["lr"] = serde_json::json!(0.1);
            Ok(resolved)
        }
    }

    #[tokio::test]
    async fn resolvers_fold_in_order_and_default_keeps_config() {
        let submitted = serde_json::json!({"epochs": 1});
        let none = Chains::new(vec![], vec![], vec![]);
        assert_eq!(none.resolve(&submitted).await.unwrap(), submitted);
        let one = Chains::new(vec![], vec![], vec![Arc::new(AddKey)]);
        assert_eq!(
            one.resolve(&submitted).await.unwrap(),
            serde_json::json!({"epochs": 1, "lr": 0.1})
        );
    }
}

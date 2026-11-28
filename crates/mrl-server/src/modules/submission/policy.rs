//! P28's job validators and the resume check (A1 rulings 2026-11-22).
//!
//! - [`MethodPolicy`]: an `experimental` method needs the workspace toggle (Doc 05 §18 row
//!   21): the org setting `experimental_methods` is `true` (every workspace) or a list of
//!   `ws_…` ids. Dev-only engines (the FakeEngine) are exempt.
//! - [`safety_at_submit`] (in the resolver): the run gets the newest estimate of its inputs,
//!   and a `tight` verdict's safety settings.
//! - [`Guardian`]: at submit, a stored estimate of these exact inputs that does not fit any
//!   connected machine rejects the run (MRL-CAP-280) with the nearest alternatives.
//! - [`resume_problem`]: a retry or resume may not change the engine's major version or the
//!   method (Doc 05 §18 row 19). Checked by `runs:validate` (`parent_checkpoint_id`) and at
//!   launch for every resumed attempt ([`super::probe::ProbeLaunch`]).
//!
//! The submit-time preflight uses `[submission]`'s headroom (the members are built from it).

use super::codes;
use super::resolve::{engine_choice, methods};
use super::SubmissionConfig;
use crate::db::Db;
use crate::modules::engines::validator::{CapabilityValidator, Pick};
use crate::modules::estimates::{self, EstimateRow, RunEstimate, Verdict};
use crate::modules::jobs::{JobSpec, JobValidator, Rejection, SubmitScope, Verdict as Check};
use crate::modules::preflight::{self, PreflightInput, RunPreflight};
use crate::seams::SeamError;
use async_trait::async_trait;
use mrl_domain::capability::codes::CapabilityRejection;
use mrl_domain::ids::WorkspaceId;
use mrl_domain::resources::{GpuVendor, ResourceRequest, WorkloadRole};
use serde_json::{json, Value};
use uuid::Uuid;

fn seam(e: sqlx::Error) -> SeamError {
    SeamError::from(crate::db::DbError::from(e))
}

/// Whether the org's `experimental_methods` setting turns experimental methods on for
/// `workspace`.
pub fn experimental_allowed(setting: &Value, workspace: Option<Uuid>) -> bool {
    match setting {
        Value::Bool(b) => *b,
        Value::Array(ids) => workspace.is_some_and(|w| {
            let text = WorkspaceId::from_uuid(w).to_string();
            ids.iter().any(|i| i.as_str() == Some(text.as_str()))
        }),
        _ => false,
    }
}

/// The experimental-method policy.
pub struct MethodPolicy {
    db: Db,
    capability: CapabilityValidator,
}

impl MethodPolicy {
    /// Reads engines, orgs and projects from `db`.
    pub fn new(db: Db) -> Self {
        let capability = CapabilityValidator::new(db.clone());
        Self::with_capability(db, capability)
    }

    /// Uses the same registry lookups as the capability validator in a custom chain.
    pub fn with_capability(db: Db, capability: CapabilityValidator) -> Self {
        Self { db, capability }
    }
}

#[async_trait]
impl JobValidator for MethodPolicy {
    fn name(&self) -> &'static str {
        "experimental-policy"
    }

    async fn validate(&self, spec: &JobSpec) -> Result<Check, SeamError> {
        let choice = engine_choice(&spec.resolved);
        let found = methods(&self.db, Some(&spec.algorithm_id), choice.as_ref(), None)
            .await
            .map_err(seam)?;
        let mut experimental = found
            .iter()
            .filter(|m| !m.dev_only && m.maturity == "experimental");
        let needs_policy = if found
            .iter()
            .all(|m| !m.dev_only && m.maturity == "experimental")
        {
            !found.is_empty()
        } else if experimental.clone().next().is_some() {
            match self
                .capability
                .pick(spec)
                .await
                .map_err(|e| SeamError::new("engines", e.to_string()))?
            {
                Pick::Engine(selected) => experimental.any(|m| m.engine_version_id == selected),
                Pick::Rejected(_) | Pick::NoEngine => false,
            }
        } else {
            false
        };
        if !needs_policy {
            return Ok(Check::Abstain);
        }
        let row: Option<(Option<Value>, Option<Uuid>)> = sqlx::query_as(
            "SELECT o.settings -> 'experimental_methods',
                    (SELECT p.workspace_id FROM projects p WHERE p.id = $2 AND p.org_id = o.id)
               FROM organizations o WHERE o.id = $1",
        )
        .bind(spec.org_id)
        .bind(spec.project_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(seam)?;
        let (setting, workspace) = row.unwrap_or((None, None));
        if experimental_allowed(&setting.unwrap_or(Value::Null), workspace) {
            return Ok(Check::Abstain);
        }
        Ok(Check::Reject(vec![Rejection::new(
            CapabilityRejection::ExperimentalDisabled.code(),
            Some("/algorithm_id"),
            format!(
                "The method `{}` is experimental and not turned on for this workspace.",
                spec.algorithm_id
            ),
        )]))
    }
}

/// The preflight of `spec` with stored estimate `row`, on `settings`' headroom: the verdict
/// and the config with any safety settings applied.
async fn judge(
    db: &Db,
    settings: &SubmissionConfig,
    spec: &JobSpec,
    row: &EstimateRow,
) -> Result<(RunPreflight, Value), sqlx::Error> {
    let estimate = RunEstimate::of(row);
    let all = preflight::machines(db, spec.org_id, None).await?;
    let found = methods(
        db,
        Some(&spec.algorithm_id),
        engine_choice(&spec.resolved).as_ref(),
        None,
    )
    .await?;
    let method = found.first();
    let input = PreflightInput {
        resources: &spec.resources,
        estimate: estimate.per_gpu_memory_gib,
        config: &spec.resolved,
        schema: method.and_then(|m| m.schema.as_ref()),
        supports_qlora: method.is_some_and(|m| m.supports_qlora()),
        probe_steps: settings.probe_steps,
    };
    Ok(preflight::decide(&input, &all, &settings.headroom))
}

/// The job as submit's resolvers see it.
pub fn scope_job(scope: &SubmitScope, resolved: Value) -> JobSpec {
    JobSpec {
        org_id: scope.org_id,
        project_id: scope.project_id,
        kind: scope.kind.clone(),
        algorithm_id: scope.algorithm_id.clone(),
        base_model_version_id: scope.base_model_version_id,
        reward_version_id: scope.reward_version_id,
        dataset_version_ids: scope.dataset_version_ids.clone(),
        resolved,
        resources: scope.resources.clone(),
        max_runtime_seconds: None,
    }
}

/// The key of [`safety_at_submit`]'s note in [`SubmitScope::notes`].
pub const SAFETY_NOTE: &str = "safety-settings";

/// Submit's half of the Guardian (run by the resolver). With an estimate of these inputs:
/// the main role's GPUs must hold the expected memory ([`require_vram`]), and a tight
/// configuration gets the validated safety settings. Only an estimate of the final config is
/// pinned to the run; the source estimate is kept in the audit details.
pub async fn safety_at_submit(
    db: &Db,
    capability: &CapabilityValidator,
    settings: &SubmissionConfig,
    scope: &mut SubmitScope,
    config: Value,
) -> Result<Value, SeamError> {
    let spec = scope_job(scope, config);
    let sha = estimates::inputs_sha256(&spec);
    let Some(row) = estimates::latest(db, spec.org_id, spec.project_id, &sha)
        .await
        .map_err(seam)?
    else {
        return Ok(spec.resolved);
    };
    scope.estimate_id = Some(row.id);
    let (p, adjusted) = judge(db, settings, &spec, &row).await.map_err(seam)?;
    // Only a run some machine can hold is narrowed; the Guardian refuses one none can.
    if let (Verdict::Fits | Verdict::Tight, Some(range)) =
        (p.verdict, RunEstimate::of(&row).per_gpu_memory_gib)
    {
        require_vram(&mut scope.resources, range.expected);
    }
    if p.verdict != Verdict::Tight || p.changes.is_empty() {
        return Ok(spec.resolved);
    }
    let mut changed = spec.clone();
    changed.resolved = adjusted;
    let picked = capability
        .pick(&changed)
        .await
        .map_err(|e| SeamError::new("engines", e.to_string()))?;
    if !matches!(picked, Pick::Engine(_)) {
        return Ok(spec.resolved);
    }
    // The probe measured the original inputs. The adjusted run needs its own estimate.
    scope.estimate_id = estimates::latest(
        db,
        changed.org_id,
        changed.project_id,
        &estimates::inputs_sha256(&changed),
    )
    .await
    .map_err(seam)?
    .map(|estimate| estimate.id);
    scope.notes.insert(
        SAFETY_NOTE.into(),
        json!({"source_estimate_id": row.id.to_string(), "verdict": p.verdict,
               "changes": p.changes, "message": p.message}),
    );
    Ok(changed.resolved)
}

/// Asks the main role's discrete GPUs for at least `expected_gib` each (placement enforces
/// it per GPU), so the run never lands on a GPU it can't fit. A larger ask is kept; Apple
/// GPUs (unified memory) and roles without a GPU maker are left alone.
pub fn require_vram(resources: &mut ResourceRequest, expected_gib: f64) {
    if !(expected_gib.is_finite() && expected_gib > 0.0) {
        return;
    }
    let main = resources
        .roles
        .iter()
        .position(|r| r.role == WorkloadRole::Trainer)
        .or(if resources.roles.is_empty() {
            None
        } else {
            Some(0)
        });
    let Some(role) = main.and_then(|i| resources.roles.get_mut(i)) else {
        return;
    };
    if role.gpu_count == 0 || !matches!(role.gpu.vendor, Some(v) if v != GpuVendor::Apple) {
        return;
    }
    let bytes = (expected_gib * f64::from(1u32 << 30)).ceil() as u64;
    role.gpu.min_vram_bytes = Some(role.gpu.min_vram_bytes.map_or(bytes, |m| m.max(bytes)));
}

/// Submit-time preflight from the newest estimate of the same inputs.
pub struct Guardian {
    db: Db,
    settings: SubmissionConfig,
}

impl Guardian {
    /// Reads estimates and machines from `db`, with `settings`' headroom.
    pub fn new(db: Db, settings: SubmissionConfig) -> Self {
        Self { db, settings }
    }
}

#[async_trait]
impl JobValidator for Guardian {
    fn name(&self) -> &'static str {
        "guardian-preflight"
    }

    async fn validate(&self, spec: &JobSpec) -> Result<Check, SeamError> {
        let sha = estimates::inputs_sha256(spec);
        let Some(row) = estimates::latest(&self.db, spec.org_id, spec.project_id, &sha)
            .await
            .map_err(seam)?
        else {
            // No estimate: `unknown`, the run starts in probe mode.
            return Ok(Check::Abstain);
        };
        let (p, _) = judge(&self.db, &self.settings, spec, &row)
            .await
            .map_err(seam)?;
        if p.verdict != Verdict::DoesNotFit {
            return Ok(Check::Abstain);
        }
        let mut r = Rejection::new(codes::DOES_NOT_FIT, Some("/resources"), p.message);
        if !p.alternatives.is_empty() {
            r.hint = p
                .alternatives
                .iter()
                .map(|a| a.message.as_str())
                .collect::<Vec<_>>()
                .join(" ");
        }
        Ok(Check::Reject(vec![r]))
    }
}

/// The major part of an engine version: `1` for `1.4.2`, `0.3` for `0.3.1` (before 1.0
/// every minor version may break checkpoints).
pub fn major(version: &str) -> String {
    let mut parts = version.split('.');
    let first = parts.next().unwrap_or_default();
    if first == "0" {
        format!("0.{}", parts.next().unwrap_or_default())
    } else {
        first.to_owned()
    }
}

/// What a checkpoint came from, or what a run runs: `(engine, version, method)`.
pub type Lineage = (String, String, String);

/// Why resuming `from` in `to` is not allowed, in plain words (`None`: allowed).
pub fn incompatible(from: &Lineage, to: &Lineage) -> Option<String> {
    if from.0 != to.0 || major(&from.1) != major(&to.1) {
        return Some(format!(
            "This checkpoint was saved by {} {}; this run uses {} {}, so it can't resume from it.",
            from.0, from.1, to.0, to.1
        ));
    }
    if from.2 != to.2 {
        return Some(format!(
            "This checkpoint was saved by the method `{}`; this run uses `{}`, so it can't resume from it.",
            from.2, to.2
        ));
    }
    None
}

/// The engine and method that produced checkpoint `checkpoint` (in `org`).
pub async fn checkpoint_lineage(
    db: &Db,
    org: Uuid,
    checkpoint: Uuid,
) -> Result<Option<Lineage>, sqlx::Error> {
    sqlx::query_as(
        "SELECT te.name, ev.version, r.algorithm_id FROM checkpoints c
           JOIN runs r ON r.id = c.run_id
           JOIN engine_versions ev ON ev.id = r.engine_version_id
           JOIN training_engines te ON te.id = ev.engine_id
          WHERE c.id = $1 AND c.org_id = $2",
    )
    .bind(checkpoint)
    .bind(org)
    .fetch_optional(db.pool())
    .await
}

/// `(engine, version)` of engine version `id`.
pub async fn engine_of(db: &Db, id: Uuid) -> Result<Option<(String, String)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT te.name, ev.version FROM engine_versions ev
           JOIN training_engines te ON te.id = ev.engine_id WHERE ev.id = $1",
    )
    .bind(id)
    .fetch_optional(db.pool())
    .await
}

/// Why `checkpoint` can't be resumed by a run of `algorithm` on engine version `engine`.
/// A checkpoint whose run has no recorded engine can't be judged and passes.
pub async fn resume_problem(
    db: &Db,
    org: Uuid,
    checkpoint: Uuid,
    engine: Uuid,
    algorithm: &str,
) -> Result<Option<String>, sqlx::Error> {
    let (Some(from), Some((name, version))) = (
        checkpoint_lineage(db, org, checkpoint).await?,
        engine_of(db, engine).await?,
    ) else {
        return Ok(None);
    };
    Ok(incompatible(&from, &(name, version, algorithm.to_owned())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn experimental_toggle() {
        let ws = Uuid::now_v7();
        let id = WorkspaceId::from_uuid(ws).to_string();
        assert!(experimental_allowed(&json!(true), None));
        assert!(!experimental_allowed(&json!(false), Some(ws)));
        assert!(
            !experimental_allowed(&Value::Null, Some(ws)),
            "off by default"
        );
        assert!(experimental_allowed(&json!([id]), Some(ws)));
        assert!(!experimental_allowed(&json!([id]), Some(Uuid::now_v7())));
        assert!(!experimental_allowed(&json!([id]), None));
        assert!(!experimental_allowed(&json!("yes"), Some(ws)));
    }

    #[test]
    fn resume_keeps_engine_major_and_method() {
        let l = |e: &str, v: &str, m: &str| (e.to_owned(), v.to_owned(), m.to_owned());
        assert_eq!(major("1.4.2"), "1");
        assert_eq!(major("0.3.1"), "0.3");
        assert_eq!(major("2"), "2");
        let from = l("trl", "1.2.0", "sft");
        assert_eq!(
            incompatible(&from, &l("trl", "1.9.3", "sft")),
            None,
            "minor bump"
        );
        assert!(
            incompatible(&from, &l("trl", "2.0.0", "sft")).is_some_and(|m| m.contains("trl 1.2.0"))
        );
        assert!(incompatible(&from, &l("verl", "1.2.0", "sft")).is_some());
        assert!(incompatible(&from, &l("trl", "1.2.0", "dpo")).is_some_and(|m| m.contains("`sft`")));
        assert!(incompatible(&l("trl", "0.1.0", "sft"), &l("trl", "0.2.0", "sft")).is_some());
        assert_eq!(
            incompatible(&l("trl", "0.1.0", "sft"), &l("trl", "0.1.5", "sft")),
            None
        );
    }
}

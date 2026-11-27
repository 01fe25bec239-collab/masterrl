//! The model gates and the capability facts.
//!
//! - [`ModelRecords`] (P24's `RegistryRecords`): facts about a READY version of the org, owned
//!   by the asking project; anything else is `None`, so missing, foreign and not-READY look
//!   the same (MRL-MODEL-240).
//! - [`ModelGates`] (a `JobValidator`): every model a run names must have its custom code
//!   approved for its exact revision (MRL-MODEL-002), its license acknowledged by the project
//!   (MRL-MODEL-262), and be trainable (MRL-MODEL-151).
//! - [`compatibility`]: P24's matcher over the version's facts, per engine version.

use super::codes;
use super::store::{self, VersionRow};
use crate::db::{Db, DbError};
use crate::modules::engines::{RecordScope, RegistryRecords};
use crate::modules::jobs::{JobSpec, JobValidator, Rejection, Verdict};
use crate::seams::SeamError;
use async_trait::async_trait;
use mrl_domain::capability::matcher::{
    self, DatasetFacts, Engine, HardwareRequest, Lookup, ModelFacts, Request, Wanted,
};
use mrl_domain::ids::{DatasetVersionId, ModelVersionId};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

fn seam(e: DbError) -> SeamError {
    SeamError::new("models", e.to_string())
}

/// The READY version `id` of `org` owned by `project`.
pub async fn ready_in(
    db: &Db,
    org: Uuid,
    project: Uuid,
    id: Uuid,
) -> Result<Option<VersionRow>, DbError> {
    Ok(store::version(db, id)
        .await?
        .filter(|v| v.org_id == org && v.project_id == project && v.state == "READY"))
}

/// The model family from an inspection report: `decoder`, `decoder-moe`, `encoder`,
/// `encoder-decoder`, `vlm`, or `unknown` (which the capability check refuses).
pub fn family(inspection: &Value) -> String {
    let arch = inspection["architecture"].as_str().unwrap_or_default();
    let family = if !inspection["vision"].is_null() {
        "vlm"
    } else if arch.ends_with("ForCausalLM") {
        if inspection["moe"].is_null() {
            "decoder"
        } else {
            "decoder-moe"
        }
    } else if arch.ends_with("ForConditionalGeneration") || arch.ends_with("ForSeq2SeqLM") {
        "encoder-decoder"
    } else if arch.ends_with("ForMaskedLM")
        || arch.ends_with("ForSequenceClassification")
        || arch.ends_with("ForTokenClassification")
    {
        "encoder"
    } else {
        "unknown"
    };
    family.to_owned()
}

/// What the capability check knows about `v`.
pub fn facts(v: &VersionRow) -> ModelFacts {
    ModelFacts {
        family: v.family.clone().unwrap_or_else(|| "unknown".into()),
        is_moe: v.is_moe,
        params: v.param_count.map(|p| p as f64),
        has_chat_template: v.chat_template_sha256.is_some(),
        requires_remote_code: v.requires_remote_code,
        remote_code_approved: v.code_approved(),
    }
}

/// P24's lookups for model versions, from this part's tables.
pub struct ModelRecords {
    db: Db,
}

impl ModelRecords {
    /// Reads `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[async_trait]
impl RegistryRecords for ModelRecords {
    async fn model(
        &self,
        scope: &RecordScope,
        id: ModelVersionId,
    ) -> Result<Option<ModelFacts>, SeamError> {
        Ok(
            ready_in(&self.db, scope.org_id, scope.project_id, id.as_uuid())
                .await
                .map_err(seam)?
                .as_ref()
                .map(facts),
        )
    }

    async fn dataset(
        &self,
        _scope: &RecordScope,
        _id: DatasetVersionId,
    ) -> Result<Option<DatasetFacts>, SeamError> {
        // Datasets are P27's.
        Ok(None)
    }
}

/// Where a run names a model, as JSON Pointers into the submission.
const MODEL_FIELDS: [(&str, &str); 3] = [
    (
        "/inputs/reference_model_version_id",
        "/config/inputs/reference_model_version_id",
    ),
    (
        "/inputs/critic_model_version_id",
        "/config/inputs/critic_model_version_id",
    ),
    (
        "/common/teacher_model_version_id",
        "/config/common/teacher_model_version_id",
    ),
];

/// The run validator for the model gates (see the module docs).
pub struct ModelGates {
    db: Db,
}

impl ModelGates {
    /// Reads `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    async fn check(
        &self,
        spec: &JobSpec,
        id: Uuid,
        path: &str,
        out: &mut Vec<Rejection>,
    ) -> Result<(), DbError> {
        // Unknown here: P24 answers MRL-MODEL-240 for it.
        let Some(v) = ready_in(&self.db, spec.org_id, spec.project_id, id).await? else {
            return Ok(());
        };
        if !v.code_approved() {
            out.push(Rejection::new(
                codes::REMOTE_CODE,
                Some(path),
                format!(
                    "This model version ships its own code, and no maintainer has approved revision {} yet.",
                    v.revision().unwrap_or_default()
                ),
            ));
        }
        if v.kind.as_deref() == Some("inference_only") {
            out.push(Rejection::new(
                codes::INFERENCE_ONLY,
                Some(path),
                "This model version can be used only for inference, not for training.",
            ));
        }
        let acknowledged = store::acknowledged(&self.db, v.project_id, v.model_id).await?;
        if !v
            .license_sha256
            .as_ref()
            .is_some_and(|hash| acknowledged.contains(hash))
        {
            out.push(Rejection::new(
                codes::LICENSE_NOT_ACKNOWLEDGED,
                Some(path),
                "Nobody in this project has accepted this model version's license yet.",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl JobValidator for ModelGates {
    fn name(&self) -> &'static str {
        "model_gates"
    }

    async fn validate(&self, spec: &JobSpec) -> Result<Verdict, SeamError> {
        let mut out = Vec::new();
        self.check(
            spec,
            spec.base_model_version_id,
            "/base_model_version_id",
            &mut out,
        )
        .await
        .map_err(seam)?;
        for (pointer, path) in MODEL_FIELDS {
            // A malformed id is P24's to report.
            if let Some(id) = spec
                .resolved
                .pointer(pointer)
                .and_then(Value::as_str)
                .and_then(|s| s.parse::<ModelVersionId>().ok())
            {
                self.check(spec, id.as_uuid(), path, &mut out)
                    .await
                    .map_err(seam)?;
            }
        }
        Ok(if out.is_empty() {
            Verdict::Abstain
        } else {
            Verdict::Reject(out)
        })
    }
}

/// Per live engine version (`name@version`) and method: whether the version's facts fit,
/// and the model problems when they don't (`{"trl@1.0.0": {"sft": {"ok": true, "problems": []}}}`).
/// Hardware, datasets and settings are left out: this is about the model alone.
pub async fn compatibility(db: &Db, version: &VersionRow) -> Result<Value, SeamError> {
    let facts = facts(version);
    let candidates = crate::modules::engines::store::candidates(db, None)
        .await
        .map_err(SeamError::from)?;
    let mut out = Map::new();
    for c in &candidates {
        let enabled = c.manifest.hardware_classes();
        let mut methods = Map::new();
        for algo in &c.manifest.spec.algorithms {
            let req = Request {
                algorithm: algo.id.clone(),
                config: None,
                model: Lookup::Found(facts.clone()),
                datasets: vec![],
                reward_attached: true,
                hardware: HardwareRequest {
                    wanted: Wanted::Any,
                    gpus: 0,
                    nodes: 1,
                    gpu_mem_gib: None,
                },
                available: enabled.clone(),
                machines: None,
                resources: None,
                model_signals: BTreeMap::new(),
                rollout: false,
            };
            let mut problems: Vec<Value> = matcher::check(
                Engine {
                    manifest: &c.manifest,
                    schemas: &c.schemas,
                    enabled: &enabled,
                },
                &req,
            )
            .into_iter()
            .filter(|p| p.path.as_deref() == Some("/base_model_version_id"))
            .map(|p| json!({"code": p.code.to_string(), "message": p.message}))
            .collect();
            if version.kind.as_deref() == Some("inference_only") {
                problems.push(json!({
                    "code": codes::INFERENCE_ONLY.to_string(),
                    "message": "This model version can be used only for inference, not for training.",
                }));
            }
            methods.insert(
                algo.id.clone(),
                json!({"ok": problems.is_empty(), "problems": problems}),
            );
        }
        out.insert(format!("{}@{}", c.name, c.version), Value::Object(methods));
    }
    Ok(Value::Object(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn family_follows_the_architecture() {
        let f = |v: Value| family(&v);
        assert_eq!(
            f(json!({"architecture": "LlamaForCausalLM", "moe": null, "vision": null})),
            "decoder"
        );
        assert_eq!(
            f(
                json!({"architecture": "MixtralForCausalLM", "moe": {"num_experts": 8}, "vision": null})
            ),
            "decoder-moe"
        );
        assert_eq!(
            f(json!({"architecture": "T5ForConditionalGeneration", "vision": null})),
            "encoder-decoder"
        );
        assert_eq!(
            f(json!({"architecture": "BertForMaskedLM", "vision": null})),
            "encoder"
        );
        assert_eq!(
            f(
                json!({"architecture": "Qwen2VLForConditionalGeneration", "vision": {"model_type": "x"}})
            ),
            "vlm"
        );
        assert_eq!(f(json!({"architecture": null})), "unknown");
    }
}

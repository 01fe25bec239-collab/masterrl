//! The L0 job validator: can some registered engine version run this job?
//!
//! Candidates are the live engine versions that declare the job's method (narrowed by
//! `config.common.engine`, a name or `name@version`). With none (and no engine named) it
//! abstains, so the chain's own MRL-CAP-001 applies. Each candidate is checked with the matcher;
//! the job is accepted when one passes. Otherwise it is rejected with the problems of the
//! candidate that came closest (fewest problems, then engine name).

use super::{store, EnginesError, Records};
use crate::db::Db;
use crate::modules::jobs::{JobSpec, JobValidator, Rejection, Verdict};
use crate::seams::SeamError;
use async_trait::async_trait;
use mrl_domain::capability::codes::CapabilityRejection as Cap;
use mrl_domain::capability::manifest::HardwareClass;
use mrl_domain::capability::matcher::{
    self, Engine, HardwareRequest, Lookup, Problem, Request, Wanted,
};
use mrl_domain::ids::{DatasetVersionId, ModelVersionId};
use mrl_domain::resources::{GpuVendor, ResourceRequest, WorkloadRole};
use serde_json::Value;

/// The engine capability check (P24), registered in `jobs::chains`.
pub struct CapabilityValidator {
    db: Db,
    records: Records,
}

impl CapabilityValidator {
    /// The validator with this build's registry lookups.
    pub fn new(db: Db) -> Self {
        let records = Records::registered(&db);
        Self { db, records }
    }

    /// The validator with these registry lookups (tests).
    pub fn with_records(db: Db, records: Records) -> Self {
        Self { db, records }
    }
}

/// The hardware a resource request asks for (its trainer role, else its first role).
/// `Err` is a problem with the request itself (a GPU maker no engine supports).
pub fn hardware_of(resources: &ResourceRequest) -> Result<HardwareRequest, Problem> {
    let role = resources
        .roles
        .iter()
        .find(|r| r.role == WorkloadRole::Trainer)
        .or_else(|| resources.roles.first());
    let Some(role) = role else {
        return Ok(HardwareRequest {
            wanted: Wanted::Class(HardwareClass::Cpu),
            gpus: 0,
            nodes: 1,
            gpu_mem_gib: None,
        });
    };
    let wanted = if role.gpu_count == 0 {
        Wanted::Class(HardwareClass::Cpu)
    } else {
        match role.gpu.vendor {
            None => Wanted::AnyGpu,
            Some(GpuVendor::Nvidia) => Wanted::Class(HardwareClass::Nvidia),
            Some(GpuVendor::Amd) => Wanted::Class(HardwareClass::Amd),
            Some(GpuVendor::Apple) => Wanted::Class(HardwareClass::Apple),
            Some(GpuVendor::Intel) => {
                return Err(Problem {
                    code: Cap::NoEngineForHardware.code(),
                    path: Some("/resources".into()),
                    message: "No engine here runs on Intel GPUs yet.".into(),
                    hint: None,
                })
            }
        }
    };
    Ok(HardwareRequest {
        wanted,
        gpus: role.gpu_count,
        nodes: role.replicas,
        gpu_mem_gib: role
            .gpu
            .min_vram_bytes
            .map(|b| b as f64 / f64::from(1u32 << 30)),
    })
}

/// A matcher problem as a job rejection (the manifest's hint, else the catalog's).
pub fn rejection(p: Problem) -> Rejection {
    let mut r = Rejection::new(p.code, p.path.as_deref(), p.message);
    if let Some(h) = p.hint {
        r.hint = h;
    }
    r
}

/// `config.common.engine`: `(name, version)` the submission asks for.
fn engine_choice(config: &Value) -> Option<(String, Option<String>)> {
    let s = config.pointer("/common/engine")?.as_str()?;
    Some(match s.split_once('@') {
        Some((n, v)) => (n.to_owned(), Some(v.to_owned())),
        None => (s.to_owned(), None),
    })
}

impl CapabilityValidator {
    async fn check(&self, spec: &JobSpec) -> Result<Verdict, EnginesError> {
        let mut candidates = store::candidates(&self.db, Some(&spec.algorithm_id)).await?;
        let choice = engine_choice(&spec.resolved);
        if let Some((name, version)) = &choice {
            candidates
                .retain(|c| &c.name == name && version.as_ref().is_none_or(|v| &c.version == v));
        }
        if candidates.is_empty() {
            // No engine offers the method: not this validator's job to judge. The chain
            // rejects it with MRL-CAP-001 when nobody vouches (and adds nothing when another
            // member already rejected it).
            let Some((n, v)) = &choice else {
                return Ok(Verdict::Abstain);
            };
            let named = v.as_ref().map_or_else(|| n.clone(), |v| format!("{n} {v}"));
            return Ok(Verdict::Reject(vec![Rejection::new(
                Cap::NoEngineForMethod.code(),
                Some("/config/common/engine"),
                format!("Engine {named} has no method `{}` here.", spec.algorithm_id),
            )]));
        }
        let hardware = match hardware_of(&spec.resources) {
            Ok(h) => h,
            Err(p) => return Ok(Verdict::Reject(vec![rejection(p)])),
        };
        let org = spec.org_id;
        let model = match self
            .records
            .model(org, ModelVersionId::from_uuid(spec.base_model_version_id))
            .await
        {
            Ok(Some(f)) => Lookup::Found(f),
            Ok(None) => Lookup::Missing,
            Err(e) => return Err(EnginesError::Seam(e)),
        };
        let mut datasets = Vec::with_capacity(spec.dataset_version_ids.len());
        for d in &spec.dataset_version_ids {
            datasets.push(
                match self
                    .records
                    .dataset(org, DatasetVersionId::from_uuid(*d))
                    .await
                    .map_err(EnginesError::Seam)?
                {
                    Some(f) => Lookup::Found(f),
                    None => Lookup::Missing,
                },
            );
        }
        let available = store::available_classes(&self.db, Some(org), None).await?;
        let req = Request {
            algorithm: spec.algorithm_id.clone(),
            config: Some(spec.resolved.clone()),
            model,
            datasets,
            reward_attached: spec.reward_version_id.is_some(),
            hardware,
            available,
        };
        let mut best: Option<Vec<Problem>> = None;
        for c in &candidates {
            let problems = matcher::check(
                Engine {
                    manifest: &c.manifest,
                    schemas: &c.schemas,
                    enabled: &c.enabled,
                },
                &req,
            );
            if problems.is_empty() {
                return Ok(Verdict::Accept);
            }
            if best.as_ref().is_none_or(|b| problems.len() < b.len()) {
                best = Some(problems);
            }
        }
        Ok(Verdict::Reject(
            best.unwrap_or_default()
                .into_iter()
                .map(rejection)
                .collect(),
        ))
    }
}

#[async_trait]
impl JobValidator for CapabilityValidator {
    fn name(&self) -> &'static str {
        "engines"
    }

    async fn validate(&self, spec: &JobSpec) -> Result<Verdict, SeamError> {
        self.check(spec).await.map_err(SeamError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_domain::resources::{GpuConstraints, RoleRequest};

    fn role(role: WorkloadRole, gpus: u32, vendor: Option<GpuVendor>) -> RoleRequest {
        RoleRequest {
            role,
            replicas: 2,
            gpu_count: gpus,
            gpu: GpuConstraints {
                vendor,
                min_vram_bytes: Some(24 << 30),
                platform: None,
            },
            node_ids: vec![],
            cpu_millis: 1000,
            mem_bytes: 1 << 30,
        }
    }

    #[test]
    fn hardware_comes_from_the_trainer_role() {
        let r = ResourceRequest {
            roles: vec![
                role(WorkloadRole::Rollout, 1, Some(GpuVendor::Amd)),
                role(WorkloadRole::Trainer, 4, Some(GpuVendor::Nvidia)),
            ],
        };
        let h = hardware_of(&r).unwrap();
        assert_eq!(h.wanted, Wanted::Class(HardwareClass::Nvidia));
        assert_eq!((h.gpus, h.nodes, h.gpu_mem_gib), (4, 2, Some(24.0)));
        let cpu = ResourceRequest {
            roles: vec![role(WorkloadRole::Trainer, 0, Some(GpuVendor::Nvidia))],
        };
        assert_eq!(
            hardware_of(&cpu).unwrap().wanted,
            Wanted::Class(HardwareClass::Cpu)
        );
        let any = ResourceRequest {
            roles: vec![role(WorkloadRole::Trainer, 1, None)],
        };
        assert_eq!(hardware_of(&any).unwrap().wanted, Wanted::AnyGpu);
        let intel = ResourceRequest {
            roles: vec![role(WorkloadRole::Trainer, 1, Some(GpuVendor::Intel))],
        };
        assert_eq!(
            hardware_of(&intel).unwrap_err().code.to_string(),
            "MRL-CAP-018"
        );
    }

    #[test]
    fn engine_choice_reads_name_and_version() {
        let c = serde_json::json!({"common": {"engine": "trl@1.2.0"}});
        assert_eq!(
            engine_choice(&c),
            Some(("trl".into(), Some("1.2.0".into())))
        );
        assert_eq!(
            engine_choice(&serde_json::json!({"common": {"engine": "fake"}})),
            Some(("fake".into(), None))
        );
        assert_eq!(engine_choice(&serde_json::json!({})), None);
    }

    #[test]
    fn manifest_hints_win_over_the_catalog() {
        let p = Problem {
            code: Cap::KtoBatchTooSmall.code(),
            path: None,
            message: "m".into(),
            hint: Some("Set the batch size to 2.".into()),
        };
        assert_eq!(rejection(p).hint, "Set the batch size to 2.");
        let p = Problem {
            code: Cap::KtoBatchTooSmall.code(),
            path: None,
            message: "m".into(),
            hint: None,
        };
        assert!(!rejection(p).hint.is_empty());
    }
}

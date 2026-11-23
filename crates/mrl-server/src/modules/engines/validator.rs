//! The L0 job validator: can some registered engine version run this job?
//!
//! Candidates are the live engine versions that declare the job's method (narrowed by
//! `config.common.engine`, a name or `name@version`). With none (and no engine named) it
//! abstains, so the chain's own MRL-CAP-001 applies. Each candidate is checked with the matcher;
//! the job is accepted when one passes. Otherwise it is rejected with the problems of the
//! candidate that came closest (fewest problems, then engine name).

use super::{store, EnginesError, RecordScope, Records};
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
use uuid::Uuid;

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

fn malformed(config: &Value) -> Option<Problem> {
    let path = ["common", "engine", "inputs", "rollout"]
        .into_iter()
        .find_map(|key| {
            config
                .get(key)
                .filter(|v| !v.is_object())
                .map(|_| format!("/config/{key}"))
        })
        .or_else(|| {
            config
                .pointer("/common/engine")
                .filter(|v| {
                    v.as_str().is_none_or(|s| {
                        s.is_empty()
                            || s.split_once('@')
                                .is_some_and(|(n, v)| n.is_empty() || v.is_empty())
                    })
                })
                .map(|_| "/config/common/engine".to_owned())
        })?;
    Some(Problem {
        code: matcher::codes::FIELD_VALUE,
        path: Some(path),
        message: "Use the engine settings format shown by the form.".into(),
        hint: None,
    })
}

/// What the capability check decides for one job.
#[derive(Debug, Clone, PartialEq)]
pub enum Pick {
    /// No live engine version offers the method and the submission names no engine: the
    /// chain decides (MRL-CAP-001 when nobody vouches).
    NoEngine,
    /// This engine version (`engine_versions.id`) runs the job: the first candidate, in
    /// engine name order, that passes every check.
    Engine(Uuid),
    /// No candidate can run it: the problems of the one that came closest.
    Rejected(Vec<Rejection>),
}

impl CapabilityValidator {
    /// The engine version this job runs on: the same decision [`JobValidator::validate`]
    /// makes (`Engine` is its `Accept`, `Rejected` its `Reject`, `NoEngine` its `Abstain`).
    /// Candidates are the live versions declaring `spec.algorithm_id`, narrowed by
    /// `config.common.engine` (`name` or `name@version`).
    pub async fn pick(&self, spec: &JobSpec) -> Result<Pick, EnginesError> {
        let choice = engine_choice(&spec.resolved);
        if let Some(problems) = malformed(&spec.resolved) {
            return Ok(Pick::Rejected(vec![rejection(problems)]));
        }
        let mut candidates = store::candidates(
            &self.db,
            choice.is_none().then_some(spec.algorithm_id.as_str()),
        )
        .await?;
        if let Some((name, version)) = &choice {
            candidates
                .retain(|c| &c.name == name && version.as_ref().is_none_or(|v| &c.version == v));
        }
        if candidates.is_empty() {
            // No engine offers the method: not this validator's job to judge. The chain
            // rejects it with MRL-CAP-001 when nobody vouches (and adds nothing when another
            // member already rejected it).
            let Some((n, v)) = &choice else {
                return Ok(Pick::NoEngine);
            };
            let named = v.as_ref().map_or_else(|| n.clone(), |v| format!("{n} {v}"));
            return Ok(Pick::Rejected(vec![Rejection::new(
                Cap::NoEngineForMethod.code(),
                Some("/config/common/engine"),
                format!("Engine {named} has no method `{}` here.", spec.algorithm_id),
            )]));
        }
        let hardware = match hardware_of(&spec.resources) {
            Ok(h) => h,
            Err(p) => return Ok(Pick::Rejected(vec![rejection(p)])),
        };
        let scope = RecordScope {
            org_id: spec.org_id,
            project_id: spec.project_id,
        };
        let model = match self
            .records
            .model(
                &scope,
                ModelVersionId::from_uuid(spec.base_model_version_id),
            )
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
                    .dataset(&scope, DatasetVersionId::from_uuid(*d))
                    .await
                    .map_err(EnginesError::Seam)?
                {
                    Some(f) => Lookup::Found(f),
                    None => Lookup::Missing,
                },
            );
        }
        let mut signals = std::collections::BTreeMap::new();
        for (signal, path) in [
            ("reference", "/inputs/reference_model_version_id"),
            ("critic", "/inputs/critic_model_version_id"),
            ("teacher", "/common/teacher_model_version_id"),
        ] {
            if let Some(raw) = spec.resolved.pointer(path) {
                let Some(id) = raw.as_str().and_then(|v| v.parse::<ModelVersionId>().ok()) else {
                    return Ok(Pick::Rejected(vec![Rejection::new(
                        matcher::codes::FIELD_VALUE,
                        Some(&format!("/config{path}")),
                        "Use a model version id.",
                    )]));
                };
                let found = self
                    .records
                    .model(&scope, id)
                    .await
                    .map_err(EnginesError::Seam)?;
                signals.insert(
                    signal.to_owned(),
                    found.map_or(Lookup::Missing, Lookup::Found),
                );
            }
        }
        let rollout = spec
            .resolved
            .get("rollout")
            .is_some_and(|value| value.is_object());
        let machines = store::machines(&self.db, Some(spec.org_id), None).await?;
        let available = machines.iter().flat_map(|m| m.classes()).collect();
        let req = Request {
            algorithm: spec.algorithm_id.clone(),
            config: Some(spec.resolved.clone()),
            model,
            datasets,
            reward_attached: spec.reward_version_id.is_some(),
            hardware,
            available,
            model_signals: signals,
            rollout,
            resources: Some(spec.resources.clone()),
            machines: Some(machines),
        };
        let mut best: Option<Vec<Problem>> = None;
        for c in &candidates {
            let mut problems = matcher::check(
                Engine {
                    manifest: &c.manifest,
                    schemas: &c.schemas,
                    enabled: &c.enabled,
                },
                &req,
            );
            if choice.is_some() {
                for problem in &mut problems {
                    if problem.code == Cap::NoEngineForMethod.code()
                        || problem.code == Cap::PpoUnavailableOnTrl.code()
                    {
                        problem.path = Some("/config/common/engine".into());
                    }
                }
            }
            if problems.is_empty() {
                return Ok(Pick::Engine(c.id));
            }
            if best.as_ref().is_none_or(|b| problems.len() < b.len()) {
                best = Some(problems);
            }
        }
        Ok(Pick::Rejected(
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
        // The chosen engine version is recorded on the run (`runs.engine_version_id`).
        Ok(match self.pick(spec).await.map_err(SeamError::from)? {
            Pick::NoEngine => Verdict::Abstain,
            Pick::Engine(id) => Verdict::AcceptEngine(id),
            Pick::Rejected(r) => Verdict::Reject(r),
        })
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

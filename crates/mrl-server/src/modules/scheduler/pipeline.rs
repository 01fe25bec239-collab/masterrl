//! The scheduler's extension points. Members are registered in `pipeline.registry.rs`.
//!
//! - [`AdmissionPolicy`] chain: may a queued job use this pool now? Every member runs in
//!   order; the first hold wins (the job stays QUEUED with that reason).
//! - [`PlacementStrategy`] registry, by name: which machine and GPUs. A pool picks one with
//!   `scheduling_policy.placement` (default `single_node_packing`).
//! - [`LaunchContributor`] chain: fills the `LaunchAttempt` (job spec, runtime, inputs,
//!   credentials) for a placed attempt, in order.
//! - [`PauseSupport`] chain: can this job's engine pause? Nobody saying yes means no.
//! - [`crate::modules::compute::ComputeProvider`]s, by pool `provider_kind`.

use super::SchedError;
use crate::modules::compute::{ComputeProvider, GpuSlot};
use crate::modules::jobs::{AttemptRow, JobRow};
use mrl_domain::failure::FailureClass;
use mrl_domain::resources::{GpuVendor, Platform, ResourceRequest};
use mrl_proto::agent::v1::{Credential, LaunchAttempt, StageItem};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// What one role of a job needs on its machine.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RoleDemand {
    /// Role name, like `trainer`.
    pub role: String,
    /// Machines for this role (only 1 is placed today).
    pub replicas: u32,
    /// GPUs on the machine.
    pub gpu_count: u32,
    /// Required GPU maker (`nvidia`, …).
    pub vendor: Option<String>,
    /// Smallest GPU memory per GPU.
    pub min_vram_bytes: Option<u64>,
    /// Required platform.
    pub platform: Option<Platform>,
    /// Allowed architectures, like `sm_80` (empty = any).
    pub arch: Vec<String>,
    /// Allowed GPU models, matched without case as a substring (empty = any).
    pub models: Vec<String>,
    /// Allowed machines (empty = any).
    pub node_ids: Vec<Uuid>,
    /// CPU, thousandths of a core.
    pub cpu_millis: u64,
    /// Memory, bytes.
    pub mem_bytes: u64,
}

impl RoleDemand {
    /// True if `g` is a GPU this role may use.
    pub fn accepts(&self, g: &GpuSlot) -> bool {
        self.vendor.as_deref().is_none_or(|v| v == g.vendor)
            && self.min_vram_bytes.is_none_or(|m| g.vram_bytes >= m)
            && (self.arch.is_empty() || self.arch.iter().any(|a| a == &g.arch))
            && (self.models.is_empty()
                || self
                    .models
                    .iter()
                    .any(|m| g.model.to_lowercase().contains(&m.to_lowercase())))
    }
}

/// Everything a job (or deployment) asks for.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Demand {
    /// One entry per role.
    pub roles: Vec<RoleDemand>,
}

/// The optional GPU fields the scheduler reads beyond [`ResourceRequest`].
#[derive(Deserialize, Default)]
struct ExtraGpu {
    #[serde(default)]
    arch: Vec<String>,
    #[serde(default)]
    models: Vec<String>,
}

#[derive(Deserialize)]
struct ExtraRole {
    #[serde(default)]
    gpu: ExtraGpu,
}

#[derive(Deserialize)]
struct Extra {
    #[serde(default)]
    roles: Vec<ExtraRole>,
}

impl Demand {
    /// From a stored `resources` request. `None`: the request can't be read.
    pub fn from_request(v: &serde_json::Value) -> Option<Self> {
        let req: ResourceRequest = serde_json::from_value(v.clone()).ok()?;
        req.validate().ok()?;
        let extra: Option<Extra> = serde_json::from_value(v.clone()).ok();
        let extra = extra.map(|e| e.roles).unwrap_or_default();
        let roles = req
            .roles
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let x = extra.get(i);
                RoleDemand {
                    role: r.role.as_str().to_owned(),
                    replicas: r.replicas,
                    gpu_count: r.gpu_count,
                    vendor: r.gpu.vendor.map(|v| vendor_name(v).to_owned()),
                    min_vram_bytes: r.gpu.min_vram_bytes,
                    platform: r.gpu.platform,
                    arch: x.map(|x| x.gpu.arch.clone()).unwrap_or_default(),
                    models: x.map(|x| x.gpu.models.clone()).unwrap_or_default(),
                    node_ids: r.node_ids.iter().map(|n| n.as_uuid()).collect(),
                    cpu_millis: r.cpu_millis,
                    mem_bytes: r.mem_bytes,
                }
            })
            .collect();
        Some(Self { roles })
    }

    /// Total GPUs on the machine.
    pub fn gpus(&self) -> u32 {
        self.roles
            .iter()
            .fold(0, |s, r| s.saturating_add(r.gpu_count))
    }

    /// Total CPU on the machine.
    pub fn cpu_millis(&self) -> u64 {
        self.roles
            .iter()
            .fold(0, |s, r| s.saturating_add(r.cpu_millis))
    }

    /// Total memory on the machine.
    pub fn mem_bytes(&self) -> u64 {
        self.roles
            .iter()
            .fold(0, |s, r| s.saturating_add(r.mem_bytes))
    }
}

fn vendor_name(v: GpuVendor) -> &'static str {
    match v {
        GpuVendor::Nvidia => "nvidia",
        GpuVendor::Amd => "amd",
        GpuVendor::Apple => "apple",
        GpuVendor::Intel => "intel",
    }
}

/// A chosen machine and GPUs.
#[derive(Debug, Clone, PartialEq)]
pub struct Placement {
    /// The machine.
    pub node_id: Uuid,
    /// GPUs per role, in the demand's role order.
    pub roles: Vec<(String, Vec<GpuSlot>)>,
    /// CPU to hold.
    pub cpu_millis: u64,
    /// Memory to hold.
    pub mem_bytes: u64,
}

impl Placement {
    /// Every chosen GPU.
    pub fn gpu_ids(&self) -> Vec<Uuid> {
        self.roles
            .iter()
            .flat_map(|(_, g)| g.iter().map(|g| g.id))
            .collect()
    }

    /// Every chosen GPU index, sorted.
    pub fn gpu_indices(&self) -> Vec<u32> {
        let mut v: Vec<u32> = self
            .roles
            .iter()
            .flat_map(|(_, g)| g.iter().map(|g| g.index))
            .collect();
        v.sort_unstable();
        v
    }
}

/// What a placement strategy decided.
#[derive(Debug, Clone, PartialEq)]
pub enum Fit {
    /// Hold these.
    Placed(Placement),
    /// Some machine could run it, but not now (busy, offline, draining). Plain reason.
    Busy(String),
    /// No machine in the pool could ever run it as asked. Plain reason.
    Never(String),
}

/// Preferences that don't rule a machine out.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PlacementHints {
    /// Machines an earlier attempt of the job failed on (its failure class asks for another
    /// machine); used only when nothing else fits.
    pub avoid_nodes: Vec<Uuid>,
}

/// Picks a machine and GPUs.
pub trait PlacementStrategy: Send + Sync {
    /// Registry name, like `single_node_packing`.
    fn name(&self) -> &'static str;
    /// Decides for `demand` over the pool's machines.
    fn place(
        &self,
        demand: &Demand,
        nodes: &[crate::modules::compute::NodeCapacity],
        hints: &PlacementHints,
    ) -> Fit;
}

/// A pool, as admission sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolInfo {
    /// Id.
    pub id: Uuid,
    /// Owning org (`None`: a platform pool every org may use).
    pub org_id: Option<Uuid>,
    /// Name.
    pub name: String,
    /// `agent`, `kubernetes`, ….
    pub provider_kind: String,
    /// Flags, like `paused`.
    pub flags: Vec<String>,
    /// `scheduling_policy` (JSON).
    pub scheduling_policy: serde_json::Value,
    /// Attempt lease length.
    pub lease_ttl: std::time::Duration,
}

/// A queued job and where it stands.
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedJob {
    /// The job.
    pub job: JobRow,
    /// Queued jobs of the same project ahead of it.
    pub ahead_in_project: u32,
}

/// An admission verdict.
#[derive(Debug, Clone, PartialEq)]
pub enum Admission {
    /// May use the pool now.
    Admit,
    /// Waits; `code` is a registry code and `message` plain words for the user.
    Hold {
        /// Like `MRL-QUOTA-211`.
        code: String,
        /// Shown as the job's reason.
        message: String,
    },
}

/// One member of the admission chain.
#[async_trait::async_trait]
pub trait AdmissionPolicy: Send + Sync {
    /// Name for logs.
    fn name(&self) -> &'static str;
    /// Decides for `job` on `pool`.
    async fn admit(&self, job: &QueuedJob, pool: &PoolInfo) -> Result<Admission, SchedError>;
}

/// What a launch carries; contributors fill it in order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LaunchPlan {
    /// The command (attempt ref, roles, allocation and lease are set before the chain).
    pub launch: LaunchAttempt,
    /// Inputs to stage before the launch (`StageArtifacts`).
    pub stage: Vec<StageItem>,
    /// Credentials to hand over before the launch (`UpdateCredentials`).
    pub credentials: Vec<Credential>,
}

/// Why a launch could not be built.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LaunchError {
    /// Try again later (counts as a short infrastructure problem).
    #[error("{0}")]
    Transient(String),
    /// This attempt can't run; fail it with this class.
    #[error("{1}")]
    Fail(FailureClass, String),
}

/// One member of the launch chain.
#[async_trait::async_trait]
pub trait LaunchContributor: Send + Sync {
    /// Name for logs.
    fn name(&self) -> &'static str;
    /// Adds to `plan` for `attempt` of `job` placed as `placement`.
    async fn contribute(
        &self,
        job: &JobRow,
        attempt: &AttemptRow,
        placement: &Placement,
        plan: &mut LaunchPlan,
    ) -> Result<(), LaunchError>;
}

/// Says whether a job's engine can pause and resume.
#[async_trait::async_trait]
pub trait PauseSupport: Send + Sync {
    /// Name for logs.
    fn name(&self) -> &'static str;
    /// `Some(true)`: it can; `Some(false)`: it can't; `None`: not this member's engine.
    async fn can_pause(&self, job: &JobRow) -> Result<Option<bool>, SchedError>;
}

/// Every registered member.
#[derive(Clone)]
pub struct Pipeline {
    /// Admission chain, in order.
    pub admission: Vec<Arc<dyn AdmissionPolicy>>,
    /// Placement strategies by name.
    pub placement: HashMap<&'static str, Arc<dyn PlacementStrategy>>,
    /// Launch chain, in order.
    pub launch: Vec<Arc<dyn LaunchContributor>>,
    /// Pause support chain.
    pub pause: Vec<Arc<dyn PauseSupport>>,
    /// Providers by `provider_kind`.
    pub providers: HashMap<&'static str, Arc<dyn ComputeProvider>>,
}

/// The placement strategy pools use unless their `scheduling_policy` names another.
pub const DEFAULT_PLACEMENT: &str = "single_node_packing";

impl Pipeline {
    /// A pipeline with these members.
    pub fn new(
        admission: Vec<Arc<dyn AdmissionPolicy>>,
        placement: Vec<Arc<dyn PlacementStrategy>>,
        launch: Vec<Arc<dyn LaunchContributor>>,
        pause: Vec<Arc<dyn PauseSupport>>,
        providers: Vec<Arc<dyn ComputeProvider>>,
    ) -> Self {
        Self {
            admission,
            placement: placement.into_iter().map(|p| (p.name(), p)).collect(),
            launch,
            pause,
            providers: providers.into_iter().map(|p| (p.kind(), p)).collect(),
        }
    }

    /// The strategy `pool` asks for; an unknown name falls back to the default.
    pub fn strategy(&self, pool: &PoolInfo) -> Option<Arc<dyn PlacementStrategy>> {
        let name = pool.scheduling_policy["placement"]
            .as_str()
            .unwrap_or(DEFAULT_PLACEMENT);
        self.placement
            .get(name)
            .or_else(|| self.placement.get(DEFAULT_PLACEMENT))
            .cloned()
    }

    /// The first hold of the admission chain, or admit.
    pub async fn admit(&self, job: &QueuedJob, pool: &PoolInfo) -> Result<Admission, SchedError> {
        for p in &self.admission {
            let a = p.admit(job, pool).await?;
            if a != Admission::Admit {
                return Ok(a);
            }
        }
        Ok(Admission::Admit)
    }

    /// True only if some member says the job's engine can pause and none says it can't.
    pub async fn can_pause(&self, job: &JobRow) -> Result<bool, SchedError> {
        let mut yes = false;
        for p in &self.pause {
            match p.can_pause(job).await? {
                Some(false) => return Ok(false),
                Some(true) => yes = true,
                None => {}
            }
        }
        Ok(yes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn demand_reads_the_request_and_the_extra_gpu_fields() {
        let d = Demand::from_request(&json!({"roles": [{
            "role": "trainer", "replicas": 1, "gpu_count": 2,
            "gpu": {"vendor": "nvidia", "min_vram_bytes": 1000, "arch": ["sm_80"], "models": ["A100"]},
            "cpu_millis": 500, "mem_bytes": 2048
        }]}))
        .unwrap_or_default();
        assert_eq!(d.roles.len(), 1);
        let r = &d.roles[0];
        assert_eq!(
            (r.vendor.as_deref(), r.min_vram_bytes, r.gpu_count),
            (Some("nvidia"), Some(1000), 2)
        );
        assert_eq!(
            (r.arch.clone(), r.models.clone()),
            (vec!["sm_80".to_owned()], vec!["A100".to_owned()])
        );
        assert_eq!((d.gpus(), d.cpu_millis(), d.mem_bytes()), (2, 500, 2048));
        // Unreadable or invalid requests give None.
        assert_eq!(Demand::from_request(&json!({"roles": []})), None);
        assert_eq!(Demand::from_request(&json!("x")), None);
    }

    #[test]
    fn a_role_accepts_matching_gpus_only() {
        let g = GpuSlot {
            id: Uuid::nil(),
            index: 0,
            vendor: "nvidia".into(),
            model: "NVIDIA A100-SXM4-80GB".into(),
            arch: "sm_80".into(),
            vram_bytes: 80 << 30,
            nvlink_peers: vec![],
            free: true,
        };
        let any = RoleDemand::default();
        assert!(any.accepts(&g));
        let ok = RoleDemand {
            vendor: Some("nvidia".into()),
            min_vram_bytes: Some(40 << 30),
            arch: vec!["sm_90".into(), "sm_80".into()],
            models: vec!["a100".into()],
            ..RoleDemand::default()
        };
        assert!(ok.accepts(&g));
        for bad in [
            RoleDemand {
                vendor: Some("amd".into()),
                ..RoleDemand::default()
            },
            RoleDemand {
                min_vram_bytes: Some(81 << 30),
                ..RoleDemand::default()
            },
            RoleDemand {
                arch: vec!["sm_90".into()],
                ..RoleDemand::default()
            },
            RoleDemand {
                models: vec!["H100".into()],
                ..RoleDemand::default()
            },
        ] {
            assert!(!bad.accepts(&g), "{bad:?}");
        }
    }
}

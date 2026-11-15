//! Resource requests, estimate verdicts and reservation ownership.

use serde::{Deserialize, Serialize};

use crate::ids::{DeploymentId, JobId, NodeId, ReservationId};
use crate::state::{
    Reservation as ReservationMachine, ReservationEvent, ReservationState, StateMachine,
    TransitionError,
};

/// GPU maker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GpuVendor {
    /// NVIDIA (CUDA).
    Nvidia,
    /// AMD (ROCm).
    Amd,
    /// Apple (Metal).
    Apple,
    /// Intel.
    Intel,
}

/// Where a node runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Platform {
    /// Linux on the machine itself.
    Linux,
    /// Linux inside WSL2 on Windows.
    Wsl2,
    /// Windows.
    Windows,
    /// macOS on Apple silicon.
    AppleSilicon,
}

/// Which GPUs a role may use. Every field left empty matches any GPU.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuConstraints {
    /// Smallest GPU memory (VRAM) per device, in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_vram_bytes: Option<u64>,
    /// Required GPU maker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vendor: Option<GpuVendor>,
    /// Required node platform.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<Platform>,
}

/// What a group of ranks does in a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadRole {
    /// Trains the model.
    Trainer,
    /// Generates rollouts (for example a vLLM server).
    Rollout,
    /// Serves a reward model.
    Reward,
    /// Ray head process.
    RayHead,
    /// Ray worker process.
    RayWorker,
    /// Any other helper service.
    Service,
}

impl WorkloadRole {
    /// The snake-case name, like `trainer`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trainer => "trainer",
            Self::Rollout => "rollout",
            Self::Reward => "reward",
            Self::RayHead => "ray_head",
            Self::RayWorker => "ray_worker",
            Self::Service => "service",
        }
    }
}

impl std::fmt::Display for WorkloadRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Resources for one role (one rank group) of a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleRequest {
    /// What this group does.
    pub role: WorkloadRole,
    /// Number of nodes (gang members).
    pub replicas: u32,
    /// GPUs on each node.
    pub gpu_count: u32,
    /// Which GPUs qualify.
    #[serde(default)]
    pub gpu: GpuConstraints,
    /// Run only on these nodes (empty = any node in the pool).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub node_ids: Vec<NodeId>,
    /// CPU per node, in thousandths of a core.
    pub cpu_millis: u64,
    /// Memory per node, in bytes.
    pub mem_bytes: u64,
}

/// Everything a job asks the scheduler for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRequest {
    /// One entry per role; each role appears once.
    pub roles: Vec<RoleRequest>,
}

/// Why a resource request can never be placed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceRequestError {
    /// The request lists no roles.
    #[error("the request needs at least one role, like `trainer`")]
    NoRoles,
    /// A role asks for zero nodes.
    #[error("the `{0}` role needs at least 1 node")]
    NoReplicas(WorkloadRole),
    /// A role appears twice.
    #[error("the `{0}` role appears more than once")]
    DuplicateRole(WorkloadRole),
    /// A role lists the same node twice.
    #[error("the `{role}` role lists node `{node}` more than once")]
    DuplicateNode {
        /// The role.
        role: WorkloadRole,
        /// The repeated node.
        node: NodeId,
    },
    /// A role pins fewer distinct nodes than it needs.
    #[error("the `{role}` role needs {replicas} nodes but allows only {pinned}")]
    TooFewNodes {
        /// The role.
        role: WorkloadRole,
        /// Nodes it needs.
        replicas: u32,
        /// Nodes it is pinned to.
        pinned: usize,
    },
}

impl ResourceRequest {
    /// Check the rules a request must follow before it is queued.
    pub fn validate(&self) -> Result<(), ResourceRequestError> {
        if self.roles.is_empty() {
            return Err(ResourceRequestError::NoRoles);
        }
        for (i, r) in self.roles.iter().enumerate() {
            if r.replicas == 0 {
                return Err(ResourceRequestError::NoReplicas(r.role));
            }
            if self.roles[..i].iter().any(|o| o.role == r.role) {
                return Err(ResourceRequestError::DuplicateRole(r.role));
            }
            for (j, node) in r.node_ids.iter().enumerate() {
                if r.node_ids[..j].contains(node) {
                    return Err(ResourceRequestError::DuplicateNode {
                        role: r.role,
                        node: *node,
                    });
                }
            }
            // ponytail: O(n²) duplicate scan; pinned lists are a handful of nodes.
            let pinned = r.node_ids.len();
            if pinned > 0 && pinned < r.replicas as usize {
                return Err(ResourceRequestError::TooFewNodes {
                    role: r.role,
                    replicas: r.replicas,
                    pinned,
                });
            }
        }
        Ok(())
    }

    /// Total GPUs across every role and node (saturating).
    pub fn total_gpus(&self) -> u32 {
        self.roles.iter().fold(0u32, |sum, r| {
            sum.saturating_add(r.replicas.saturating_mul(r.gpu_count))
        })
    }
}

/// How well a job is expected to fit the chosen hardware.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EstimateVerdict {
    /// Fits with room to spare.
    Fits,
    /// Fits, but with little memory left.
    Tight,
    /// Needs more memory than the hardware has.
    DoesNotFit,
    /// The app could not estimate this setup.
    Unknown,
}

/// What a reservation is held for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReservationOwnerKind {
    /// A training or import job.
    Job,
    /// A model deployment.
    Deployment,
}

/// The job or deployment a reservation is held for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "owner_kind", content = "owner_id", rename_all = "snake_case")]
pub enum ReservationOwner {
    /// Held for a job.
    Job(JobId),
    /// Held for a deployment.
    Deployment(DeploymentId),
}

impl ReservationOwner {
    /// The kind of owner.
    pub const fn kind(&self) -> ReservationOwnerKind {
        match self {
            Self::Job(_) => ReservationOwnerKind::Job,
            Self::Deployment(_) => ReservationOwnerKind::Deployment,
        }
    }
}

/// Why a reservation change was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReservationError {
    /// The caller works from an older epoch; a newer holder took over.
    #[error("this reservation moved on to epoch {current}, so a change from epoch {given} doesn't count")]
    StaleEpoch {
        /// The reservation's epoch.
        current: u64,
        /// The epoch the caller gave.
        given: u64,
    },
    /// The state change is not allowed.
    #[error(transparent)]
    Transition(#[from] TransitionError),
}

/// A reservation: who holds it, its state and its fencing `epoch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reservation {
    /// The reservation.
    pub id: ReservationId,
    /// Who holds it.
    #[serde(flatten)]
    pub owner: ReservationOwner,
    /// Its state.
    pub state: ReservationState,
    /// Fencing token: only changes from this epoch count.
    pub epoch: u64,
}

impl Reservation {
    /// Apply `event` if `epoch` is current. Leaves the reservation unchanged on error.
    pub fn apply(
        &mut self,
        event: ReservationEvent,
        epoch: u64,
    ) -> Result<ReservationState, ReservationError> {
        if epoch != self.epoch {
            return Err(ReservationError::StaleEpoch {
                current: self.epoch,
                given: epoch,
            });
        }
        self.state = ReservationMachine::can_transition(self.state, event)?;
        Ok(self.state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn role(role: WorkloadRole, replicas: u32, gpu_count: u32) -> RoleRequest {
        RoleRequest {
            role,
            replicas,
            gpu_count,
            gpu: GpuConstraints::default(),
            node_ids: Vec::new(),
            cpu_millis: 4000,
            mem_bytes: 16 << 30,
        }
    }

    #[test]
    fn validate_rules() {
        let ok = ResourceRequest {
            roles: vec![
                role(WorkloadRole::Trainer, 2, 4),
                role(WorkloadRole::Rollout, 1, 2),
            ],
        };
        assert_eq!(ok.validate(), Ok(()));
        assert_eq!(ok.total_gpus(), 10);

        assert_eq!(
            ResourceRequest { roles: vec![] }.validate(),
            Err(ResourceRequestError::NoRoles)
        );
        let zero = ResourceRequest {
            roles: vec![role(WorkloadRole::Trainer, 0, 1)],
        };
        assert_eq!(
            zero.validate(),
            Err(ResourceRequestError::NoReplicas(WorkloadRole::Trainer))
        );
        let dup = ResourceRequest {
            roles: vec![
                role(WorkloadRole::Trainer, 1, 1),
                role(WorkloadRole::Trainer, 1, 1),
            ],
        };
        assert_eq!(
            dup.validate(),
            Err(ResourceRequestError::DuplicateRole(WorkloadRole::Trainer))
        );

        let mut pinned = role(WorkloadRole::Trainer, 2, 1);
        pinned.node_ids.push(NodeId::from_uuid(Uuid::from_u128(1)));
        let short = ResourceRequest {
            roles: vec![pinned.clone()],
        };
        let err = short.validate().map_err(|e| e.to_string());
        assert_eq!(
            err,
            Err("the `trainer` role needs 2 nodes but allows only 1".into())
        );
        let node_a = NodeId::from_uuid(Uuid::from_u128(1));
        let mut twice = pinned.clone();
        twice.node_ids.push(node_a);
        let err = ResourceRequest { roles: vec![twice] }.validate();
        assert_eq!(
            err,
            Err(ResourceRequestError::DuplicateNode {
                role: WorkloadRole::Trainer,
                node: node_a
            })
        );
        assert_eq!(
            err.map_err(|e| e.to_string()),
            Err(format!(
                "the `trainer` role lists node `{node_a}` more than once"
            ))
        );
        pinned.node_ids.push(NodeId::from_uuid(Uuid::from_u128(2)));
        assert_eq!(
            ResourceRequest {
                roles: vec![pinned]
            }
            .validate(),
            Ok(())
        );

        let huge = ResourceRequest {
            roles: vec![
                role(WorkloadRole::Trainer, u32::MAX, 2),
                role(WorkloadRole::Reward, 1, 1),
            ],
        };
        assert_eq!(huge.total_gpus(), u32::MAX);
    }

    #[test]
    fn request_serializes_plainly() {
        let mut r = role(WorkloadRole::RayHead, 1, 1);
        r.gpu = GpuConstraints {
            min_vram_bytes: Some(8 << 30),
            vendor: Some(GpuVendor::Apple),
            platform: Some(Platform::AppleSilicon),
        };
        let json =
            serde_json::to_value(ResourceRequest { roles: vec![r] }).map_err(|e| e.to_string());
        let want = serde_json::json!({"roles": [{
            "role": "ray_head", "replicas": 1, "gpu_count": 1,
            "gpu": {"min_vram_bytes": 8u64 << 30, "vendor": "apple", "platform": "apple_silicon"},
            "cpu_millis": 4000, "mem_bytes": 16u64 << 30,
        }]});
        assert_eq!(json, Ok(want));
        let verdicts = [
            EstimateVerdict::Fits,
            EstimateVerdict::Tight,
            EstimateVerdict::DoesNotFit,
            EstimateVerdict::Unknown,
        ]
        .map(|v| serde_json::to_string(&v).unwrap_or_default());
        assert_eq!(
            verdicts,
            ["\"fits\"", "\"tight\"", "\"does_not_fit\"", "\"unknown\""]
        );
        assert_eq!(
            serde_json::to_string(&Platform::Wsl2).ok(),
            Some("\"wsl2\"".into())
        );
    }

    #[test]
    fn reservation_fencing() {
        let job = JobId::from_uuid(Uuid::from_u128(9));
        let mut rsv = Reservation {
            id: ReservationId::from_uuid(Uuid::from_u128(1)),
            owner: ReservationOwner::Job(job),
            state: ReservationState::Held,
            epoch: 3,
        };
        assert_eq!(rsv.owner.kind(), ReservationOwnerKind::Job);
        assert_eq!(
            ReservationOwner::Deployment(DeploymentId::from_uuid(Uuid::nil())).kind(),
            ReservationOwnerKind::Deployment
        );

        let stale = rsv.apply(ReservationEvent::Activate, 2);
        assert_eq!(
            stale,
            Err(ReservationError::StaleEpoch {
                current: 3,
                given: 2
            })
        );
        assert_eq!(rsv.state, ReservationState::Held);
        assert_eq!(
            stale.map_err(|e| e.to_string()),
            Err(
                "this reservation moved on to epoch 3, so a change from epoch 2 doesn't count"
                    .into()
            )
        );

        assert_eq!(
            rsv.apply(ReservationEvent::Activate, 3),
            Ok(ReservationState::Active)
        );
        assert!(matches!(
            rsv.apply(ReservationEvent::Expire, 3),
            Err(ReservationError::Transition(_))
        ));
        assert_eq!(
            rsv.apply(ReservationEvent::Quarantine, 3),
            Ok(ReservationState::Quarantined)
        );
        assert_eq!(
            rsv.apply(ReservationEvent::ConfirmRelease, 3),
            Ok(ReservationState::Released)
        );

        let json = serde_json::to_value(rsv).map_err(|e| e.to_string());
        let want = serde_json::json!({
            "id": rsv.id.to_string(), "owner_kind": "job", "owner_id": job.to_string(),
            "state": "RELEASED", "epoch": 3,
        });
        assert_eq!(json, Ok(want));
        let back: Result<Reservation, _> = serde_json::from_value(serde_json::json!({
            "id": rsv.id.to_string(), "owner_kind": "job", "owner_id": job.to_string(),
            "state": "RELEASED", "epoch": 3,
        }));
        assert_eq!(back.ok(), Some(rsv));
    }
}

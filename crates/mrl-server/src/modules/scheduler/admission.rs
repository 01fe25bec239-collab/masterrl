//! The default admission chain: pool membership, pool flags, queued jobs per project.

use super::codes;
use super::pipeline::{Admission, AdmissionPolicy, PoolInfo, QueuedJob};
use super::SchedError;

fn hold(code: mrl_domain::errors::ErrorCode, message: impl Into<String>) -> Admission {
    Admission::Hold {
        code: code.to_string(),
        message: message.into(),
    }
}

/// A job may use its org's pools and platform pools (no org), and only the pool it was
/// placed in before, if any.
pub struct PoolMembership;

#[async_trait::async_trait]
impl AdmissionPolicy for PoolMembership {
    fn name(&self) -> &'static str {
        "pool_membership"
    }

    async fn admit(&self, job: &QueuedJob, pool: &PoolInfo) -> Result<Admission, SchedError> {
        let member = pool.org_id.is_none_or(|o| o == job.job.org_id);
        let pinned = job.job.pool_id.is_none_or(|p| p == pool.id);
        Ok(if member && pinned {
            Admission::Admit
        } else {
            hold(
                codes::NO_POOL,
                "No pool this project may use has room for it. Ask an admin to open a pool to your organization.",
            )
        })
    }
}

/// A pool flagged `paused` takes no new work.
pub struct PoolFlags;

/// The flag that stops new work in a pool.
pub const PAUSED_FLAG: &str = "paused";

#[async_trait::async_trait]
impl AdmissionPolicy for PoolFlags {
    fn name(&self) -> &'static str {
        "pool_flags"
    }

    async fn admit(&self, _job: &QueuedJob, pool: &PoolInfo) -> Result<Admission, SchedError> {
        Ok(if pool.flags.iter().any(|f| f == PAUSED_FLAG) {
            hold(
                codes::POOL_PAUSED,
                format!(
                    "The pool `{}` is paused and starts no new work. It waits until an admin resumes the pool.",
                    pool.name
                ),
            )
        } else {
            Admission::Admit
        })
    }
}

/// Only the first `max` queued jobs of a project are placed; later ones wait their turn.
pub struct MaxQueuedPerProject {
    /// The limit.
    pub max: u32,
}

#[async_trait::async_trait]
impl AdmissionPolicy for MaxQueuedPerProject {
    fn name(&self) -> &'static str {
        "max_queued_per_project"
    }

    async fn admit(&self, job: &QueuedJob, _pool: &PoolInfo) -> Result<Admission, SchedError> {
        Ok(if job.ahead_in_project >= self.max {
            hold(
                codes::PROJECT_QUEUE_FULL,
                format!(
                    "{} jobs of this project are ahead in the queue; at most {} are placed at a time. This one starts after earlier ones.",
                    job.ahead_in_project, self.max
                ),
            )
        } else {
            Admission::Admit
        })
    }
}

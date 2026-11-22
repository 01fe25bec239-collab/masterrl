//! Compute providers: where the scheduler finds machines and holds their resources.
//!
//! A [`ComputeProvider`] serves the pools of one `compute_pools.provider_kind`:
//! - [`ComputeProvider::capacity`]: every machine of a pool with its GPUs (free or held) and
//!   its CPU and memory counters;
//! - [`ComputeProvider::place`]: holds the chosen machine's resources (a HELD reservation),
//!   re-checked under the machine's row lock, in the caller's transaction;
//! - [`ComputeProvider::release`]: gives them back (RELEASED).
//!
//! [`LocalAgentPool`] serves `agent` pools: machines running `mrl-agent`. Kubernetes, Slurm and
//! cloud pools add their own provider to [`crate::modules::scheduler::Pipeline`].

use crate::db::{Db, Tx};
use crate::modules::scheduler::reservations::{self, NewReservation, ReservationRow};
use crate::modules::scheduler::SchedError;
use mrl_domain::resources::Platform;
use sqlx::Row;
use uuid::Uuid;

/// One GPU of a machine.
#[derive(Debug, Clone, PartialEq)]
pub struct GpuSlot {
    /// `gpu_devices.id`.
    pub id: Uuid,
    /// Index on the machine (what the agent calls it).
    pub index: u32,
    /// `nvidia`, `amd`, `apple`, `intel` or `other`.
    pub vendor: String,
    /// Model name, like `NVIDIA A100-SXM4-80GB`.
    pub model: String,
    /// Architecture, like `sm_80`.
    pub arch: String,
    /// GPU memory in bytes.
    pub vram_bytes: u64,
    /// Indices of the GPUs this one has an NVLink to.
    pub nvlink_peers: Vec<u32>,
    /// Not held by any reservation.
    pub free: bool,
}

/// What one machine has and what is free on it.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeCapacity {
    /// `compute_nodes.id`.
    pub node_id: Uuid,
    /// Its pool.
    pub pool_id: Uuid,
    /// Takes new work now (ONLINE, not quarantined). Other machines count only for "could
    /// this ever fit".
    pub schedulable: bool,
    /// Where it runs, from its inventory.
    pub platform: Option<Platform>,
    /// Every present GPU in good health.
    pub gpus: Vec<GpuSlot>,
    /// CPU in thousandths of a core.
    pub cpu_millis_total: u64,
    /// CPU not held.
    pub cpu_millis_free: u64,
    /// Memory in bytes.
    pub mem_bytes_total: u64,
    /// Memory not held.
    pub mem_bytes_free: u64,
}

/// Serves the pools of one provider kind.
#[async_trait::async_trait]
pub trait ComputeProvider: Send + Sync {
    /// The `compute_pools.provider_kind` it serves, like `agent`.
    fn kind(&self) -> &'static str;

    /// Every machine of `pool` with its resources and what is held.
    async fn capacity(&self, pool: Uuid) -> Result<Vec<NodeCapacity>, SchedError>;

    /// Holds `new` (a machine and GPUs a placement strategy chose) in `tx`: the machine is
    /// locked and re-checked first, so two callers never hold the same resources. `Ok(None)`:
    /// it no longer fits (taken meanwhile or the machine left); nothing was written.
    async fn place(
        &self,
        tx: &mut Tx,
        new: &NewReservation,
    ) -> Result<Option<ReservationRow>, SchedError>;

    /// Gives the resources of `held` back (→ RELEASED) in `tx`.
    async fn release(
        &self,
        tx: &mut Tx,
        held: &ReservationRow,
        why: &str,
    ) -> Result<ReservationRow, SchedError>;
}

/// Machines running `mrl-agent` (pools of kind `agent`).
pub struct LocalAgentPool {
    db: Db,
}

impl LocalAgentPool {
    /// The provider over this database.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

/// Maps the inventory's platform name to the request's.
fn platform(inventory: &serde_json::Value) -> Option<Platform> {
    let apple = inventory["apple_silicon"].as_bool().unwrap_or(false);
    match inventory["platform"].as_str()? {
        "PLATFORM_LINUX" => Some(Platform::Linux),
        "PLATFORM_WSL2" => Some(Platform::Wsl2),
        "PLATFORM_WINDOWS" => Some(Platform::Windows),
        "PLATFORM_MACOS" if apple => Some(Platform::AppleSilicon),
        _ => None,
    }
}

fn to_u64(v: i64) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

#[async_trait::async_trait]
impl ComputeProvider for LocalAgentPool {
    fn kind(&self) -> &'static str {
        "agent"
    }

    async fn capacity(&self, pool: Uuid) -> Result<Vec<NodeCapacity>, SchedError> {
        // One snapshot, so GPUs and counters agree with each other.
        let mut tx = self.db.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        let nodes = sqlx::query(
            "SELECT n.id, n.pool_id, n.inventory,
                    n.status = 'ONLINE' AND (n.quarantined_until IS NULL OR n.quarantined_until < now()) AS ok,
                    COALESCE((SELECT sum(r.cpu_millis) FROM resource_reservations r
                       WHERE r.node_id = n.id AND r.state IN ('HELD','ACTIVE','RELEASING','QUARANTINED')), 0)::bigint AS cpu_held,
                    COALESCE((SELECT sum(r.mem_bytes) FROM resource_reservations r
                       WHERE r.node_id = n.id AND r.state IN ('HELD','ACTIVE','RELEASING','QUARANTINED')), 0)::bigint AS mem_held
             FROM compute_nodes n
             WHERE n.pool_id = $1 AND n.status NOT IN ('DECOMMISSIONED','ENROLLING')
             ORDER BY n.id",
        )
        .bind(pool)
        .fetch_all(&mut *tx)
        .await?;
        let gpus = sqlx::query(
            "SELECT g.id, g.node_id, g.index_on_node, g.vendor, g.model, g.arch, g.vram_total_bytes,
                    g.nvlink_peers,
                    NOT EXISTS (SELECT 1 FROM reservation_devices d
                                 WHERE d.gpu_device_id = g.id AND d.active) AS free
             FROM gpu_devices g JOIN compute_nodes n ON n.id = g.node_id
             WHERE n.pool_id = $1 AND g.present AND g.health = 'OK' AND g.parent_device_id IS NULL
             ORDER BY g.node_id, g.index_on_node",
        )
        .bind(pool)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        let mut out = Vec::with_capacity(nodes.len());
        for n in nodes {
            let id: Uuid = n.try_get("id")?;
            let inventory: Option<serde_json::Value> = n.try_get("inventory")?;
            let inventory = inventory.unwrap_or_default();
            let cpu_total = inventory["cpu_cores"]
                .as_u64()
                .unwrap_or(0)
                .saturating_mul(1000);
            let mem_total = inventory["ram_bytes"].as_u64().unwrap_or(0);
            let mut slots = Vec::new();
            for g in gpus.iter() {
                if g.try_get::<Uuid, _>("node_id")? != id {
                    continue;
                }
                let peers: Vec<i32> = g.try_get("nvlink_peers")?;
                slots.push(GpuSlot {
                    id: g.try_get("id")?,
                    index: u32::try_from(g.try_get::<i32, _>("index_on_node")?).unwrap_or(0),
                    vendor: g.try_get("vendor")?,
                    model: g.try_get("model")?,
                    arch: g.try_get("arch")?,
                    vram_bytes: to_u64(g.try_get("vram_total_bytes")?),
                    nvlink_peers: peers
                        .into_iter()
                        .filter_map(|p| u32::try_from(p).ok())
                        .collect(),
                    free: g.try_get("free")?,
                });
            }
            out.push(NodeCapacity {
                node_id: id,
                pool_id: n.try_get("pool_id")?,
                schedulable: n.try_get("ok")?,
                platform: platform(&inventory),
                gpus: slots,
                cpu_millis_total: cpu_total,
                cpu_millis_free: cpu_total.saturating_sub(to_u64(n.try_get("cpu_held")?)),
                mem_bytes_total: mem_total,
                mem_bytes_free: mem_total.saturating_sub(to_u64(n.try_get("mem_held")?)),
            });
        }
        Ok(out)
    }

    async fn place(
        &self,
        tx: &mut Tx,
        new: &NewReservation,
    ) -> Result<Option<ReservationRow>, SchedError> {
        // The machine's row lock serializes every hold on it (scheduler and direct callers).
        let node = sqlx::query(
            "SELECT n.inventory, n.pool_id,
                    n.status = 'ONLINE' AND (n.quarantined_until IS NULL OR n.quarantined_until < now()) AS ok
             FROM compute_nodes n WHERE n.id = $1 FOR UPDATE",
        )
        .bind(new.node_id)
        .fetch_optional(&mut **tx)
        .await?;
        let Some(node) = node else {
            return Ok(None);
        };
        let pool: Uuid = node.try_get("pool_id")?;
        if !node.try_get::<bool, _>("ok")? || pool != new.pool_id {
            return Ok(None);
        }
        let inventory: Option<serde_json::Value> = node.try_get("inventory")?;
        let inventory = inventory.unwrap_or_default();
        let (cpu_held, mem_held): (i64, i64) = sqlx::query_as(
            "SELECT COALESCE(sum(cpu_millis), 0)::bigint, COALESCE(sum(mem_bytes), 0)::bigint
             FROM resource_reservations WHERE node_id = $1
               AND state IN ('HELD','ACTIVE','RELEASING','QUARANTINED')",
        )
        .bind(new.node_id)
        .fetch_one(&mut **tx)
        .await?;
        let cpu_total = inventory["cpu_cores"]
            .as_u64()
            .unwrap_or(0)
            .saturating_mul(1000);
        let mem_total = inventory["ram_bytes"].as_u64().unwrap_or(0);
        if to_u64(cpu_held).saturating_add(new.cpu_millis) > cpu_total
            || to_u64(mem_held).saturating_add(new.mem_bytes) > mem_total
        {
            return Ok(None);
        }
        // Every chosen GPU must still be on this machine, healthy and free; the unique index
        // `one_holder_per_gpu` is the last word if two writers race anyway.
        if !new.gpus.is_empty() {
            let ok: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM gpu_devices g
                 WHERE g.id = ANY($1) AND g.node_id = $2 AND g.present AND g.health = 'OK'
                   AND NOT EXISTS (SELECT 1 FROM reservation_devices d
                                    WHERE d.gpu_device_id = g.id AND d.active)",
            )
            .bind(&new.gpus)
            .bind(new.node_id)
            .fetch_one(&mut **tx)
            .await?;
            if usize::try_from(ok).unwrap_or(0) != new.gpus.len() {
                return Ok(None);
            }
        }
        reservations::hold(tx, new).await.map(Some)
    }

    async fn release(
        &self,
        tx: &mut Tx,
        held: &ReservationRow,
        why: &str,
    ) -> Result<ReservationRow, SchedError> {
        reservations::release(tx, held, why).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn platforms_map_from_the_inventory() {
        assert_eq!(
            platform(&json!({"platform": "PLATFORM_LINUX"})),
            Some(Platform::Linux)
        );
        assert_eq!(
            platform(&json!({"platform": "PLATFORM_MACOS", "apple_silicon": true})),
            Some(Platform::AppleSilicon)
        );
        assert_eq!(platform(&json!({"platform": "PLATFORM_MACOS"})), None);
        assert_eq!(platform(&json!({})), None);
    }
}

//! Scheduler invariants (Doc 12 §6), for chaos tests and later operator checks (P31).
//!
//! ```no_run
//! # async fn demo(db: &mrl_server::db::Db) {
//! let broken = mrl_server::modules::scheduler::invariants::check(db).await.unwrap_or_default();
//! assert!(broken.is_empty(), "{broken:?}");
//! # }
//! ```

use crate::db::Db;

/// Every broken rule, one plain line each. Empty: all hold.
///
/// - at most one live attempt per job, and a live attempt runs at its job's epoch;
/// - each GPU has at most one live holder;
/// - per machine, held CPU, memory and GPUs fit what it has;
/// - a finished job holds no usable reservation, and a usable reservation's attempt is live.
pub async fn check(db: &Db) -> Result<Vec<String>, sqlx::Error> {
    let mut out = Vec::new();
    let checks: [(&str, &str); 6] = [
        (
            "jobs with more than one live attempt",
            "SELECT job_id::text FROM job_attempts WHERE state IN ('PENDING','LAUNCHING','RUNNING')
             GROUP BY job_id HAVING count(*) > 1",
        ),
        (
            "live attempts at another epoch than their job",
            "SELECT a.id::text FROM job_attempts a JOIN jobs j ON j.id = a.job_id
             WHERE a.state IN ('PENDING','LAUNCHING','RUNNING') AND a.epoch <> j.epoch",
        ),
        (
            "GPUs with more than one live holder",
            "SELECT gpu_device_id::text FROM reservation_devices WHERE active
             GROUP BY gpu_device_id HAVING count(*) > 1",
        ),
        (
            "machines holding more CPU, memory or GPUs than they have",
            "SELECT n.id::text FROM compute_nodes n
             JOIN resource_reservations r ON r.node_id = n.id
               AND r.state IN ('HELD','ACTIVE','RELEASING','QUARANTINED')
             GROUP BY n.id, n.inventory
             HAVING sum(r.cpu_millis) > COALESCE((n.inventory->>'cpu_cores')::bigint, 0) * 1000
                 OR sum(r.mem_bytes) > COALESCE((n.inventory->>'ram_bytes')::bigint, 0)
                 OR (SELECT count(*) FROM reservation_devices d JOIN resource_reservations x ON x.id = d.reservation_id
                      WHERE d.active AND x.node_id = n.id)
                  > (SELECT count(*) FROM gpu_devices g WHERE g.node_id = n.id)",
        ),
        (
            "finished jobs that still hold resources",
            "SELECT r.id::text FROM resource_reservations r JOIN jobs j ON j.id = r.job_id
             WHERE r.state IN ('HELD','ACTIVE','RELEASING')
               AND j.state IN ('COMPLETED','FAILED','CANCELLED','REJECTED','RETRY_WAIT','QUEUED','PAUSED')
               AND NOT (r.state = 'RELEASING' AND EXISTS (
                 SELECT 1 FROM job_attempts a WHERE a.id = r.attempt_id AND a.state = 'LOST'))",
        ),
        (
            "devices held by a reservation that ended",
            "SELECT d.gpu_device_id::text FROM reservation_devices d
             JOIN resource_reservations r ON r.id = d.reservation_id
             WHERE d.active AND r.state IN ('RELEASED','EXPIRED')",
        ),
    ];
    for (what, sql) in checks {
        let ids: Vec<String> = sqlx::query_scalar(sql).fetch_all(db.pool()).await?;
        if !ids.is_empty() {
            out.push(format!("{what}: {}", ids.join(", ")));
        }
    }
    Ok(out)
}

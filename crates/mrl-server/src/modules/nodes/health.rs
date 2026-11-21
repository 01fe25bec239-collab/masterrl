//! Node health (Doc 09 §4.5), command deadlines and retention. Leader-elected: one replica runs
//! it at a time.
//!
//! - ONLINE with a heartbeat older than `degraded_after_seconds` → DEGRADED (`heartbeat_late`).
//! - ONLINE with fresh heartbeats but a health problem → DEGRADED (`health:<signals>`):
//!   `clock_skew` (heartbeat clock off by more than `max_clock_skew_seconds`), `disk_low`
//!   (free disk under `min_disk_free_bytes`), `gpu_hot` (a GPU at `gpu_hot_celsius` or above),
//!   `gpu_lost` (a GPU missing from the readings).
//! - DEGRADED → ONLINE once the heartbeat is fresh and no signal remains.
//! - ONLINE/DEGRADED older than `offline_after_seconds` → OFFLINE (plus a `node_events` row).
//!   Reconnecting brings it back ONLINE with `session_epoch + 1`.
//! - Every decision is re-evaluated, from both thresholds and the signals, with the node row
//!   locked, so a heartbeat that lands after the scan is never followed by a stale transition.
//! - CONTROL messages a sink did not take yet (`node_control_inbox`) are delivered again.
//! - PENDING/SENT commands past their deadline → EXPIRED.
//! - Retention: `node_events` 180 days, final commands 30 days, used/expired tokens 30 days.

use super::store::{self, HEARTBEAT_LATE};
use super::{Gateway, NodesError};
use crate::db::LeaderElection;
use mrl_domain::state::NodeEvent;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Runs health checks every `health_check_seconds` while this replica leads.
pub async fn run(gw: Arc<Gateway>, cancel: CancellationToken) {
    let election = LeaderElection::new(gw.db.clone(), "nodes.health", Duration::from_secs(15));
    election
        .run(cancel, |lead| {
            let gw = gw.clone();
            async move {
                let every = Duration::from_secs(gw.config.health_check_seconds);
                let mut last_retention = None::<tokio::time::Instant>;
                loop {
                    if let Err(e) = check_once(&gw).await {
                        tracing::warn!(code = e.code(), error = %e, "node health check failed");
                    }
                    if last_retention.is_none_or(|t| t.elapsed() > Duration::from_secs(3600)) {
                        if let Err(e) = retention(&gw).await {
                            tracing::warn!(code = e.code(), error = %e, "node retention failed");
                        }
                        last_retention = Some(tokio::time::Instant::now());
                    }
                    tokio::select! {
                        () = lead.cancelled() => return Ok(()),
                        () = tokio::time::sleep(every) => {}
                    }
                }
            }
        })
        .await;
}

/// Reason prefix of a DEGRADED node with health signals.
pub const HEALTH: &str = "health:";

/// What one node's state should become, from its status, reason, heartbeat age and signals.
pub fn decide(
    status: &str,
    reason: Option<&str>,
    late: bool,
    offline: bool,
    signals: &[&str],
) -> Option<(NodeEvent, String)> {
    if offline {
        return Some((NodeEvent::LoseContact, "no heartbeat".to_owned()));
    }
    match status {
        "ONLINE" if late => Some((NodeEvent::Degrade, HEARTBEAT_LATE.to_owned())),
        "ONLINE" if !signals.is_empty() => {
            Some((NodeEvent::Degrade, format!("{HEALTH}{}", signals.join(","))))
        }
        "DEGRADED"
            if !late
                && signals.is_empty()
                && reason.is_some_and(|r| r == HEARTBEAT_LATE || r.starts_with(HEALTH)) =>
        {
            Some((NodeEvent::Recover, "healthy again".to_owned()))
        }
        _ => None,
    }
}

/// One pass: every ONLINE/DEGRADED node, then command deadlines.
// ponytail: scans every live node each pass; page through ids if fleets reach many thousands.
pub async fn check_once(gw: &Gateway) -> Result<(), NodesError> {
    let nodes: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM compute_nodes WHERE status IN ('ONLINE','DEGRADED')")
            .fetch_all(gw.db.pool())
            .await?;
    for node in nodes {
        settle(gw, node).await?;
    }
    for node in store::inbox_nodes(&gw.db).await? {
        crate::grpc::session::deliver_kept(gw, node);
    }
    sqlx::query(
        "UPDATE agent_commands SET state = 'EXPIRED', updated_at = now()
         WHERE state IN ('PENDING','SENT') AND deadline_at <= now()",
    )
    .execute(gw.db.pool())
    .await?;
    Ok(())
}

/// Re-reads one node under its row lock and applies what [`decide`] says.
pub async fn settle(gw: &Gateway, node: Uuid) -> Result<(), NodesError> {
    let c = gw.config.clone();
    gw.db
        .transaction(move |tx| {
            let c = c.clone();
            Box::pin(async move {
                let to_db = |e: NodesError| match e {
                    NodesError::Db(d) => d,
                    o => crate::db::DbError::Query(sqlx::Error::Protocol(o.to_string())),
                };
                let row: Option<(String, Option<String>, bool, bool, bool, bool)> = sqlx::query_as(
                    "SELECT status, status_reason,
                            COALESCE(last_heartbeat_at < now() - make_interval(secs => $2), true),
                            COALESCE(last_heartbeat_at < now() - make_interval(secs => $3), true),
                            COALESCE(abs(clock_skew_ms) > $4, false),
                            COALESCE((telemetry->>'disk_free_bytes')::numeric < $5, false)
                     FROM compute_nodes WHERE id = $1 FOR UPDATE",
                )
                .bind(node)
                .bind(c.degraded_after_seconds as f64)
                .bind(c.offline_after_seconds as f64)
                .bind(i64::try_from(c.max_clock_skew_seconds.saturating_mul(1000)).unwrap_or(i64::MAX))
                .bind(i64::try_from(c.min_disk_free_bytes).unwrap_or(i64::MAX))
                .fetch_optional(&mut **tx)
                .await?;
                let Some((status, reason, late, offline, skew, disk_low)) = row else {
                    return Ok(());
                };
                let (hot, lost): (bool, bool) = sqlx::query_as(
                    "SELECT COALESCE(bool_or(health = 'DEGRADED'), false), COALESCE(bool_or(health = 'FAILED'), false)
                     FROM gpu_devices WHERE node_id = $1 AND present",
                )
                .bind(node)
                .fetch_one(&mut **tx)
                .await?;
                let mut signals = Vec::new();
                for (on, name) in [(skew, "clock_skew"), (disk_low, "disk_low"), (hot, "gpu_hot"), (lost, "gpu_lost")] {
                    if on {
                        signals.push(name);
                    }
                }
                let Some((event, why)) = decide(&status, reason.as_deref(), late, offline, &signals) else {
                    return Ok(());
                };
                if store::transition(tx, node, event, Some(&why)).await.map_err(to_db)?.is_some()
                    && event == NodeEvent::LoseContact
                {
                    store::event(tx, node, "offline", "warning", json!({ "after_seconds": c.offline_after_seconds }))
                        .await
                        .map_err(to_db)?;
                }
                Ok(())
            })
        })
        .await?;
    Ok(())
}

/// Deletes old node events, final commands and used tokens. A launch whose attempt still
/// renews its lease stays (every lease grant refreshes the launch row, see `store::leases`).
pub async fn retention(gw: &Gateway) -> Result<(), NodesError> {
    let p = gw.db.pool();
    sqlx::query("DELETE FROM node_events WHERE at < now() - interval '180 days'")
        .execute(p)
        .await?;
    sqlx::query(
        "DELETE FROM agent_commands WHERE state IN ('COMPLETED','FAILED','EXPIRED','SUPERSEDED')
           AND updated_at < now() - interval '30 days'",
    )
    .execute(p)
    .await?;
    sqlx::query(
        "DELETE FROM node_enrollment_tokens
         WHERE COALESCE(used_at, expires_at) < now() - interval '30 days'",
    )
    .execute(p)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decisions_cover_heartbeats_and_health_signals() {
        assert_eq!(decide("ONLINE", None, false, false, &[]), None);
        assert_eq!(
            decide("ONLINE", None, true, false, &[]).unwrap().1,
            HEARTBEAT_LATE
        );
        assert_eq!(
            decide("ONLINE", None, true, true, &[]).unwrap().0,
            NodeEvent::LoseContact
        );
        let d = decide("ONLINE", None, false, false, &["disk_low", "gpu_hot"]).unwrap();
        assert_eq!(
            (d.0, d.1.as_str()),
            (NodeEvent::Degrade, "health:disk_low,gpu_hot")
        );
        assert_eq!(
            decide("DEGRADED", Some("health:gpu_hot"), false, false, &[])
                .unwrap()
                .0,
            NodeEvent::Recover
        );
        assert_eq!(
            decide(
                "DEGRADED",
                Some("health:gpu_hot"),
                false,
                false,
                &["gpu_hot"]
            ),
            None
        );
        assert_eq!(
            decide("DEGRADED", Some(HEARTBEAT_LATE), true, false, &[]),
            None,
            "still late"
        );
        // A fresh heartbeat seen under the lock: never degraded after the scan.
        assert_eq!(decide("ONLINE", None, false, false, &[]), None);
        assert_eq!(
            decide("DEGRADED", Some("operator"), false, false, &[]),
            None
        );
    }
}

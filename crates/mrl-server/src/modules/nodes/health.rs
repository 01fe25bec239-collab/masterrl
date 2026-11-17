//! Node health (Doc 09 §4.5), command deadlines and retention. Leader-elected: one replica runs
//! it at a time.
//!
//! - ONLINE with a heartbeat older than `degraded_after_seconds` → DEGRADED (`heartbeat_late`);
//!   a fresh heartbeat brings it back (`store::heartbeat`).
//! - ONLINE/DEGRADED older than `offline_after_seconds` → OFFLINE (plus a `node_events` row).
//!   Reconnecting brings it back ONLINE with `session_epoch + 1`.
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

/// One pass: late heartbeats and command deadlines.
pub async fn check_once(gw: &Gateway) -> Result<(), NodesError> {
    let c = &gw.config;
    let late: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT id, status FROM compute_nodes
         WHERE status IN ('ONLINE','DEGRADED') AND last_heartbeat_at < now() - make_interval(secs => $1)",
    )
    .bind(c.degraded_after_seconds as f64)
    .fetch_all(gw.db.pool())
    .await?;
    let offline_after = c.offline_after_seconds as f64;
    for (node, _) in late {
        gw.db
            .transaction(move |tx| {
                Box::pin(async move {
                    let offline: bool = sqlx::query_scalar(
                        "SELECT last_heartbeat_at < now() - make_interval(secs => $2) FROM compute_nodes WHERE id = $1",
                    )
                    .bind(node)
                    .bind(offline_after)
                    .fetch_one(&mut **tx)
                    .await?;
                    let to_db = |e: NodesError| match e {
                        NodesError::Db(d) => d,
                        o => crate::db::DbError::Query(sqlx::Error::Protocol(o.to_string())),
                    };
                    if offline {
                        if store::transition(tx, node, NodeEvent::LoseContact, Some("no heartbeat"))
                            .await
                            .map_err(to_db)?
                            .is_some()
                        {
                            store::event(tx, node, "offline", "warning", json!({ "after_seconds": offline_after }))
                                .await
                                .map_err(to_db)?;
                        }
                    } else {
                        store::transition(tx, node, NodeEvent::Degrade, Some(HEARTBEAT_LATE))
                            .await
                            .map_err(to_db)?;
                    }
                    Ok(())
                })
            })
            .await?;
    }
    sqlx::query(
        "UPDATE agent_commands SET state = 'EXPIRED', updated_at = now()
         WHERE state IN ('PENDING','SENT') AND deadline_at <= now()",
    )
    .execute(gw.db.pool())
    .await?;
    Ok(())
}

async fn retention(gw: &Gateway) -> Result<(), NodesError> {
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

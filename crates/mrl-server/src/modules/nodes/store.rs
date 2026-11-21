//! SQL for pools, nodes, GPUs, join tokens, certificates, telemetry, node events and commands.
//!
//! Node status changes go through [`transition`]: the row is locked, the step is checked against
//! `mrl_domain::state::Node`, `status_version` goes up by one and a `node.*` event is written to
//! the outbox in the same transaction.

use super::api::{GpuView, NodeDetail, NodeView, PoolView};
use super::ca::{Issued, sha256_hex};
use super::{NodesError, Pki};
use crate::db::{Db, Tx};
use crate::modules::events::{NewEvent, OutboxWriter};
use mrl_domain::ids::{AttemptId, GpuId, NodeId, PoolId};
use mrl_domain::state::{Node, NodeEvent, NodeState, StateMachine};
use mrl_proto::agent::v1 as pb;
use serde_json::{Value, json};
use sqlx::Row;
use std::str::FromStr;
use time::OffsetDateTime;
use uuid::Uuid;

/// Node lifecycle (Doc 04 PBC-04-5).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Lifecycle {
    /// A machine that stays (30-day certificates).
    Static,
    /// A short-lived machine (certificates of at most 24 hours).
    Ephemeral,
}

impl Lifecycle {
    /// Stored name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Static => "STATIC",
            Self::Ephemeral => "EPHEMERAL",
        }
    }

    fn parse(s: &str) -> Self {
        if s == "EPHEMERAL" {
            Self::Ephemeral
        } else {
            Self::Static
        }
    }

    /// Certificate lifetime for this lifecycle.
    pub fn cert_lifetime(self, cfg: &super::GatewayConfig) -> time::Duration {
        match self {
            Self::Static => time::Duration::days(cfg.static_cert_days.into()),
            Self::Ephemeral => time::Duration::hours(cfg.ephemeral_cert_hours.min(24).into()),
        }
    }
}

// ---------- pools ----------

fn pool_view(r: &sqlx::postgres::PgRow) -> Result<PoolView, sqlx::Error> {
    Ok(PoolView {
        id: PoolId::from_uuid(r.try_get("id")?).to_string(),
        name: r.try_get("name")?,
        provider_kind: r.try_get("provider_kind")?,
        attempt_lease_ttl_seconds: r.try_get::<i32, _>("ttl")?.max(0) as u32,
        flags: r.try_get("flags")?,
        created_at: r.try_get("created_at")?,
    })
}

const POOL_COLS: &str = "id, org_id, name, provider_kind, flags, created_at, extract(epoch from attempt_lease_ttl)::int AS ttl";

/// Creates a pool. `Ok(None)` when the name is taken.
pub async fn create_pool(
    db: &Db,
    org: Option<Uuid>,
    name: &str,
    provider_kind: &str,
    lease_ttl_seconds: u32,
    flags: &[String],
) -> Result<Option<PoolView>, NodesError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO compute_pools (id, name, provider_kind, flags, attempt_lease_ttl, org_id)
         VALUES ($1, $2, $3, $4, make_interval(secs => $5), $6)
         ON CONFLICT (org_id, name) DO NOTHING RETURNING {POOL_COLS}"
    )))
    .bind(Uuid::now_v7())
    .bind(name)
    .bind(provider_kind)
    .bind(flags)
    .bind(f64::from(lease_ttl_seconds))
    .bind(org)
    .fetch_optional(db.pool())
    .await?;
    Ok(row.as_ref().map(pool_view).transpose()?)
}

/// Pools ordered by id, after `after`.
pub async fn list_pools(
    db: &Db,
    org: Option<Uuid>,
    after: Option<Uuid>,
    limit: usize,
) -> Result<Vec<(PoolView, Option<Uuid>)>, NodesError> {
    // Platform pools (no org) and the caller's own org's pools only.
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {POOL_COLS} FROM compute_pools
         WHERE ($1::uuid IS NULL OR id > $1) AND (org_id IS NULL OR org_id = $3)
         ORDER BY id LIMIT $2"
    )))
    .bind(after)
    .bind(limit as i64)
    .bind(org)
    .fetch_all(db.pool())
    .await?;
    Ok(rows
        .iter()
        .map(|row| Ok((pool_view(row)?, row.try_get("org_id")?)))
        .collect::<Result<_, sqlx::Error>>()?)
}

/// The org that owns `pool` (`Some(None)`: a platform pool; `None`: no such pool).
pub async fn pool_org(db: &Db, pool: Uuid) -> Result<Option<Option<Uuid>>, NodesError> {
    Ok(
        sqlx::query_scalar("SELECT org_id FROM compute_pools WHERE id = $1")
            .bind(pool)
            .fetch_optional(db.pool())
            .await?,
    )
}

/// The org that owns `node`, through its pool (`None`: no such node).
pub async fn node_org(db: &Db, node: Uuid) -> Result<Option<Option<Uuid>>, NodesError> {
    Ok(sqlx::query_scalar(
        "SELECT p.org_id FROM compute_nodes n JOIN compute_pools p ON p.id = n.pool_id WHERE n.id = $1",
    )
    .bind(node)
    .fetch_optional(db.pool())
    .await?)
}

// ---------- nodes ----------

const NODE_COLS: &str = "id, pool_id, hostname, lifecycle, status, status_version, status_reason, \
    agent_version, protocol_version, transport, session_epoch, last_heartbeat_at, labels, \
    cert_expires_at, running_attempts, created_at";

fn node_view(r: &sqlx::postgres::PgRow) -> Result<NodeView, sqlx::Error> {
    Ok(NodeView {
        id: NodeId::from_uuid(r.try_get("id")?).to_string(),
        pool_id: PoolId::from_uuid(r.try_get("pool_id")?).to_string(),
        hostname: r.try_get("hostname")?,
        lifecycle: Lifecycle::parse(r.try_get::<&str, _>("lifecycle")?),
        status: r.try_get("status")?,
        status_version: r.try_get("status_version")?,
        status_reason: r.try_get("status_reason")?,
        agent_version: r.try_get("agent_version")?,
        protocol_version: r.try_get("protocol_version")?,
        transport: r.try_get("transport")?,
        session_epoch: r.try_get("session_epoch")?,
        last_heartbeat_at: r.try_get("last_heartbeat_at")?,
        labels: r.try_get("labels")?,
        cert_expires_at: r.try_get("cert_expires_at")?,
        running_attempts: r.try_get("running_attempts")?,
        created_at: r.try_get("created_at")?,
    })
}

/// Nodes of a pool ordered by id, after `after`.
pub async fn list_nodes(
    db: &Db,
    pool: Uuid,
    after: Option<Uuid>,
    limit: usize,
) -> Result<Vec<NodeView>, NodesError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {NODE_COLS} FROM compute_nodes WHERE pool_id = $1 AND ($2::uuid IS NULL OR id > $2)
         ORDER BY id LIMIT $3"
    )))
    .bind(pool)
    .bind(after)
    .bind(limit as i64)
    .fetch_all(db.pool())
    .await?;
    Ok(rows.iter().map(node_view).collect::<Result<_, _>>()?)
}

/// One node with its GPUs, latest telemetry and inventory.
pub async fn get_node(db: &Db, node: Uuid) -> Result<Option<NodeDetail>, NodesError> {
    // One snapshot for the node row and its GPUs: a telemetry write between the two reads would
    // otherwise show new GPU readings beside the older node telemetry.
    let mut tx = db.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let Some(row) = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {NODE_COLS}, inventory, telemetry, telemetry_at FROM compute_nodes WHERE id = $1"
    )))
    .bind(node)
    .fetch_optional(&mut *tx)
    .await?
    else {
        return Ok(None);
    };
    let gpus = sqlx::query(
        "SELECT g.id, g.vendor, g.device_uuid, g.index_on_node, g.model, g.arch, g.vram_total_bytes,
                g.health, t.util_pct, t.mem_used_bytes, t.temp_c, t.thermal_throttling, t.observed_at
         FROM gpu_devices g LEFT JOIN node_telemetry_latest t ON t.gpu_device_id = g.id
         WHERE g.node_id = $1 AND g.present ORDER BY g.index_on_node",
    )
    .bind(node)
    .fetch_all(&mut *tx)
    .await?
    .iter()
    .map(|g| {
        Ok(GpuView {
            id: GpuId::from_uuid(g.try_get("id")?).to_string(),
            vendor: g.try_get("vendor")?,
            device_uuid: g.try_get("device_uuid")?,
            index: g.try_get("index_on_node")?,
            model: g.try_get("model")?,
            arch: g.try_get("arch")?,
            vram_total_bytes: g.try_get("vram_total_bytes")?,
            health: g.try_get("health")?,
            utilization_percent: g.try_get("util_pct")?,
            memory_used_bytes: g.try_get("mem_used_bytes")?,
            temperature_c: g.try_get("temp_c")?,
            thermal_throttling: g.try_get("thermal_throttling")?,
            observed_at: g.try_get("observed_at")?,
        })
    })
    .collect::<Result<Vec<_>, sqlx::Error>>()?;
    Ok(Some(NodeDetail {
        node: node_view(&row)?,
        gpus,
        inventory: row.try_get("inventory")?,
        telemetry: row.try_get("telemetry")?,
        telemetry_at: row.try_get("telemetry_at")?,
    }))
}

/// Locks the node and applies `event` when the node machine allows it. Returns the new state,
/// or `None` (nothing changed) when the step is not allowed from the current state.
pub async fn transition(
    tx: &mut Tx,
    node: Uuid,
    event: NodeEvent,
    reason: Option<&str>,
) -> Result<Option<NodeState>, NodesError> {
    let Some(row) =
        sqlx::query("SELECT status, status_version FROM compute_nodes WHERE id = $1 FOR UPDATE")
            .bind(node)
            .fetch_optional(&mut **tx)
            .await?
    else {
        return Ok(None);
    };
    let status: String = row.try_get("status")?;
    let version: i64 = row.try_get("status_version")?;
    let Ok(from) = NodeState::from_str(&status) else {
        return Ok(None);
    };
    let Ok(to) = Node::can_transition(from, event) else {
        return Ok(None);
    };
    sqlx::query("UPDATE compute_nodes SET status = $2, status_version = $3, status_reason = $4 WHERE id = $1")
        .bind(node)
        .bind(Node::state_name(to))
        .bind(version + 1)
        .bind(reason)
        .execute(&mut **tx)
        .await?;
    let e = NewEvent::new(
        Node::event_type(event),
        Node::AGGREGATE,
        node,
        version + 1,
        json!({ "node_id": NodeId::from_uuid(node).to_string(), "from": status,
                "to": Node::state_name(to), "reason": reason }),
    )
    .map_err(|e| NodesError::Protocol(e.to_string()))?;
    OutboxWriter::write(tx, &e).await?;
    Ok(Some(to))
}

/// Records a node incident.
pub async fn event(
    tx: &mut Tx,
    node: Uuid,
    kind: &str,
    severity: &str,
    details: Value,
) -> Result<(), NodesError> {
    sqlx::query("INSERT INTO node_events (id, node_id, kind, severity, details) VALUES ($1, $2, $3, $4, $5)")
        .bind(Uuid::now_v7())
        .bind(node)
        .bind(kind)
        .bind(severity)
        .bind(details)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

// ---------- tokens and enrollment ----------

/// Creates a single-use join token; returns its id. Only `sha256(token)` is stored.
#[allow(clippy::too_many_arguments)]
pub async fn create_token(
    db: &Db,
    pool: Uuid,
    token: &str,
    labels: &Value,
    lifecycle: Lifecycle,
    expires_at: OffsetDateTime,
    endpoint: Option<&crate::transports::Endpoint>,
    created_by: &str,
) -> Result<Uuid, NodesError> {
    let id = Uuid::now_v7();
    let endpoint = endpoint
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| NodesError::Protocol(e.to_string()))?;
    sqlx::query(
        "INSERT INTO node_enrollment_tokens (id, pool_id, token_hash, labels, lifecycle, endpoint, expires_at, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(id)
    .bind(pool)
    .bind(sha256_hex(token.as_bytes()).into_bytes())
    .bind(labels)
    .bind(lifecycle.as_str())
    .bind(endpoint)
    .bind(expires_at)
    .bind(created_by)
    .execute(db.pool())
    .await?;
    Ok(id)
}

/// A finished enrollment.
#[derive(Debug, Clone)]
pub struct Enrolled {
    /// The new node.
    pub node: Uuid,
    /// Its first certificate.
    pub cert: Issued,
}

/// Uses `req.join_token` (once), creates the node in ENROLLING and signs its first certificate.
pub async fn enroll(
    db: &Db,
    pki: std::sync::Arc<Pki>,
    cfg: &super::GatewayConfig,
    req: &pb::EnrollRequest,
) -> Result<Enrolled, NodesError> {
    let hash = sha256_hex(req.join_token.as_bytes()).into_bytes();
    let req = req.clone();
    let lifetimes = (
        Lifecycle::Static.cert_lifetime(cfg),
        Lifecycle::Ephemeral.cert_lifetime(cfg),
    );
    db.transaction(move |tx| {
        let (hash, req, pki) = (hash.clone(), req.clone(), pki.clone());
        Box::pin(async move {
            let ca = &pki.ca;
            let row = sqlx::query(
                "SELECT id, pool_id, labels, lifecycle, endpoint, expires_at, used_at, used_by_node_id,
                        used_key_sha256
                 FROM node_enrollment_tokens WHERE token_hash = $1 FOR UPDATE",
            )
            .bind(&hash)
            .fetch_optional(&mut **tx)
            .await?;
            let Some(row) = row else {
                return Ok(Err(NodesError::Token("this token is not known; create a new one".into())));
            };
            let key_sha = match super::ca::Ca::csr_key_sha256(&req.csr_der) {
                Ok(k) => k,
                Err(e) => return Ok(Err(e)),
            };
            let expired = row.try_get::<OffsetDateTime, _>("expires_at")? <= OffsetDateTime::now_utc();
            let lifecycle = Lifecycle::parse(row.try_get::<&str, _>("lifecycle")?);
            let lifetime = match lifecycle {
                Lifecycle::Static => lifetimes.0,
                Lifecycle::Ephemeral => lifetimes.1,
            };
            if row.try_get::<Option<OffsetDateTime>, _>("used_at")?.is_some() {
                // The same machine asking again (it lost the answer): same token, same key, the
                // token still valid and the node never connected. It gets a certificate for the
                // node it already is.
                let same_key = row
                    .try_get::<Option<String>, _>("used_key_sha256")?
                    .is_some_and(|k| k == key_sha);
                let node: Option<Uuid> = row.try_get("used_by_node_id")?;
                if let (true, false, Some(node)) = (same_key, expired, node) {
                    let recoverable: Option<bool> = sqlx::query_scalar(
                        "SELECT status = 'ENROLLING' AND session_epoch = 0 FROM compute_nodes WHERE id = $1 FOR UPDATE",
                    )
                    .bind(node)
                    .fetch_optional(&mut **tx)
                    .await?;
                    if recoverable == Some(true) && sqlx::query_scalar::<_, bool>(
                        "SELECT count(*) > 0 AND bool_and(revoked_at IS NULL) AND bool_or(not_after > now())
                         FROM (SELECT revoked_at, not_after FROM node_certificates WHERE node_id = $1 FOR SHARE) AS certs",
                    )
                    .bind(node)
                    .fetch_one(&mut **tx)
                    .await? {
                        let cert = match ca.issue_node(&req.csr_der, node, lifetime) {
                            Ok(c) => c,
                            Err(e) => return Ok(Err(e)),
                        };
                        insert_cert(tx, node, &cert).await.map_err(db_of)?;
                        sqlx::query(
                            "UPDATE compute_nodes SET cert_serial = $2, cert_expires_at = $3 WHERE id = $1",
                        )
                        .bind(node)
                        .bind(&cert.serial)
                        .bind(cert.not_after)
                        .execute(&mut **tx)
                        .await?;
                        return Ok(Ok(Enrolled { node, cert }));
                    }
                }
                return Ok(Err(NodesError::Token(
                    "this token was already used; each token adds one machine".into(),
                )));
            }
            if expired {
                return Ok(Err(NodesError::Token("this token has expired; create a new one".into())));
            }
            let node = Uuid::now_v7();
            let cert = match ca.issue_node(&req.csr_der, node, lifetime) {
                Ok(c) => c,
                Err(e) => return Ok(Err(e)),
            };
            let hostname = if req.hostname.trim().is_empty() {
                "unnamed".to_owned()
            } else {
                req.hostname.chars().take(255).collect()
            };
            sqlx::query(
                "INSERT INTO compute_nodes (id, pool_id, hostname, lifecycle, status, labels, endpoint,
                    hardware_fingerprint, agent_version, cert_serial, cert_expires_at)
                 VALUES ($1, $2, $3, $4, 'ENROLLING', $5, $6, $7, $8, $9, $10)",
            )
            .bind(node)
            .bind(row.try_get::<Uuid, _>("pool_id")?)
            .bind(&hostname)
            .bind(lifecycle.as_str())
            .bind(row.try_get::<Value, _>("labels")?)
            .bind(row.try_get::<Option<Value>, _>("endpoint")?)
            .bind(&req.hardware_fingerprint)
            .bind(&req.agent_version)
            .bind(&cert.serial)
            .bind(cert.not_after)
            .execute(&mut **tx)
            .await?;
            insert_cert(tx, node, &cert).await.map_err(db_of)?;
            sqlx::query(
                "UPDATE node_enrollment_tokens SET used_at = now(), used_by_node_id = $2, used_key_sha256 = $3
                 WHERE id = $1",
            )
                .bind(row.try_get::<Uuid, _>("id")?)
                .bind(node)
                .bind(&key_sha)
                .execute(&mut **tx)
                .await?;
            let e = NewEvent::new(
                Node::CREATED_EVENT,
                Node::AGGREGATE,
                node,
                0,
                json!({ "node_id": NodeId::from_uuid(node).to_string(), "hostname": hostname,
                        "lifecycle": lifecycle.as_str() }),
            )
            .map_err(|e| crate::db::DbError::Query(sqlx::Error::Protocol(e.to_string())))?;
            OutboxWriter::write(tx, &e).await?;
            Ok(Ok(Enrolled { node, cert }))
        })
    })
    .await?
}

fn db_of(e: NodesError) -> crate::db::DbError {
    match e {
        NodesError::Db(d) => d,
        other => crate::db::DbError::Query(sqlx::Error::Protocol(other.to_string())),
    }
}

async fn insert_cert(tx: &mut Tx, node: Uuid, cert: &Issued) -> Result<(), NodesError> {
    sqlx::query("INSERT INTO node_certificates (sha256, node_id, serial, not_after) VALUES ($1, $2, $3, $4)")
        .bind(&cert.sha256)
        .bind(node)
        .bind(&cert.serial)
        .bind(cert.not_after)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// What a presented certificate stands for.
#[derive(Debug, Clone)]
pub struct CertIdentity {
    /// The node it was issued to.
    pub node: Uuid,
    /// Revoked (rotated, decommissioned or by an operator).
    pub revoked: bool,
    /// Past its end date.
    pub expired: bool,
    /// The node's status.
    pub status: String,
    /// The node's lifecycle.
    pub lifecycle: Lifecycle,
}

/// Looks up a certificate by the hex SHA-256 of its DER.
pub async fn cert_identity(db: &Db, sha256: &str) -> Result<Option<CertIdentity>, NodesError> {
    let row = sqlx::query(
        "SELECT c.node_id, c.revoked_at IS NOT NULL AS revoked, c.not_after <= now() AS expired,
                n.status, n.lifecycle
         FROM node_certificates c JOIN compute_nodes n ON n.id = c.node_id WHERE c.sha256 = $1",
    )
    .bind(sha256)
    .fetch_optional(db.pool())
    .await?;
    row.map(|r| {
        Ok(CertIdentity {
            node: r.try_get("node_id")?,
            revoked: r.try_get("revoked")?,
            expired: r.try_get("expired")?,
            status: r.try_get("status")?,
            lifecycle: Lifecycle::parse(r.try_get::<&str, _>("lifecycle")?),
        })
    })
    .transpose()
    .map_err(|e: sqlx::Error| e.into())
}

/// Signs a new certificate for `node`, after re-checking — in the same transaction, with the
/// presenting certificate's row locked — that the certificate this request came with
/// (`presented_sha256`) is still valid. Older certificates stay valid until the new one first
/// authenticates ([`retire_older`]), so a lost reply never strands the agent.
pub async fn renew(
    db: &Db,
    pki: std::sync::Arc<Pki>,
    cfg: &super::GatewayConfig,
    node: Uuid,
    lifecycle: Lifecycle,
    presented_sha256: &str,
    csr_der: &[u8],
) -> Result<Issued, NodesError> {
    let lifetime = lifecycle.cert_lifetime(cfg);
    let (sha, csr) = (presented_sha256.to_owned(), csr_der.to_vec());
    db.transaction(move |tx| {
        let (sha, csr, pki) = (sha.clone(), csr.clone(), pki.clone());
        Box::pin(async move {
            let valid: Option<bool> = sqlx::query_scalar(
                "SELECT c.revoked_at IS NULL AND c.not_after > now() AND n.status <> 'DECOMMISSIONED'
                 FROM node_certificates c JOIN compute_nodes n ON n.id = c.node_id
                 WHERE c.sha256 = $1 AND c.node_id = $2 FOR UPDATE OF c",
            )
            .bind(&sha)
            .bind(node)
            .fetch_optional(&mut **tx)
            .await?;
            if valid != Some(true) {
                return Ok(Err(NodesError::Certificate(
                    "the certificate this request came with is revoked or expired; no new one was issued".into(),
                )));
            }
            let cert = match pki.ca.issue_node(&csr, node, lifetime) {
                Ok(c) => c,
                Err(e) => return Ok(Err(e)),
            };
            insert_cert(tx, node, &cert).await.map_err(db_of)?;
            sqlx::query("UPDATE compute_nodes SET cert_serial = $2, cert_expires_at = $3 WHERE id = $1")
                .bind(node)
                .bind(&cert.serial)
                .bind(cert.not_after)
                .execute(&mut **tx)
                .await?;
            Ok(Ok(cert))
        })
    })
    .await?
}

/// The node authenticated with `sha256`: revoke (`rotated`) its certificates issued before it.
/// Returns the revoked fingerprints.
pub async fn retire_older(db: &Db, node: Uuid, sha256: &str) -> Result<Vec<String>, NodesError> {
    Ok(sqlx::query_scalar(
        "UPDATE node_certificates SET revoked_at = now(), revoked_reason = 'rotated'
         WHERE node_id = $1 AND revoked_at IS NULL AND sha256 <> $2
           AND issued_at < (SELECT issued_at FROM node_certificates WHERE sha256 = $2)
         RETURNING sha256",
    )
    .bind(node)
    .bind(sha256)
    .fetch_all(db.pool())
    .await?)
}

/// Revokes every certificate of `node`.
pub async fn revoke_all(tx: &mut Tx, node: Uuid, reason: &str) -> Result<(), NodesError> {
    sqlx::query(
        "UPDATE node_certificates SET revoked_at = now(), revoked_reason = $2
         WHERE node_id = $1 AND revoked_at IS NULL",
    )
    .bind(node)
    .bind(reason)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

// ---------- sessions ----------

/// Serializes lossy sink effects with ownership changes without locking out CONTROL writes.
pub(crate) async fn lock_lane_epoch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node: Uuid,
    shared: bool,
) -> Result<(), sqlx::Error> {
    let sql = if shared {
        "SELECT pg_advisory_xact_lock_shared(hashtextextended('mrl:lane:' || $1::uuid::text, 0))"
    } else {
        "SELECT pg_advisory_xact_lock(hashtextextended('mrl:lane:' || $1::uuid::text, 0))"
    };
    sqlx::query(sql).bind(node).execute(&mut **tx).await?;
    Ok(())
}

/// Checks the active epoch while the caller holds the lane epoch lock.
pub(crate) async fn holds_lane(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node: Uuid,
    epoch: i64,
) -> Result<bool, NodesError> {
    let current: Option<i64> = sqlx::query_scalar(
        "SELECT session_epoch FROM compute_nodes WHERE id = $1 AND session_holder IS NOT NULL",
    )
    .bind(node)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(current == Some(epoch))
}

/// A new CONTROL session for the machine certificate `cert_sha256`: bumps `session_epoch`,
/// records who holds it; OFFLINE → ONLINE. Returns `(epoch, control_rx_seq)`.
///
/// The certificate and the node are re-checked here, with the node row locked, in the same
/// transaction that opens the session (the handshake's check was an earlier read): a
/// decommission or revocation that committed since then refuses the session
/// ([`NodesError::Certificate`]), and one that commits later ends it ([`end_sessions`]).
pub async fn open_control(
    db: &Db,
    node: Uuid,
    cert_sha256: &str,
    holder: &str,
    hello: &pb::AgentHello,
    transport: Option<&str>,
) -> Result<(i64, i64), NodesError> {
    let (sha, holder, hello, transport) = (
        cert_sha256.to_owned(),
        holder.to_owned(),
        hello.clone(),
        transport.map(str::to_owned),
    );
    db.transaction(move |tx| {
            let (sha, holder, hello, transport) =
                (sha.clone(), holder.clone(), hello.clone(), transport.clone());
            Box::pin(async move {
                lock_lane_epoch(tx, node, false).await?;
                sqlx::query("SELECT 1 FROM compute_nodes WHERE id = $1 FOR UPDATE")
                    .bind(node)
                    .execute(&mut **tx)
                    .await?;
                // Read after the lock: sees every decommission or revocation committed before.
                let valid: Option<bool> = sqlx::query_scalar(
                    "SELECT c.revoked_at IS NULL AND c.not_after > now() AND n.status <> 'DECOMMISSIONED'
                     FROM node_certificates c JOIN compute_nodes n ON n.id = c.node_id
                     WHERE c.sha256 = $1 AND c.node_id = $2",
                )
                .bind(&sha)
                .bind(node)
                .fetch_optional(&mut **tx)
                .await?;
                if valid != Some(true) {
                    return Ok(Err(NodesError::Certificate(
                        "this machine's certificate was revoked or the machine was removed".into(),
                    )));
                }
                let row = sqlx::query(
                    "UPDATE compute_nodes SET session_epoch = session_epoch + 1, session_holder = $2,
                        last_heartbeat_at = now(), agent_version = $3, protocol_version = $4, transport = $5
                     WHERE id = $1 RETURNING session_epoch, control_rx_seq",
                )
                .bind(node)
                .bind(&holder)
                .bind(&hello.agent_version)
                .bind(crate::grpc::PROTOCOL_VERSION)
                .bind(transport)
                .fetch_one(&mut **tx)
                .await?;
                let epoch: i64 = row.try_get("session_epoch")?;
                // Wakes the older session's stream on whichever replica holds it: it finds the
                // newer epoch at once and closes (with the data streams bound to it).
                sqlx::query("SELECT pg_notify($1, $2)")
                    .bind(super::commands::WAKE_CHANNEL)
                    .bind(node.to_string())
                    .execute(&mut **tx)
                    .await?;
                transition(tx, node, NodeEvent::Reconnect, Some("reconnected")).await.map_err(db_of)?;
                event(tx, node, "connected", "info", json!({ "session_epoch": epoch, "core": holder }))
                    .await
                    .map_err(db_of)?;
                Ok(Ok((epoch, row.try_get::<i64, _>("control_rx_seq")?)))
            })
        })
        .await?
}

/// Ends every session of `node` on every replica, like a takeover does: `session_epoch` moves
/// on, so each stream's next fenced step (ack, message, claim, telemetry) finds itself stale,
/// and the wake-up makes each holder check at once. The caller takes the exclusive lane
/// epoch lock before locking the node row.
pub async fn end_sessions(tx: &mut Tx, node: Uuid) -> Result<(), NodesError> {
    sqlx::query(
        "UPDATE compute_nodes SET session_epoch = session_epoch + 1, session_holder = NULL WHERE id = $1",
    )
    .bind(node)
    .execute(&mut **tx)
    .await?;
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(super::commands::WAKE_CHANNEL)
        .bind(node.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// The epoch of the node's CONTROL session. A data stream may open first, so this waits (up to
/// 5 s) until a CONTROL session holds the node, then returns its epoch.
pub async fn session_epoch(db: &Db, node: Uuid) -> Result<i64, NodesError> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let (epoch, held): (i64, bool) = sqlx::query_as(
            "SELECT session_epoch, session_holder IS NOT NULL FROM compute_nodes WHERE id = $1",
        )
        .bind(node)
        .fetch_one(db.pool())
        .await?;
        if held || tokio::time::Instant::now() >= deadline {
            return Ok(epoch);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// The CONTROL session `epoch` ended.
pub async fn close_control(db: &Db, node: Uuid, epoch: i64) -> Result<(), NodesError> {
    db.transaction(move |tx| {
        Box::pin(async move {
            lock_lane_epoch(tx, node, false).await?;
            let r = sqlx::query(
                "UPDATE compute_nodes SET session_holder = NULL WHERE id = $1 AND session_epoch = $2",
            )
            .bind(node)
            .bind(epoch)
            .execute(&mut **tx)
            .await?;
            if r.rows_affected() == 1 {
                event(tx, node, "disconnected", "info", json!({ "session_epoch": epoch }))
                    .await
                    .map_err(db_of)?;
            }
            Ok(())
        })
    })
    .await?;
    Ok(())
}

/// Result of recording a CONTROL message seq.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accept {
    /// New: handle it in this transaction.
    New,
    /// Already handled (a replay).
    Duplicate,
    /// A newer session for the node exists: close this one.
    Stale,
}

/// Records CONTROL `seq` for `(node, epoch)` (dedupe on node, stream, seq). The node row is
/// locked first and stays locked until the transaction ends, so a takeover by another stream
/// (which bumps `session_epoch`) waits for this message to finish, and a stale stream sees
/// `Stale` before it changes anything.
pub async fn accept(tx: &mut Tx, node: Uuid, epoch: i64, seq: u64) -> Result<Accept, NodesError> {
    let seq =
        i64::try_from(seq).map_err(|_| NodesError::Protocol("message seq is too large".into()))?;
    let row: Option<(i64, i64)> = sqlx::query_as(
        "SELECT session_epoch, control_rx_seq FROM compute_nodes WHERE id = $1 FOR UPDATE",
    )
    .bind(node)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((current, rx)) = row else {
        return Ok(Accept::Stale);
    };
    if current != epoch {
        return Ok(Accept::Stale);
    }
    if seq <= rx {
        return Ok(Accept::Duplicate);
    }
    sqlx::query("UPDATE compute_nodes SET control_rx_seq = $2 WHERE id = $1")
        .bind(node)
        .bind(seq)
        .execute(&mut **tx)
        .await?;
    Ok(Accept::New)
}

/// True when `epoch` is still the node's session; locks the row (shared) until `tx` ends.
pub async fn holds(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node: Uuid,
    epoch: i64,
) -> Result<bool, NodesError> {
    let current: Option<i64> =
        sqlx::query_scalar("SELECT session_epoch FROM compute_nodes WHERE id = $1 AND session_holder IS NOT NULL FOR SHARE")
            .bind(node)
            .fetch_optional(&mut **tx)
            .await?;
    Ok(current == Some(epoch))
}

/// A heartbeat: freshens `last_heartbeat_at`, the running-attempt count and the clock skew
/// (`sent` is the agent's clock). Whether the node is DEGRADED is decided by `health`.
pub async fn heartbeat(
    tx: &mut Tx,
    node: Uuid,
    running: usize,
    sent: Option<&prost_types::Timestamp>,
) -> Result<(), NodesError> {
    let skew_ms = sent.and_then(|t| {
        let agent = OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(t.seconds) * 1_000_000_000 + i128::from(t.nanos),
        )
        .ok()?;
        i64::try_from((agent - OffsetDateTime::now_utc()).whole_milliseconds()).ok()
    });
    sqlx::query(
        "UPDATE compute_nodes SET last_heartbeat_at = now(), running_reported_at = now(),
            running_attempts = $2, clock_skew_ms = $3 WHERE id = $1",
    )
    .bind(node)
    .bind(i32::try_from(running).unwrap_or(i32::MAX))
    .bind(skew_ms)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Status reason when a node is DEGRADED because its heartbeat is late.
pub const HEARTBEAT_LATE: &str = "heartbeat_late";

fn vendor(v: &str) -> &'static str {
    match v.to_ascii_lowercase().as_str() {
        "nvidia" => "nvidia",
        "amd" => "amd",
        "apple" => "apple",
        "intel" => "intel",
        _ => "other",
    }
}

fn arch(g: &pb::Gpu) -> String {
    match (vendor(&g.vendor), g.compute_capability.as_str()) {
        ("nvidia", cc) if !cc.is_empty() => format!("sm_{}", cc.replace('.', "")),
        ("apple", _) => "apple".to_owned(),
        (_, cc) if !cc.is_empty() => cc.to_owned(),
        _ => "unknown".to_owned(),
    }
}

fn inventory_json(inv: &pb::Inventory) -> Value {
    json!({
        "hostname": inv.hostname,
        "platform": pb::Platform::try_from(inv.platform).map(|p| p.as_str_name()).unwrap_or("PLATFORM_UNSPECIFIED"),
        "os_version": inv.os_version,
        "arch": inv.arch,
        "cpu_model": inv.cpu_model,
        "cpu_cores": inv.cpu_cores,
        "ram_bytes": inv.ram_bytes,
        "disk_free_bytes": inv.disk_free_bytes,
        "apple_silicon": inv.apple_silicon,
        "unified_memory_bytes": inv.unified_memory_bytes,
        "cuda_runtime": inv.cuda_runtime,
        "wsl2_mirrored_networking": inv.wsl2_mirrored_networking,
        "gpus": inv.gpus.iter().map(|g| json!({
            "index": g.index, "uuid": g.uuid, "vendor": g.vendor, "model": g.model,
            "memory_bytes": g.memory_bytes, "compute_capability": g.compute_capability,
            "driver_version": g.driver_version,
        })).collect::<Vec<_>>(),
    })
}

/// Stores a full inventory and syncs `gpu_devices`; ENROLLING → ONLINE on the first one.
pub async fn inventory(tx: &mut Tx, node: Uuid, inv: &pb::Inventory) -> Result<(), NodesError> {
    let host = if inv.hostname.is_empty() {
        None
    } else {
        Some(&inv.hostname)
    };
    sqlx::query(
        "UPDATE compute_nodes SET inventory = $2, hostname = COALESCE($3, hostname) WHERE id = $1",
    )
    .bind(node)
    .bind(inventory_json(inv))
    .bind(host)
    .execute(&mut **tx)
    .await?;
    let mut seen = Vec::new();
    for g in &inv.gpus {
        let device_uuid = if g.uuid.is_empty() {
            format!("index-{}", g.index)
        } else {
            g.uuid.clone()
        };
        sqlx::query(
            "INSERT INTO gpu_devices (id, node_id, vendor, device_uuid, index_on_node, model, arch, vram_total_bytes)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (node_id, device_uuid) DO UPDATE SET vendor = EXCLUDED.vendor,
               index_on_node = EXCLUDED.index_on_node, model = EXCLUDED.model, arch = EXCLUDED.arch,
               vram_total_bytes = EXCLUDED.vram_total_bytes, present = true",
        )
        .bind(Uuid::now_v7())
        .bind(node)
        .bind(vendor(&g.vendor))
        .bind(&device_uuid)
        .bind(i32::try_from(g.index).unwrap_or(i32::MAX))
        .bind(&g.model)
        .bind(arch(g))
        .bind(i64::try_from(g.memory_bytes).unwrap_or(i64::MAX))
        .execute(&mut **tx)
        .await?;
        seen.push(device_uuid);
    }
    sqlx::query(
        "UPDATE gpu_devices SET present = false WHERE node_id = $1 AND NOT (device_uuid = ANY($2))",
    )
    .bind(node)
    .bind(&seen)
    .execute(&mut **tx)
    .await?;
    transition(tx, node, NodeEvent::Enroll, Some("first inventory")).await?;
    Ok(())
}

/// Latest-wins node telemetry and per-GPU readings. A reading older than the stored one is
/// ignored (two writers, e.g. across a reconnect, can't move a reading back in time). GPU
/// health follows the reading: OK, DEGRADED at `hot_c` or above, FAILED for a present GPU the
/// reading leaves out (lost). Only the current session writes. Returns the saved observation
/// time when the reading was accepted.
pub async fn telemetry(
    db: &Db,
    node: Uuid,
    epoch: i64,
    t: &pb::NodeTelemetry,
    hot_c: u32,
    max_ahead: time::Duration,
) -> Result<Option<OffsetDateTime>, NodesError> {
    let at = t
        .time
        .as_ref()
        .and_then(|ts| {
            OffsetDateTime::from_unix_timestamp_nanos(
                i128::from(ts.seconds) * 1_000_000_000 + i128::from(ts.nanos),
            )
            .ok()
        })
        .unwrap_or_else(OffsetDateTime::now_utc);
    // A reading from a clock running ahead would hold back every later one until real time
    // caught up: past `max_ahead` it counts as taken now.
    let now = OffsetDateTime::now_utc();
    let at = if at > now + max_ahead { now } else { at };
    let at = at
        .replace_nanosecond(at.nanosecond() / 1_000 * 1_000)
        .map_err(|e| NodesError::Protocol(e.to_string()))?;
    let doc = json!({
        "ram_used_bytes": t.ram_used_bytes, "swap_used_bytes": t.swap_used_bytes,
        "disk_free_bytes": t.disk_free_bytes, "memory_pressure": t.memory_pressure,
        "on_battery": t.on_battery, "battery_percent": t.battery_percent,
    });
    let mut tx = db.begin().await?;
    let fresh = sqlx::query(
        "UPDATE compute_nodes SET telemetry = $2, telemetry_at = $3
         WHERE id = $1 AND session_epoch = $4 AND session_holder IS NOT NULL AND (telemetry_at IS NULL OR telemetry_at < $3)",
    )
    .bind(node)
    .bind(doc)
    .bind(at)
    .bind(epoch)
    .execute(&mut *tx)
    .await?;
    if fresh.rows_affected() == 0 {
        // An older (or repeated) reading, or one from a session that was taken over (the node
        // row stays locked until commit, so a takeover can't slip in between): keep what is there.
        return Ok(None);
    }
    let mut seen = Vec::new();
    for g in &t.gpus {
        let index = i32::try_from(g.index).unwrap_or(i32::MAX);
        seen.push(index);
        sqlx::query(
            "INSERT INTO node_telemetry_latest (gpu_device_id, util_pct, mem_used_bytes, temp_c, thermal_throttling, observed_at)
             SELECT id, $3, $4, $5, $6, $7 FROM gpu_devices WHERE node_id = $1 AND index_on_node = $2 AND present
             ON CONFLICT (gpu_device_id) DO UPDATE SET util_pct = EXCLUDED.util_pct,
               mem_used_bytes = EXCLUDED.mem_used_bytes, temp_c = EXCLUDED.temp_c,
               thermal_throttling = EXCLUDED.thermal_throttling, observed_at = EXCLUDED.observed_at
             WHERE node_telemetry_latest.observed_at < EXCLUDED.observed_at",
        )
        .bind(node)
        .bind(index)
        .bind(i32::try_from(g.utilization_percent).unwrap_or(i32::MAX))
        .bind(i64::try_from(g.memory_used_bytes).unwrap_or(i64::MAX))
        .bind(i32::try_from(g.temperature_c).unwrap_or(i32::MAX))
        .bind(g.thermal_throttling)
        .bind(at)
        .execute(&mut *tx)
        .await?;
        let health = if g.temperature_c >= hot_c {
            "DEGRADED"
        } else {
            "OK"
        };
        sqlx::query(
            "UPDATE gpu_devices SET health = $3 WHERE node_id = $1 AND index_on_node = $2 AND present
               AND health IN ('OK','DEGRADED','FAILED')",
        )
        .bind(node)
        .bind(index)
        .bind(health)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "UPDATE gpu_devices SET health = 'FAILED' WHERE node_id = $1 AND present
           AND NOT (index_on_node = ANY($2)) AND health IN ('OK','DEGRADED')",
    )
    .bind(node)
    .bind(&seen)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(at))
}

/// Journals a CONTROL message no sink takes yet (`node_events.kind = control_unrouted`).
pub async fn unrouted(
    tx: &mut Tx,
    node: Uuid,
    seq: u64,
    msg: &pb::AgentMessage,
) -> Result<(), NodesError> {
    use base64::Engine;
    use prost::Message;
    let body = super::lanes::body_name(msg);
    event(
        tx,
        node,
        "control_unrouted",
        "warning",
        json!({ "seq": seq, "body": body,
                "message_b64": base64::engine::general_purpose::STANDARD.encode(msg.encode_to_vec()) }),
    )
    .await
}

// ---------- CONTROL inbox ----------

/// Keeps a CONTROL-class message for the CONTROL sink (`node_control_inbox`), in the
/// transaction that checked `epoch`; [`dispatch`] delivers it once that transaction is over.
/// A message already kept (same node, stream, seq) is not kept twice.
pub async fn inbox(
    tx: &mut Tx,
    node: Uuid,
    epoch: i64,
    stream: pb::Lane,
    msg: &pb::AgentMessage,
) -> Result<(), NodesError> {
    use prost::Message;
    let seq = i64::try_from(msg.seq)
        .map_err(|_| NodesError::Protocol("message seq is too large".into()))?;
    sqlx::query(
        "INSERT INTO node_control_inbox (node_id, stream, seq, session_epoch, message)
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT (node_id, stream, seq) DO NOTHING",
    )
    .bind(node)
    .bind(stream as i16)
    .bind(seq)
    .bind(epoch)
    .bind(msg.encode_to_vec())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// How long a dispatcher's lease on the oldest kept message of a node lasts; it is renewed
/// every third of that while the sink runs.
pub const INBOX_LEASE: std::time::Duration = std::time::Duration::from_secs(60);

/// Delivers `node`'s kept CONTROL messages to `sink` in arrival order, deleting each one the sink
/// took; stops at the first sink error (that message and later ones stay for the next try).
/// [`dispatch_with_lease`] with [`INBOX_LEASE`].
pub async fn dispatch(
    db: &Db,
    sink: &dyn super::lanes::LaneSink,
    node: Uuid,
) -> Result<(), NodesError> {
    dispatch_with_lease(db, sink, node, INBOX_LEASE).await
}

/// [`dispatch`] with a lease of `lease`.
///
/// No connection is held while the sink runs (it may send commands to the same node, even with a
/// one-connection pool): the dispatcher leases the node's oldest row in one short statement under
/// a token of its own, renews the lease while the sink runs, then deletes the row. Only the
/// oldest row can be leased, so one dispatcher per node runs at a time (on any replica) and order
/// is kept. Renewal, release and delete all require the token, so a dispatcher whose lease was
/// lost (it stalled past expiry and another took over) never touches the other's row. Delivery
/// is at-least-once.
pub async fn dispatch_with_lease(
    db: &Db,
    sink: &dyn super::lanes::LaneSink,
    node: Uuid,
    lease: std::time::Duration,
) -> Result<(), NodesError> {
    use prost::Message;
    let secs = lease.as_secs_f64();
    loop {
        let token = Uuid::new_v4();
        let row: Option<(i64, i16, i64, i64, Vec<u8>)> = sqlx::query_as(
            "UPDATE node_control_inbox
                SET leased_until = now() + make_interval(secs => $2), lease_token = $3
             WHERE id = (SELECT id FROM node_control_inbox WHERE node_id = $1
                         ORDER BY id LIMIT 1 FOR UPDATE)
               AND (leased_until IS NULL OR leased_until < now())
             RETURNING id, stream, seq, session_epoch, message",
        )
        .bind(node)
        .bind(secs)
        .bind(token)
        .fetch_optional(db.pool())
        .await?;
        // Empty, or another dispatcher holds the oldest row (it continues with the rest).
        let Some((id, stream, seq, epoch, bytes)) = row else {
            return Ok(());
        };
        let message = pb::AgentMessage::decode(bytes.as_slice())
            .map_err(|e| NodesError::Protocol(format!("a kept CONTROL message is damaged: {e}")))?;
        let item = super::lanes::LaneItem {
            node_id: node,
            session_epoch: epoch,
            lane: super::lanes::LaneKind::Control,
            stream: pb::Lane::try_from(i32::from(stream)).unwrap_or(pb::Lane::Control),
            seq: u64::try_from(seq).unwrap_or(0),
            dropped_before: 0,
            message,
        };
        let deliver = sink.deliver(item);
        tokio::pin!(deliver);
        let mut renew = tokio::time::interval(lease / 3);
        renew.tick().await;
        let delivered = loop {
            tokio::select! {
                r = &mut deliver => break r,
                _ = renew.tick() => {
                    let kept = sqlx::query(
                        "UPDATE node_control_inbox SET leased_until = now() + make_interval(secs => $3)
                         WHERE id = $1 AND lease_token = $2",
                    )
                    .bind(id)
                    .bind(token)
                    .bind(secs)
                    .execute(db.pool())
                    .await;
                    if !matches!(kept, Ok(ref r) if r.rows_affected() == 1) {
                        tracing::warn!(node = %node, "a CONTROL inbox lease was lost while its sink ran; another dispatcher may deliver it again");
                    }
                }
            }
        };
        if let Err(e) = delivered {
            sqlx::query(
                "UPDATE node_control_inbox SET leased_until = NULL, lease_token = NULL
                 WHERE id = $1 AND lease_token = $2",
            )
            .bind(id)
            .bind(token)
            .execute(db.pool())
            .await?;
            return Err(NodesError::Protocol(format!(
                "the CONTROL sink did not take a message; it is kept and retried: {e}"
            )));
        }
        let done = sqlx::query("DELETE FROM node_control_inbox WHERE id = $1 AND lease_token = $2")
            .bind(id)
            .bind(token)
            .execute(db.pool())
            .await?;
        if done.rows_affected() == 0 {
            // Another dispatcher holds it now and delivers it again (at-least-once).
            return Ok(());
        }
    }
}

/// Nodes with kept CONTROL messages (for the retry pass).
pub async fn inbox_nodes(db: &Db) -> Result<Vec<Uuid>, NodesError> {
    Ok(
        sqlx::query_scalar("SELECT DISTINCT node_id FROM node_control_inbox")
            .fetch_all(db.pool())
            .await?,
    )
}

// ---------- commands ----------

/// Marks SENT commands with `seq <= upto` as RECEIVED (the agent journaled them), only while
/// `epoch` is still the node's session. `Ok(false)`: a newer session took over.
pub async fn commands_received(
    db: &Db,
    node: Uuid,
    epoch: i64,
    upto: u64,
) -> Result<bool, NodesError> {
    let mut tx = db.begin().await?;
    if !holds(&mut tx, node, epoch).await? {
        return Ok(false);
    }
    sqlx::query(
        "UPDATE agent_commands SET state = 'RECEIVED', updated_at = now()
         WHERE node_id = $1 AND seq <= $2 AND state = 'SENT'",
    )
    .bind(node)
    .bind(i64::try_from(upto).unwrap_or(i64::MAX))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// Commands claimed for sending.
#[derive(Debug)]
pub enum Claim {
    /// A newer session owns the node: send nothing, close.
    Stale,
    /// Rows now SENT, in seq order: send exactly these.
    Send(Vec<(Uuid, u64, Vec<u8>)>),
}

/// Atomically claims commands to send for session `epoch`: PENDING (or SENT but not yet
/// journaled) rows with `seq > after` whose deadline has not passed become SENT. The state and
/// deadline are re-checked on the locked rows, so a row superseded or expired meanwhile is never
/// claimed, and only claimed rows may be transmitted.
pub async fn claim_commands(
    db: &Db,
    node: Uuid,
    epoch: i64,
    after: u64,
) -> Result<Claim, NodesError> {
    let mut tx = db.begin().await?;
    if !holds(&mut tx, node, epoch).await? {
        return Ok(Claim::Stale);
    }
    // While the node drains (or is gone), work-starting commands above `after` are failed, not
    // sent: the agent has not got them (`after` starts at its journal position), and leaving them
    // SENT would let an ack of a later command mark them RECEIVED. `holds` keeps the node row
    // share-locked, so a drain commits before or after this claim, never during it.
    sqlx::query(
        "UPDATE agent_commands c SET state = 'FAILED', result = $4, updated_at = now()
         FROM compute_nodes n
         WHERE n.id = c.node_id AND c.node_id = $1 AND c.state IN ('PENDING','SENT')
           AND c.seq > $2 AND c.kind = ANY($3) AND n.status IN ('DRAINING','DECOMMISSIONED')",
    )
    .bind(node)
    .bind(i64::try_from(after).unwrap_or(i64::MAX))
    .bind(&super::commands::STARTS_WORK[..])
    .bind(draining_failure())
    .execute(&mut *tx)
    .await?;
    // Commands above `after` past their deadline are never sent: they become EXPIRED here, so an
    // ack of a later command can't mark them RECEIVED (the health pass may not have run yet).
    sqlx::query(
        "UPDATE agent_commands SET state = 'EXPIRED', updated_at = now()
         WHERE node_id = $1 AND state IN ('PENDING','SENT') AND seq > $2 AND deadline_at <= now()",
    )
    .bind(node)
    .bind(i64::try_from(after).unwrap_or(i64::MAX))
    .execute(&mut *tx)
    .await?;
    let rows = sqlx::query(
        "UPDATE agent_commands SET state = 'SENT', sent_at = now(), updated_at = now()
         WHERE id IN (SELECT id FROM agent_commands
                      WHERE node_id = $1 AND state IN ('PENDING','SENT') AND seq > $2
                        AND deadline_at > now()
                      ORDER BY seq LIMIT 64)
           AND state IN ('PENDING','SENT') AND deadline_at > now()
         RETURNING id, seq, payload",
    )
    .bind(node)
    .bind(i64::try_from(after).unwrap_or(i64::MAX))
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    let mut out = rows
        .iter()
        .map(|r| {
            Ok((
                r.try_get("id")?,
                r.try_get::<i64, _>("seq")?.max(0) as u64,
                r.try_get("payload")?,
            ))
        })
        .collect::<Result<Vec<(Uuid, u64, Vec<u8>)>, sqlx::Error>>()?;
    out.sort_by_key(|(_, seq, _)| *seq);
    Ok(Claim::Send(out))
}

/// Fails command `id`, which is `size` bytes on the wire and so can never be sent.
pub async fn fail_unsendable(db: &Db, id: Uuid, size: usize) -> Result<(), NodesError> {
    let failure = json!({ "accepted": false, "failure": {
        "class": "FAILURE_CLASS_USER_CONFIG", "sub_code": "command_too_large",
        "message": format!("the command is {size} bytes on the wire, over the {}-byte limit; it was not sent", crate::grpc::MAX_MESSAGE_BYTES) } });
    sqlx::query(
        "UPDATE agent_commands SET state = 'FAILED', result = $2, updated_at = now()
         WHERE id = $1 AND state IN ('PENDING','SENT')",
    )
    .bind(id)
    .bind(failure)
    .execute(db.pool())
    .await?;
    Ok(())
}

/// Whether this session should send its ordered drain fence.
pub(crate) async fn draining(db: &Db, node: Uuid, epoch: i64) -> Result<bool, NodesError> {
    Ok(sqlx::query_scalar(
        "SELECT status = 'DRAINING' FROM compute_nodes WHERE id = $1 AND session_epoch = $2",
    )
    .bind(node)
    .bind(epoch)
    .fetch_optional(db.pool())
    .await?
    .unwrap_or(false))
}

/// The agent has processed the fence behind all previously sent work starts. Require a
/// fresh heartbeat after this acknowledgement, which carries the actual running count.
pub(crate) async fn acknowledge_drain(
    tx: &mut Tx,
    node: Uuid,
    epoch: i64,
) -> Result<(), NodesError> {
    sqlx::query(
        "UPDATE compute_nodes SET drain_ack_epoch = $2, running_reported_at = NULL
         WHERE id = $1 AND status = 'DRAINING' AND session_epoch = $2
           AND drain_ack_epoch IS DISTINCT FROM $2",
    )
    .bind(node)
    .bind(epoch)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Why a draining node can't be removed yet, if it can't: a work start still in flight
/// (pending, sent or received), or a missing drain fence and subsequent empty heartbeat.
/// A node that never opened a session and never received work needs no acknowledgement.
/// Reads with the node row locked by the caller.
pub async fn decommission_blocker(tx: &mut Tx, node: Uuid) -> Result<Option<String>, NodesError> {
    let in_flight: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent_commands
         WHERE node_id = $1 AND kind = ANY($2) AND state IN ('PENDING','SENT','RECEIVED')",
    )
    .bind(node)
    .bind(&super::commands::STARTS_WORK[..])
    .fetch_one(&mut **tx)
    .await?;
    if in_flight > 0 {
        return Ok(Some(format!(
            "{in_flight} work start(s) sent before the drain have not finished yet; wait for them"
        )));
    }
    let settled: bool = sqlx::query_scalar(
        "SELECT n.running_attempts = 0 AND (
                (n.session_epoch = 0 AND NOT EXISTS (
                    SELECT 1 FROM agent_commands c WHERE c.node_id = n.id
                      AND c.kind = ANY($2) AND c.sent_at IS NOT NULL))
                OR (n.drain_ack_epoch IS NOT DISTINCT FROM n.session_epoch
                    AND n.running_reported_at IS NOT NULL
                    AND n.running_reported_at > COALESCE(n.drained_at, '-infinity')
                    AND n.running_reported_at > COALESCE((SELECT max(c.updated_at) FROM agent_commands c
                        WHERE c.node_id = n.id AND c.kind = ANY($2) AND c.state = 'COMPLETED'), '-infinity')))
         FROM compute_nodes n WHERE n.id = $1",
    )
    .bind(node)
    .bind(&super::commands::STARTS_WORK[..])
    .fetch_one(&mut **tx)
    .await?;
    Ok((!settled).then(|| {
        "the machine has not yet acknowledged the drain and sent a heartbeat showing no running work".to_owned()
    }))
}

/// The result of a work-starting command the agent never got because the node drains.
fn draining_failure() -> Value {
    json!({ "accepted": false, "failure": {
        "class": "FAILURE_CLASS_INFRA_TRANSIENT", "sub_code": "node_draining",
        "message": "the machine started draining before this command reached it; it takes no new work" } })
}

/// Fails the node's work-starting commands that were not sent yet (drain: no new work). Their
/// senders see FAILED `node_draining` and can place the work elsewhere.
pub async fn refuse_new_work(tx: &mut Tx, node: Uuid) -> Result<u64, NodesError> {
    let failure = draining_failure();
    sqlx::query("UPDATE compute_nodes SET drained_at = now() WHERE id = $1")
        .bind(node)
        .execute(&mut **tx)
        .await?;
    Ok(sqlx::query(
        "UPDATE agent_commands SET state = 'FAILED', result = $3, updated_at = now()
         WHERE node_id = $1 AND state = 'PENDING' AND kind = ANY($2)",
    )
    .bind(node)
    .bind(&super::commands::STARTS_WORK[..])
    .bind(failure)
    .execute(&mut **tx)
    .await?
    .rows_affected())
}

/// A `CommandResult`: COMPLETED when accepted, else FAILED with the failure.
pub async fn command_result(
    tx: &mut Tx,
    node: Uuid,
    r: &pb::CommandResult,
) -> Result<(), NodesError> {
    let Ok(id) = Uuid::parse_str(&r.command_id) else {
        tracing::warn!(node = %node, command_id = %r.command_id, "result for an unknown command id");
        return Ok(());
    };
    let failure = r.failure.as_ref().map(|f| {
        json!({ "class": mrl_proto::common::v1::FailureClass::try_from(f.class).map(|c| c.as_str_name()).unwrap_or("FAILURE_CLASS_UNSPECIFIED"),
                "sub_code": f.sub_code, "message": f.message })
    });
    let done = sqlx::query(
        "UPDATE agent_commands SET state = CASE WHEN $3 THEN 'COMPLETED' ELSE 'FAILED' END,
            result = $4, updated_at = now()
         WHERE id = $1 AND node_id = $2 AND state IN ('SENT','RECEIVED','EXPIRED')",
    )
    .bind(id)
    .bind(node)
    .bind(r.accepted)
    .bind(json!({ "accepted": r.accepted, "failure": failure }))
    .execute(&mut **tx)
    .await?;
    if done.rows_affected() == 0 {
        tracing::debug!(node = %node, command = %id, "command result ignored (already final or unknown)");
    }
    Ok(())
}

/// Leases for the attempts a heartbeat reports: granted (pool `attempt_lease_ttl`) for each
/// attempt this core launched on this node with that epoch and has not cancelled.
pub async fn leases(
    db: &Db,
    node: Uuid,
    running: &[mrl_proto::common::v1::AttemptRef],
) -> Result<Vec<pb::AttemptLease>, NodesError> {
    if running.is_empty() {
        return Ok(Vec::new());
    }
    let ttl: i32 = sqlx::query_scalar(
        "SELECT extract(epoch from p.attempt_lease_ttl)::int FROM compute_nodes n
         JOIN compute_pools p ON p.id = n.pool_id WHERE n.id = $1",
    )
    .bind(node)
    .fetch_one(db.pool())
    .await?;
    let ttl = ttl.max(1) as u32;
    let mut out = Vec::new();
    for a in running {
        let Ok(attempt) = AttemptId::from_str(&a.attempt_id) else {
            continue;
        };
        // ponytail: lease = "launched here at this epoch and not cancelled"; P17 may refine with attempt state.
        // A cancellation counts for its own fencing epoch only (one without an epoch: for the
        // launches sent before it), so a relaunch at a newer epoch gets its lease.
        let renewed = sqlx::query(
            "UPDATE agent_commands l SET updated_at = now()
             WHERE l.node_id = $1 AND l.kind = 'LaunchAttempt' AND l.attempt_id = $2
               AND l.epoch = $3 AND l.state IN ('SENT','RECEIVED','COMPLETED')
               AND NOT EXISTS (SELECT 1 FROM agent_commands c WHERE c.node_id = l.node_id
                    AND c.kind = 'CancelAttempt' AND c.attempt_id = l.attempt_id AND c.state = 'COMPLETED'
                    AND (c.epoch = l.epoch OR (c.epoch IS NULL AND c.seq > l.seq)))",
        )
        .bind(node)
        .bind(attempt.as_uuid())
        .bind(i64::try_from(a.fencing_epoch).unwrap_or(i64::MAX))
        .execute(db.pool())
        .await?;
        if renewed.rows_affected() > 0 {
            out.push(pb::AttemptLease {
                attempt: Some(a.clone()),
                duration_s: ttl,
                kill_budget_s: (ttl / 4).max(1),
            });
        }
    }
    Ok(out)
}

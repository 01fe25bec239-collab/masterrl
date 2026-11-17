//! REST endpoints under `/api/v1`: pools, enrollment tokens, nodes, drain and decommission.
//!
//! Reading needs `compute.read`; changing needs `compute.manage`. `POST /nodes/{node}:drain`
//! and `:decommission` share the `/nodes/{node}` route (the router has no suffix patterns).

use super::store::{self, Lifecycle};
use super::{ca::hex, Gateway, NodesError};
use crate::error::{ApiError, ErrorCode};
use crate::http::pagination::{Page, PageQuery};
use crate::seams::{RequestContext, Resource};
use crate::transports::Endpoint;
use crate::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use mrl_domain::ids::{NodeId, PoolId};
use mrl_domain::state::NodeEvent;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::str::FromStr;
use std::sync::Arc;
use time::OffsetDateTime;
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

/// A pool.
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub struct PoolView {
    /// `pool_…`.
    pub id: String,
    /// Unique name.
    pub name: String,
    /// `agent`, `kubernetes`, `slurm` or `cloud_vm`.
    pub provider_kind: String,
    /// How long an attempt lease lasts without a renewing heartbeat.
    pub attempt_lease_ttl_seconds: u32,
    /// Flags like `pii_allowed`.
    pub flags: Vec<String>,
    /// Created.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
}

/// A machine.
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub struct NodeView {
    /// `node_…`.
    pub id: String,
    /// `pool_…`.
    pub pool_id: String,
    /// Host name the agent reported.
    pub hostname: String,
    /// STATIC or EPHEMERAL.
    pub lifecycle: Lifecycle,
    /// ENROLLING, ONLINE, DEGRADED, OFFLINE, DRAINING or DECOMMISSIONED.
    pub status: String,
    /// Bumped on every status change.
    pub status_version: i64,
    /// Why the status last changed.
    pub status_reason: Option<String>,
    /// Agent version.
    pub agent_version: Option<String>,
    /// Gateway protocol version.
    pub protocol_version: Option<String>,
    /// `direct`, `ssh`, `tailscale` or `cloudflare_tunnel`.
    pub transport: Option<String>,
    /// Bumped on every new CONTROL session.
    pub session_epoch: i64,
    /// Last heartbeat.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub last_heartbeat_at: Option<OffsetDateTime>,
    /// Labels from the join token.
    #[schema(value_type = Object)]
    pub labels: Value,
    /// When the current certificate ends.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub cert_expires_at: Option<OffsetDateTime>,
    /// Attempts running at the last heartbeat.
    pub running_attempts: i32,
    /// Enrolled.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
}

/// A GPU and its latest reading.
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub struct GpuView {
    /// `gpu_…`.
    pub id: String,
    /// `nvidia`, `amd`, `apple`, `intel` or `other`.
    pub vendor: String,
    /// Vendor UUID (stable identity).
    pub device_uuid: String,
    /// Index on the node.
    pub index: i32,
    /// Model name.
    pub model: String,
    /// Architecture, like `sm_80`.
    pub arch: String,
    /// Memory.
    pub vram_total_bytes: i64,
    /// OK, DEGRADED, FAILED or QUARANTINED.
    pub health: String,
    /// Latest utilization (TELEMETRY lane).
    pub utilization_percent: Option<i32>,
    /// Latest memory use.
    pub memory_used_bytes: Option<i64>,
    /// Latest temperature.
    pub temperature_c: Option<i32>,
    /// Latest thermal throttling flag.
    pub thermal_throttling: Option<bool>,
    /// When the reading was taken.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub observed_at: Option<OffsetDateTime>,
}

/// A machine with its GPUs, inventory and latest telemetry.
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub struct NodeDetail {
    /// The machine.
    #[serde(flatten)]
    pub node: NodeView,
    /// Its GPUs with their latest readings.
    pub gpus: Vec<GpuView>,
    /// The last full inventory.
    #[schema(value_type = Option<Object>)]
    pub inventory: Option<Value>,
    /// Latest node readings (memory, disk, battery).
    #[schema(value_type = Option<Object>)]
    pub telemetry: Option<Value>,
    /// When those were taken.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub telemetry_at: Option<OffsetDateTime>,
}

/// `POST /pools`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreatePool {
    /// Unique name, 1–100 characters.
    pub name: String,
    /// `agent` (default), `kubernetes`, `slurm` or `cloud_vm`.
    #[serde(default)]
    pub provider_kind: Option<String>,
    /// Attempt lease length, 10–3600 s (default 90).
    #[serde(default)]
    pub attempt_lease_ttl_seconds: Option<u32>,
    /// Flags.
    #[serde(default)]
    pub flags: Vec<String>,
}

/// `POST /pools/{pool}/enrollment-tokens`.
#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateToken {
    /// How long the token works, 60 s – 7 days (default 1 hour).
    #[serde(default)]
    pub ttl_seconds: Option<u32>,
    /// Labels the new machine gets.
    #[serde(default)]
    #[schema(value_type = Option<Object>)]
    pub labels: Option<Value>,
    /// STATIC (default) or EPHEMERAL.
    #[serde(default)]
    pub lifecycle: Option<Lifecycle>,
    /// Set when the core should dial the new machine (its agent waits with `enroll --listen`).
    #[serde(default)]
    pub endpoint: Option<Endpoint>,
}

/// A new join token. The token is shown once.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TokenCreated {
    /// Token row id.
    pub id: String,
    /// `mrlj_<secret>_<CA fingerprint>`; single use.
    pub token: String,
    /// When it stops working.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub expires_at: OffsetDateTime,
    /// The CA fingerprint the agent pins (also inside the token).
    pub ca_sha256: String,
    /// The machine's lifecycle.
    pub lifecycle: Lifecycle,
    /// What to run on the machine.
    pub enroll_command: String,
}

#[derive(OpenApi)]
#[openapi(
    paths(list_pools, create_pool, list_nodes, create_token, get_node, drain, decommission),
    components(schemas(PoolView, NodeView, GpuView, NodeDetail, CreatePool, CreateToken, TokenCreated,
        Lifecycle, Endpoint, Page<PoolView>, Page<NodeView>)),
    tags((name = "compute", description = "Pools, machines and their GPUs."))
)]
struct Doc;

/// The OpenAPI fragment.
pub fn openapi() -> utoipa::openapi::OpenApi {
    Doc::openapi()
}

/// The routes.
pub fn routes(gw: Arc<Gateway>) -> Router<AppState> {
    Router::new()
        .route("/pools", get(list_pools).post(create_pool))
        .route("/pools/{pool}/nodes", get(list_nodes))
        .route("/pools/{pool}/enrollment-tokens", post(create_token))
        .route("/nodes/{node}", get(get_node).post(node_action))
        .layer(Extension(gw))
}

fn internal(e: NodesError) -> ApiError {
    tracing::error!(code = e.code(), error = %e, "nodes request failed");
    ApiError::new(ErrorCode::Unavailable, "The database did not answer.")
}

fn field(path: &str, message: impl Into<String>) -> ApiError {
    let message = message.into();
    let mut e = ApiError::validation(vec![crate::error::FieldError {
        path: path.to_owned(),
        code: ErrorCode::FieldValue.as_str().to_owned(),
        message: message.clone(),
        hint: message.clone(),
    }]);
    e.detail = message;
    e
}

fn pool_id(s: &str) -> Result<Uuid, ApiError> {
    PoolId::from_str(s)
        .map(|p| p.as_uuid())
        .map_err(|_| ApiError::new(ErrorCode::NotFound, "No pool with this id exists."))
}

fn node_id(s: &str) -> Result<Uuid, ApiError> {
    NodeId::from_str(s)
        .map(|n| n.as_uuid())
        .map_err(|_| ApiError::new(ErrorCode::NotFound, "No machine with this id exists."))
}

fn after(
    cursor: Option<String>,
    parse: fn(&str) -> Result<Uuid, ApiError>,
) -> Result<Option<Uuid>, ApiError> {
    cursor
        .map(|c| {
            parse(&c).map_err(|_| ApiError::new(ErrorCode::Pagination, "The cursor can't be used."))
        })
        .transpose()
}

#[utoipa::path(get, path = "/pools", tag = "compute",
    params(("limit" = Option<String>, Query, description = "Page size, 1–200."),
           ("cursor" = Option<String>, Query, description = "From `next_cursor`.")),
    responses((status = 200, description = "Pools.", body = Page<PoolView>)))]
async fn list_pools(
    State(s): State<AppState>,
    Extension(gw): Extension<Arc<Gateway>>,
    ctx: RequestContext,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<PoolView>>, ApiError> {
    s.authorize(&ctx, "compute.read", &Resource::kind("pool"))
        .await?;
    let page = q.resolve()?;
    let rows = store::list_pools(&gw.db, after(page.after.clone(), pool_id)?, page.fetch())
        .await
        .map_err(internal)?;
    Ok(Json(Page::from_overfetch(rows, &page, |p| p.id.clone())))
}

#[utoipa::path(post, path = "/pools", tag = "compute", request_body = CreatePool,
    responses((status = 201, description = "Created.", body = PoolView),
              (status = 422, description = "A value needs a change.", body = crate::error::Problem)))]
async fn create_pool(
    State(s): State<AppState>,
    Extension(gw): Extension<Arc<Gateway>>,
    ctx: RequestContext,
    Json(body): Json<CreatePool>,
) -> Result<(StatusCode, Json<PoolView>), ApiError> {
    s.authorize(&ctx, "compute.manage", &Resource::kind("pool"))
        .await?;
    let name = body.name.trim();
    if name.is_empty() || name.chars().count() > 100 {
        return Err(field("/name", "Give the pool a name of 1–100 characters."));
    }
    let kind = body.provider_kind.as_deref().unwrap_or("agent");
    if !["agent", "kubernetes", "slurm", "cloud_vm"].contains(&kind) {
        return Err(field(
            "/provider_kind",
            "Use agent, kubernetes, slurm or cloud_vm.",
        ));
    }
    let ttl = body.attempt_lease_ttl_seconds.unwrap_or(90);
    if !(10..=3600).contains(&ttl) {
        return Err(field("/attempt_lease_ttl_seconds", "Use 10–3600 seconds."));
    }
    match store::create_pool(&gw.db, name, kind, ttl, &body.flags)
        .await
        .map_err(internal)?
    {
        Some(p) => Ok((StatusCode::CREATED, Json(p))),
        None => Err(field("/name", "A pool with this name already exists.")),
    }
}

#[utoipa::path(get, path = "/pools/{pool}/nodes", tag = "compute",
    params(("pool" = String, Path, description = "`pool_…`"),
           ("limit" = Option<String>, Query, description = "Page size, 1–200."),
           ("cursor" = Option<String>, Query, description = "From `next_cursor`.")),
    responses((status = 200, description = "Machines in the pool.", body = Page<NodeView>),
              (status = 404, description = "No such pool.", body = crate::error::Problem)))]
async fn list_nodes(
    State(s): State<AppState>,
    Extension(gw): Extension<Arc<Gateway>>,
    ctx: RequestContext,
    Path(pool): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<NodeView>>, ApiError> {
    s.authorize(&ctx, "compute.read", &Resource::kind("pool"))
        .await?;
    let pool = pool_id(&pool)?;
    if !store::pool_exists(&gw.db, pool).await.map_err(internal)? {
        return Err(ApiError::new(
            ErrorCode::NotFound,
            "No pool with this id exists.",
        ));
    }
    let page = q.resolve()?;
    let rows = store::list_nodes(
        &gw.db,
        pool,
        after(page.after.clone(), node_id)?,
        page.fetch(),
    )
    .await
    .map_err(internal)?;
    Ok(Json(Page::from_overfetch(rows, &page, |n| n.id.clone())))
}

/// A new token: `mrlj_` + 256 random bits + `_` + the CA fingerprint the agent pins.
fn new_token(ca_sha256: &str) -> String {
    let mut secret = [0u8; 32];
    secret[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    secret[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    format!("mrlj_{}_{ca_sha256}", hex(&secret))
}

#[utoipa::path(post, path = "/pools/{pool}/enrollment-tokens", tag = "compute", request_body = CreateToken,
    params(("pool" = String, Path, description = "`pool_…`")),
    responses((status = 201, description = "The token (shown once).", body = TokenCreated),
              (status = 404, description = "No such pool.", body = crate::error::Problem),
              (status = 422, description = "A value needs a change.", body = crate::error::Problem)))]
async fn create_token(
    State(s): State<AppState>,
    Extension(gw): Extension<Arc<Gateway>>,
    ctx: RequestContext,
    Path(pool): Path<String>,
    Json(body): Json<CreateToken>,
) -> Result<(StatusCode, Json<TokenCreated>), ApiError> {
    s.authorize(&ctx, "compute.manage", &Resource::kind("pool"))
        .await?;
    let pool_uuid = pool_id(&pool)?;
    if !store::pool_exists(&gw.db, pool_uuid)
        .await
        .map_err(internal)?
    {
        return Err(ApiError::new(
            ErrorCode::NotFound,
            "No pool with this id exists.",
        ));
    }
    let ttl = body.ttl_seconds.unwrap_or(3600);
    if !(60..=7 * 86_400).contains(&ttl) {
        return Err(field("/ttl_seconds", "Use 60 seconds to 7 days."));
    }
    let labels = body.labels.clone().unwrap_or_else(|| json!({}));
    if !labels.is_object()
        || labels
            .as_object()
            .is_some_and(|m| m.values().any(|v| !v.is_string()))
    {
        return Err(field("/labels", "Labels are an object of text values."));
    }
    if let Some(e) = &body.endpoint {
        if let Some(p) = e.problems().into_iter().next() {
            return Err(field("/endpoint", p));
        }
    }
    let pki = gw.pki().map_err(|e| {
        tracing::error!(code = e.code(), error = %e, "no CA for a join token");
        ApiError::new(
            ErrorCode::Unavailable,
            "The certificate authority is not ready yet.",
        )
    })?;
    let lifecycle = body.lifecycle.unwrap_or(Lifecycle::Static);
    let token = new_token(pki.ca.fingerprint());
    let expires_at = OffsetDateTime::now_utc() + time::Duration::seconds(ttl.into());
    let id = store::create_token(
        &gw.db,
        pool_uuid,
        &token,
        &labels,
        lifecycle,
        expires_at,
        body.endpoint.as_ref(),
        &ctx.principal.id,
    )
    .await
    .map_err(internal)?;
    let enroll_command = match &body.endpoint {
        None => format!(
            "mrl-agent enroll --server <this server>:{} --token {token}",
            gw.listen.port()
        ),
        Some(Endpoint::Ssh { agent_port, .. }) => {
            format!("mrl-agent enroll --listen 127.0.0.1:{agent_port} --token {token}")
        }
        Some(Endpoint::Direct { address } | Endpoint::Tailscale { address }) => {
            let port = crate::transports::split_host_port(address).map_or(7443, |(_, p)| p);
            format!("mrl-agent enroll --listen 0.0.0.0:{port} --token {token}")
        }
        Some(Endpoint::CloudflareTunnel { .. }) => {
            format!("mrl-agent enroll --listen 127.0.0.1:7443 --token {token}")
        }
    };
    Ok((
        StatusCode::CREATED,
        Json(TokenCreated {
            id: id.to_string(),
            ca_sha256: pki.ca.fingerprint().to_owned(),
            token,
            expires_at,
            lifecycle,
            enroll_command,
        }),
    ))
}

#[utoipa::path(get, path = "/nodes/{node}", tag = "compute",
    params(("node" = String, Path, description = "`node_…`")),
    responses((status = 200, description = "The machine, its GPUs and latest telemetry.", body = NodeDetail),
              (status = 404, description = "No such machine.", body = crate::error::Problem)))]
async fn get_node(
    State(s): State<AppState>,
    Extension(gw): Extension<Arc<Gateway>>,
    ctx: RequestContext,
    Path(node): Path<String>,
) -> Result<Json<NodeDetail>, ApiError> {
    s.authorize(&ctx, "compute.read", &Resource::kind("node"))
        .await?;
    store::get_node(&gw.db, node_id(&node)?)
        .await
        .map_err(internal)?
        .map(Json)
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "No machine with this id exists."))
}

#[utoipa::path(post, path = "/nodes/{node}:drain", tag = "compute",
    params(("node" = String, Path, description = "`node_…`")),
    responses((status = 200, description = "The machine takes no new work.", body = NodeView),
              (status = 404, description = "No such machine.", body = crate::error::Problem),
              (status = 422, description = "Not possible in this state.", body = crate::error::Problem)))]
#[allow(dead_code)]
async fn drain() {}

#[utoipa::path(post, path = "/nodes/{node}:decommission", tag = "compute",
    params(("node" = String, Path, description = "`node_…`")),
    responses((status = 200, description = "Removed; its certificates are revoked.", body = NodeView),
              (status = 404, description = "No such machine.", body = crate::error::Problem),
              (status = 422, description = "Drain it first, or wait for its attempts.", body = crate::error::Problem)))]
#[allow(dead_code)]
async fn decommission() {}

/// `POST /nodes/{node}:drain` and `POST /nodes/{node}:decommission`.
async fn node_action(
    State(s): State<AppState>,
    Extension(gw): Extension<Arc<Gateway>>,
    ctx: RequestContext,
    Path(raw): Path<String>,
) -> Result<Json<NodeView>, ApiError> {
    let (id, action) = raw.rsplit_once(':').ok_or_else(|| {
        ApiError::new(ErrorCode::MethodNotAllowed, "Use :drain or :decommission.")
    })?;
    if action != "drain" && action != "decommission" {
        return Err(ApiError::new(
            ErrorCode::RouteNotFound,
            "Use :drain or :decommission.",
        ));
    }
    s.authorize(&ctx, "compute.manage", &Resource::kind("node"))
        .await?;
    let node = node_id(id)?;
    let decommission = action == "decommission";
    let outcome = gw
        .db
        .transaction(move |tx| {
            Box::pin(async move {
                let row: Option<(String, i32)> = sqlx::query_as(
                    "SELECT status, running_attempts FROM compute_nodes WHERE id = $1 FOR UPDATE",
                )
                .bind(node)
                .fetch_optional(&mut **tx)
                .await?;
                let Some((status, running)) = row else {
                    return Ok(Err(ApiError::new(ErrorCode::NotFound, "No machine with this id exists.")));
                };
                let to_db = |e: NodesError| match e {
                    NodesError::Db(d) => d,
                    o => crate::db::DbError::Query(sqlx::Error::Protocol(o.to_string())),
                };
                if !decommission {
                    if status == "DRAINING" {
                        return Ok(Ok(false));
                    }
                    return Ok(match store::transition(tx, node, NodeEvent::Drain, Some("operator")).await.map_err(to_db)? {
                        Some(_) => Ok(false),
                        None => Err(field("/status", "This machine was removed; it can't be drained.")),
                    });
                }
                if status == "DECOMMISSIONED" {
                    return Ok(Ok(false));
                }
                if status != "DRAINING" {
                    return Ok(Err(field("/status", "Drain this machine first, then remove it.")));
                }
                if running > 0 {
                    return Ok(Err(field(
                        "/running_attempts",
                        format!("{running} attempt(s) still run on this machine; wait for them or cancel them."),
                    )));
                }
                store::transition(tx, node, NodeEvent::Decommission, Some("operator")).await.map_err(to_db)?;
                store::revoke_all(tx, node, "decommissioned").await.map_err(to_db)?;
                store::event(tx, node, "decommissioned", "info", json!({ "by": "operator" }))
                    .await
                    .map_err(to_db)?;
                Ok(Ok(true))
            })
        })
        .await
        .map_err(|e| internal(e.into()))??;
    if outcome {
        gw.kick(node);
    }
    let detail = store::get_node(&gw.db, node)
        .await
        .map_err(internal)?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "No machine with this id exists."))?;
    Ok(Json(detail.node))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_carry_the_ca_pin() {
        let t = new_token(&"ab".repeat(32));
        let rest = t.strip_prefix("mrlj_").unwrap();
        let (secret, pin) = rest.split_once('_').unwrap();
        assert_eq!(secret.len(), 64);
        assert_eq!(pin, "ab".repeat(32));
        assert_ne!(new_token(pin), t);
    }

    #[test]
    fn document_lists_every_path() {
        let doc = openapi();
        for p in [
            "/pools",
            "/pools/{pool}/nodes",
            "/pools/{pool}/enrollment-tokens",
            "/nodes/{node}",
            "/nodes/{node}:drain",
            "/nodes/{node}:decommission",
        ] {
            assert!(doc.paths.paths.contains_key(p), "{p}");
        }
    }
}

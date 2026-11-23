//! Alerts: any module raises one through [`AlertSink`]; people list, acknowledge and resolve
//! them; the `alerts` stream topic shows changes live.
//!
//! - One live (OPEN or ACKED) alert per `(org, dedup_key)`: firing again updates it
//!   (`fire_count`, `last_fired_at`, details); once RESOLVED, the next fire opens a new one.
//! - States: OPEN → ACKED → RESOLVED, OPEN → RESOLVED. Acknowledging a resolved alert is
//!   MRL-STATE-220; acknowledging or resolving twice answers the current alert.
//! - Access (refused 403, hidden 404): run alerts need `metrics_logs.read` to read and the run's cancel permission
//!   (`runs.cancel_own` for your own run, else `runs.cancel_any`) to change; node and system
//!   alerts need `compute.read` / `compute.manage`.
//! - [`NodeHealthAlerts`] (event consumer) turns `node.*` events into alerts: each health signal
//!   (`disk_low`, `gpu_hot`, `gpu_lost`, `clock_skew`, `heartbeat_late`) and going offline.
//!
//! Register with `Box::new(crate::modules::alerts::AlertsModule::new(db.clone())),` and the
//! consumer `Arc::new(crate::modules::alerts::NodeHealthAlerts),`.

use crate::app::{AppState, ServerModule};
use crate::db::{Db, DbError, Tx};
use crate::error::{ApiError, ErrorCode};
use crate::http::pagination::{Page, PageQuery};
use crate::modules::events::{ConsumerError, Event, EventConsumer};
use crate::modules::jobs::scope::{self, org_text};
use crate::modules::jobs::JobsError;
use crate::modules::telemetry::codes;
use crate::seams::{RequestContext, Resource};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use mrl_domain::ids::{AlertId, NodeId, ProjectId, RunId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;
use time::OffsetDateTime;
use utoipa::OpenApi;
use uuid::Uuid;

/// What an alert is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertScope {
    /// A run.
    Run(Uuid),
    /// A compute node.
    Node(Uuid),
    /// The installation.
    System,
}

/// How bad it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// For information.
    Info,
    /// Needs a look.
    Warning,
    /// Something broke.
    Error,
    /// Needs action now.
    Critical,
}

impl Severity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Critical => "critical",
        }
    }
}

/// An alert to raise.
#[derive(Debug, Clone, PartialEq)]
pub struct NewAlert {
    /// Owning org (nil for the solo tenant and platform nodes).
    pub org: Uuid,
    /// Owning project, for run alerts.
    pub project: Option<Uuid>,
    /// What it is about.
    pub scope: AlertScope,
    /// `safety`, `node`, `budget`, `system` or `metric`.
    pub source: &'static str,
    /// Stable machine code, like `node.disk_low` (1–100 characters).
    pub code: String,
    /// Severity.
    pub severity: Severity,
    /// Plain-language title (1–300 characters).
    pub title: String,
    /// Extra facts (JSON object).
    pub details: Value,
    /// Same key = same alert while it is live, like `node:<id>:disk_low`.
    pub dedup_key: String,
}

/// Raises and resolves alerts inside the caller's transaction.
pub struct AlertSink;

impl AlertSink {
    /// Opens an alert, or refreshes the live one with the same org and key. Returns its id.
    pub async fn fire(tx: &mut Tx, a: &NewAlert) -> Result<Uuid, DbError> {
        let (kind, scope_id) = match a.scope {
            AlertScope::Run(id) => ("run", Some(id)),
            AlertScope::Node(id) => ("node", Some(id)),
            AlertScope::System => ("system", None),
        };
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO alerts (id, org_id, project_id, scope_kind, scope_id, source, code, severity,
                    title, details, dedup_key, state)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'OPEN')
             ON CONFLICT (org_id, dedup_key) WHERE state <> 'RESOLVED' DO UPDATE SET
               severity = EXCLUDED.severity, title = EXCLUDED.title, details = EXCLUDED.details,
               last_fired_at = now(), fire_count = alerts.fire_count + 1,
               version = alerts.version + 1, xid = pg_current_xact_id()
             RETURNING id",
        )
        .bind(Uuid::now_v7())
        .bind(a.org)
        .bind(a.project)
        .bind(kind)
        .bind(scope_id)
        .bind(a.source)
        .bind(&a.code)
        .bind(a.severity.as_str())
        .bind(&a.title)
        .bind(&a.details)
        .bind(&a.dedup_key)
        .fetch_one(&mut **tx)
        .await?;
        Ok(id)
    }

    /// Resolves the live alert of `org` with `dedup_key`, if there is one.
    pub async fn resolve(
        tx: &mut Tx,
        org: Uuid,
        dedup_key: &str,
        by: &str,
    ) -> Result<Option<Uuid>, DbError> {
        Ok(sqlx::query_scalar(
            "UPDATE alerts SET state = 'RESOLVED', resolved_at = now(), resolved_by = $3,
                    version = version + 1, xid = pg_current_xact_id()
              WHERE org_id = $1 AND dedup_key = $2 AND state <> 'RESOLVED' RETURNING id",
        )
        .bind(org)
        .bind(dedup_key)
        .bind(by)
        .fetch_optional(&mut **tx)
        .await?)
    }
}

/// One alert.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct AlertView {
    /// `alr_…`.
    pub id: String,
    /// `run`, `node` or `system`.
    pub scope_kind: String,
    /// `run_…` or `node_…`; null for system alerts.
    pub scope_id: Option<String>,
    /// `prj_…` for run alerts.
    pub project_id: Option<String>,
    /// Who raised it: `safety`, `node`, `budget`, `system` or `metric`.
    pub source: String,
    /// Machine code, like `node.disk_low`.
    pub code: String,
    /// `info`, `warning`, `error` or `critical`.
    pub severity: String,
    /// What happened.
    pub title: String,
    /// Extra facts.
    #[schema(value_type = Object)]
    pub details: Value,
    /// `OPEN`, `ACKED` or `RESOLVED`.
    pub state: String,
    /// Times it fired while live.
    pub fire_count: i32,
    /// First fired.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub fired_at: OffsetDateTime,
    /// Last fired.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub last_fired_at: OffsetDateTime,
    /// Acknowledged at.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub acked_at: Option<OffsetDateTime>,
    /// Acknowledged by (principal id).
    pub acked_by: Option<String>,
    /// Resolved at.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub resolved_at: Option<OffsetDateTime>,
    /// Resolved by (principal id, or `system`).
    pub resolved_by: Option<String>,
}

/// An alert row with what authorization needs.
#[derive(Debug, Clone, PartialEq)]
pub struct AlertRow {
    /// Id.
    pub id: Uuid,
    /// Org.
    pub org: Uuid,
    /// Project.
    pub project: Option<Uuid>,
    /// Scope kind.
    pub scope_kind: String,
    /// Scope id.
    pub scope_id: Option<Uuid>,
    /// Creator of the run, for run alerts.
    pub run_created_by: Option<String>,
    /// The API view.
    pub view: AlertView,
}

/// Columns [`row_of`] reads, from `alerts a LEFT JOIN runs r ON r.id = a.scope_id`.
pub const ALERT_COLS: &str =
    "a.id, a.org_id, a.project_id, a.scope_kind, a.scope_id, a.source, a.code,
    a.severity, a.title, a.details, a.state, a.fire_count, a.fired_at, a.last_fired_at, a.acked_at,
    a.acked_by, a.resolved_at, a.resolved_by, r.created_by AS run_created_by";

/// Reads one row selected with [`ALERT_COLS`].
pub fn row_of(r: &sqlx::postgres::PgRow) -> Result<AlertRow, sqlx::Error> {
    let id: Uuid = r.try_get("id")?;
    let project: Option<Uuid> = r.try_get("project_id")?;
    let scope_kind: String = r.try_get("scope_kind")?;
    let scope_id: Option<Uuid> = r.try_get("scope_id")?;
    let shown = scope_id.map(|s| match scope_kind.as_str() {
        "run" => RunId::from_uuid(s).to_string(),
        _ => NodeId::from_uuid(s).to_string(),
    });
    Ok(AlertRow {
        id,
        org: r.try_get("org_id")?,
        project,
        scope_kind: scope_kind.clone(),
        scope_id,
        run_created_by: r.try_get("run_created_by")?,
        view: AlertView {
            id: AlertId::from_uuid(id).to_string(),
            scope_kind,
            scope_id: shown,
            project_id: project.map(|p| ProjectId::from_uuid(p).to_string()),
            source: r.try_get("source")?,
            code: r.try_get("code")?,
            severity: r.try_get("severity")?,
            title: r.try_get("title")?,
            details: r.try_get("details")?,
            state: r.try_get("state")?,
            fire_count: r.try_get("fire_count")?,
            fired_at: r.try_get("fired_at")?,
            last_fired_at: r.try_get("last_fired_at")?,
            acked_at: r.try_get("acked_at")?,
            acked_by: r.try_get("acked_by")?,
            resolved_at: r.try_get("resolved_at")?,
            resolved_by: r.try_get("resolved_by")?,
        },
    })
}

impl AlertRow {
    /// The authorizer's resource and the action that reads it.
    fn read(&self) -> (&'static str, Resource) {
        match (self.scope_kind.as_str(), self.scope_id, self.project) {
            ("run", Some(run), Some(project)) => (
                "metrics_logs.read",
                scope::resource(
                    "run",
                    Some(RunId::from_uuid(run).to_string()),
                    self.org,
                    project,
                ),
            ),
            _ => ("compute.read", self.machine_resource()),
        }
    }

    fn machine_resource(&self) -> Resource {
        Resource {
            kind: if self.scope_kind == "node" {
                "node"
            } else {
                "alert"
            }
            .to_owned(),
            id: Some(match self.scope_id {
                Some(n) if self.scope_kind == "node" => NodeId::from_uuid(n).to_string(),
                _ => AlertId::from_uuid(self.id).to_string(),
            }),
            org: org_text(self.org),
            project: None,
        }
    }
}

/// True if the caller may see `row` (Deny and Hide both hide it; an authorizer error is an
/// error).
pub async fn visible(
    state: &AppState,
    ctx: &RequestContext,
    row: &AlertRow,
) -> Result<bool, JobsError> {
    let (action, res) = row.read();
    scope::allowed(state, ctx, action, &res).await
}

/// Authorizes a change of `row` (hidden → 404, refused → 403).
async fn authorize_change(
    state: &AppState,
    ctx: &RequestContext,
    row: &AlertRow,
) -> Result<(), JobsError> {
    // Reading it comes first: hidden answers like missing (404), refused is 403.
    let (action, res) = row.read();
    scope::authorize(state, ctx, action, &res).await?;
    match (row.scope_kind.as_str(), &row.run_created_by) {
        ("run", Some(by)) => {
            let (_, res) = row.read();
            scope::authorize_cancel(state, ctx, &res, by).await
        }
        _ => scope::authorize(state, ctx, "compute.manage", &row.machine_resource()).await,
    }
}

/// Query parameters of `GET /alerts`.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AlertListParams {
    /// `OPEN`, `ACKED` or `RESOLVED`.
    pub state: Option<String>,
    /// `run`, `node` or `system`.
    pub scope_kind: Option<String>,
    /// Only this run's alerts, `run_…` (needs `metrics_logs.read` on the run).
    pub run: Option<String>,
    /// Page size, 1–200 (default 50).
    pub limit: Option<String>,
    /// `next_cursor` from the previous page.
    pub cursor: Option<String>,
}

fn one_of(field: &str, v: Option<&str>, allowed: &[&str]) -> Result<Option<String>, JobsError> {
    match v {
        None => Ok(None),
        Some(s) if allowed.contains(&s) => Ok(Some(s.to_owned())),
        Some(s) => Err(crate::modules::telemetry::field_error(
            field,
            &format!("{field} {s:?} is not one of {}.", allowed.join(", ")),
            "Use one of the listed values, or leave it out.",
        )),
    }
}

/// `GET /alerts`: the alerts you may see, newest first.
#[utoipa::path(get, path = "/alerts", tag = "alerts", operation_id = "list_alerts",
    params(AlertListParams),
    responses((status = 200, body = AlertPage)))]
pub async fn list(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Query(q): Query<AlertListParams>,
) -> Result<Json<Page<AlertView>>, JobsError> {
    let page = PageQuery {
        limit: q.limit.clone(),
        cursor: q.cursor.clone(),
    }
    .resolve()?;
    let st = one_of("state", q.state.as_deref(), &["OPEN", "ACKED", "RESOLVED"])?;
    let kind = one_of(
        "scope_kind",
        q.scope_kind.as_deref(),
        &["run", "node", "system"],
    )?;
    let mut after = scope::after::<AlertId>(&page, |a| a.as_uuid())?;
    // One run's alerts: authorized on the run itself (hidden or missing → 404, refused → 403).
    let (org, run) = match q.run.as_deref() {
        Some(run) => {
            let row =
                crate::modules::telemetry::read_run(&state, &db, &ctx, run, "metrics_logs.read")
                    .await?;
            (row.org_id, Some(row.id))
        }
        None => (scope::tenant(&ctx)?, None),
    };
    let mut items = Vec::new();
    // ponytail: scans in pages of 200 until the page is full; filtering by the authorizer per
    // row costs more when the caller sees few alerts of a big org.
    loop {
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {ALERT_COLS} FROM alerts a LEFT JOIN runs r ON a.scope_kind = 'run' AND r.id = a.scope_id
              WHERE a.org_id = $1 AND ($2::uuid IS NULL OR a.id < $2)
                AND ($3::text IS NULL OR a.state = $3) AND ($4::text IS NULL OR a.scope_kind = $4)
                AND ($5::uuid IS NULL OR (a.scope_kind = 'run' AND a.scope_id = $5))
              ORDER BY a.id DESC LIMIT 200"
        )))
        .bind(org)
        .bind(after)
        .bind(st.as_deref())
        .bind(kind.as_deref())
        .bind(run)
        .fetch_all(db.pool())
        .await?;
        let done = rows.len() < 200;
        for r in &rows {
            let row = row_of(r)?;
            after = Some(row.id);
            if visible(&state, &ctx, &row).await? {
                items.push(row);
                if items.len() > page.limit {
                    break;
                }
            }
        }
        if done || items.len() > page.limit {
            break;
        }
    }
    let (rows, next) = scope::cut(items, &page, |r| AlertId::from_uuid(r.id).to_string());
    Ok(Json(Page {
        items: rows.into_iter().map(|r| r.view).collect(),
        next_cursor: next,
    }))
}

/// A page of alerts (OpenAPI name).
#[derive(Serialize, utoipa::ToSchema)]
pub struct AlertPage {
    /// Alerts.
    pub items: Vec<AlertView>,
    /// Pass as `cursor` for the next page; null on the last page.
    pub next_cursor: Option<String>,
}

async fn find(db: &Db, id: Uuid) -> Result<Option<AlertRow>, JobsError> {
    let r = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {ALERT_COLS} FROM alerts a LEFT JOIN runs r ON a.scope_kind = 'run' AND r.id = a.scope_id
          WHERE a.id = $1"
    )))
    .bind(id)
    .fetch_optional(db.pool())
    .await?;
    Ok(r.as_ref().map(row_of).transpose()?)
}

/// `POST /alerts/{alr}:ack` and `POST /alerts/{alr}:resolve`.
#[utoipa::path(post, path = "/alerts/{alr}:ack", tag = "alerts", operation_id = "ack_alert",
    params(("alr" = String, Path, description = "`alr_…`")),
    responses((status = 200, body = AlertView), (status = 404, description = "No such alert"),
        (status = 409, description = "The alert is resolved (MRL-STATE-220)")))]
pub async fn action(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Path(target): Path<String>,
) -> Result<Json<AlertView>, JobsError> {
    let (id, verb) = target
        .rsplit_once(':')
        .ok_or_else(|| ApiError::new(ErrorCode::RouteNotFound, "Use :ack or :resolve."))?;
    let resolve = match verb {
        "ack" => false,
        "resolve" => true,
        _ => return Err(ApiError::new(ErrorCode::RouteNotFound, "Use :ack or :resolve.").into()),
    };
    let id: AlertId = scope::parse_id(id, "alert")?;
    let row = find(&db, id.as_uuid())
        .await?
        .ok_or_else(|| JobsError::not_found("alert"))?;
    // Authorize on the alert's fixed identity before the write transaction.
    authorize_change(&state, &ctx, &row).await?;
    let mut tx = db.begin().await?;
    let current: String = sqlx::query_scalar("SELECT state FROM alerts WHERE id = $1 FOR UPDATE")
        .bind(row.id)
        .fetch_one(&mut *tx)
        .await?;
    let changed = match (current.as_str(), resolve) {
        ("RESOLVED", false) => {
            return Err(JobsError::domain(
                codes::ALERT_RESOLVED,
                StatusCode::CONFLICT,
                "This alert is already resolved, so it can't be acknowledged.",
            ))
        }
        ("RESOLVED", true) | ("ACKED", false) => false,
        (_, false) => {
            sqlx::query(
                "UPDATE alerts SET state = 'ACKED', acked_at = now(), acked_by = $2,
                        version = version + 1, xid = pg_current_xact_id() WHERE id = $1",
            )
            .bind(row.id)
            .bind(&ctx.principal.id)
            .execute(&mut *tx)
            .await?;
            true
        }
        (_, true) => {
            sqlx::query(
                "UPDATE alerts SET state = 'RESOLVED', resolved_at = now(), resolved_by = $2,
                        version = version + 1, xid = pg_current_xact_id() WHERE id = $1",
            )
            .bind(row.id)
            .bind(&ctx.principal.id)
            .execute(&mut *tx)
            .await?;
            true
        }
    };
    if changed {
        let event = crate::seams::AuditEvent {
            at: state.seams.clock.now(),
            actor: ctx.principal.id.clone(),
            org: org_text(row.org),
            project: row.project.map(|p| ProjectId::from_uuid(p).to_string()),
            action: if resolve {
                "alerts.resolve"
            } else {
                "alerts.ack"
            }
            .to_owned(),
            target: AlertId::from_uuid(row.id).to_string(),
            outcome: crate::seams::AuditOutcome::Success,
            trace_id: ctx.trace_id.clone(),
        };
        state.seams.audit.record_in(&mut tx, event).await?;
    }
    tx.commit().await?;
    let row = find(&db, row.id)
        .await?
        .ok_or_else(|| JobsError::not_found("alert"))?;
    Ok(Json(row.view))
}

/// The `alerts` module (routes).
pub struct AlertsModule {
    db: Db,
}

impl AlertsModule {
    /// The module on `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(list, action),
    components(schemas(AlertView, AlertPage)),
    tags((name = "alerts", description = "Alerts: list, acknowledge and resolve."))
)]
struct Doc;

impl ServerModule for AlertsModule {
    fn name(&self) -> &'static str {
        "alerts"
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route("/alerts", get(list))
            .route("/alerts/{alr}", post(action))
            .route_layer(axum::middleware::from_fn(
                crate::modules::jobs::error::render,
            ))
            .layer(Extension(self.db.clone()))
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        doc()
    }
}

/// The OpenAPI fragment: `:resolve` has the same shape as `:ack`.
fn doc() -> utoipa::openapi::OpenApi {
    let mut doc = Doc::openapi();
    if let Some(mut item) = doc.paths.paths.get("/alerts/{alr}:ack").cloned() {
        if let Some(op) = item.post.as_mut() {
            op.operation_id = Some("resolve_alert".to_owned());
            op.summary = Some("Resolve an alert.".to_owned());
        }
        doc.paths
            .paths
            .insert("/alerts/{alr}:resolve".to_owned(), item);
    }
    doc
}

/// Health signals that become node alerts: `(signal, severity, title)`.
const NODE_SIGNALS: &[(&str, Severity, &str)] = &[
    (
        "disk_low",
        Severity::Warning,
        "A machine is low on disk space.",
    ),
    (
        "gpu_hot",
        Severity::Warning,
        "A GPU on a machine is running too hot.",
    ),
    (
        "gpu_lost",
        Severity::Error,
        "A GPU on a machine stopped responding.",
    ),
    ("clock_skew", Severity::Warning, "A machine's clock is off."),
    (
        "heartbeat_late",
        Severity::Warning,
        "A machine is answering late.",
    ),
    ("offline", Severity::Error, "A machine went offline."),
];

/// Event consumer: node health events become alerts and resolve them again.
pub struct NodeHealthAlerts;

impl NodeHealthAlerts {
    /// The signals named by a node's status reason, like `health:disk_low,gpu_hot`.
    pub fn signals(reason: &str) -> Vec<&str> {
        if reason == "heartbeat_late" {
            return vec!["heartbeat_late"];
        }
        reason
            .strip_prefix("health:")
            .map(|s| s.split(',').filter(|s| !s.is_empty()).collect())
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl EventConsumer for NodeHealthAlerts {
    fn name(&self) -> &'static str {
        "alerts-node-health"
    }

    fn accepts(&self, t: &str) -> bool {
        matches!(
            t,
            "node.degraded"
                | "node.recovered"
                | "node.disconnected"
                | "node.reconnected"
                | "node.decommissioned"
        )
    }

    async fn handle(&self, tx: &mut Tx, e: &Event) -> Result<(), ConsumerError> {
        let node = e.aggregate_id;
        let org: Option<Uuid> = sqlx::query_scalar(
            "SELECT p.org_id FROM compute_nodes n JOIN compute_pools p ON p.id = n.pool_id WHERE n.id = $1",
        )
        .bind(node)
        .fetch_optional(&mut **tx)
        .await?
        .flatten();
        let org = org.unwrap_or_else(Uuid::nil);
        let reason = e.payload["reason"].as_str().unwrap_or_default();
        let on: Vec<&str> = match e.event_type.as_str() {
            "node.degraded" => Self::signals(reason),
            "node.disconnected" => vec!["offline"],
            _ => Vec::new(),
        };
        // Which signals this event clears.
        let clear: Vec<&str> = match e.event_type.as_str() {
            "node.degraded" => NODE_SIGNALS
                .iter()
                .map(|s| s.0)
                .filter(|s| *s != "offline" && !on.contains(s))
                .collect(),
            "node.recovered" => NODE_SIGNALS
                .iter()
                .map(|s| s.0)
                .filter(|s| *s != "offline")
                .collect(),
            "node.reconnected" => vec!["offline"],
            "node.decommissioned" => NODE_SIGNALS.iter().map(|s| s.0).collect(),
            _ => Vec::new(),
        };
        let node_id = NodeId::from_uuid(node).to_string();
        for (signal, severity, title) in NODE_SIGNALS.iter().filter(|s| on.contains(&s.0)) {
            AlertSink::fire(
                tx,
                &NewAlert {
                    org,
                    project: None,
                    scope: AlertScope::Node(node),
                    source: "node",
                    code: format!("node.{signal}"),
                    severity: *severity,
                    title: (*title).to_owned(),
                    details: json!({ "node_id": node_id, "reason": reason, "event_id": e.event_id.to_string() }),
                    dedup_key: format!("node:{node_id}:{signal}"),
                },
            )
            .await?;
        }
        for signal in clear {
            AlertSink::resolve(tx, org, &format!("node:{node_id}:{signal}"), "system").await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reasons_name_their_signals() {
        assert_eq!(
            NodeHealthAlerts::signals("health:disk_low,gpu_hot"),
            vec!["disk_low", "gpu_hot"]
        );
        assert_eq!(
            NodeHealthAlerts::signals("heartbeat_late"),
            vec!["heartbeat_late"]
        );
        assert!(NodeHealthAlerts::signals("operator").is_empty());
        let c = NodeHealthAlerts;
        assert!(
            c.accepts("node.degraded") && !c.accepts("node.enrolled") && !c.accepts("job.queued")
        );
    }

    #[test]
    fn openapi_has_both_actions() {
        let doc = doc();
        for p in ["/alerts", "/alerts/{alr}:ack", "/alerts/{alr}:resolve"] {
            assert!(doc.paths.paths.contains_key(p), "{p} missing");
        }
    }
}

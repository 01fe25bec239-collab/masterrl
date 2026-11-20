//! The audit log (`audit_events`): who did what, when, and how it ended.
//!
//! Handlers that change data write the row in their own transaction with [`write`], so the
//! record and the change commit together. [`PgAuditSink`] is P07's `AuditSink` seam for
//! callers without a transaction. `GET /audit` lists the log (action `audit.read`).

use crate::app::{AppState, Task};
use crate::config::Role;
use crate::db::{Db, DbError, Tx};
use crate::error::{ApiError, ErrorCode};
use crate::http::pagination::{Page, PageQuery};
use crate::modules::auth::db_error;
use crate::seams::{
    AuditEvent, AuditOutcome, AuditSink, RequestContext, Resource, SeamError, StartupStep,
};
use crate::ServerModule;
use async_trait::async_trait;
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use utoipa::{IntoParams, OpenApi, ToSchema};
use uuid::Uuid;

/// An audit event from its parts.
pub fn event(
    at: OffsetDateTime,
    actor: &str,
    org: Option<&str>,
    action: &str,
    target: &str,
    outcome: AuditOutcome,
    trace_id: &str,
) -> AuditEvent {
    AuditEvent {
        at,
        actor: actor.to_owned(),
        org: org.map(str::to_owned),
        project: None,
        action: action.to_owned(),
        target: target.to_owned(),
        outcome,
        trace_id: trace_id.to_owned(),
    }
}

fn outcome_name(o: AuditOutcome) -> &'static str {
    match o {
        AuditOutcome::Success => "success",
        AuditOutcome::Denied => "denied",
        AuditOutcome::Failed => "failed",
    }
}

/// Writes `e` in the caller's transaction. `details` must never hold secret values.
pub async fn write(
    tx: &mut Tx,
    e: &AuditEvent,
    details: &serde_json::Value,
) -> Result<(), DbError> {
    sqlx::query(
        "INSERT INTO audit_events (id, at, org_id, project_id, actor, action, target, outcome, details, trace_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(Uuid::now_v7())
    .bind(e.at)
    .bind(&e.org)
    .bind(&e.project)
    .bind(&e.actor)
    .bind(&e.action)
    .bind(&e.target)
    .bind(outcome_name(e.outcome))
    .bind(details)
    .bind(&e.trace_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The `AuditSink` seam over `audit_events`, for records that stand alone: a refusal or a
/// failed attempt, which no transaction of the caller's can undo.
///
/// The seam's `record` cannot join the caller's transaction (it takes no `Tx`), so a success
/// written here could stay committed while the change it describes rolls back. Successes are
/// therefore refused: write them with [`write`] inside the change's own transaction.
pub struct PgAuditSink {
    db: Db,
}

impl PgAuditSink {
    /// A sink writing to `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[async_trait]
impl AuditSink for PgAuditSink {
    async fn record(&self, e: AuditEvent) -> Result<(), SeamError> {
        if e.outcome == AuditOutcome::Success {
            return Err(SeamError::new(
                "audit",
                format!(
                    "{}: record a successful change with audit::write in the transaction that makes it, so both commit or neither does",
                    e.action
                ),
            ));
        }
        let mut tx = self.db.begin().await?;
        write(&mut tx, &e, &serde_json::json!({})).await?;
        tx.commit().await.map_err(DbError::from)?;
        Ok(())
    }
}

/// Makes this month's and the next two months' partitions.
pub async fn ensure_partitions(db: &Db) -> Result<(), DbError> {
    sqlx::query(
        "SELECT audit_ensure_partition((date_trunc('month', now()) + make_interval(months => i))::date) \
         FROM generate_series(0, 2) AS i",
    )
    .execute(db.pool())
    .await?;
    Ok(())
}

/// `GET /audit`, monthly partitions at startup and once a day.
pub struct AuditModule {
    db: Db,
}

impl AuditModule {
    /// The module over `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

struct Partitions(Db);

#[async_trait]
impl StartupStep for Partitions {
    fn name(&self) -> &str {
        "audit-partitions"
    }

    async fn run(&self) -> Result<(), SeamError> {
        Ok(ensure_partitions(&self.0).await?)
    }
}

impl ServerModule for AuditModule {
    fn name(&self) -> &'static str {
        "audit"
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route("/audit", get(list))
            .layer(Extension(self.db.clone()))
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        AuditDoc::openapi()
    }

    fn startup_steps(&self) -> Vec<Arc<dyn StartupStep>> {
        vec![Arc::new(Partitions(self.db.clone()))]
    }

    fn background_tasks(&self) -> Vec<Task> {
        let db = self.db.clone();
        vec![Task::new(
            "audit.partitions",
            Role::Background,
            move |cancel| async move {
                loop {
                    tokio::select! {
                        () = cancel.cancelled() => return Ok(()),
                        () = tokio::time::sleep(Duration::from_secs(24 * 3600)) => {}
                    }
                    // Rows outside every partition still land in the default one, so a miss is not fatal.
                    if let Err(e) = ensure_partitions(&db).await {
                        tracing::warn!(error = %e, "could not add audit log partitions");
                    }
                }
            },
        )]
    }
}

#[derive(OpenApi)]
#[openapi(paths(list), components(schemas(AuditRecord)), tags((name = "audit", description = "The audit log.")))]
struct AuditDoc;

/// Filters for `GET /audit`.
#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AuditQuery {
    /// Page size, 1–200 (default 50).
    pub limit: Option<String>,
    /// `next_cursor` from the previous page.
    pub cursor: Option<String>,
    /// Only this action, like `auth.session`.
    pub action: Option<String>,
    /// Only this actor, like `usr_…`.
    pub actor: Option<String>,
}

/// One audit record.
#[derive(Debug, Serialize, ToSchema)]
pub struct AuditRecord {
    /// Record id.
    pub id: String,
    /// When.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub at: OffsetDateTime,
    /// Org.
    pub org: Option<String>,
    /// Project.
    pub project: Option<String>,
    /// Who.
    pub actor: String,
    /// What, like `auth.manage_tokens`.
    pub action: String,
    /// On what.
    pub target: String,
    /// `success`, `denied` or `failed`.
    pub outcome: String,
    /// Extra facts (never secret values).
    #[schema(value_type = Object)]
    pub details: serde_json::Value,
    /// Trace id.
    pub trace_id: String,
}

type Row = (
    Uuid,
    OffsetDateTime,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
    String,
    serde_json::Value,
    String,
);

#[utoipa::path(get, path = "/audit", tag = "audit", params(AuditQuery),
    responses((status = 200, description = "Newest first.", body = Page<AuditRecord>), (status = 403, body = crate::error::Problem)))]
async fn list(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Query(q): Query<AuditQuery>,
) -> Result<Json<Page<AuditRecord>>, ApiError> {
    let resource = Resource {
        org: ctx.org.clone(),
        ..Resource::kind("audit_log")
    };
    state.authorize(&ctx, "audit.read", &resource).await?;
    let page = PageQuery {
        limit: q.limit.clone(),
        cursor: q.cursor.clone(),
    }
    .resolve()?;
    let after = page
        .after
        .as_deref()
        .map(|c| {
            let (at, id) = c.split_once('|')?;
            let nanos = at.parse::<i128>().ok()?;
            Some((
                OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()?,
                Uuid::parse_str(id).ok()?,
            ))
        })
        .map(|p| {
            p.ok_or_else(|| {
                ApiError::new(
                    ErrorCode::Pagination,
                    "cursor must be the next_cursor value from an earlier page, unchanged.",
                )
            })
        })
        .transpose()?;
    // Solo has one person; team mode shows only the caller's org.
    let org = match ctx.principal.kind {
        crate::seams::PrincipalKind::LocalOwner => None,
        _ => Some(ctx.org.clone().unwrap_or_default()),
    };
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT id, at, org_id, project_id, actor, action, target, outcome, details, trace_id FROM audit_events \
         WHERE ($1::text IS NULL OR org_id = $1) AND ($2::text IS NULL OR action = $2) AND ($3::text IS NULL OR actor = $3) \
           AND ($4::timestamptz IS NULL OR (at, id) < ($4, $5)) \
         ORDER BY at DESC, id DESC LIMIT $6",
    )
    .bind(org)
    .bind(&q.action)
    .bind(&q.actor)
    .bind(after.map(|a| a.0))
    .bind(after.map(|a| a.1))
    .bind(i64::try_from(page.fetch()).unwrap_or(201))
    .fetch_all(db.pool())
    .await
    .map_err(|e| db_error(e.into()))?;
    let items = rows
        .into_iter()
        .map(
            |(id, at, org, project, actor, action, target, outcome, details, trace_id)| {
                AuditRecord {
                    id: id.to_string(),
                    at,
                    org,
                    project,
                    actor,
                    action,
                    target,
                    outcome,
                    details,
                    trace_id,
                }
            },
        )
        .collect();
    Ok(Json(Page::from_overfetch(items, &page, |r| {
        format!("{}|{}", r.at.unix_timestamp_nanos(), r.id)
    })))
}

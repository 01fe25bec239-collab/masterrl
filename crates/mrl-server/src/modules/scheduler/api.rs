//! `GET /queue?pool=`: the jobs waiting for a pool, in the order they will be placed.
//!
//! Needs `compute.read` on the pool (a missing or hidden pool is 404). Each job is listed
//! only when the caller may read it (`runs.read`); positions count every job ahead, seen or
//! not.

use super::{Core, SchedError};
use crate::error::{ApiError, ErrorCode};
use crate::modules::jobs::scope;
use crate::seams::{RequestContext, Resource};
use crate::AppState;
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Extension, Json, Router};
use mrl_domain::ids::{JobId, OrgId, PoolId, ProjectId, RunId};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use time::OffsetDateTime;
use utoipa::{IntoParams, OpenApi, ToSchema};
use uuid::Uuid;

/// One waiting job.
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub struct SchedulerQueueEntry {
    /// `job_…`.
    pub job_id: String,
    /// `run_…`, for training and evaluation jobs.
    pub run_id: Option<String>,
    /// `prj_…`.
    pub project_id: String,
    /// 1 = placed next.
    pub position: u32,
    /// Higher goes first.
    pub priority: i32,
    /// When it joined the queue.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub queued_at: Option<OffsetDateTime>,
    /// Why it is still waiting, in plain words.
    pub reason: Option<String>,
    /// The reason's code, like `MRL-QUOTA-210`.
    pub reason_code: Option<String>,
}

/// The queue of a pool.
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub struct SchedulerQueue {
    /// `pool_…`.
    pub pool_id: String,
    /// Jobs waiting for this pool, including ones the caller can't see.
    pub total: u32,
    /// The jobs the caller may see, first in line first.
    pub jobs: Vec<SchedulerQueueEntry>,
}

/// Query of `GET /queue`.
#[derive(Debug, Deserialize, IntoParams)]
pub struct SchedulerQueueQuery {
    /// `pool_…` (required).
    pub pool: Option<String>,
    /// Most jobs to list, 1–500 (default 100).
    pub limit: Option<u32>,
}

#[derive(OpenApi)]
#[openapi(
    paths(get_queue),
    components(schemas(SchedulerQueue, SchedulerQueueEntry))
)]
struct Doc;

/// The OpenAPI paths.
pub fn openapi() -> utoipa::openapi::OpenApi {
    Doc::openapi()
}

/// The routes.
pub fn routes(core: Arc<Core>) -> Router<AppState> {
    Router::new()
        .route("/queue", get(get_queue))
        .layer(Extension(core))
}

fn unavailable(e: impl std::fmt::Display) -> ApiError {
    tracing::error!(error = %e, "queue request failed");
    ApiError::new(ErrorCode::Unavailable, "The database did not answer.")
}

fn field(path: &str, message: &str) -> ApiError {
    let mut e = ApiError::validation(vec![crate::error::FieldError {
        path: path.to_owned(),
        code: ErrorCode::FieldValue.as_str().to_owned(),
        message: message.to_owned(),
        hint: message.to_owned(),
    }]);
    e.detail = message.to_owned();
    e
}

#[utoipa::path(get, path = "/queue", tag = "compute", operation_id = "get_scheduler_queue",
    params(SchedulerQueueQuery),
    responses((status = 200, description = "The pool's queue.", body = SchedulerQueue),
              (status = 404, description = "No such pool.", body = crate::error::Problem),
              (status = 422, description = "Bad query.", body = crate::error::Problem)))]
async fn get_queue(
    State(s): State<AppState>,
    Extension(core): Extension<Arc<Core>>,
    ctx: RequestContext,
    Query(q): Query<SchedulerQueueQuery>,
) -> Result<Json<SchedulerQueue>, ApiError> {
    let pool = q
        .pool
        .as_deref()
        .ok_or_else(|| field("/pool", "Name the pool, like ?pool=pool_…."))?;
    let pool = PoolId::from_str(pool)
        .map_err(|_| field("/pool", "Use a pool id, like pool_…."))?
        .as_uuid();
    let limit = q.limit.unwrap_or(100);
    if !(1..=500).contains(&limit) {
        return Err(field("/limit", "Use 1–500."));
    }
    let missing = || ApiError::new(ErrorCode::NotFound, "No pool with this id exists.");
    let info = super::pool_info(&core.db, pool)
        .await
        .map_err(unavailable)?
        .ok_or_else(missing)?;
    let res = Resource {
        id: Some(PoolId::from_uuid(pool).to_string()),
        org: info.org_id.map(|o| OrgId::from_uuid(o).to_string()),
        ..Resource::kind("pool")
    };
    s.authorize(&ctx, "compute.read", &res).await?;
    let rows = queued_for(&core, &info).await.map_err(unavailable)?;
    let total = u32::try_from(rows.len()).unwrap_or(u32::MAX);
    let mut jobs = Vec::new();
    for (i, r) in rows.into_iter().enumerate() {
        if jobs.len() >= limit as usize {
            break;
        }
        let job = JobId::from_uuid(r.id).to_string();
        let res = scope::resource("job", Some(job.clone()), r.org_id, r.project_id);
        if !scope::allowed(&s, &ctx, "runs.read", &res)
            .await
            .map_err(unavailable)?
        {
            continue;
        }
        jobs.push(SchedulerQueueEntry {
            job_id: job,
            run_id: r.run_id.map(|x| RunId::from_uuid(x).to_string()),
            project_id: ProjectId::from_uuid(r.project_id).to_string(),
            position: u32::try_from(i + 1).unwrap_or(u32::MAX),
            priority: r.priority,
            queued_at: r.queued_at,
            reason: r
                .reason
                .as_ref()
                .and_then(|v| v["message"].as_str())
                .map(str::to_owned),
            reason_code: r
                .reason
                .as_ref()
                .and_then(|v| v["code"].as_str())
                .map(str::to_owned),
        });
    }
    Ok(Json(SchedulerQueue {
        pool_id: PoolId::from_uuid(pool).to_string(),
        total,
        jobs,
    }))
}

#[derive(sqlx::FromRow)]
struct Queued {
    id: Uuid,
    org_id: Uuid,
    project_id: Uuid,
    run_id: Option<Uuid>,
    priority: i32,
    queued_at: Option<OffsetDateTime>,
    reason: Option<serde_json::Value>,
}

/// QUEUED jobs that may use `pool`, in placement order.
async fn queued_for(
    core: &Core,
    pool: &super::pipeline::PoolInfo,
) -> Result<Vec<Queued>, SchedError> {
    Ok(sqlx::query_as(
        "SELECT id, org_id, project_id, run_id, priority, queued_at, state_reason AS reason
         FROM jobs WHERE state = 'QUEUED'
           AND ($1::uuid IS NULL OR org_id = $1) AND (pool_id IS NULL OR pool_id = $2)
         ORDER BY priority DESC, queued_at, id LIMIT 10000",
    )
    .bind(pool.org_id)
    .bind(pool.id)
    .fetch_all(core.db.pool())
    .await?)
}

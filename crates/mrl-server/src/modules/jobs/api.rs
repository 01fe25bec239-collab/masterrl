//! `/jobs` endpoints: list, read, cancel and change priority.

use super::error::JobsError;
use super::model::{Job, JobRow};
use super::scope::{self, resource};
use super::transitions::{job_cols, JobTransitions};
use crate::app::AppState;
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::http::etag;
use crate::http::pagination::{encode_cursor, Page, PageQuery};
use crate::seams::RequestContext;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum::Json;
use mrl_domain::ids::{JobId, ProjectId};
use mrl_domain::state::{Job as JobMachine, JobState, StateMachine};
use serde::Deserialize;

/// Filters for `GET /jobs`.
#[derive(Debug, Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct JobFilter {
    /// Only jobs in this state, like `QUEUED`.
    pub state: Option<String>,
    /// Only jobs of this kind, like `TRAIN`.
    pub kind: Option<String>,
    /// Only jobs in this project (`prj_…`).
    pub project: Option<String>,
}

/// `PATCH /jobs/{job}` body.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobPatch {
    /// New priority, −1000 to 1000. Higher runs first.
    pub priority: i32,
}

/// Highest priority a job may have.
pub const MAX_PRIORITY: i32 = 1000;

/// A JSON body with the resource's `ETag`.
pub fn with_etag<T: serde::Serialize>(version: i64, body: T) -> Response {
    ([(header::ETAG, etag::from_version(version))], Json(body)).into_response()
}

/// `GET /jobs`
#[utoipa::path(get, path = "/jobs", operation_id = "list_jobs", tag = "jobs",
    params(PageQuery, JobFilter),
    responses((status = 200, description = "A page of jobs, newest first.", body = Page<Job>)))]
pub async fn list(
    State(state): State<AppState>,
    Extension(db): Extension<crate::Db>,
    ctx: RequestContext,
    Query(q): Query<PageQuery>,
    Query(f): Query<JobFilter>,
) -> Result<Json<Page<Job>>, JobsError> {
    let org = scope::tenant(&ctx)?;
    let page = q.resolve()?;
    let after = scope::after::<JobId>(&page, |j| j.as_uuid())?;
    if let Some(s) = &f.state {
        s.parse::<JobState>().map_err(|_| {
            ApiError::new(
                ErrorCode::Filter,
                format!("state `{s}` is not a job state."),
            )
        })?;
    }
    let project = f
        .project
        .as_deref()
        .map(|p| {
            p.parse::<ProjectId>().map(|p| p.as_uuid()).map_err(|_| {
                ApiError::new(
                    ErrorCode::Filter,
                    format!("project `{p}` is not a project id."),
                )
            })
        })
        .transpose()?;
    // Scan in id order, keeping only jobs the caller may read, until the page is full. The
    // cursor is the last *visible* job, so hidden jobs never show up in it.
    // ponytail: one authorizer call per row; add a bulk "visible projects" seam call if pages get slow.
    let batch = i64::try_from(page.fetch()).unwrap_or(i64::MAX);
    let mut scan_from = after;
    let mut items: Vec<Job> = Vec::with_capacity(page.limit);
    let mut last_kept = None;
    let mut more = false;
    'scan: loop {
        let rows = sqlx::query_as::<_, JobRow>(concat!(
            "SELECT ",
            job_cols!(),
            " FROM jobs WHERE org_id = $1 AND ($2::text IS NULL OR state = $2) \
               AND ($3::text IS NULL OR kind = $3) AND ($4::uuid IS NULL OR project_id = $4) \
               AND ($5::uuid IS NULL OR id < $5) ORDER BY id DESC LIMIT $6"
        ))
        .bind(org)
        .bind(&f.state)
        .bind(&f.kind)
        .bind(project)
        .bind(scan_from)
        .bind(batch)
        .fetch_all(db.pool())
        .await?;
        let exhausted = rows.len() < page.fetch();
        for r in rows {
            scan_from = Some(r.id);
            let res = resource(
                "job",
                Some(JobId::from_uuid(r.id).to_string()),
                r.org_id,
                r.project_id,
            );
            if scope::allowed(&state, &ctx, "runs.read", &res).await? {
                if items.len() == page.limit {
                    more = true;
                    break 'scan;
                }
                last_kept = Some(r.id);
                items.push(r.view());
            }
        }
        if exhausted {
            break;
        }
    }
    let next_cursor = last_kept
        .filter(|_| more)
        .map(|id| encode_cursor(&JobId::from_uuid(id).to_string()));
    Ok(Json(Page { items, next_cursor }))
}

/// Reads a job for `ctx` and checks `runs.read`.
pub async fn read_job(
    state: &AppState,
    db: &crate::Db,
    ctx: &RequestContext,
    id: &str,
) -> Result<JobRow, JobsError> {
    let org = scope::tenant(ctx)?;
    let id: JobId = scope::parse_id(id, "job")?;
    let row = sqlx::query_as::<_, JobRow>(concat!(
        "SELECT ",
        job_cols!(),
        " FROM jobs WHERE id = $1 AND org_id = $2"
    ))
    .bind(id.as_uuid())
    .bind(org)
    .fetch_optional(db.pool())
    .await?
    .ok_or_else(|| JobsError::not_found("job"))?;
    let res = resource("job", Some(id.to_string()), row.org_id, row.project_id);
    scope::authorize(state, ctx, "runs.read", &res).await?;
    Ok(row)
}

/// `GET /jobs/{job}`
#[utoipa::path(get, path = "/jobs/{job}", operation_id = "get_job", tag = "jobs",
    params(("job" = String, Path, description = "Job id, `job_…`.")),
    responses((status = 200, description = "The job; `ETag` is its version.", body = Job),
              (status = 404, description = "No such job.", body = crate::error::Problem)))]
pub async fn get(
    State(state): State<AppState>,
    Extension(db): Extension<crate::Db>,
    ctx: RequestContext,
    Path(job): Path<String>,
) -> Result<Response, JobsError> {
    let row = read_job(&state, &db, &ctx, &job).await?;
    Ok(with_etag(row.version, row.view()))
}

/// `POST /jobs/{job}:cancel` (axum sees `/jobs/{job}` with the action in the segment).
#[utoipa::path(post, path = "/jobs/{job}:cancel", operation_id = "cancel_job", tag = "jobs",
    params(("job" = String, Path, description = "Job id, `job_…`.")),
    responses((status = 200, description = "The job, now CANCELLING or CANCELLED.", body = Job),
              (status = 409, description = "The job already finished.", body = crate::error::Problem)))]
pub async fn action(
    State(state): State<AppState>,
    Extension(db): Extension<crate::Db>,
    ctx: RequestContext,
    Path(segment): Path<String>,
) -> Result<Response, JobsError> {
    let Some(job) = segment.strip_suffix(":cancel") else {
        return Err(ApiError::new(ErrorCode::RouteNotFound, "No endpoint has this path.").into());
    };
    let row = cancel(&state, &db, &ctx, job).await?;
    Ok(with_etag(row.version, row.view()))
}

/// Cancels the job `id` for `ctx` (shared with `POST /runs/{run}:cancel`).
pub async fn cancel(
    state: &AppState,
    db: &crate::Db,
    ctx: &RequestContext,
    id: &str,
) -> Result<JobRow, JobsError> {
    // Authorize on the job's fixed identity (org, project, creator) before the write
    // transaction, so the authorizer never waits for a pool connection a lock holds.
    let seen = read_job(state, db, ctx, id).await?;
    let job = JobId::from_uuid(seen.id).to_string();
    let res = resource("job", Some(job.clone()), seen.org_id, seen.project_id);
    scope::authorize_cancel(state, ctx, &res, &seen.created_by).await?;
    let mut tx = db.begin().await?;
    let row = JobTransitions::lock(&mut tx, seen.org_id, seen.id)
        .await?
        .ok_or_else(|| JobsError::not_found("job"))?;
    let row = JobTransitions::request_cancel(&mut tx, &row, &ctx.principal.id).await?;
    tx.commit().await?;
    scope::audit(state, ctx, row.project_id, "runs.cancel", job).await;
    Ok(row)
}

/// `PATCH /jobs/{job}`: change priority (maintainers). `If-Match` is optional.
#[utoipa::path(patch, path = "/jobs/{job}", operation_id = "update_job", tag = "jobs",
    params(("job" = String, Path, description = "Job id, `job_…`.")),
    request_body = JobPatch,
    responses((status = 200, description = "The changed job.", body = Job),
              (status = 412, description = "`If-Match` is stale.", body = crate::error::Problem)))]
pub async fn patch(
    State(state): State<AppState>,
    Extension(db): Extension<crate::Db>,
    ctx: RequestContext,
    Path(job): Path<String>,
    headers: HeaderMap,
    Json(body): Json<JobPatch>,
) -> Result<Response, JobsError> {
    if !(-MAX_PRIORITY..=MAX_PRIORITY).contains(&body.priority) {
        return Err(ApiError::validation(vec![FieldError::new(
            "/priority",
            ErrorCode::FieldValue,
            format!(
                "priority {} is outside −{MAX_PRIORITY} to {MAX_PRIORITY}.",
                body.priority
            ),
            "Use a whole number from -1000 to 1000.",
        )])
        .into());
    }
    // Authorize before the write transaction (see `cancel`).
    let seen = read_job(&state, &db, &ctx, &job).await?;
    let job = JobId::from_uuid(seen.id).to_string();
    let res = resource("job", Some(job.clone()), seen.org_id, seen.project_id);
    scope::authorize(&state, &ctx, "runs.cancel_any", &res).await?;
    let org = seen.org_id;
    let mut tx = db.begin().await?;
    let row = JobTransitions::lock(&mut tx, org, seen.id)
        .await?
        .ok_or_else(|| JobsError::not_found("job"))?;
    etag::check_if_match(&headers, &etag::from_version(row.version), false)?;
    if JobMachine::is_terminal(row.job_state()?) {
        return Err(JobsError::illegal(format!(
            "This job is already {}, so its priority can't change.",
            row.state
        )));
    }
    let next = sqlx::query_as::<_, JobRow>(concat!(
        "UPDATE jobs SET priority = $3, version = version + 1, updated_at = now() \
         WHERE id = $1 AND org_id = $2 AND version = $4 RETURNING ",
        job_cols!()
    ))
    .bind(row.id)
    .bind(org)
    .bind(body.priority)
    .bind(row.version)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| JobsError::changed("job"))?;
    tx.commit().await?;
    scope::audit(&state, &ctx, next.project_id, "jobs.set_priority", job).await;
    Ok(with_etag(next.version, next.view()))
}

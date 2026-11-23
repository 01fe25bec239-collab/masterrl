//! `/runs/{run}/checkpoints` and `/checkpoints/{ckpt}`.
//!
//! Every route authorizes on the checkpoint's run (its org and project): a hidden, foreign or
//! missing run or checkpoint is the same 404. Reads need `checkpoints.read`; verify and delete
//! need `checkpoints.write`.

use super::cleanup::{in_use, tombstone};
use super::service::{
    ckpt_cols, hex, lock_job, lock_row, lock_run, CheckpointRow, CheckpointService, FileRow,
};
use crate::app::AppState;
use crate::error::{ApiError, ErrorCode};
use crate::http::pagination::{Page, PageQuery};
use crate::modules::jobs::{scope, JobsError};
use crate::seams::{RequestContext, Resource};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use mrl_domain::ids::{AttemptId, CheckpointId, RunId};
use serde::Serialize;
use std::sync::Arc;
use time::OffsetDateTime;
use uuid::Uuid;

/// A checkpoint as the API shows it.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct CheckpointInfo {
    /// `ckpt_…`.
    pub id: String,
    /// `run_…`.
    pub run_id: String,
    /// The attempt that wrote it.
    pub attempt_id: String,
    /// That attempt's fencing epoch.
    pub attempt_epoch: i64,
    /// Training step.
    pub step: i64,
    /// `full_resume`, `weights_only` or `adapter_only`.
    pub kind: String,
    /// Format tag, like `hf-trainer-v1`.
    pub layout: String,
    /// The run's final checkpoint.
    pub is_final: bool,
    /// `WRITING`, `UPLOADED`, `VERIFYING`, `COMMITTED`, `CORRUPT` or `TOMBSTONED`.
    pub state: String,
    /// `manifest_head` (files present with their sizes) or `full_rehash` (every byte re-read).
    pub verification_level: Option<String>,
    /// Why it is CORRUPT.
    pub corrupt_reason: Option<String>,
    /// Sum of file sizes.
    pub total_bytes: Option<i64>,
    /// Number of files.
    pub file_count: Option<i32>,
    /// Metrics at this step.
    #[schema(value_type = Object)]
    pub metrics: serde_json::Value,
    /// Staged.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
    /// Committed.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub committed_at: Option<OffsetDateTime>,
    /// Last verified.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub verified_at: Option<OffsetDateTime>,
    /// The files (only on `GET /checkpoints/{ckpt}`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<CheckpointFileInfo>>,
}

/// One file of a checkpoint.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct CheckpointFileInfo {
    /// Path inside the checkpoint.
    pub path: String,
    /// `weights`, `optimizer`, `manifest`, ….
    pub role: String,
    /// Size in bytes.
    pub size: i64,
    /// sha256 (hex).
    pub sha256: String,
}

fn view(row: &CheckpointRow, files: Option<&[FileRow]>) -> CheckpointInfo {
    CheckpointInfo {
        id: CheckpointId::from_uuid(row.id).to_string(),
        run_id: RunId::from_uuid(row.run_id).to_string(),
        attempt_id: AttemptId::from_uuid(row.attempt_id).to_string(),
        attempt_epoch: row.attempt_epoch,
        step: row.global_step,
        kind: row.kind.clone(),
        layout: row.layout.clone(),
        is_final: row.is_final,
        state: row.state.clone(),
        verification_level: row.verification_level.clone(),
        corrupt_reason: row.corrupt_reason.clone(),
        total_bytes: row.total_bytes,
        file_count: row.file_count,
        metrics: row.metrics_at_save.clone().unwrap_or_default(),
        created_at: row.created_at,
        committed_at: row.committed_at,
        verified_at: row.verified_at,
        files: files.map(|f| {
            f.iter()
                .map(|f| CheckpointFileInfo {
                    path: f.path.clone(),
                    role: f.role.clone(),
                    size: f.size_bytes,
                    sha256: hex(&f.sha256),
                })
                .collect()
        }),
    }
}

/// The run's org, project and creator, authorized for `action` (missing or hidden: 404).
async fn authorize_run(
    state: &AppState,
    db: &crate::db::Db,
    ctx: &RequestContext,
    run: Uuid,
    action: &str,
) -> Result<(Uuid, Uuid), JobsError> {
    let (org, project): (Uuid, Uuid) =
        sqlx::query_as("SELECT org_id, project_id FROM runs WHERE id = $1")
            .bind(run)
            .fetch_optional(db.pool())
            .await?
            .ok_or_else(|| JobsError::not_found("run"))?;
    let res: Resource =
        scope::resource("run", Some(RunId::from_uuid(run).to_string()), org, project);
    scope::authorize(state, ctx, action, &res)
        .await
        .map_err(|e| match e {
            JobsError::Api(a) if a.code == ErrorCode::NotFound => {
                JobsError::not_found("checkpoint")
            }
            other => other,
        })?;
    Ok((org, project))
}

/// The checkpoint, authorized on its run.
async fn read(
    state: &AppState,
    service: &CheckpointService,
    ctx: &RequestContext,
    ckpt: &str,
    action: &str,
) -> Result<(CheckpointRow, (Uuid, Uuid)), JobsError> {
    let id: CheckpointId = scope::parse_id(ckpt, "checkpoint")?;
    let row = service
        .find(id.as_uuid())
        .await?
        .ok_or_else(|| JobsError::not_found("checkpoint"))?;
    let scope = authorize_run(state, service.db(), ctx, row.run_id, action)
        .await
        .map_err(|e| match e {
            JobsError::Api(a) if a.code == ErrorCode::NotFound => {
                JobsError::not_found("checkpoint")
            }
            other => other,
        })?;
    Ok((row, scope))
}

/// `GET /runs/{run}/checkpoints`.
#[utoipa::path(get, path = "/runs/{run}/checkpoints", operation_id = "list_checkpoints", tag = "checkpoints",
    params(("run" = String, Path, description = "Run id, `run_…`."), PageQuery),
    responses((status = 200, description = "The run's checkpoints (not deleted ones), highest step first.", body = Page<CheckpointInfo>),
              (status = 404, description = "No such run.", body = crate::error::Problem)))]
pub async fn list(
    State(state): State<AppState>,
    Extension(service): Extension<Arc<CheckpointService>>,
    ctx: RequestContext,
    Path(run): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<CheckpointInfo>>, JobsError> {
    let run: RunId = scope::parse_id(&run, "run")?;
    authorize_run(
        &state,
        service.db(),
        &ctx,
        run.as_uuid(),
        "checkpoints.read",
    )
    .await?;
    let page = q.resolve()?;
    let after = scope::after::<CheckpointId>(&page, |c| c.as_uuid())?;
    let rows = sqlx::query_as::<_, CheckpointRow>(concat!(
        "SELECT ",
        ckpt_cols!(),
        " FROM checkpoints c WHERE run_id = $1 AND state <> 'TOMBSTONED' \
           AND ($2::uuid IS NULL OR (global_step, id) < \
                (SELECT global_step, id FROM checkpoints WHERE id = $2 AND run_id = $1)) \
         ORDER BY global_step DESC, id DESC LIMIT $3"
    ))
    .bind(run.as_uuid())
    .bind(after)
    .bind(page.fetch() as i64)
    .fetch_all(service.db().pool())
    .await?;
    let (rows, next) = scope::cut(rows, &page, |r| CheckpointId::from_uuid(r.id).to_string());
    Ok(Json(Page {
        items: rows.iter().map(|r| view(r, None)).collect(),
        next_cursor: next,
    }))
}

/// `GET /checkpoints/{ckpt}`.
#[utoipa::path(get, path = "/checkpoints/{ckpt}", operation_id = "get_checkpoint", tag = "checkpoints",
    params(("ckpt" = String, Path, description = "Checkpoint id, `ckpt_…`.")),
    responses((status = 200, description = "The checkpoint with its files.", body = CheckpointInfo),
              (status = 404, description = "No such checkpoint.", body = crate::error::Problem)))]
pub async fn get(
    State(state): State<AppState>,
    Extension(service): Extension<Arc<CheckpointService>>,
    ctx: RequestContext,
    Path(ckpt): Path<String>,
) -> Result<Json<CheckpointInfo>, JobsError> {
    let (row, _) = read(&state, &service, &ctx, &ckpt, "checkpoints.read").await?;
    let files = service.files(row.id).await?;
    Ok(Json(view(&row, Some(&files))))
}

/// `POST /checkpoints/{ckpt}:verify` (axum sees `/checkpoints/{ckpt}` with the action in the
/// segment): re-reads every stored byte (`full_rehash`).
#[utoipa::path(post, path = "/checkpoints/{ckpt}:verify", operation_id = "verify_checkpoint", tag = "checkpoints",
    params(("ckpt" = String, Path, description = "Checkpoint id, `ckpt_…`.")),
    responses((status = 200, description = "The checkpoint afterwards: COMMITTED at `full_rehash`, or CORRUPT with the reason.", body = CheckpointInfo),
              (status = 409, description = "The checkpoint is not COMMITTED (MRL-STATE-230).", body = crate::error::Problem)))]
pub async fn action(
    State(state): State<AppState>,
    Extension(service): Extension<Arc<CheckpointService>>,
    ctx: RequestContext,
    Path(segment): Path<String>,
) -> Result<Response, JobsError> {
    let Some(ckpt) = segment.strip_suffix(":verify") else {
        return Err(ApiError::new(ErrorCode::RouteNotFound, "No endpoint has this path.").into());
    };
    let (row, scope) = read(&state, &service, &ctx, ckpt, "checkpoints.write").await?;
    let row = service.committed(row.id).await?;
    let bad = service.check_hashes(&row).await?;
    let mut tx = service.db().begin().await?;
    lock_run(&mut tx, row.run_id).await?;
    let row = CheckpointService::record_verification(&mut tx, &row, bad.as_deref()).await?;
    scope::audit(
        &mut tx,
        &state,
        &ctx,
        scope,
        "checkpoints.verify",
        CheckpointId::from_uuid(row.id).to_string(),
    )
    .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(Json(view(&row, None)).into_response())
}

/// `DELETE /checkpoints/{ckpt}`: TOMBSTONED, objects deleted in the background. Refused while
/// the checkpoint is the run's final result, its resume point, or a starting point.
#[utoipa::path(delete, path = "/checkpoints/{ckpt}", operation_id = "delete_checkpoint", tag = "checkpoints",
    params(("ckpt" = String, Path, description = "Checkpoint id, `ckpt_…`.")),
    responses((status = 204, description = "Deleted (or already deleted)."),
              (status = 409, description = "Still in use (MRL-CONFLICT-230) or still being saved (MRL-STATE-230).", body = crate::error::Problem)))]
pub async fn delete(
    State(state): State<AppState>,
    Extension(service): Extension<Arc<CheckpointService>>,
    ctx: RequestContext,
    Path(ckpt): Path<String>,
) -> Result<StatusCode, JobsError> {
    let (row, scope) = read(&state, &service, &ctx, &ckpt, "checkpoints.write").await?;
    let mut tx = service.db().begin().await?;
    // Job first (a launch picking its resume point holds it), then run, then checkpoint.
    lock_job(&mut tx, row.job_id).await?;
    lock_run(&mut tx, row.run_id).await?;
    let row = lock_row(&mut tx, row.id)
        .await?
        .ok_or_else(|| JobsError::not_found("checkpoint"))?;
    match row.state.as_str() {
        "TOMBSTONED" => return Ok(StatusCode::NO_CONTENT),
        "COMMITTED" | "CORRUPT" => {}
        other => {
            return Err(JobsError::domain(
                super::codes::NOT_COMMITTED,
                StatusCode::CONFLICT,
                format!(
                    "This checkpoint is still being saved ({other}), so it can't be deleted yet."
                ),
            ))
        }
    }
    if let Some(why) = in_use(&mut tx, &row).await? {
        return Err(JobsError::domain(
            super::codes::IN_USE,
            StatusCode::CONFLICT,
            format!("This checkpoint can't be deleted: {why}."),
        ));
    }
    tombstone(&mut tx, &row).await?;
    scope::audit(
        &mut tx,
        &state,
        &ctx,
        scope,
        "checkpoints.delete",
        CheckpointId::from_uuid(row.id).to_string(),
    )
    .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

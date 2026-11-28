//! `GET /desktop/activity`: what the tray, OS notifications and the sleep blocker need.
//!
//! - `running`: jobs that hold or are getting a machine (ALLOCATING … FINALIZING).
//! - `training_active`: a job is training or saving right now (keeps the computer awake).
//! - `changes`: jobs that finished, failed, were cancelled or paused, after `since`.
//!
//! Job timestamps come from each change's transaction start, so a change can commit a little
//! after a later-stamped one. Changes are therefore returned from [`OVERLAP`] before `since`;
//! the app shows each `(job_id, state_version)` once. The first call (no `since`) returns that
//! window too, for the app to mark as already seen.

use super::{api, Desktop};
use crate::app::AppState;
use crate::modules::jobs::JobsError;
use crate::seams::RequestContext;
use axum::extract::{Query, State};
use axum::{Extension, Json};
use mrl_domain::ids::{JobId, RunId};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::sync::Arc;
use time::OffsetDateTime;
use uuid::Uuid;

/// How far before `since` changes are returned again (late commits).
pub const OVERLAP: time::Duration = time::Duration::seconds(60);
/// Most jobs listed in `running` and in `changes`.
pub const LIMIT: i64 = 50;

/// States listed in `running`.
pub const RUNNING: [&str; 7] = [
    "ALLOCATING",
    "PREPARING",
    "RUNNING",
    "PAUSING",
    "RESUMING",
    "FINALIZING",
    "CANCELLING",
];
/// States that keep the computer awake.
pub const TRAINING: [&str; 4] = ["PREPARING", "RUNNING", "PAUSING", "FINALIZING"];
/// States the app tells the person about.
pub const NOTIFIED: [&str; 4] = ["COMPLETED", "FAILED", "CANCELLED", "PAUSED"];

/// Query of `GET /desktop/activity`.
#[derive(Debug, Clone, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ActivityQuery {
    /// `cursor` from the previous answer (an RFC 3339 time). Absent on the first call.
    pub since: Option<String>,
}

/// A job that holds or is getting a machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct DesktopJob {
    /// `job_…`.
    pub job_id: String,
    /// `run_…` for training and evaluation jobs.
    pub run_id: Option<String>,
    /// The run's name, or the job kind.
    pub name: String,
    /// Job state, like `RUNNING`.
    pub state: String,
    /// When it started on a machine.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub started_at: Option<OffsetDateTime>,
}

/// A job that finished, failed, was cancelled or paused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct DesktopJobChange {
    /// `job_…`.
    pub job_id: String,
    /// `run_…` for training and evaluation jobs.
    pub run_id: Option<String>,
    /// The run's name, or the job kind.
    pub name: String,
    /// `COMPLETED`, `FAILED`, `CANCELLED` or `PAUSED`.
    pub state: String,
    /// Grows with every state change of the job; show each `(job_id, state_version)` once.
    pub state_version: i64,
    /// When it changed.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub at: OffsetDateTime,
}

/// Activity on this core.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct DesktopActivity {
    /// Pass as `since` next time.
    pub cursor: String,
    /// A job is training or saving right now.
    pub training_active: bool,
    /// Jobs on or getting a machine, oldest first (at most 50).
    pub running: Vec<DesktopJob>,
    /// Changes from shortly before `since`, oldest first (at most 50).
    pub changes: Vec<DesktopJobChange>,
}

fn name_of(run_name: Option<String>, kind: &str) -> String {
    run_name.unwrap_or_else(|| {
        let lower = kind.replace('_', " ").to_lowercase();
        format!("{} job", lower)
    })
}

fn run_text(run: Option<Uuid>) -> Option<String> {
    run.map(|r| RunId::from_uuid(r).to_string())
}

fn parse_since(since: Option<&str>) -> Result<Option<OffsetDateTime>, JobsError> {
    since
        .map(|s| {
            OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).map_err(|_| {
                JobsError::from(crate::error::ApiError::new(
                    crate::error::ErrorCode::Pagination,
                    "since must be the cursor from the previous answer.",
                ))
            })
        })
        .transpose()
}

#[utoipa::path(get, path = "/desktop/activity", tag = "desktop", operation_id = "desktop_get_activity",
    params(ActivityQuery),
    responses((status = 200, body = DesktopActivity),
        (status = 400, description = "since is not a cursor (MRL-VAL-005)"),
        (status = 409, description = "A team core (MRL-STATE-300)")))]
/// Running jobs and recent changes, for the tray, notifications and the sleep blocker.
pub async fn get_activity(
    State(state): State<AppState>,
    Extension(desktop): Extension<Arc<Desktop>>,
    ctx: RequestContext,
    Query(q): Query<ActivityQuery>,
) -> Result<Json<DesktopActivity>, JobsError> {
    api::guard(&state, &ctx).await?;
    let since = parse_since(q.since.as_deref())?;
    let pool = desktop.db.pool();
    // One clock for the cursor and the window: the database's.
    let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await?;
    let running = sqlx::query(
        "SELECT j.id, j.run_id, j.kind, j.state, j.started_at, r.name
           FROM jobs j LEFT JOIN runs r ON r.id = j.run_id
          WHERE j.state = ANY($1)
          ORDER BY j.started_at NULLS LAST, j.id LIMIT $2",
    )
    .bind(&RUNNING[..])
    .bind(LIMIT)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| -> Result<DesktopJob, sqlx::Error> {
        let kind: String = row.try_get("kind")?;
        Ok(DesktopJob {
            job_id: JobId::from_uuid(row.try_get("id")?).to_string(),
            run_id: run_text(row.try_get("run_id")?),
            name: name_of(row.try_get("name")?, &kind),
            state: row.try_get("state")?,
            started_at: row.try_get("started_at")?,
        })
    })
    .collect::<Result<Vec<_>, _>>()?;
    let training_active: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM jobs WHERE state = ANY($1))")
            .bind(&TRAINING[..])
            .fetch_one(pool)
            .await?;
    let from = since.unwrap_or(now) - OVERLAP;
    let changes = sqlx::query(
        "SELECT j.id, j.run_id, j.kind, j.state, j.state_version, j.updated_at, r.name
           FROM jobs j LEFT JOIN runs r ON r.id = j.run_id
          WHERE j.state = ANY($3) AND j.updated_at >= $1
          ORDER BY j.updated_at, j.id LIMIT $2",
    )
    .bind(from)
    .bind(LIMIT)
    .bind(&NOTIFIED[..])
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| -> Result<DesktopJobChange, sqlx::Error> {
        let kind: String = row.try_get("kind")?;
        Ok(DesktopJobChange {
            job_id: JobId::from_uuid(row.try_get("id")?).to_string(),
            run_id: run_text(row.try_get("run_id")?),
            name: name_of(row.try_get("name")?, &kind),
            state: row.try_get("state")?,
            state_version: row.try_get("state_version")?,
            at: row.try_get("updated_at")?,
        })
    })
    .collect::<Result<Vec<_>, _>>()?;
    // With a full page, continue from the last change shown, so none is skipped.
    let cursor = match changes.last() {
        Some(last) if changes.len() as i64 == LIMIT => last.at + OVERLAP,
        _ => now,
    };
    let cursor = cursor
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|e| {
            JobsError::from(crate::error::ApiError::new(
                crate::error::ErrorCode::Internal,
                format!("the activity cursor could not be written: {e}"),
            ))
        })?;
    Ok(Json(DesktopActivity {
        cursor,
        training_active,
        running,
        changes,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jobs_without_a_run_are_named_by_kind() {
        assert_eq!(name_of(None, "DATA_PREP"), "data prep job");
        assert_eq!(name_of(Some("sft-1".into()), "TRAIN"), "sft-1");
    }

    #[test]
    fn since_must_be_a_cursor() {
        assert!(parse_since(Some("yesterday")).is_err());
        assert!(parse_since(None).is_ok_and(|s| s.is_none()));
        assert!(parse_since(Some("2026-11-27T10:00:00Z")).is_ok_and(|s| s.is_some()));
    }
}

//! `GET /desktop/state` and `PUT /desktop/onboarding`, plus the guard every desktop route
//! runs first ([`guard`]) and the audit helper ([`audit`]).

use super::{codes, Desktop};
use crate::app::AppState;
use crate::db::Tx;
use crate::modules::jobs::JobsError;
use crate::seams::{AuditEvent, AuditOutcome, RequestContext, Resource};
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::OffsetDateTime;

/// The permission every desktop route needs.
pub const ACTION: &str = "admin.manage";

/// Checks `admin.manage` on the caller's org, then that this core runs in Solo mode.
/// The permission comes first, so a refused caller gets the same 403 in both modes.
pub async fn guard(state: &AppState, ctx: &RequestContext) -> Result<(), JobsError> {
    let resource = Resource {
        org: ctx.org.clone(),
        ..Resource::kind("desktop")
    };
    state.authorize(ctx, ACTION, &resource).await?;
    if state.config.server.mode != crate::Mode::Solo {
        return Err(JobsError::domain(
            codes::SOLO_ONLY,
            StatusCode::CONFLICT,
            "This is a team server. These settings belong to the desktop app on your own computer.",
        ));
    }
    Ok(())
}

/// Records a successful desktop change inside `tx`, before it commits.
pub async fn audit(
    tx: &mut Tx,
    state: &AppState,
    ctx: &RequestContext,
    action: &str,
    target: &str,
) -> Result<(), JobsError> {
    let event = AuditEvent {
        at: state.seams.clock.now(),
        actor: ctx.principal.id.clone(),
        org: ctx.org.clone(),
        project: None,
        action: action.to_owned(),
        target: target.to_owned(),
        outcome: AuditOutcome::Success,
        trace_id: ctx.trace_id.clone(),
    };
    state.seams.audit.record_in(tx, event).await?;
    Ok(())
}

/// The first-run steps, in order.
pub const STEPS: [&str; 5] = ["welcome", "sign_in", "restore", "check", "done"];

/// Where first-run onboarding stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct DesktopOnboarding {
    /// The step to show next: `welcome`, `sign_in`, `restore`, `check` or `done`.
    pub step: String,
    /// When onboarding was first finished; absent until then.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub completed_at: Option<OffsetDateTime>,
}

/// The desktop app's view of this core.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct DesktopState {
    /// First-run onboarding.
    pub onboarding: DesktopOnboarding,
    /// The folder the app's own database lives in; absent when the core uses another database.
    pub data_dir: Option<String>,
    /// True when this core runs its own PostgreSQL (backups work).
    pub bundled_database: bool,
    /// The core's version.
    pub version: String,
}

/// Move onboarding to a step.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DesktopOnboardingUpdate {
    /// `welcome`, `sign_in`, `restore`, `check` or `done`. `done` finishes onboarding.
    pub step: String,
}

async fn read_onboarding(desktop: &Desktop) -> Result<DesktopOnboarding, JobsError> {
    let row: Option<(String, Option<OffsetDateTime>)> = sqlx::query_as(
        "SELECT onboarding_step, onboarding_completed_at FROM desktop_app_state WHERE id = 1",
    )
    .fetch_optional(desktop.db.pool())
    .await?;
    let (step, completed_at) = row.unwrap_or_else(|| ("welcome".to_owned(), None));
    Ok(DesktopOnboarding { step, completed_at })
}

#[utoipa::path(get, path = "/desktop/state", tag = "desktop", operation_id = "desktop_get_state",
    responses((status = 200, body = DesktopState),
        (status = 409, description = "A team core (MRL-STATE-300)")))]
/// The desktop app's view of this core: onboarding, data folder and version.
pub async fn get_state(
    State(state): State<AppState>,
    Extension(desktop): Extension<Arc<Desktop>>,
    ctx: RequestContext,
) -> Result<Json<DesktopState>, JobsError> {
    guard(&state, &ctx).await?;
    Ok(Json(DesktopState {
        onboarding: read_onboarding(&desktop).await?,
        data_dir: desktop
            .data_dir
            .as_ref()
            .map(|d| d.to_string_lossy().into_owned()),
        bundled_database: desktop.db.backups().is_some(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
    }))
}

#[utoipa::path(put, path = "/desktop/onboarding", tag = "desktop", operation_id = "desktop_put_onboarding",
    request_body = DesktopOnboardingUpdate,
    responses((status = 200, body = DesktopOnboarding),
        (status = 422, description = "Unknown step (MRL-VAL-003)"),
        (status = 409, description = "A team core (MRL-STATE-300)")))]
/// Saves the onboarding step. `done` records when onboarding first finished; later steps never
/// clear it, so a finished app never shows first-run again.
pub async fn put_onboarding(
    State(state): State<AppState>,
    Extension(desktop): Extension<Arc<Desktop>>,
    ctx: RequestContext,
    Json(body): Json<DesktopOnboardingUpdate>,
) -> Result<Json<DesktopOnboarding>, JobsError> {
    guard(&state, &ctx).await?;
    if !STEPS.contains(&body.step.as_str()) {
        return Err(
            crate::error::ApiError::validation(vec![crate::error::FieldError::new(
                "/step",
                crate::error::ErrorCode::FieldValue,
                "This is not a first-run step.",
                format!("Use one of: {}.", STEPS.join(", ")),
            )])
            .into(),
        );
    }
    let mut tx = desktop.db.begin().await?;
    let row: (String, Option<OffsetDateTime>) = sqlx::query_as(
        "INSERT INTO desktop_app_state (id, onboarding_step, onboarding_completed_at, updated_at)
         VALUES (1, $1, CASE WHEN $1 = 'done' THEN now() END, now())
         ON CONFLICT (id) DO UPDATE SET
           onboarding_step = EXCLUDED.onboarding_step,
           onboarding_completed_at = COALESCE(desktop_app_state.onboarding_completed_at,
                                              EXCLUDED.onboarding_completed_at),
           updated_at = now()
         RETURNING onboarding_step, onboarding_completed_at",
    )
    .bind(&body.step)
    .fetch_one(&mut *tx)
    .await?;
    audit(&mut tx, &state, &ctx, "desktop.onboarding", &body.step).await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(Json(DesktopOnboarding {
        step: row.0,
        completed_at: row.1,
    }))
}

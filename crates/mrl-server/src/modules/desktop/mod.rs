//! The desktop app's endpoints (P30). They work in Solo mode only; a team core answers
//! MRL-STATE-300.
//!
//! - `GET /desktop/state`, `PUT /desktop/onboarding`: the first-run steps and where data lives.
//! - `POST /desktop/doctor`: the Environment Doctor for this computer ("can / can't / install").
//! - `GET /desktop/activity`: running jobs and recent finished, failed or paused ones (tray,
//!   notifications and the sleep blocker).
//! - `GET|POST /desktop/backups`, `POST /desktop/backups/{name}/restore`: the bundled database's
//!   backups (P11).
//! - `POST /desktop/update/prepare`: emergency checkpoints of running jobs and a backup before
//!   the app installs an update.
//!
//! Every route needs `admin.manage` on the caller's org (the Solo owner has it), checked before
//! the mode, so the role table sees the same 403 everywhere. Changes are audited in the
//! transaction that makes them.
//!
//! Register with `Box::new(crate::modules::desktop::DesktopModule::new(db.clone(), config)),`.

pub mod activity;
pub mod api;
pub mod backups;
pub mod doctor;
pub mod update;

use crate::app::{AppState, ServerModule};
use crate::db::Db;
use crate::seams::{SeamError, StartupStep};
use axum::routing::{get, post, put};
use axum::{Extension, Router};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use utoipa::OpenApi;

/// Codes this part adds (guidance/fragments/errors/p30.yaml).
pub mod codes {
    use mrl_domain::errors::{ErrorCode, ErrorFamily};

    const fn code(family: ErrorFamily, n: u16) -> ErrorCode {
        match ErrorCode::new(family, n) {
            Some(c) => c,
            None => panic!("error code numbers must be 1 to 999"),
        }
    }

    /// A desktop route was called on a team core.
    pub const SOLO_ONLY: ErrorCode = code(ErrorFamily::State, 300);
    /// Backups need the database the app runs itself.
    pub const NO_BUNDLED_DATABASE: ErrorCode = code(ErrorFamily::Env, 300);
    /// No backup with this name.
    pub const NO_SUCH_BACKUP: ErrorCode = code(ErrorFamily::Res, 300);
    /// Making or restoring a backup failed.
    pub const BACKUP_FAILED: ErrorCode = code(ErrorFamily::Internal, 300);
    /// The Environment Doctor could not read this computer's agent settings.
    pub const DOCTOR_SETTINGS: ErrorCode = code(ErrorFamily::Env, 301);
    /// Another backup, restore or update check is running.
    pub const BUSY: ErrorCode = code(ErrorFamily::Conflict, 300);
}

/// `[desktop]`: settings for the desktop app's endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct DesktopConfig {
    /// The local agent's settings file, read by the Environment Doctor for its state folder.
    /// Unset: the agent's own lookup (`$MRL_AGENT_CONFIG`, then the OS settings folder).
    pub agent_config: Option<PathBuf>,
    /// How long each emergency checkpoint before an update may take, in seconds (5–600).
    pub update_checkpoint_seconds: u64,
}

impl Default for DesktopConfig {
    fn default() -> Self {
        Self {
            agent_config: None,
            update_checkpoint_seconds: 120,
        }
    }
}

impl DesktopConfig {
    /// Problems with these settings, in plain words.
    pub fn problems(&self) -> Vec<String> {
        let mut p = Vec::new();
        if !(5..=600).contains(&self.update_checkpoint_seconds) {
            p.push("desktop.update_checkpoint_seconds must be 5 to 600".to_owned());
        }
        if self.agent_config.as_ref().is_some_and(|f| !f.is_absolute()) {
            p.push("desktop.agent_config must be a full path".to_owned());
        }
        p
    }
}

/// What the handlers share.
pub struct Desktop {
    /// The app database.
    pub db: Db,
    /// Settings.
    pub config: DesktopConfig,
    /// Where the bundled database keeps its data (shown in Settings).
    pub data_dir: Option<PathBuf>,
    /// Runs the Environment Doctor.
    pub doctor: Arc<dyn doctor::DoctorSource>,
    /// Emergency checkpoints before an update.
    pub checkpoints: Arc<dyn update::EmergencyCheckpoints>,
    /// One backup, restore or update preparation at a time.
    pub busy: tokio::sync::Mutex<()>,
}

/// The module.
pub struct DesktopModule {
    desktop: Arc<Desktop>,
}

impl DesktopModule {
    /// The module for this build: the real doctor and the checkpoint service.
    pub fn new(db: Db, config: &crate::Config) -> Self {
        let checkpoints = update::ServiceCheckpoints::new(db.clone(), config);
        Self::with_parts(
            db,
            config,
            Arc::new(doctor::LocalDoctor::new(
                config.desktop.agent_config.clone(),
            )),
            Arc::new(checkpoints),
        )
    }

    /// The module with explicit parts (tests).
    pub fn with_parts(
        db: Db,
        config: &crate::Config,
        doctor: Arc<dyn doctor::DoctorSource>,
        checkpoints: Arc<dyn update::EmergencyCheckpoints>,
    ) -> Self {
        let data_dir = match config.server.mode {
            crate::Mode::Solo if config.database.url.is_none() => {
                crate::pgbundle::data_dir(&config.pgbundle).ok()
            }
            _ => None,
        };
        Self {
            desktop: Arc::new(Desktop {
                db,
                config: config.desktop.clone(),
                data_dir,
                doctor,
                checkpoints,
                busy: tokio::sync::Mutex::new(()),
            }),
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        api::get_state,
        api::put_onboarding,
        doctor::run_doctor,
        activity::get_activity,
        backups::list_backups,
        backups::create_backup,
        backups::restore_backup,
        update::prepare_update,
    ),
    components(schemas(
        api::DesktopState,
        api::DesktopOnboarding,
        api::DesktopOnboardingUpdate,
        doctor::DesktopDoctorReport,
        doctor::DesktopDoctorCheck,
        doctor::DesktopDoctorTask,
        doctor::DesktopDoctorInstall,
        activity::DesktopActivity,
        activity::DesktopJob,
        activity::DesktopJobChange,
        backups::DesktopBackup,
        backups::DesktopBackupList,
        backups::DesktopRestored,
        update::DesktopUpdateReadiness,
        update::DesktopUpdateCheckpoint,
    )),
    tags((name = "desktop", description = "The desktop app: first run, this computer, backups and updates (Solo only)."))
)]
struct Doc;

impl ServerModule for DesktopModule {
    fn name(&self) -> &'static str {
        "desktop"
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route("/desktop/state", get(api::get_state))
            .route("/desktop/onboarding", put(api::put_onboarding))
            .route("/desktop/doctor", post(doctor::run_doctor))
            .route("/desktop/activity", get(activity::get_activity))
            .route(
                "/desktop/backups",
                get(backups::list_backups).post(backups::create_backup),
            )
            .route(
                "/desktop/backups/{name}/restore",
                post(backups::restore_backup),
            )
            .route("/desktop/update/prepare", post(update::prepare_update))
            .route_layer(axum::middleware::from_fn(
                crate::modules::jobs::error::render,
            ))
            .layer(Extension(self.desktop.clone()))
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        Doc::openapi()
    }

    fn startup_steps(&self) -> Vec<Arc<dyn StartupStep>> {
        vec![Arc::new(CheckSettings(self.desktop.config.clone()))]
    }
}

/// Refuses `[desktop]` settings the module can't use (MRL-INTERNAL-006 with the reasons).
struct CheckSettings(DesktopConfig);

#[async_trait::async_trait]
impl StartupStep for CheckSettings {
    fn name(&self) -> &str {
        "desktop-settings"
    }

    async fn run(&self) -> Result<(), SeamError> {
        let problems = self.0.problems();
        if problems.is_empty() {
            Ok(())
        } else {
            Err(SeamError::new("desktop", problems.join("; ")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_are_checked() {
        assert!(DesktopConfig::default().problems().is_empty());
        let bad = DesktopConfig {
            agent_config: Some(PathBuf::from("agent.toml")),
            update_checkpoint_seconds: 1,
        };
        assert_eq!(bad.problems().len(), 2);
    }

    #[test]
    fn codes_are_in_the_registry() {
        let known: Vec<String> = mrl_domain::errors::all()
            .map(|(c, _)| c.to_string())
            .collect();
        for c in [
            codes::SOLO_ONLY,
            codes::NO_BUNDLED_DATABASE,
            codes::NO_SUCH_BACKUP,
            codes::BACKUP_FAILED,
            codes::DOCTOR_SETTINGS,
            codes::BUSY,
        ] {
            assert!(known.contains(&c.to_string()), "{c} is not registered");
        }
    }
}

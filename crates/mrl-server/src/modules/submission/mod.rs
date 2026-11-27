//! Run submission path (P28): presets and config resolution, `runs:validate` (L0 + L1),
//! estimates as ranges with confidence (see [`crate::modules::estimates`]), estimate vs
//! actual, and the Guardian preflight (see [`crate::modules::preflight`]).
//!
//! Register with `Box::new(crate::modules::submission::SubmissionModule::new(db.clone(), config)),`.
//! Chain members: [`resolve::Presets`] (resolver), [`policy::MethodPolicy`] and
//! [`policy::Guardian`] (validators), [`probe::ProbeVerifier`] (verifier),
//! [`probe::ProbeLaunch`] (launch contributor); [`crate::modules::estimates::Calibrate`]
//! (event consumer).
//!
//! Routes: `POST /projects/{prj}/runs:validate`, `POST /runs:preflight`,
//! `GET /runs/{run}/resources`.

pub mod api;
pub mod policy;
pub mod probe;
pub mod resolve;

use crate::app::{AppState, ServerModule, Task};
use crate::config::Role;
use crate::db::Db;
use crate::modules::jobs::{Chains, Projects};
use crate::modules::nodes::lanes::LaneRouter;
use crate::modules::preflight::{Headroom, ProbeModeGuard};
use crate::modules::telemetry::MetricObservers;
use crate::seams::{SeamError, StartupStep};
use axum::routing::{get, post};
use axum::{Extension, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use utoipa::OpenApi;

pub(crate) async fn audit(
    db: &Db,
    tx: &mut crate::db::Tx,
    (org, project): (uuid::Uuid, uuid::Uuid),
    actor: &str,
    action: &str,
    target: String,
) -> Result<(), SeamError> {
    use crate::seams::{AuditEvent, AuditOutcome, AuditSink};
    crate::modules::audit::PgAuditSink::new(db.clone())
        .record_in(
            tx,
            AuditEvent {
                at: time::OffsetDateTime::now_utc(),
                actor: actor.to_owned(),
                org: crate::modules::jobs::scope::org_text(org),
                project: Some(mrl_domain::ids::ProjectId::from_uuid(project).to_string()),
                action: action.to_owned(),
                target,
                outcome: AuditOutcome::Success,
                trace_id: uuid::Uuid::new_v4().simple().to_string(),
            },
        )
        .await
}

/// Codes this part adds (guidance/fragments/errors/p28.yaml), plus the ones it reuses.
pub mod codes {
    use mrl_domain::errors::{ErrorCode, ErrorFamily};

    const fn code(family: ErrorFamily, n: u16) -> ErrorCode {
        match ErrorCode::new(family, n) {
            Some(c) => c,
            None => panic!("error code numbers must be 1 to 999"),
        }
    }

    /// The run is estimated not to fit any machine that can run it.
    pub const DOES_NOT_FIT: ErrorCode = code(ErrorFamily::Cap, 280);
    /// A resume would change the engine's major version or the method.
    pub const RESUME_INCOMPATIBLE: ErrorCode = code(ErrorFamily::Cap, 281);
    /// The settings were checked without a machine (none connected, none answered).
    pub const CHECKED_WITHOUT_MACHINE: ErrorCode = code(ErrorFamily::Cap, 282);
    /// The engine checks its settings only when the run starts (no `validate.l1`).
    pub const NO_L1: ErrorCode = code(ErrorFamily::Cap, 283);
    /// The fit is tight: safety settings were applied.
    pub const SAFETY_SETTINGS: ErrorCode = code(ErrorFamily::Cap, 284);
    /// No engine offers the preset (or several do).
    pub const UNKNOWN_PRESET: ErrorCode = code(ErrorFamily::Val, 280);
}

/// `[submission]` settings.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SubmissionConfig {
    /// How long `runs:validate` waits for a machine to check the settings, seconds (5–600).
    pub probe_timeout_seconds: u64,
    /// Probe mode (`unknown` verdict): steps measured before the app decides (1–1000).
    pub probe_steps: u32,
    /// Memory each machine keeps free.
    pub headroom: Headroom,
}

impl Default for SubmissionConfig {
    fn default() -> Self {
        Self {
            probe_timeout_seconds: 60,
            probe_steps: 5,
            headroom: Headroom::default(),
        }
    }
}

impl SubmissionConfig {
    /// Settings that can't be used, in plain words.
    pub fn problems(&self) -> Vec<String> {
        let mut out = self.headroom.problems();
        if !(5..=600).contains(&self.probe_timeout_seconds) {
            out.push(format!(
                "submission.probe_timeout_seconds: use 5–600 seconds (got {})",
                self.probe_timeout_seconds
            ));
        }
        if !(1..=1000).contains(&self.probe_steps) {
            out.push(format!(
                "submission.probe_steps: use 1–1000 (got {})",
                self.probe_steps
            ));
        }
        out
    }
}

// ponytail: one copy per process, set by the last module built. The submit chains are built
// from the database alone (P17), so the resolver and the Guardian read `[submission]` here;
// production runs one server per process.
static SETTINGS: std::sync::RwLock<Option<SubmissionConfig>> = std::sync::RwLock::new(None);

/// This process's `[submission]` settings (the defaults until the module is built).
pub fn settings() -> SubmissionConfig {
    SETTINGS.read().ok().and_then(|s| *s).unwrap_or_default()
}

/// What the handlers share.
#[derive(Clone)]
pub struct Ctx {
    /// Database.
    pub db: Db,
    /// The validator chain (the same one submit uses).
    pub chains: Chains,
    /// P24's engine pick, for resolving a config that names no engine.
    pub capability: Arc<crate::modules::engines::CapabilityValidator>,
    /// Which org owns a project.
    pub projects: Projects,
    /// Settings.
    pub config: SubmissionConfig,
}

/// The submission module.
pub struct SubmissionModule {
    ctx: Ctx,
}

impl SubmissionModule {
    /// The module for this build: registered chains and project directory, the
    /// process-wide lane router and metric observers.
    pub fn new(db: Db, config: &crate::Config) -> Self {
        let chains = Chains::registered(&db);
        let projects = Projects::registered(&db);
        Self::with(
            db,
            config.submission,
            chains,
            projects,
            &LaneRouter::global(),
            &MetricObservers::global(),
        )
    }

    /// A module with these parts (tests and embedders).
    pub fn with(
        db: Db,
        config: SubmissionConfig,
        chains: Chains,
        projects: Projects,
        lanes: &LaneRouter,
        observers: &MetricObservers,
    ) -> Self {
        if let Ok(mut s) = SETTINGS.write() {
            *s = Some(config);
        }
        lanes.register(Arc::new(probe::PreparedSink::new(db.clone())));
        observers.register(Arc::new(ProbeModeGuard::new(
            db.clone(),
            config.headroom,
            config.probe_steps,
        )));
        Self {
            ctx: Ctx {
                capability: Arc::new(crate::modules::engines::CapabilityValidator::new(
                    db.clone(),
                )),
                db,
                chains,
                projects,
                config,
            },
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(api::validate, api::preflight_route, crate::modules::estimates::resources),
    components(schemas(
        api::ValidateResponse, api::ValidateIssue, api::IssueSeverity, api::PreflightRequest,
        crate::modules::estimates::RunEstimate, crate::modules::estimates::EstimateRange,
        crate::modules::estimates::Verdict, crate::modules::estimates::Confidence,
        crate::modules::estimates::CalibrationView, crate::modules::estimates::RunResources,
        crate::modules::estimates::PhaseComparison, crate::modules::estimates::ResourceActual,
        crate::modules::preflight::RunPreflight, crate::modules::preflight::PreflightMachine,
        crate::modules::preflight::ConfigChange, crate::modules::preflight::PreflightAlternative,
        crate::modules::preflight::MemoryKind,
    )),
    tags((name = "runs", description = "Experiments, runs, config snapshots and timelines."))
)]
struct Doc;

struct CheckSettings(SubmissionConfig);

#[async_trait::async_trait]
impl StartupStep for CheckSettings {
    fn name(&self) -> &str {
        "submission.settings"
    }

    async fn run(&self) -> Result<(), SeamError> {
        let problems = self.0.problems();
        if problems.is_empty() {
            Ok(())
        } else {
            Err(SeamError::new("submission", problems.join("; ")))
        }
    }
}

impl ServerModule for SubmissionModule {
    fn name(&self) -> &'static str {
        "submission"
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route("/projects/{prj}/runs:validate", post(api::validate))
            .route("/runs:preflight", post(api::preflight_route))
            .route(
                "/runs/{run}/resources",
                get(crate::modules::estimates::resources),
            )
            .route_layer(axum::middleware::from_fn(
                crate::modules::jobs::error::render,
            ))
            .layer(Extension(self.ctx.db.clone()))
            .layer(Extension(self.ctx.config.headroom))
            .layer(Extension(self.ctx.clone()))
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        Doc::openapi()
    }

    fn startup_steps(&self) -> Vec<Arc<dyn StartupStep>> {
        vec![Arc::new(CheckSettings(self.ctx.config))]
    }

    fn background_tasks(&self) -> Vec<Task> {
        let db = self.ctx.db.clone();
        let calibration = Arc::new(crate::modules::estimates::Calibrate::new(db.clone()));
        let guard = Arc::new(ProbeModeGuard::new(
            db.clone(),
            self.ctx.config.headroom,
            self.ctx.config.probe_steps,
        ));
        vec![Task::new(
            "submission.probe-sweep",
            Role::Background,
            move |cancel| {
                let calibration = calibration.clone();
                let guard = guard.clone();
                async move {
                    let mut tick = tokio::time::interval(Duration::from_secs(30));
                    loop {
                        tokio::select! {
                            _ = cancel.cancelled() => return Ok(()),
                            _ = tick.tick() => {
                                if let Err(e) = guard.reconcile().await {
                                    tracing::warn!(error = %e, "could not recover pending Guardian checks");
                                }
                                if let Err(e) = calibration.reconcile().await {
                                    tracing::warn!(error = %e, "could not reconcile late resource measurements");
                                }
                                if let Err(e) = probe::sweep(&db).await {
                                    tracing::warn!(error = %e, "could not cancel stale settings checks; retrying");
                                }
                            }
                        }
                    }
                }
            },
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openapi_lists_every_route_with_its_own_names() {
        let doc = Doc::openapi();
        for p in [
            "/projects/{prj}/runs:validate",
            "/runs:preflight",
            "/runs/{run}/resources",
        ] {
            assert!(doc.paths.paths.contains_key(p), "{p} missing");
        }
        let schemas = doc.components.map(|c| c.schemas).unwrap_or_default();
        assert!(schemas.contains_key("ValidateResponse") && schemas.contains_key("RunPreflight"));
        let change = serde_json::to_value(&schemas["ConfigChange"]).unwrap();
        assert_eq!(
            change["properties"]["before"]["type"],
            serde_json::json!(["boolean", "number", "string", "null"])
        );
        assert_eq!(
            change["properties"]["after"]["type"],
            serde_json::json!(["boolean", "number", "string", "null"])
        );
    }

    #[test]
    fn settings_ranges() {
        assert!(SubmissionConfig::default().problems().is_empty());
        let bad = SubmissionConfig {
            probe_timeout_seconds: 1,
            probe_steps: 0,
            ..SubmissionConfig::default()
        };
        assert_eq!(bad.problems().len(), 2);
    }
}

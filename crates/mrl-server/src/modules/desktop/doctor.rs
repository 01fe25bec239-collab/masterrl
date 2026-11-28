//! `POST /desktop/doctor`: the Environment Doctor (P12) for this computer, run by the core so
//! the first-run "Check this computer" step works before any agent has joined.
//!
//! The doctor only reads: hardware, the agent's state folder and its installed environments.

use super::{api, codes, Desktop};
use crate::app::AppState;
use crate::modules::jobs::JobsError;
use crate::seams::RequestContext;
use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use mrl_agent::doctor::{self, DoctorReport, Outcome, PlanComponent, TaskLine};
use mrl_agent::hw::HwBackend;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Where doctor reports come from (the real one, or a fixed one in tests).
#[async_trait]
pub trait DoctorSource: Send + Sync {
    /// Runs the doctor. `Err` holds a plain reason the settings could not be read.
    async fn run(&self) -> Result<DoctorReport, String>;
}

/// The doctor on this computer, with the local agent's settings.
pub struct LocalDoctor {
    agent_config: Option<PathBuf>,
    backends: fn() -> Vec<Arc<dyn HwBackend>>,
}

impl LocalDoctor {
    /// The doctor with the real hardware backends.
    pub fn new(agent_config: Option<PathBuf>) -> Self {
        Self::with_backends(agent_config, mrl_agent::hw::registry::backends)
    }

    /// The doctor with other hardware backends (tests).
    pub fn with_backends(
        agent_config: Option<PathBuf>,
        backends: fn() -> Vec<Arc<dyn HwBackend>>,
    ) -> Self {
        Self {
            agent_config,
            backends,
        }
    }
}

/// The nearest folder of `path` that exists, so disk checks work before the agent ever ran.
fn existing(path: &Path) -> PathBuf {
    path.ancestors()
        .find(|p| p.is_dir())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| path.to_path_buf())
}

#[async_trait]
impl DoctorSource for LocalDoctor {
    async fn run(&self) -> Result<DoctorReport, String> {
        let (cfg, _) = mrl_agent::config::AgentConfig::load(self.agent_config.as_deref())
            .map_err(|e| e.to_string())?;
        let root = existing(&cfg.state_dir);
        let envs = mrl_agent::envs::installed_in(&cfg.state_dir.join("envs"));
        let facts = doctor::Facts::gather_from((self.backends)(), &root, envs).await;
        Ok(doctor::run(&facts))
    }
}

/// One check, like "NVIDIA driver".
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct DesktopDoctorCheck {
    /// Check name, like `nvidia_driver`.
    pub check: String,
    /// `ok`, `warn`, `fail` or `not_applicable`.
    pub outcome: String,
    /// One plain line.
    pub summary: String,
    /// Guidance code for a warning or failure.
    pub code: Option<String>,
}

/// A task this computer can or can't do.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct DesktopDoctorTask {
    /// Task id, like `sft-lora`.
    pub task: String,
    /// Plain name.
    pub name: String,
    /// A note from the catalog.
    pub note: Option<String>,
    /// What stops it here ("can't" only).
    pub blocked_by: Option<String>,
    /// Download still needed first, low estimate in GB ("can" only).
    pub download_first_gb_low: Option<f64>,
    /// Download still needed first, high estimate in GB ("can" only).
    pub download_first_gb_high: Option<f64>,
}

/// Something to install to unlock more.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct DesktopDoctorInstall {
    /// Component id, like `pytorch`.
    pub id: String,
    /// Plain name.
    pub name: String,
    /// What it does.
    pub summary: String,
    /// Download size, low estimate in GB.
    pub download_gb_low: f64,
    /// Download size, high estimate in GB.
    pub download_gb_high: f64,
}

/// What this computer has, can train, and could unlock.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct DesktopDoctorReport {
    /// Platform id, like `macos-apple-silicon`.
    pub platform: String,
    /// What models fit here, in plain words.
    pub fits: Option<String>,
    /// Check results.
    pub checks: Vec<DesktopDoctorCheck>,
    /// "This computer can".
    pub can: Vec<DesktopDoctorTask>,
    /// "It can't".
    pub cannot: Vec<DesktopDoctorTask>,
    /// "To unlock more, install".
    pub to_install: Vec<DesktopDoctorInstall>,
}

fn outcome(o: Outcome) -> &'static str {
    match o {
        Outcome::Ok => "ok",
        Outcome::Warn => "warn",
        Outcome::Fail => "fail",
        Outcome::NotApplicable => "not_applicable",
    }
}

fn task(t: &TaskLine) -> DesktopDoctorTask {
    DesktopDoctorTask {
        task: t.task.clone(),
        name: t.name.clone(),
        note: t.note.clone(),
        blocked_by: t.blocked_by.clone(),
        download_first_gb_low: t.download_first_gb.map(|g| g.0),
        download_first_gb_high: t.download_first_gb.map(|g| g.1),
    }
}

fn install(c: &PlanComponent) -> DesktopDoctorInstall {
    DesktopDoctorInstall {
        id: c.id.clone(),
        name: c.name.clone(),
        summary: c.summary.clone(),
        download_gb_low: c.download_gb.0,
        download_gb_high: c.download_gb.1,
    }
}

/// The report as the API shows it.
pub fn summarize(r: &DoctorReport) -> DesktopDoctorReport {
    DesktopDoctorReport {
        platform: r.capabilities.platform.to_owned(),
        fits: r.capabilities.fits.clone(),
        checks: r
            .checks
            .iter()
            .map(|c| DesktopDoctorCheck {
                check: c.check.to_owned(),
                outcome: outcome(c.outcome).to_owned(),
                summary: c.summary.clone(),
                code: c.code.map(str::to_owned),
            })
            .collect(),
        can: r.capabilities.can.iter().map(task).collect(),
        cannot: r.capabilities.cannot.iter().map(task).collect(),
        to_install: r.capabilities.to_install.iter().map(install).collect(),
    }
}

#[utoipa::path(post, path = "/desktop/doctor", tag = "desktop", operation_id = "desktop_run_doctor",
    responses((status = 200, body = DesktopDoctorReport),
        (status = 503, description = "The agent settings could not be read (MRL-ENV-301)"),
        (status = 409, description = "A team core (MRL-STATE-300)")))]
/// Checks this computer: what it has, what it can train, and what to install for more.
pub async fn run_doctor(
    State(state): State<AppState>,
    Extension(desktop): Extension<Arc<Desktop>>,
    ctx: RequestContext,
) -> Result<Json<DesktopDoctorReport>, JobsError> {
    api::guard(&state, &ctx).await?;
    let report = desktop.doctor.run().await.map_err(|why| {
        JobsError::domain(
            codes::DOCTOR_SETTINGS,
            StatusCode::SERVICE_UNAVAILABLE,
            format!("The computer check could not start: {why}"),
        )
    })?;
    Ok(Json(summarize(&report)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake() -> Vec<Arc<dyn HwBackend>> {
        vec![Arc::new(mrl_agent::hw::fake::FakeHw::with_gpus(
            1,
            "NVIDIA RTX 4090",
            24 << 30,
        ))]
    }

    #[tokio::test]
    async fn the_doctor_reads_this_computer_and_sums_it_up() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let file = dir.path().join("agent.toml");
        let state = dir.path().join("state");
        std::fs::write(
            &file,
            format!("state_dir = {:?}\n", state.to_string_lossy()),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let report = LocalDoctor::with_backends(Some(file), fake)
            .run()
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let summary = summarize(&report);
        assert_eq!(summary.checks.len(), report.checks.len());
        assert!(summary
            .checks
            .iter()
            .all(|c| ["ok", "warn", "fail", "not_applicable"].contains(&c.outcome.as_str())));
        assert!(!summary.can.is_empty() || !summary.cannot.is_empty());
        assert!(!state.exists(), "the doctor only reads");
    }

    #[tokio::test]
    async fn unreadable_agent_settings_are_a_plain_reason() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let file = dir.path().join("agent.toml");
        std::fs::write(&file, "nope = [").unwrap_or_else(|e| panic!("{e}"));
        let err = LocalDoctor::with_backends(Some(file), fake)
            .run()
            .await
            .err()
            .unwrap_or_default();
        assert!(!err.is_empty());
    }
}

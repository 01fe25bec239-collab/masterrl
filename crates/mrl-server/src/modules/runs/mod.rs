//! Experiments, runs and their immutable config snapshots; serves the `/jobs` endpoints too.
//!
//! Register with `Box::new(crate::modules::runs::RunsModule::new(db.clone())),`.

pub mod api;
pub mod experiments;

use crate::app::{AppState, ServerModule};
use crate::db::Db;
use crate::modules::jobs::{self, Chains, Projects, RehomeLocalRows};
use crate::seams::StartupStep;
use axum::routing::get;
use axum::{Extension, Router};
use std::sync::Arc;
use utoipa::OpenApi;

/// The runs and jobs module.
pub struct RunsModule {
    db: Db,
    chains: Chains,
    projects: Projects,
}

impl RunsModule {
    /// The module with every registered chain member and project directory
    /// ([`Chains::registered`], [`Projects::registered`]).
    pub fn new(db: Db) -> Self {
        let chains = Chains::registered(&db);
        let projects = Projects::registered(&db);
        Self {
            db,
            chains,
            projects,
        }
    }

    /// The module with exactly these chains and no project directory (tests use this).
    pub fn with_chains(db: Db, chains: Chains) -> Self {
        Self {
            db,
            chains,
            projects: Projects::default(),
        }
    }

    /// The same module with this project directory (tests use this).
    pub fn with_projects(mut self, projects: Projects) -> Self {
        self.projects = projects;
        self
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        experiments::create, experiments::list, experiments::get, experiments::patch,
        api::submit, api::list, api::get, api::action, api::config, api::attempts, api::timeline,
        jobs::api::list, jobs::api::get, jobs::api::action, jobs::api::patch,
    ),
    components(schemas(
        experiments::Experiment, experiments::NewExperiment, experiments::ExperimentPatch,
        api::Run, api::RunSubmission, api::RunKind, api::RunArtifactInput, api::RunConfig, api::TimelineEntry,
        jobs::Job, jobs::Attempt, jobs::Rejection, jobs::api::JobPatch,
    )),
    tags(
        (name = "runs", description = "Experiments, runs, config snapshots and timelines."),
        (name = "jobs", description = "Jobs: state, cancel and priority."),
    )
)]
struct Doc;

impl ServerModule for RunsModule {
    fn name(&self) -> &'static str {
        "runs"
    }

    fn handler_idempotency_paths(&self) -> &'static [&'static str] {
        &["/projects/{prj}/runs"]
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route(
                "/projects/{prj}/experiments",
                get(experiments::list).post(experiments::create),
            )
            .route(
                "/experiments/{exp}",
                get(experiments::get).patch(experiments::patch),
            )
            .route("/projects/{prj}/runs", get(api::list).post(api::submit))
            .route("/runs/{run}", get(api::get).post(api::action))
            .route("/runs/{run}/config", get(api::config))
            .route("/runs/{run}/attempts", get(api::attempts))
            .route("/runs/{run}/timeline", get(api::timeline))
            .route("/jobs", get(jobs::api::list))
            .route(
                "/jobs/{job}",
                get(jobs::api::get)
                    .post(jobs::api::action)
                    .patch(jobs::api::patch),
            )
            .route_layer(axum::middleware::from_fn(jobs::error::render))
            .layer(Extension(self.db.clone()))
            .layer(Extension(self.chains.clone()))
            .layer(Extension(self.projects.clone()))
    }

    fn startup_steps(&self) -> Vec<Arc<dyn StartupStep>> {
        vec![Arc::new(RehomeLocalRows::new(
            self.db.clone(),
            self.projects.clone(),
        ))]
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        Doc::openapi()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openapi_lists_every_endpoint() {
        let doc = Doc::openapi();
        for p in [
            "/projects/{prj}/experiments",
            "/experiments/{exp}",
            "/projects/{prj}/runs",
            "/runs/{run}",
            "/runs/{run}:cancel",
            "/runs/{run}/config",
            "/runs/{run}/attempts",
            "/runs/{run}/timeline",
            "/jobs",
            "/jobs/{job}",
            "/jobs/{job}:cancel",
        ] {
            assert!(doc.paths.paths.contains_key(p), "{p} missing");
        }
    }
}

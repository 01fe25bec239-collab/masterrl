//! The model registry (P26): models and immutable model versions, their files, the safety
//! gates and per-engine compatibility, and staging a run's model on its machine.
//!
//! - A version is created IMPORTING by an import, a promotion or a merge, and becomes READY or
//!   INVALID once its built-in job's outputs are recorded ([`outputs`]). Its files are
//!   content-addressed blobs, so it never changes afterwards.
//! - Gates ([`gates`]): pickle weights never become a version; a version that ships its own
//!   code is unusable until a maintainer approves that exact revision; a project must
//!   acknowledge a version's license before its first run on it.
//! - [`gates::ModelRecords`] answers P24's capability check from these tables;
//!   [`launch::ModelInputs`] stages the run's model as the folder input `model`.
//!
//! Register with `Box::new(crate::modules::models::ModelsModule::new(db.clone(), config)),`.

pub mod api;
pub mod gates;
pub mod jobs;
pub mod launch;
pub mod outputs;
pub mod store;

use crate::app::{AppState, ServerModule};
use crate::db::Db;
use crate::modules::artifacts::{ArtifactStore, ArtifactsModule};
use crate::modules::jobs::Projects;
use axum::routing::{get, post};
use axum::{Extension, Router};
use std::sync::Arc;
use utoipa::OpenApi;

/// Codes this part adds (guidance/fragments/errors/p26.yaml).
pub mod codes {
    use mrl_domain::errors::{ErrorCode, ErrorFamily};

    const fn code(family: ErrorFamily, n: u16) -> ErrorCode {
        match ErrorCode::new(family, n) {
            Some(c) => c,
            None => panic!("error code numbers must be 1 to 999"),
        }
    }

    /// Turning pickle weights into safetensors is not available in this release.
    pub const CONVERSION_UNAVAILABLE: ErrorCode = code(ErrorFamily::Model, 260);
    /// Exporting to GGUF or a quantized format is not available in this release.
    pub const EXPORT_FORMAT_UNAVAILABLE: ErrorCode = code(ErrorFamily::Model, 261);
    /// The project has not acknowledged this version's license.
    pub const LICENSE_NOT_ACKNOWLEDGED: ErrorCode = code(ErrorFamily::Model, 262);
    /// The version is not READY (still importing, or invalid).
    pub const NOT_READY: ErrorCode = code(ErrorFamily::Model, 263);
    /// An adapter can't be used or merged without a full-precision base model version.
    pub const ADAPTER_BASE: ErrorCode = code(ErrorFamily::Model, 264);
    /// The checkpoint can't become a model version (kind or state).
    pub const NOT_PROMOTABLE: ErrorCode = code(ErrorFamily::Model, 265);
    /// The approval or acknowledgement names another revision or license text.
    pub const STALE_CONFIRMATION: ErrorCode = code(ErrorFamily::Model, 266);
    /// Pickle weights are refused (reused from the base catalog).
    pub const PICKLE_REFUSED: ErrorCode = code(ErrorFamily::Model, 1);
    /// The model ships its own code (reused from the base catalog).
    pub const REMOTE_CODE: ErrorCode = code(ErrorFamily::Model, 2);
    /// The model can be used only for inference (reused from P15).
    pub const INFERENCE_ONLY: ErrorCode = code(ErrorFamily::Model, 151);
    /// The model files can't be read (reused from P15).
    pub const UNREADABLE: ErrorCode = code(ErrorFamily::Model, 150);
}

/// What every models route and hook shares.
pub struct Models {
    pub(crate) db: Db,
    pub(crate) store: Arc<ArtifactStore>,
    pub(crate) projects: Projects,
}

impl Models {
    /// The database.
    pub fn db(&self) -> &Db {
        &self.db
    }
}

/// The `models` module.
pub struct ModelsModule {
    models: Arc<Models>,
}

impl ModelsModule {
    /// The module for this build, with its own handle on object storage.
    pub fn new(db: Db, config: &crate::Config) -> Self {
        // ponytail: modules can't reach another module's instance; the store is stateless
        // apart from its lazily opened backend, so a second handle reads the same objects.
        let store = ArtifactsModule::new(db.clone(), config).store();
        let projects = Projects::registered(&db);
        let module = Self::with_parts(db, store, projects);
        outputs::install(module.models.clone());
        module
    }

    /// The module with this store and project directory (tests and embedders).
    pub fn with_parts(db: Db, store: Arc<ArtifactStore>, projects: Projects) -> Self {
        Self {
            models: Arc::new(Models {
                db,
                store,
                projects,
            }),
        }
    }

    /// The shared state, for the completion verifier and tests.
    pub fn models(&self) -> Arc<Models> {
        self.models.clone()
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(api::list_models, api::create_model, api::get_model, api::list_versions,
          api::get_version, api::approve, api::acknowledge, jobs::import, jobs::merge,
          jobs::export, jobs::promote),
    components(schemas(api::ModelInfo, api::NewModel, api::ModelVersionInfo,
                       api::ModelFileInfo, api::ModelLicense, api::ModelVersionAction,
                       jobs::ImportModel, jobs::ImportSource, jobs::MergeAdapter,
                       jobs::ExportModel, jobs::ExportTarget, jobs::ModelExportJob)),
    tags((name = "models", description = "Models and immutable model versions."))
)]
struct Doc;

impl ServerModule for ModelsModule {
    fn name(&self) -> &'static str {
        "models"
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route(
                "/projects/{prj}/models",
                get(api::list_models).post(api::create_model),
            )
            .route("/projects/{prj}/models:import", post(jobs::import))
            .route("/models/{mdl}", get(api::get_model))
            .route("/models/{mdl}/versions", get(api::list_versions))
            .route(
                "/model-versions/{mv}",
                get(api::get_version).post(api::version_action),
            )
            .route_layer(axum::middleware::from_fn(
                crate::modules::jobs::error::render,
            ))
            .layer(Extension(self.models.clone()))
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        Doc::openapi()
    }
}

//! Dataset Studio (P27): datasets with immutable versions, their validation and stats,
//! record browsing, version diffs and exports, the registry lookup engines check runs
//! against, and the launch contributor that stages dataset versions for runs.
//!
//! The server never reads a user's data files. Built-in jobs parse, validate and normalize
//! them in a sandbox (`python/mrl-runtime/.../builtin/data_prep.py`) and write a version
//! folder: `dataset.json`, Parquet shards, browse pages, hash lists and reports. The server
//! reads only those small, size-capped, hash-checked JSON and text files ([`store`]) and
//! records READY or INVALID in one transaction.
//!
//! Register with `Box::new(crate::modules::datasets::DatasetsModule::new(db.clone(), config)),`.

pub mod api;
pub mod expr;
pub mod launch;
pub mod records;
pub mod store;
mod submit;
/// Dataset job registration and completion.
pub mod work;

use crate::app::{AppState, ServerModule};
use crate::db::Db;
use crate::modules::artifacts::{ArtifactStore, ArtifactsModule};
use crate::modules::jobs::Projects;
use axum::routing::get;
use axum::{Extension, Router};
use std::sync::Arc;
use utoipa::OpenApi;

/// Codes this part adds (guidance/fragments/errors/p27.yaml), and the ones it reuses.
pub mod codes {
    use mrl_domain::errors::{ErrorCode, ErrorFamily};

    const fn code(family: ErrorFamily, n: u16) -> ErrorCode {
        match ErrorCode::new(family, n) {
            Some(c) => c,
            None => panic!("error code numbers must be 1 to 999"),
        }
    }

    /// The project already has a dataset with this name.
    pub const NAME_TAKEN: ErrorCode = code(ErrorFamily::Conflict, 270);
    /// The dataset is `restricted-pii`, so it can't leave the platform.
    pub const EXPORT_REFUSED: ErrorCode = code(ErrorFamily::Perm, 270);
    /// The version is not READY (reused from P15).
    pub const NOT_READY: ErrorCode = code(ErrorFamily::Data, 2);
    /// The job's outputs are missing or don't match their file list.
    pub const OUTPUTS_BROKEN: ErrorCode = code(ErrorFamily::Data, 273);
}

/// What the handlers share: the database, object storage and the project directory.
pub struct Datasets {
    /// The database.
    pub db: Db,
    /// Object storage (P19): version files and transfer links.
    pub store: Arc<ArtifactStore>,
    /// Who owns each project.
    pub projects: Projects,
}

/// The module: dataset and version routes.
pub struct DatasetsModule {
    svc: Arc<Datasets>,
}

impl DatasetsModule {
    /// The module for this build, with its own (stateless) handle on object storage.
    pub fn new(db: Db, config: &crate::Config) -> Self {
        let store = ArtifactsModule::new(db.clone(), config).store();
        let projects = Projects::registered(&db);
        Self::with_parts(db, store, projects)
    }

    /// The module with explicit parts (tests).
    pub fn with_parts(db: Db, store: Arc<ArtifactStore>, projects: Projects) -> Self {
        Self {
            svc: Arc::new(Datasets {
                db,
                store,
                projects,
            }),
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        api::list_datasets, api::create_dataset, api::get_dataset, api::update_dataset,
        api::delete_dataset, api::list_versions, api::get_version, api::get_validation,
        api::get_stats, api::list_records, api::diff, api::export,
        submit::create_version, submit::tokenize, submit::transform_preview, submit::result
    ),
    components(schemas(
        api::DatasetPolicy, api::DatasetInfo, api::DatasetFileInfo, api::DatasetVersionInfo,
        api::DatasetCreate, api::DatasetPatch, api::DatasetValidation, api::DatasetContamination,
        api::DatasetRecord, api::DatasetRecordsPage, api::DatasetDiffRequest,
        api::DatasetRecordDiff, api::DatasetDiff, api::DatasetExportRequest,
        api::DatasetExportFile, api::DatasetExport, submit::DatasetVersionSource,
        submit::DatasetTokenizeRequest, submit::DatasetTransformPreviewRequest,
        submit::DatasetWorkAccepted, submit::DatasetWorkResult
    )),
    tags((name = "datasets", description = "Datasets: versions, validation, records, diffs and exports."))
)]
struct Doc;

impl ServerModule for DatasetsModule {
    fn name(&self) -> &'static str {
        "datasets"
    }

    fn handler_idempotency_paths(&self) -> &'static [&'static str] {
        &[
            "/projects/{prj}/datasets",
            "/datasets/{ds}/versions",
            "/dataset-versions/{dsv}",
        ]
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route(
                "/projects/{prj}/datasets",
                get(api::list_datasets).post(api::create_dataset),
            )
            .route(
                "/datasets/{ds}",
                get(api::get_dataset)
                    .patch(api::update_dataset)
                    .delete(api::delete_dataset),
            )
            .route(
                "/datasets/{ds}/versions",
                get(api::list_versions).post(submit::create_version),
            )
            .route("/dataset-versions/{dsv}/work/{job}", get(submit::result))
            .route(
                "/dataset-versions/{dsv}",
                get(api::get_version).post(api::version_action),
            )
            .route(
                "/dataset-versions/{dsv}/validation",
                get(api::get_validation),
            )
            .route("/dataset-versions/{dsv}/stats", get(api::get_stats))
            .route("/dataset-versions/{dsv}/records", get(api::list_records))
            .route_layer(axum::middleware::from_fn(
                crate::modules::jobs::error::render,
            ))
            .layer(Extension(self.svc.clone()))
    }

    fn background_tasks(&self) -> Vec<crate::app::Task> {
        let svc = self.svc.clone();
        vec![crate::app::Task::new(
            "datasets.outputs",
            crate::config::Role::Background,
            move |cancel| async move {
                loop {
                    if let Err(error) = work::check_pending(&svc).await {
                        tracing::warn!(%error, "could not check dataset outputs");
                    }
                    tokio::select! {
                        () = cancel.cancelled() => return Ok(()),
                        () = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                    }
                }
            },
        )]
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        Doc::openapi()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openapi_lists_every_endpoint_with_unique_ids() {
        let doc = Doc::openapi();
        let mut ids = Vec::new();
        for p in [
            "/projects/{prj}/datasets",
            "/datasets/{ds}",
            "/datasets/{ds}/versions",
            "/dataset-versions/{dsv}",
            "/dataset-versions/{dsv}/validation",
            "/dataset-versions/{dsv}/stats",
            "/dataset-versions/{dsv}/records",
            "/dataset-versions/{dsv}:diff",
            "/dataset-versions/{dsv}:export",
            "/dataset-versions/{dsv}:tokenize-preview",
            "/dataset-versions/{dsv}:transform-preview",
            "/dataset-versions/{dsv}/work/{job}",
        ] {
            let item = doc
                .paths
                .paths
                .get(p)
                .unwrap_or_else(|| panic!("{p} missing"));
            for op in [&item.get, &item.post, &item.patch, &item.delete]
                .into_iter()
                .flatten()
            {
                ids.push(op.operation_id.clone().unwrap_or_default());
            }
        }
        let n = ids.len();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), n, "operation ids repeat");
        assert_eq!(n, 16);
        assert!(ids.iter().all(|i| i.contains("dataset")), "{ids:?}");
    }
}

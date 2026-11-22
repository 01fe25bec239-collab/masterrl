//! Checkpoints: the two-phase commit on the server, verification levels, `latest_committed`
//! tracking, retention, and the FINALIZING verifier that makes COMPLETED mean verified (P8).
//!
//! Flow (all fenced by the attempt's epoch and the job's state):
//! 1. A relayed `CheckpointStaged` creates the row in WRITING with its file list; the core
//!    issues PUT tickets under `runs/{run}/checkpoints/{ckpt}/_staging/attempt-{a}/` and sends
//!    `UploadArtifacts` to the node ([`sink`]).
//! 2. A relayed `CheckpointCommitRequest` marks the commit requested. Once every file is in
//!    storage (UPLOADED) the core checks the manifest (`manifest_head`) and, in one
//!    transaction, moves the row through VERIFYING to COMMITTED, raises
//!    `runs.latest_committed_checkpoint_id` and writes the outbox events ([`service`]).
//! 3. `POST /checkpoints/{ckpt}:verify` re-reads every byte (`full_rehash`); a mismatch makes
//!    the checkpoint CORRUPT and points the run back at the previous COMMITTED one.
//! 4. [`FinalCheckpointVerifier`] (in the completion chain) and the finalizer pass decide
//!    FINALIZING jobs; [`cleanup`] applies retention, strips optimizer state and deletes
//!    objects.
//!
//! Register with `Box::new(crate::modules::checkpoints::CheckpointsModule::new(db.clone(), config)),`.

pub mod api;
pub mod cleanup;
pub mod service;
pub mod sink;
pub mod verifier;

pub use service::{CheckpointService, EmergencyOutcome};
pub use verifier::FinalCheckpointVerifier;

use crate::app::{AppState, ServerModule, Task};
use crate::config::Role;
use crate::db::{Db, LeaderElection};
use crate::modules::artifacts::{ArtifactStore, ArtifactsModule};
use crate::modules::jobs::Chains;
use crate::modules::nodes::lanes::LaneRouter;
use crate::modules::nodes::{Gateway, NodesModule};
use crate::seams::SecretStore;
use axum::routing::get;
use axum::{Extension, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use utoipa::OpenApi;

/// Codes this part adds (guidance/fragments/errors/p23.yaml).
pub mod codes {
    use mrl_domain::errors::{ErrorCode, ErrorFamily};

    const fn code(family: ErrorFamily, n: u16) -> ErrorCode {
        match ErrorCode::new(family, n) {
            Some(c) => c,
            None => panic!("error code numbers must be 1 to 999"),
        }
    }

    /// The checkpoint is not COMMITTED, so it can't be verified or used.
    pub const NOT_COMMITTED: ErrorCode = code(ErrorFamily::State, 230);
    /// The checkpoint is still needed (final result, resume point, parent of a run).
    pub const IN_USE: ErrorCode = code(ErrorFamily::Conflict, 230);
    /// Stored files don't match their list (reused from P08).
    pub const FILES_MISMATCH: ErrorCode = code(ErrorFamily::Store, 11);
}

/// `[checkpoints]`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct CheckpointsConfig {
    /// How often open commits, expired tickets, FINALIZING jobs and the time-based cadence are
    /// checked (1–60 s).
    pub tick_seconds: u64,
    /// How often retention, optimizer stripping and object deletion run (≥ 10 s).
    pub cleanup_seconds: u64,
    /// An uncommitted checkpoint older than this becomes CORRUPT, and a CORRUPT one's files
    /// are deleted after this long (staging TTL).
    pub staging_ttl_seconds: u64,
    /// Time-based checkpoint cadence in minutes (0 = off). A run sets its own with
    /// `checkpoint.time_based` in its config.
    pub time_based_minutes: u64,
    /// How long an emergency checkpoint may take before the request times out.
    pub emergency_deadline_seconds: u64,
    /// Retention: keep the newest N committed checkpoints.
    pub keep_last_n: u32,
    /// Retention: keep the best N by `best_metric` (0 = off).
    pub keep_best_n: u32,
    /// Retention: the metric `keep_best_n` ranks by (lower is better unless `best_mode = "max"`).
    pub best_metric: String,
    /// Retention: `min` or `max`.
    pub best_mode: String,
    /// Retention: keep every checkpoint whose step is a multiple of K (0 = off).
    pub keep_every_k: u64,
    /// Drop optimizer, scheduler, RNG and trainer state once a run COMPLETED.
    pub strip_optimizer_after_completion: bool,
}

impl Default for CheckpointsConfig {
    fn default() -> Self {
        Self {
            tick_seconds: 5,
            cleanup_seconds: 600,
            staging_ttl_seconds: 24 * 3600,
            time_based_minutes: 10,
            emergency_deadline_seconds: 120,
            keep_last_n: 3,
            keep_best_n: 0,
            best_metric: "eval/loss".to_owned(),
            best_mode: "min".to_owned(),
            keep_every_k: 0,
            strip_optimizer_after_completion: true,
        }
    }
}

impl CheckpointsConfig {
    /// Problems with the values (pushed into `Config::validate`).
    pub fn problems(&self) -> Vec<String> {
        let mut p = Vec::new();
        if !(1..=60).contains(&self.tick_seconds) {
            p.push("checkpoints.tick_seconds must be 1 to 60".to_owned());
        }
        if self.cleanup_seconds < 10 {
            p.push("checkpoints.cleanup_seconds must be at least 10".to_owned());
        }
        if self.staging_ttl_seconds < 1 {
            p.push("checkpoints.staging_ttl_seconds must be at least 1".to_owned());
        }
        if !(1..=3600).contains(&self.emergency_deadline_seconds) {
            p.push("checkpoints.emergency_deadline_seconds must be 1 to 3600".to_owned());
        }
        if self.keep_last_n < 1 {
            p.push(
                "checkpoints.keep_last_n must be at least 1 (the newest checkpoint is the resume point)"
                    .to_owned(),
            );
        }
        if !matches!(self.best_mode.as_str(), "min" | "max") {
            p.push("checkpoints.best_mode must be \"min\" or \"max\"".to_owned());
        }
        p
    }
}

/// The module: checkpoint routes, the CONTROL sink and the background passes.
pub struct CheckpointsModule {
    service: Arc<CheckpointService>,
    chains: Chains,
}

impl CheckpointsModule {
    /// The module for this build: its own handle on object storage and on command sending (both
    /// stateless), the server's keychain for the ticket signing key, and the process-wide
    /// lane router.
    pub fn new(db: Db, config: &crate::Config) -> Self {
        let store = ArtifactsModule::new(db.clone(), config).store();
        let gateway = NodesModule::from_config(db.clone(), config).gateway();
        let secrets = service::Secrets::keychain(config);
        let module = Self::build(
            db.clone(),
            store,
            secrets,
            &gateway,
            config.checkpoints.clone(),
            Chains::registered(&db),
        );
        LaneRouter::global().register(Arc::new(sink::CheckpointSink::new(module.service.clone())));
        module
    }

    /// The module with explicit parts (tests, other builds). Registers its sink on `lanes`.
    pub fn with_parts(
        db: Db,
        store: Arc<ArtifactStore>,
        secrets: Arc<dyn SecretStore>,
        gateway: &Arc<Gateway>,
        lanes: &LaneRouter,
        config: CheckpointsConfig,
        chains: Chains,
    ) -> Self {
        let module = Self::build(
            db,
            store,
            service::Secrets::given(secrets),
            gateway,
            config,
            chains,
        );
        lanes.register(Arc::new(sink::CheckpointSink::new(module.service.clone())));
        module
    }

    fn build(
        db: Db,
        store: Arc<ArtifactStore>,
        secrets: service::Secrets,
        gateway: &Arc<Gateway>,
        config: CheckpointsConfig,
        chains: Chains,
    ) -> Self {
        Self {
            service: Arc::new(CheckpointService::new(db, store, secrets, gateway, config)),
            chains,
        }
    }

    /// The shared service (resume points, emergency checkpoints, full verification).
    pub fn service(&self) -> Arc<CheckpointService> {
        self.service.clone()
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(api::list, api::get, api::action, api::delete),
    components(schemas(api::CheckpointInfo, api::CheckpointFileInfo)),
    tags((name = "checkpoints", description = "Checkpoints: listing, verification and deletion."))
)]
struct Doc;

impl ServerModule for CheckpointsModule {
    fn name(&self) -> &'static str {
        "checkpoints"
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route("/runs/{run}/checkpoints", get(api::list))
            .route(
                "/checkpoints/{ckpt}",
                get(api::get).post(api::action).delete(api::delete),
            )
            .route_layer(axum::middleware::from_fn(
                crate::modules::jobs::error::render,
            ))
            .layer(Extension(self.service.clone()))
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        Doc::openapi()
    }

    fn background_tasks(&self) -> Vec<Task> {
        let (service, chains) = (self.service.clone(), self.chains.clone());
        vec![Task::new(
            "checkpoints.passes",
            Role::Background,
            move |cancel| async move {
                let election = LeaderElection::new(
                    service.db().clone(),
                    "checkpoints",
                    Duration::from_secs(30),
                );
                election
                    .run(cancel, |lead| {
                        let (service, chains) = (service.clone(), chains.clone());
                        async move {
                            let tick = Duration::from_secs(service.config().tick_seconds);
                            let every = Duration::from_secs(service.config().cleanup_seconds);
                            let mut last_cleanup: Option<Instant> = None;
                            loop {
                                service.tick(&chains).await;
                                if last_cleanup.is_none_or(|t| t.elapsed() >= every) {
                                    cleanup::run(&service).await;
                                    last_cleanup = Some(Instant::now());
                                }
                                tokio::select! {
                                    () = lead.cancelled() => return Ok(()),
                                    () = tokio::time::sleep(tick) => {}
                                }
                            }
                        }
                    })
                    .await;
                Ok(())
            },
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_bad_values_are_named() {
        assert!(CheckpointsConfig::default().problems().is_empty());
        let bad = CheckpointsConfig {
            tick_seconds: 0,
            keep_last_n: 0,
            best_mode: "median".into(),
            ..CheckpointsConfig::default()
        };
        let p = bad.problems().join("\n");
        assert!(p.contains("tick_seconds") && p.contains("keep_last_n") && p.contains("best_mode"));
    }

    #[test]
    fn openapi_lists_every_endpoint_with_unique_ids() {
        let doc = Doc::openapi();
        let mut ids = Vec::new();
        for p in [
            "/runs/{run}/checkpoints",
            "/checkpoints/{ckpt}",
            "/checkpoints/{ckpt}:verify",
        ] {
            let item = doc
                .paths
                .paths
                .get(p)
                .unwrap_or_else(|| panic!("{p} missing"));
            for op in [&item.get, &item.post, &item.delete].into_iter().flatten() {
                ids.push(op.operation_id.clone().unwrap_or_default());
            }
        }
        ids.sort();
        assert_eq!(
            ids,
            [
                "delete_checkpoint",
                "get_checkpoint",
                "list_checkpoints",
                "verify_checkpoint"
            ]
        );
    }
}

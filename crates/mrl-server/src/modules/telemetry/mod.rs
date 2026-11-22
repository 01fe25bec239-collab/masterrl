//! Live run data: metric ingest and history, logs, samples, run comparison and the
//! [`MetricObserver`] hook.
//!
//! - Lanes (D-04-05): one METRICS sink (metrics, samples, rollout and reward stats), one LOGS
//!   sink and one CONTROL sink for `worker.progress`, all on [`LaneRouter`].
//! - Storage: `metric_series` + weekly `metric_points` partitions + `metric_rollups` (`1m`,
//!   `step_bucket`), `metric_batches` receipts, `log_chunks` sealed into `log_index` segments,
//!   `samples`, `run_progress`, `resource_actuals`.
//! - Routes: `GET /runs/{run}/metrics`, `GET /runs/{run}/metrics/query`,
//!   `GET /runs/{run}/logs`, `GET /runs/{run}/logs:download`, `GET /runs/{run}/samples`,
//!   `POST /runs:compare`.
//!
//! Register with `Box::new(crate::modules::telemetry::TelemetryModule::new(db.clone(), config)),`.

pub mod history;
pub mod ingest;
pub mod logs;
pub mod observer;
pub mod rollup;
pub mod samples;

pub use ingest::{Ingest, LogsSink, MetricsSink, ProgressSink};
pub use observer::{IngestedBatch, MetricObserver, MetricObservers, ObservedPoint};

use crate::app::{AppState, ServerModule, Task};
use crate::config::Role;
use crate::db::{Db, LeaderElection};
use crate::modules::artifacts::{ArtifactStore, ArtifactsModule, LOCAL_ORG};
use crate::modules::jobs::scope;
use crate::modules::jobs::JobsError;
use crate::modules::nodes::lanes::LaneRouter;
use crate::modules::runs::api::{find_run, RunRow};
use crate::seams::{RequestContext, SeamError, StartupStep};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Router};
use mrl_domain::errors::{ErrorCode as DomainCode, ErrorFamily};
use mrl_domain::ids::{OrgId, RunId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use utoipa::OpenApi;
use uuid::Uuid;

/// Codes this part adds (guidance/fragments/errors/p22.yaml).
pub mod codes {
    use super::{DomainCode, ErrorFamily};

    const fn code(family: ErrorFamily, n: u16) -> DomainCode {
        match DomainCode::new(family, n) {
            Some(c) => c,
            None => panic!("error code numbers must be 1 to 999"),
        }
    }

    /// Too many live stream connections or topics for this sign-in.
    pub const TOO_MANY_STREAMS: DomainCode = code(ErrorFamily::Quota, 220);
    /// The alert is resolved and takes no more changes.
    pub const ALERT_RESOLVED: DomainCode = code(ErrorFamily::State, 220);
    /// A sealed log segment or sample file is missing or fails its checksum.
    pub const STORED_FILE_BAD: DomainCode = code(ErrorFamily::Store, 220);
}

/// `[live]`: live run data (metrics, logs, samples, streams and alerts).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct LiveConfig {
    /// Days raw metric points are kept (weekly partitions are dropped after this).
    pub raw_retention_days: u32,
    /// Days one-minute rollups are kept. Step rollups live as long as the run.
    pub rollup_1m_retention_days: u32,
    /// Seconds a log chunk stays in the database before it is sealed into object storage.
    pub log_seal_after_seconds: u64,
    /// Days sealed log segments are kept.
    pub log_retention_days: u32,
    /// Days sample files are kept.
    pub sample_retention_days: u32,
    /// Seconds between maintenance passes (rollups, sealing, partitions, retention).
    pub maintenance_interval_seconds: u64,
    /// Milliseconds between stream polls; at least 500, so a series updates at most twice a
    /// second.
    pub stream_poll_millis: u64,
    /// Seconds between `heartbeat` events on an idle stream.
    pub stream_heartbeat_seconds: u64,
    /// Open streams allowed per principal on one server.
    pub max_streams_per_principal: u32,
    /// Topics allowed on one stream.
    pub max_topics_per_stream: u32,
    /// Series allowed per run; points of further new names are dropped.
    pub max_series_per_run: u32,
}

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            raw_retention_days: 7,
            rollup_1m_retention_days: 90,
            log_seal_after_seconds: 48 * 3600,
            log_retention_days: 90,
            sample_retention_days: 30,
            maintenance_interval_seconds: 30,
            stream_poll_millis: 500,
            stream_heartbeat_seconds: 15,
            max_streams_per_principal: 10,
            max_topics_per_stream: 50,
            max_series_per_run: 10_000,
        }
    }
}

impl LiveConfig {
    /// Problems with these settings, in plain words.
    pub fn problems(&self) -> Vec<String> {
        let mut p = Vec::new();
        let mut range = |name: &str, v: u64, lo: u64, hi: u64| {
            if !(lo..=hi).contains(&v) {
                p.push(format!("live.{name}: {v} is outside {lo}–{hi}"));
            }
        };
        range("raw_retention_days", self.raw_retention_days.into(), 7, 365);
        range(
            "rollup_1m_retention_days",
            self.rollup_1m_retention_days.into(),
            1,
            3650,
        );
        range(
            "log_seal_after_seconds",
            self.log_seal_after_seconds,
            1,
            30 * 86_400,
        );
        range(
            "log_retention_days",
            self.log_retention_days.into(),
            1,
            3650,
        );
        range(
            "sample_retention_days",
            self.sample_retention_days.into(),
            1,
            3650,
        );
        range(
            "maintenance_interval_seconds",
            self.maintenance_interval_seconds,
            1,
            3600,
        );
        range("stream_poll_millis", self.stream_poll_millis, 500, 10_000);
        range(
            "stream_heartbeat_seconds",
            self.stream_heartbeat_seconds,
            1,
            300,
        );
        range(
            "max_streams_per_principal",
            self.max_streams_per_principal.into(),
            1,
            1000,
        );
        range(
            "max_topics_per_stream",
            self.max_topics_per_stream.into(),
            1,
            500,
        );
        range(
            "max_series_per_run",
            self.max_series_per_run.into(),
            1,
            1_000_000,
        );
        p
    }
}

/// Shared state of the telemetry routes and tasks.
pub struct Telemetry {
    /// The database.
    pub db: Db,
    /// Settings.
    pub config: LiveConfig,
    /// Object storage (sealed logs, sample files).
    pub store: Arc<ArtifactStore>,
    /// Ingest (also used by the sinks).
    pub ingest: Arc<Ingest>,
}

/// The telemetry module.
pub struct TelemetryModule {
    inner: Arc<Telemetry>,
}

impl TelemetryModule {
    /// The module for this server: storage from `[object_store]`, sinks on
    /// [`LaneRouter::global`], observers from [`MetricObservers::global`].
    pub fn new(db: Db, config: &crate::Config) -> Self {
        let store = ArtifactsModule::new(db.clone(), config).store();
        Self::with_parts(
            db,
            config.live.clone(),
            store,
            &LaneRouter::global(),
            MetricObservers::global(),
        )
    }

    /// The module with explicit parts (tests and embedders). Registers its three sinks on
    /// `lanes`.
    pub fn with_parts(
        db: Db,
        config: LiveConfig,
        store: Arc<ArtifactStore>,
        lanes: &LaneRouter,
        observers: Arc<MetricObservers>,
    ) -> Self {
        let ingest = Arc::new(Ingest::new(
            db.clone(),
            store.clone(),
            observers,
            config.max_series_per_run,
        ));
        lanes.register(Arc::new(MetricsSink(ingest.clone())));
        lanes.register(Arc::new(LogsSink(ingest.clone())));
        lanes.register(Arc::new(ProgressSink(ingest.clone())));
        Self {
            inner: Arc::new(Telemetry {
                db,
                config,
                store,
                ingest,
            }),
        }
    }

    /// The shared state (tests run maintenance passes with it).
    pub fn telemetry(&self) -> Arc<Telemetry> {
        self.inner.clone()
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        history::list_series, history::query, history::compare,
        logs::list, logs::download, samples::list,
    ),
    components(schemas(
        history::MetricSeriesView, history::RunMetrics, history::RunProgressView,
        history::ResourceActualView, history::MetricQueryResult, history::MetricSeriesPoints,
        history::MetricPointView, history::RunCompareRequest, history::RunCompareResult,
        history::RunCompareRun, history::RunCompareSeries, history::RunCompareDiff,
        logs::LogEntryView, logs::LogPage, samples::SampleView, samples::SamplePage,
    )),
    tags((name = "telemetry", description = "Metric history, logs, samples and run comparison."))
)]
struct Doc;

impl ServerModule for TelemetryModule {
    fn name(&self) -> &'static str {
        "telemetry"
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route("/runs/{run}/metrics", get(history::list_series))
            .route("/runs/{run}/metrics/query", get(history::query))
            .route("/runs:compare", post(history::compare))
            .route("/runs/{run}/logs", get(logs::list))
            .route("/runs/{run}/logs:download", get(logs::download))
            .route("/runs/{run}/samples", get(samples::list))
            .route_layer(axum::middleware::from_fn(
                crate::modules::jobs::error::render,
            ))
            .layer(Extension(self.inner.clone()))
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        Doc::openapi()
    }

    fn startup_steps(&self) -> Vec<Arc<dyn StartupStep>> {
        vec![Arc::new(Partitions(self.inner.db.clone()))]
    }

    fn background_tasks(&self) -> Vec<Task> {
        let t = self.inner.clone();
        vec![Task::new(
            "telemetry.maintenance",
            Role::Background,
            move |cancel| async move {
                let election = LeaderElection::new(
                    t.db.clone(),
                    "telemetry-maintenance",
                    Duration::from_secs(30),
                );
                election
                    .run(cancel, |lead| {
                        let t = t.clone();
                        async move {
                            let every = Duration::from_secs(t.config.maintenance_interval_seconds);
                            let mut last_retention: Option<std::time::Instant> = None;
                            loop {
                                let retention = last_retention
                                    .is_none_or(|at| at.elapsed() >= Duration::from_secs(3600));
                                match rollup::maintain(&t, retention).await {
                                    Ok(done) => {
                                        if retention {
                                            last_retention = Some(std::time::Instant::now());
                                        }
                                        tracing::debug!(?done, "telemetry maintenance done");
                                    }
                                    Err(e) => tracing::warn!(code = %e.code(), error = %e, "telemetry maintenance failed; trying again later"),
                                }
                                tokio::select! {
                                    () = lead.cancelled() => return Ok(()),
                                    () = tokio::time::sleep(every) => {}
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

/// Startup step: the metric partitions for this week and the next ones exist.
struct Partitions(Db);

#[async_trait::async_trait]
impl StartupStep for Partitions {
    fn name(&self) -> &str {
        "metric-partitions"
    }

    async fn run(&self) -> Result<(), SeamError> {
        rollup::ensure_partitions(&self.0, time::OffsetDateTime::now_utc())
            .await
            .map_err(|e| SeamError::new("metric-partitions", e.to_string()))
    }
}

/// The run `run` (a `run_…` id) after `action` on it was allowed; hidden and missing runs are
/// 404, refused ones 403.
pub(crate) async fn read_run(
    state: &AppState,
    db: &Db,
    ctx: &RequestContext,
    run: &str,
    action: &str,
) -> Result<RunRow, JobsError> {
    let id: RunId = scope::parse_id(run, "run")?;
    let row = find_run(db, id.as_uuid())
        .await?
        .ok_or_else(|| JobsError::not_found("run"))?;
    scope::authorize(state, ctx, action, &run_resource(&row)).await?;
    Ok(row)
}

/// The authorizer's resource for a run.
pub(crate) fn run_resource(row: &RunRow) -> crate::seams::Resource {
    scope::resource(
        "run",
        Some(RunId::from_uuid(row.id).to_string()),
        row.org_id,
        row.project_id,
    )
}

/// The object-storage org of rows stored under `org` (the nil org is the solo tenant).
pub(crate) fn storage_org(org: Uuid) -> String {
    if org.is_nil() {
        LOCAL_ORG.to_owned()
    } else {
        OrgId::from_uuid(org).to_string()
    }
}

/// 503 MRL-STORE-220 (or 503 MRL-INTERNAL-002 for an unreachable store) for a stored file.
pub(crate) fn stored_file_error(what: &str, e: &dyn std::fmt::Display) -> JobsError {
    tracing::warn!(error = %e, what, "stored telemetry file could not be read");
    JobsError::domain(
        codes::STORED_FILE_BAD,
        StatusCode::SERVICE_UNAVAILABLE,
        format!("The saved {what} could not be read from storage."),
    )
}

/// Parses an optional whole-number query value; anything else is a 422 field error.
pub(crate) fn parse_num<T: std::str::FromStr>(
    v: Option<&str>,
    field: &str,
) -> Result<Option<T>, JobsError> {
    v.map(|s| {
        s.parse::<T>().map_err(|_| {
            JobsError::Api(crate::error::ApiError::validation(vec![
                crate::error::FieldError::new(
                    format!("/{field}"),
                    crate::error::ErrorCode::FieldValue,
                    format!("{field} must be a number, not {s:?}."),
                    "Send a plain number.",
                ),
            ]))
        })
    })
    .transpose()
}

/// A 422 field error at `/field`.
pub(crate) fn field_error(field: &str, msg: &str, hint: &str) -> JobsError {
    JobsError::Api(crate::error::ApiError::validation(vec![
        crate::error::FieldError::new(
            format!("/{field}"),
            crate::error::ErrorCode::FieldValue,
            msg,
            hint,
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_limits_are_checked() {
        assert!(LiveConfig::default().problems().is_empty());
        let bad = LiveConfig {
            stream_poll_millis: 100,
            raw_retention_days: 0,
            ..LiveConfig::default()
        };
        let p = bad.problems();
        assert_eq!(p.len(), 2, "{p:?}");
        assert!(p[0].starts_with("live.raw_retention_days"));
    }

    #[test]
    fn codes_are_registered_and_catalogued() {
        for c in [
            codes::TOO_MANY_STREAMS,
            codes::ALERT_RESOLVED,
            codes::STORED_FILE_BAD,
        ] {
            assert!(c.title().is_some(), "{c} missing from mrl-domain");
            assert!(
                !crate::modules::jobs::error::hint_for(&c.to_string()).is_empty(),
                "{c} has no guidance steps"
            );
        }
    }

    #[test]
    fn the_solo_tenant_stores_under_local() {
        assert_eq!(storage_org(Uuid::nil()), LOCAL_ORG);
        let org = OrgId::new();
        assert_eq!(storage_org(org.as_uuid()), org.to_string());
    }

    #[test]
    fn openapi_lists_every_route() {
        let doc = Doc::openapi();
        for p in [
            "/runs/{run}/metrics",
            "/runs/{run}/metrics/query",
            "/runs:compare",
            "/runs/{run}/logs",
            "/runs/{run}/logs:download",
            "/runs/{run}/samples",
        ] {
            assert!(doc.paths.paths.contains_key(p), "{p} missing");
        }
    }
}

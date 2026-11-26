//! The engine registry (P24): first-party engine manifests, probes and L0 capability checks.
//!
//! - At startup ([`RegisterEngines`]) every embedded bundle (`engines/first_party/*.json`,
//!   written by `tools/gen/engines.py`) is checked and upserted into `training_engines`,
//!   `engine_versions` (status PROBING) and `algorithm_definitions`. A stored version's
//!   manifest never changes: a different manifest under the same name and version stops start.
//! - [`probes`] sends `Probe` commands to connected machines and records the answers in
//!   `capability_evidence`; the first passed probe on a hardware class enables the version
//!   there (PROBING → ENABLED).
//! - [`CapabilityValidator`] is the L0 [`JobValidator`](crate::modules::jobs::JobValidator):
//!   it accepts a job only when some engine version can run it (see
//!   `mrl_domain::capability::matcher`).
//! - [`api`] serves `/engines`, `/capabilities` and `/algorithms/{algo}/schema`.

pub mod api;
pub mod launch;
pub mod probes;
pub mod store;
pub mod validator;

pub use validator::{CapabilityValidator, Pick};

use crate::app::{ServerModule, Task};
use crate::config::Role;
use crate::db::{Db, DbError};
use crate::modules::nodes::{lanes::LaneRouter, Gateway, NodesError};
use crate::seams::{SeamError, StartupStep};
use crate::AppState;
use async_trait::async_trait;
use axum::Router;
use mrl_domain::capability::manifest::{Bundle, ManifestError};
use mrl_domain::capability::matcher::{DatasetFacts, ModelFacts};
use mrl_domain::ids::{DatasetVersionId, ModelVersionId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// The bundles compiled into this server (generated list).
mod embedded {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/engines/first_party/embedded.rs"
    ));
}

/// `[engines]` settings.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct EnginesConfig {
    /// Register dev-only engines (the FakeEngine) and list them. For test and dev servers.
    pub dev_engines: bool,
    /// How often the leader looks for engine versions to probe, in seconds (1–3600).
    pub probe_interval_seconds: u64,
    /// A probe with no answer after this long is sent again, in seconds (10–86400).
    pub probe_timeout_seconds: u64,
    /// After a failed probe, wait this long before asking again, in seconds (10–604800).
    pub probe_retry_seconds: u64,
}

impl Default for EnginesConfig {
    fn default() -> Self {
        Self {
            dev_engines: false,
            probe_interval_seconds: 30,
            probe_timeout_seconds: 300,
            probe_retry_seconds: 3600,
        }
    }
}

impl EnginesConfig {
    /// Settings that can't be used, in plain words.
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut range = |name: &str, v: u64, lo: u64, hi: u64| {
            if !(lo..=hi).contains(&v) {
                out.push(format!("engines.{name}: use {lo}–{hi} seconds (got {v})"));
            }
        };
        range(
            "probe_interval_seconds",
            self.probe_interval_seconds,
            1,
            3600,
        );
        range(
            "probe_timeout_seconds",
            self.probe_timeout_seconds,
            10,
            86_400,
        );
        range("probe_retry_seconds", self.probe_retry_seconds, 10, 604_800);
        out
    }
}

/// Codes this part uses from other parts.
pub mod codes {
    /// An engine manifest can't be used (reused from P13).
    pub const MANIFEST_INVALID: &str = "MRL-VAL-130";
}

/// What can go wrong in the registry.
#[derive(Debug, thiserror::Error)]
pub enum EnginesError {
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// An embedded bundle can't be used.
    #[error("the engine bundle {source_name} can't be used: {error}")]
    Manifest {
        /// Bundle source (adapter folder) or index.
        source_name: String,
        /// Why.
        error: ManifestError,
    },
    /// A stored engine version has another manifest than the embedded one.
    #[error(
        "engine {name} {version} is already registered with a different manifest; a version's manifest never changes, so give the changed manifest a new version"
    )]
    Changed {
        /// Engine name.
        name: String,
        /// Version.
        version: String,
    },
    /// Sending a command failed.
    #[error(transparent)]
    Nodes(#[from] NodesError),
    /// A registry lookup (model or dataset records) failed.
    #[error(transparent)]
    Seam(SeamError),
    /// A stored value or an event could not be made.
    #[error("an engine record could not be read or written: {0}")]
    Event(String),
}

impl From<sqlx::Error> for EnginesError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e.into())
    }
}

impl EnginesError {
    /// The catalog code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Db(e) => e.code(),
            Self::Manifest { .. } | Self::Changed { .. } => codes::MANIFEST_INVALID,
            Self::Nodes(e) => e.code(),
            Self::Seam(_) => crate::db::codes::UNAVAILABLE,
            Self::Event(_) => crate::db::codes::INTERNAL,
        }
    }
}

impl From<EnginesError> for SeamError {
    fn from(e: EnginesError) -> Self {
        match e {
            EnginesError::Seam(s) => s,
            e => SeamError::new("engines", format!("{}: {e}", e.code())),
        }
    }
}

/// Every embedded bundle, checked. A bundle that fails its checks is an error (the generator
/// checked it too, so this means a hand-edited or stale file).
pub fn embedded_bundles() -> Result<Vec<Bundle>, EnginesError> {
    parse_all(embedded::BUNDLES.iter().copied(), "engines/first_party")
}

fn parse_all<'a>(
    texts: impl Iterator<Item = &'a str>,
    origin: &str,
) -> Result<Vec<Bundle>, EnginesError> {
    texts
        .enumerate()
        .map(|(i, text)| {
            Bundle::parse(text).map_err(|error| EnginesError::Manifest {
                source_name: format!("#{i} in {origin}"),
                error,
            })
        })
        .collect()
}

/// Tenant and project in which a registry record must be visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordScope {
    /// Owning organization.
    pub org_id: Uuid,
    /// Project requesting access.
    pub project_id: Uuid,
}

/// What the model and dataset registries know (Doc 05 §12.4). Wave 6 registries implement
/// it; each answers `None` for missing, deleted, or out-of-scope records.
#[async_trait]
pub trait RegistryRecords: Send + Sync {
    /// Facts about a model version visible in `scope`, if it holds one with this id.
    async fn model(
        &self,
        scope: &RecordScope,
        id: ModelVersionId,
    ) -> Result<Option<ModelFacts>, SeamError>;
    /// Facts about a dataset version visible in `scope`, if it holds one with this id.
    async fn dataset(
        &self,
        scope: &RecordScope,
        id: DatasetVersionId,
    ) -> Result<Option<DatasetFacts>, SeamError>;
}

/// The registered [`RegistryRecords`]; the first member that knows a record answers.
#[derive(Clone, Default)]
pub struct Records(Arc<[Arc<dyn RegistryRecords>]>);

impl Records {
    /// The members this build registers. Wave 6 registries add one line inside the block,
    /// like `Arc::new(crate::modules::models::ModelFacts::new(db.clone())),`.
    pub fn registered(db: &Db) -> Self {
        let _ = db;
        let members: Vec<Arc<dyn RegistryRecords>> = vec![
            // <mrl:engine-records> Registry lookups go right below this line.
            // <mrl:engine-records P26> P26 lookups go right below this line.
            // <mrl:engine-records P27> P27 lookups go right below this line.
        ];
        Self(members.into())
    }

    /// Exactly these members (tests use this).
    pub fn new(members: Vec<Arc<dyn RegistryRecords>>) -> Self {
        Self(members.into())
    }

    /// The model's facts; `None` when no member knows it.
    pub async fn model(
        &self,
        scope: &RecordScope,
        id: ModelVersionId,
    ) -> Result<Option<ModelFacts>, SeamError> {
        for m in self.0.iter() {
            if let Some(f) = m.model(scope, id).await? {
                return Ok(Some(f));
            }
        }
        Ok(None)
    }

    /// The dataset's facts; `None` when no member knows it.
    pub async fn dataset(
        &self,
        scope: &RecordScope,
        id: DatasetVersionId,
    ) -> Result<Option<DatasetFacts>, SeamError> {
        for m in self.0.iter() {
            if let Some(f) = m.dataset(scope, id).await? {
                return Ok(Some(f));
            }
        }
        Ok(None)
    }
}

/// The `engines` module.
pub struct EnginesModule {
    db: Db,
    config: EnginesConfig,
    bundles: Result<Arc<[Bundle]>, Arc<EnginesError>>,
    prober: Arc<probes::Prober>,
    records: Records,
}

impl EnginesModule {
    /// The module for this build: embedded bundles, the process-wide lane router.
    pub fn new(db: Db, config: &crate::Config) -> Self {
        // ponytail: `CommandSender` reads only the database and the gateway settings, so a
        // gateway value built from the same config sends exactly like the nodes module's own.
        let gateway = crate::modules::nodes::NodesModule::from_config(db.clone(), config).gateway();
        let records = Records::registered(&db);
        Self::with(
            db,
            config.engines.clone(),
            &gateway,
            &LaneRouter::global(),
            Vec::new(),
            records,
        )
    }

    /// A module with this gateway and lane router, plus `extra` bundle texts after the
    /// embedded ones (tests and embedders). A bundle that fails its checks stops startup.
    pub fn with(
        db: Db,
        config: EnginesConfig,
        gateway: &Arc<Gateway>,
        lanes: &LaneRouter,
        extra: Vec<String>,
        records: Records,
    ) -> Self {
        let bundles = embedded_bundles()
            .and_then(|mut b| {
                b.extend(parse_all(
                    extra.iter().map(String::as_str),
                    "extra bundles",
                )?);
                Ok(b.into())
            })
            .map_err(Arc::new);
        let prober = Arc::new(probes::Prober::new(db.clone(), config.clone(), gateway));
        lanes.register(Arc::new(probes::ProbeSink::new(db.clone())));
        Self {
            db,
            config,
            bundles,
            prober,
            records,
        }
    }
}

impl ServerModule for EnginesModule {
    fn name(&self) -> &'static str {
        "engines"
    }

    fn routes(&self) -> Router<AppState> {
        api::routes(self.db.clone(), self.records.clone())
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        api::openapi()
    }

    fn startup_steps(&self) -> Vec<Arc<dyn StartupStep>> {
        vec![Arc::new(RegisterEngines {
            db: self.db.clone(),
            bundles: self.bundles.clone(),
            dev_engines: self.config.dev_engines,
        })]
    }

    fn background_tasks(&self) -> Vec<Task> {
        let prober = self.prober.clone();
        vec![Task::new(
            "engines.probes",
            Role::Background,
            move |cancel| async move {
                prober.run(cancel).await;
                Ok(())
            },
        )]
    }
}

/// Startup step: checks every bundle and upserts it.
pub struct RegisterEngines {
    db: Db,
    bundles: Result<Arc<[Bundle]>, Arc<EnginesError>>,
    dev_engines: bool,
}

#[async_trait]
impl StartupStep for RegisterEngines {
    fn name(&self) -> &str {
        "engines.register"
    }

    async fn run(&self) -> Result<(), SeamError> {
        let bundles = self
            .bundles
            .as_ref()
            .map_err(|e| SeamError::new("engines", format!("{}: {e}", e.code())))?;
        let wanted: Vec<&Bundle> = bundles
            .iter()
            .filter(|b| self.dev_engines || !b.dev_only)
            .collect();
        store::register(&self.db, &wanted, self.dev_engines).await?;
        tracing::info!(engines = wanted.len(), "engine bundles registered");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_bundles_read_and_include_the_builtin_and_fake_engines() {
        let b = embedded_bundles().unwrap();
        let names: Vec<_> = b.iter().map(|b| (b.name.as_str(), b.dev_only)).collect();
        assert!(names.contains(&("fake", true)), "{names:?}");
        assert!(names.contains(&("mrl-builtin", false)), "{names:?}");
    }

    #[test]
    fn settings_ranges_are_checked() {
        assert!(EnginesConfig::default().problems().is_empty());
        let bad = EnginesConfig {
            probe_interval_seconds: 0,
            probe_timeout_seconds: 1,
            ..EnginesConfig::default()
        };
        assert_eq!(bad.problems().len(), 2, "{:?}", bad.problems());
    }
}

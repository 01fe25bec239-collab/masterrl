//! Reward Studio's records (P29): reward definitions, immutable content-hashed versions,
//! their lifecycle and the publish gate (Doc 06 §1.4, Doc 09 §6.3 Rewards).
//!
//! - A **definition** (`rwd_…`) names a reward in a project. Its **versions** (`rwdv_…`) hold
//!   a RewardPolicy v1, an optional code bundle (a P19 artifact of pure-Python files that the
//!   server stores and hashes but never opens), test cases and the [`hash::HashInputs`].
//!   Creating a version with the same content as an existing one returns that version.
//! - States follow `mrl_domain::state::RewardVersion`: `DRAFT → TESTING → PUBLISHED |
//!   FAILED_TESTS`, `PUBLISHED → DEPRECATED`. Content never changes (a database trigger
//!   refuses it); once PUBLISHED, only deprecating is left.
//! - `:publish` needs a passing test run in the reward sandbox ([`verifier`] records it from
//!   the REWARD_PREVIEW job's report) and, when an earlier version is published, a regression
//!   report against it.
//! - `GET /reward-components` lists the components the reward sandbox ships; the list is
//!   generated from `mrl-rewards` by `tools/gen/rewards.py` and seeded at startup.
//! - [`launch::RewardInputs`] stages a run's pinned reward (policy, code files, hashes) into
//!   its launch and refuses versions a run may not use.
//! - `:test`, `:preview`, `:regression` and `POST /rewards:compare` start REWARD_PREVIEW
//!   built-in jobs ([`checks`]); [`verifier::ReportVerifier`] records their reports and
//!   [`verifier::EndedEarly`] settles the ones whose job ended without one.

pub mod api;
pub mod check_api;
pub mod checks;
pub mod hash;
pub mod launch;
pub mod policy;
pub mod stats;
pub mod storage;
pub mod store;
pub mod submission;
pub mod verifier;

use crate::app::{AppState, ServerModule};
use crate::db::Db;
use crate::error::{FieldError, Problem, PROBLEM_JSON};
use crate::modules::jobs::JobsError;
use crate::seams::{RequestMeta, SeamError, StartupStep};
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{OriginalUri, Request};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Router;
use mrl_domain::errors::ErrorCode as DomainCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// `[rewards]` settings.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct RewardsConfig {
    /// Most test cases in one version (1–1000).
    pub max_test_cases: u32,
    /// Most files in a code bundle (1–512).
    pub max_code_files: u32,
    /// Largest code bundle, in bytes (1 KiB – 16 MiB).
    pub max_code_bytes: u64,
    /// Most samples one preview may score (1–5000).
    pub max_preview_samples: u32,
    /// Share of prompt groups without any reward spread above which a preview warns that
    /// group-based methods learn nothing (0–1).
    pub learnability_warn_share: f64,
}

impl Default for RewardsConfig {
    fn default() -> Self {
        Self {
            max_test_cases: 200,
            max_code_files: 512,
            max_code_bytes: 16 << 20,
            max_preview_samples: 500,
            learnability_warn_share: 0.9,
        }
    }
}

impl RewardsConfig {
    /// Settings that can't be used, in plain words.
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut range = |name: &str, v: u64, lo: u64, hi: u64| {
            if !(lo..=hi).contains(&v) {
                out.push(format!("rewards.{name}: use {lo}–{hi} (got {v})"));
            }
        };
        range("max_test_cases", self.max_test_cases.into(), 1, 1000);
        range("max_code_files", self.max_code_files.into(), 1, 512);
        range("max_code_bytes", self.max_code_bytes, 1024, 16 << 20);
        range(
            "max_preview_samples",
            self.max_preview_samples.into(),
            1,
            5000,
        );
        if !(0.0..=1.0).contains(&self.learnability_warn_share) {
            out.push(format!(
                "rewards.learnability_warn_share: use 0–1 (got {})",
                self.learnability_warn_share
            ));
        }
        out
    }
}

/// Codes this part adds (guidance/fragments/errors/p29.yaml) or reuses.
pub mod codes {
    use mrl_domain::errors::{ErrorCode, ErrorFamily};

    const fn code(family: ErrorFamily, n: u16) -> ErrorCode {
        match ErrorCode::new(family, n) {
            Some(c) => c,
            None => panic!("error code numbers must be 1 to 999"),
        }
    }

    /// The policy uses a learned reward model or a judge model (until P41).
    pub const LEARNED_UNAVAILABLE: ErrorCode = code(ErrorFamily::Rwd, 290);
    /// A REWARD_PREVIEW report can't be read or doesn't match the version.
    pub const REPORT_UNREADABLE: ErrorCode = code(ErrorFamily::Rwd, 297);
    /// The version can't take that step in its state.
    pub const ILLEGAL_STEP: ErrorCode = code(ErrorFamily::State, 290);
    /// The version's checks haven't passed, so it can't be published.
    pub const NOT_PUBLISHABLE: ErrorCode = code(ErrorFamily::State, 291);
    /// A test run without test cases, or a comparison without a published earlier version.
    pub const NOTHING_TO_CHECK: ErrorCode = code(ErrorFamily::State, 292);
    /// A reward with this name exists in the project.
    pub const NAME_TAKEN: ErrorCode = code(ErrorFamily::Conflict, 290);
    /// A test case can't be read.
    pub const CASE_INVALID: ErrorCode = code(ErrorFamily::Val, 290);
    /// The policy has a problem (reused from P14).
    pub const POLICY_INVALID: ErrorCode = code(ErrorFamily::Rwd, 140);
    /// A component isn't installed (reused from P14).
    pub const COMPONENT_UNKNOWN: ErrorCode = code(ErrorFamily::Rwd, 142);
    /// A component's params need a change (reused from P14).
    pub const PARAMS_INVALID: ErrorCode = code(ErrorFamily::Rwd, 143);
    /// The code bundle can't be used (reused from P14).
    pub const BUNDLE_INVALID: ErrorCode = code(ErrorFamily::Rwd, 147);
    /// Step-by-step rewards aren't available (reused from P14).
    pub const STEP_UNSUPPORTED: ErrorCode = code(ErrorFamily::Rwd, 148);
}

/// A problem with a domain code and, optionally, field problems.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewardProblem {
    /// The registry code.
    pub code: DomainCode,
    /// HTTP status.
    pub status: StatusCode,
    /// What happened, in plain words.
    pub detail: String,
    /// Field problems (each with its own code).
    pub errors: Vec<FieldError>,
}

/// What the rewards module returns.
#[derive(Debug, thiserror::Error)]
pub enum RewardsError {
    /// Shared runs/jobs errors (404, 403, database, seams, domain codes).
    #[error(transparent)]
    Jobs(#[from] JobsError),
    /// A rewards problem.
    #[error("{}: {}", .0.code, .0.detail)]
    Problem(RewardProblem),
}

impl RewardsError {
    /// A domain-coded problem.
    pub fn problem(code: DomainCode, status: StatusCode, detail: impl Into<String>) -> Self {
        Self::Problem(RewardProblem {
            code,
            status,
            detail: detail.into(),
            errors: Vec::new(),
        })
    }

    /// 422 with field problems; the first problem's code is the document's code.
    pub fn fields(errors: Vec<FieldError>) -> Self {
        let code = errors
            .first()
            .and_then(|e| e.code.parse().ok())
            .unwrap_or(codes::POLICY_INVALID);
        let detail = match errors.as_slice() {
            [one] => one.message.clone(),
            many => format!(
                "{} problems need a change before this can be saved.",
                many.len()
            ),
        };
        Self::Problem(RewardProblem {
            code,
            status: StatusCode::UNPROCESSABLE_ENTITY,
            detail,
            errors,
        })
    }

    /// The code string (tests and logs).
    pub fn code(&self) -> String {
        match self {
            Self::Jobs(e) => e.code(),
            Self::Problem(p) => p.code.to_string(),
        }
    }
}

impl From<crate::db::DbError> for RewardsError {
    fn from(e: crate::db::DbError) -> Self {
        Self::Jobs(e.into())
    }
}

impl From<sqlx::Error> for RewardsError {
    fn from(e: sqlx::Error) -> Self {
        Self::Jobs(e.into())
    }
}

impl From<SeamError> for RewardsError {
    fn from(e: SeamError) -> Self {
        Self::Jobs(e.into())
    }
}

impl From<crate::error::ApiError> for RewardsError {
    fn from(e: crate::error::ApiError) -> Self {
        Self::Jobs(e.into())
    }
}

impl IntoResponse for RewardsError {
    fn into_response(self) -> Response {
        match self {
            Self::Jobs(e) => e.into_response(),
            Self::Problem(p) => {
                let mut res = p.status.into_response();
                res.extensions_mut().insert(Arc::new(p));
                res
            }
        }
    }
}

/// Route layer: renders [`RewardProblem`]s as `application/problem+json`.
pub async fn render(req: Request, next: Next) -> Response {
    let instance = req
        .extensions()
        .get::<OriginalUri>()
        .map_or_else(|| req.uri().path().to_owned(), |u| u.path().to_owned());
    let trace_id = req
        .extensions()
        .get::<RequestMeta>()
        .map(|m| m.trace_id.clone())
        .unwrap_or_default();
    let res = next.run(req).await;
    let Some(p) = res.extensions().get::<Arc<RewardProblem>>().cloned() else {
        return res;
    };
    let code = p.code.to_string();
    let problem = Problem {
        r#type: format!("{}{code}", crate::error::ERROR_TYPE_BASE),
        title: p.code.title().unwrap_or("Something went wrong.").to_owned(),
        status: p.status.as_u16(),
        detail: p.detail.clone(),
        instance,
        code,
        errors: p.errors.clone(),
        trace_id,
    };
    let (mut parts, _) = res.into_parts();
    parts.extensions.remove::<Arc<RewardProblem>>();
    parts.headers.remove(header::CONTENT_LENGTH);
    parts
        .headers
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(PROBLEM_JSON));
    let body = serde_json::to_vec(&problem).unwrap_or_else(|_| b"{}".to_vec());
    Response::from_parts(parts, Body::from(body))
}

/// One component the reward sandbox ships (a row of the generated list).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(as = RewardComponent)]
pub struct ComponentRow {
    /// `builtin/<name>@<major>`.
    #[serde(rename = "ref")]
    pub reference: String,
    /// Semver.
    pub version: String,
    /// `builtin` (Tier 1) or `code_exec` (Tier 2: runs model-written code).
    pub kind: String,
    /// Sandbox tier: 1 or 2.
    pub tier: u8,
    /// `scalar.sequence`.
    pub shape: String,
    /// Same input gives the same value.
    pub deterministic: bool,
    /// Keeps state between batches.
    pub stateful: bool,
    /// Beta: may change.
    pub beta: bool,
    /// Lowest and highest value.
    pub value_range: [f64; 2],
    /// Sample fields it reads, like `reference`.
    pub needs: Vec<String>,
    /// What it does.
    pub description: String,
    /// JSON Schema of its `params`.
    #[schema(value_type = Object)]
    pub param_schema: serde_json::Value,
    /// sha256 of its source file.
    pub code_sha256: String,
}

/// The generated component list (`components.json`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Catalog {
    /// `mrl-reward-components/1`.
    pub format: String,
    /// The reward SDK contract (`mrl-rewards/1`).
    pub sdk_contract: String,
    /// Identity of the executor environment (part of every content hash).
    pub executor_sha256: String,
    /// The components.
    pub components: Vec<ComponentRow>,
}

impl Catalog {
    /// The list compiled into this server.
    pub fn embedded() -> Result<Self, String> {
        let c: Self = serde_json::from_str(include_str!("components.json"))
            .map_err(|e| format!("components.json can't be read: {e}"))?;
        if c.format != "mrl-reward-components/1" {
            return Err(format!("components.json has format {}", c.format));
        }
        Ok(c)
    }

    /// The component with this reference.
    pub fn get(&self, reference: &str) -> Option<&ComponentRow> {
        self.components.iter().find(|c| c.reference == reference)
    }
}

/// The `rewards` module.
pub struct RewardsModule {
    db: Db,
    config: RewardsConfig,
    catalog: Result<Arc<Catalog>, Arc<str>>,
    store: Arc<crate::modules::artifacts::ArtifactStore>,
}

impl RewardsModule {
    /// The module for this build.
    pub fn new(db: Db, config: &crate::Config) -> Self {
        let store = crate::modules::artifacts::ArtifactsModule::new(db.clone(), config).store();
        Self::with_store(db, config.rewards.clone(), store)
    }

    /// The module with an explicit artifact store (tests and embedders). Registers the
    /// store and settings for the module's chain members ([`storage`]).
    pub fn with_store(
        db: Db,
        config: RewardsConfig,
        store: Arc<crate::modules::artifacts::ArtifactStore>,
    ) -> Self {
        storage::register(
            &db,
            storage::Shared {
                store: store.clone(),
                config: config.clone(),
            },
        );
        Self {
            db,
            config,
            catalog: Catalog::embedded().map(Arc::new).map_err(Arc::from),
            store,
        }
    }
}

impl ServerModule for RewardsModule {
    fn name(&self) -> &'static str {
        "rewards"
    }

    fn routes(&self) -> Router<AppState> {
        let catalog = self.catalog.clone().unwrap_or_else(|_| {
            Arc::new(Catalog {
                format: String::new(),
                sdk_contract: String::new(),
                executor_sha256: String::new(),
                components: Vec::new(),
            })
        });
        api::routes(
            self.db.clone(),
            self.config.clone(),
            catalog,
            self.store.clone(),
        )
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        api::openapi()
    }

    fn startup_steps(&self) -> Vec<Arc<dyn StartupStep>> {
        vec![Arc::new(SeedComponents {
            db: self.db.clone(),
            config: self.config.clone(),
            catalog: self.catalog.clone(),
        })]
    }
}

/// Startup step: checks the `[rewards]` settings and upserts the component list.
pub struct SeedComponents {
    db: Db,
    config: RewardsConfig,
    catalog: Result<Arc<Catalog>, Arc<str>>,
}

#[async_trait]
impl StartupStep for SeedComponents {
    fn name(&self) -> &str {
        "rewards.components"
    }

    async fn run(&self) -> Result<(), SeamError> {
        let problems = self.config.problems();
        if !problems.is_empty() {
            return Err(SeamError::new("rewards", problems.join("; ")));
        }
        let catalog = self
            .catalog
            .as_ref()
            .map_err(|e| SeamError::new("rewards", e.to_string()))?;
        store::seed_components(&self.db, catalog)
            .await
            .map_err(|e| SeamError::new("rewards", e.to_string()))?;
        tracing::info!(
            components = catalog.components.len(),
            "reward components registered"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_catalog_reads_and_has_the_tier2_components() {
        let c = Catalog::embedded().unwrap();
        assert_eq!(c.executor_sha256.len(), 64);
        let unit = c.get("builtin/code.unit_tests@1").unwrap();
        assert_eq!(
            (unit.tier, unit.kind.as_str(), unit.beta),
            (2, "code_exec", true)
        );
        assert_eq!(c.get("builtin/math.verify@1").unwrap().tier, 1);
    }

    #[test]
    fn settings_ranges_are_checked() {
        assert!(RewardsConfig::default().problems().is_empty());
        let bad = RewardsConfig {
            max_test_cases: 0,
            learnability_warn_share: 1.5,
            ..RewardsConfig::default()
        };
        assert_eq!(bad.problems().len(), 2, "{:?}", bad.problems());
    }

    #[test]
    fn every_code_is_registered_and_catalogued() {
        for c in [
            codes::LEARNED_UNAVAILABLE,
            codes::REPORT_UNREADABLE,
            codes::ILLEGAL_STEP,
            codes::NOT_PUBLISHABLE,
            codes::NOTHING_TO_CHECK,
            codes::NAME_TAKEN,
            codes::CASE_INVALID,
            codes::POLICY_INVALID,
            codes::COMPONENT_UNKNOWN,
            codes::PARAMS_INVALID,
            codes::BUNDLE_INVALID,
            codes::STEP_UNSUPPORTED,
        ] {
            assert!(c.title().is_some(), "{c} missing from mrl-domain");
            assert!(
                !crate::modules::jobs::error::hint_for(&c.to_string()).is_empty(),
                "{c} has no guidance steps"
            );
        }
    }
}

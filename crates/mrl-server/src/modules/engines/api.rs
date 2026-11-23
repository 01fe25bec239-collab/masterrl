//! REST endpoints under `/api/v1` (all need `engines.read`):
//!
//! - `GET /engines`: every listed engine with its versions, status per hardware class and methods.
//! - `GET /engines/{eng}/versions/{ver}`: one version with its manifest and probe results.
//! - `GET /capabilities?model_version=&dataset_version=&pool=`: for every live engine version
//!   and method, whether it can run with this model, dataset and the machines of the pool
//!   (`pool` also needs `compute.read` on it), and why not. The UI greys out what can't run.
//! - `GET /algorithms/{algo}/schema?engine_version=<name>@<version>`: the method's config
//!   form (JSON Schema with `x-mrl-ui` hints), UI schema, presets, pinned and default values.

use super::{store, RecordScope, Records};
use crate::db::Db;
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::seams::{RequestContext, Resource};
use crate::AppState;
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Extension, Json, Router};
use mrl_domain::capability::matcher::{self, Engine, HardwareRequest, Lookup, Request, Wanted};
use mrl_domain::ids::{DatasetVersionId, ModelVersionId, OrgId, PoolId, ProjectId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use utoipa::{IntoParams, OpenApi, ToSchema};
use uuid::Uuid;

/// One version in the engine list.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct EngineVersionSummary {
    /// Adapter version.
    pub version: String,
    /// PENDING_APPROVAL, PROBING, ENABLED, DISABLED or REVOKED.
    pub status: String,
    /// Probe outcome per hardware class (`cpu`, `nvidia`, …): ENABLED or FAILED.
    pub hardware: BTreeMap<String, String>,
    /// Method ids.
    pub algorithms: Vec<String>,
}

/// One engine.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct EngineSummary {
    /// Adapter name, like `trl`.
    pub name: String,
    /// `engine` (training) or `builtin` (utility jobs).
    pub kind: String,
    /// Shown in the app.
    pub display_name: String,
    /// Longer text.
    pub description: Option<String>,
    /// Its versions, oldest first.
    pub versions: Vec<EngineVersionSummary>,
}

/// `GET /engines`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct EngineList {
    /// Engines by name.
    pub items: Vec<EngineSummary>,
}

/// A method of one engine version.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct EngineAlgorithm {
    /// Method id.
    pub id: String,
    /// Shown in the app.
    pub display_name: String,
    /// One line.
    pub summary: String,
    /// stable, beta or experimental.
    pub maturity: String,
    /// full, partial or approximation.
    pub fidelity: String,
    /// Hardware classes with a passed probe.
    pub verified_hw: Vec<String>,
}

/// `GET /engines/{eng}/versions/{ver}`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct EngineVersionDetail {
    /// Adapter name.
    pub name: String,
    /// Version.
    pub version: String,
    /// Status.
    pub status: String,
    /// sha256 of the stored manifest.
    pub manifest_sha256: String,
    /// Lock hash or image of the trainer environment.
    pub runtime_ref: Option<String>,
    /// Probe results per hardware class.
    #[schema(value_type = Object)]
    pub probe_results: Value,
    /// The merged manifest.
    #[schema(value_type = Object)]
    pub manifest: Value,
    /// Its methods.
    pub algorithms: Vec<EngineAlgorithm>,
}

/// `GET /capabilities` query.
#[derive(Debug, Clone, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct CapabilitiesQuery {
    /// `mv_…`: check against this model.
    pub model_version: Option<String>,
    /// `dsv_…`: check against this training dataset.
    pub dataset_version: Option<String>,
    /// Project in which model and dataset versions must be visible.
    pub project: Option<String>,
    /// `pool_…`: only this pool's machines.
    pub pool: Option<String>,
}

/// One problem (same shape as a job rejection).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CapabilityProblem {
    /// Catalog code, like `MRL-CAP-018`.
    pub code: String,
    /// What it concerns, like `/resources`.
    pub path: Option<String>,
    /// What is wrong.
    pub message: String,
    /// What to do.
    pub hint: String,
}

/// One method of one engine version.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CapabilityItem {
    /// Engine name.
    pub engine: String,
    /// Engine version.
    pub version: String,
    /// Method id.
    pub algorithm: String,
    /// Shown in the app.
    pub display_name: String,
    /// stable, beta or experimental.
    pub maturity: String,
    /// True when nothing stops it (settings are checked when you submit).
    pub runnable: bool,
    /// Update strategies it offers on the available hardware.
    pub update_strategies: Vec<String>,
    /// Precisions it offers.
    pub precision: Vec<String>,
    /// Hardware classes where the engine passed its probe.
    pub enabled_hardware: Vec<String>,
    /// Why it can't run.
    pub problems: Vec<CapabilityProblem>,
}

/// `GET /capabilities`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CapabilityReport {
    /// Hardware classes of the connected machines considered.
    pub hardware: Vec<String>,
    /// Every method of every live engine version.
    pub items: Vec<CapabilityItem>,
}

/// `GET /algorithms/{algo}/schema` query.
#[derive(Debug, Clone, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct AlgorithmSchemaQuery {
    /// `<engine>@<version>`, like `trl@1.0.0`; default: the newest live version with the method.
    pub engine_version: Option<String>,
}

/// `GET /algorithms/{algo}/schema`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AlgorithmSchemaDoc {
    /// Engine name.
    pub engine: String,
    /// Engine version.
    pub version: String,
    /// Method id.
    pub algorithm: String,
    /// Shown in the app.
    pub display_name: String,
    /// One line.
    pub summary: String,
    /// stable, beta or experimental.
    pub maturity: String,
    /// full, partial or approximation.
    pub fidelity: String,
    /// Why fidelity is not full.
    pub fidelity_note: Option<String>,
    /// Pinned engine fields (read-only in the form).
    #[schema(value_type = Object)]
    pub switch: Value,
    /// Recommended values.
    #[schema(value_type = Object)]
    pub defaults: Value,
    /// JSON Schema for `config.engine` (with `x-mrl-ui` hints), if the method has a form.
    #[schema(value_type = Option<Object>)]
    pub config_schema: Option<Value>,
    /// UI hints file, if any.
    #[schema(value_type = Option<Object>)]
    pub ui_schema: Option<Value>,
    /// Presets: `{id, title, tier, when, patch}` (JSON Merge Patch).
    #[schema(value_type = Vec<Object>)]
    pub presets: Value,
}

#[derive(OpenApi)]
#[openapi(
    paths(list_engines, get_engine_version, get_capabilities, get_algorithm_schema),
    components(schemas(EngineList, EngineSummary, EngineVersionSummary, EngineVersionDetail,
        EngineAlgorithm, CapabilityReport, CapabilityItem, CapabilityProblem, AlgorithmSchemaDoc)),
    tags((name = "engines", description = "Training engines, their methods and what can run where."))
)]
struct Doc;

/// The OpenAPI fragment.
pub fn openapi() -> utoipa::openapi::OpenApi {
    Doc::openapi()
}

#[derive(Clone)]
struct Ctx {
    db: Db,
    records: Records,
}

/// The routes.
pub fn routes(db: Db, records: Records) -> Router<AppState> {
    Router::new()
        .route("/engines", get(list_engines))
        .route("/engines/{eng}/versions/{ver}", get(get_engine_version))
        .route("/capabilities", get(get_capabilities))
        .route("/algorithms/{algo}/schema", get(get_algorithm_schema))
        .layer(Extension(Ctx { db, records }))
}

fn internal(e: impl std::fmt::Display) -> ApiError {
    tracing::error!(error = %e, "engines request failed");
    ApiError::new(ErrorCode::Unavailable, "The database did not answer.")
}

fn field(path: &str, message: &str) -> ApiError {
    let mut e = ApiError::validation(vec![FieldError {
        path: path.to_owned(),
        code: ErrorCode::FieldValue.as_str().to_owned(),
        message: message.to_owned(),
        hint: message.to_owned(),
    }]);
    e.detail = message.to_owned();
    e
}

fn missing(what: &str) -> ApiError {
    ApiError::new(
        ErrorCode::NotFound,
        format!("No {what} with this name exists."),
    )
}

async fn read(s: &AppState, ctx: &RequestContext, id: Option<String>) -> Result<(), ApiError> {
    let resource = Resource {
        id,
        ..Resource::kind("engine")
    };
    s.authorize(ctx, "engines.read", &resource).await
}

fn caller_org(ctx: &RequestContext) -> Result<Option<Uuid>, ApiError> {
    ctx.org
        .as_deref()
        .map(|o| {
            OrgId::from_str(o).map(|o| o.as_uuid()).map_err(|_| {
                ApiError::new(
                    ErrorCode::Forbidden,
                    "Your account's organization can't be read.",
                )
            })
        })
        .transpose()
}

#[utoipa::path(get, path = "/engines", tag = "engines", operation_id = "list_engines",
    responses((status = 200, description = "Engines.", body = EngineList)))]
async fn list_engines(
    State(s): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
) -> Result<Json<EngineList>, ApiError> {
    read(&s, &ctx, None).await?;
    let rows = sqlx::query(
        "SELECT te.name, te.kind, ev.version, ev.status, ev.probe_results,
                ev.manifest #>> '{metadata,displayName}' AS display_name,
                ev.manifest #>> '{metadata,description}' AS description,
                ARRAY(SELECT algorithm_id FROM algorithm_definitions ad
                      WHERE ad.engine_version_id = ev.id ORDER BY algorithm_id) AS algorithms
         FROM training_engines te JOIN engine_versions ev ON ev.engine_id = te.id
         WHERE te.listed ORDER BY te.name, ev.id",
    )
    .fetch_all(c.db.pool())
    .await
    .map_err(internal)?;
    let mut items: Vec<EngineSummary> = Vec::new();
    for r in rows {
        let name: String = r.try_get("name").map_err(internal)?;
        let probes: Value = r.try_get("probe_results").map_err(internal)?;
        let version = EngineVersionSummary {
            version: r.try_get("version").map_err(internal)?,
            status: r.try_get("status").map_err(internal)?,
            hardware: hardware_status(&probes),
            algorithms: r.try_get("algorithms").map_err(internal)?,
        };
        match items.last_mut() {
            Some(e) if e.name == name => {
                e.display_name = r.try_get("display_name").map_err(internal)?;
                e.description = r.try_get("description").map_err(internal)?;
                e.versions.push(version);
            }
            _ => items.push(EngineSummary {
                name,
                kind: r.try_get("kind").map_err(internal)?,
                display_name: r.try_get("display_name").map_err(internal)?,
                description: r.try_get("description").map_err(internal)?,
                versions: vec![version],
            }),
        }
    }
    Ok(Json(EngineList { items }))
}

fn hardware_status(probes: &Value) -> BTreeMap<String, String> {
    probes
        .as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.get("status")?.as_str()?.to_owned())))
                .collect()
        })
        .unwrap_or_default()
}

#[utoipa::path(get, path = "/engines/{eng}/versions/{ver}", tag = "engines",
    operation_id = "get_engine_version",
    params(("eng" = String, Path, description = "Engine name, like `trl`."),
           ("ver" = String, Path, description = "Version, like `1.0.0`.")),
    responses((status = 200, description = "The version.", body = EngineVersionDetail),
              (status = 404, description = "No such engine version.", body = crate::error::Problem)))]
async fn get_engine_version(
    State(s): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path((eng, ver)): Path<(String, String)>,
) -> Result<Json<EngineVersionDetail>, ApiError> {
    read(&s, &ctx, Some(format!("{eng}@{ver}"))).await?;
    let row = sqlx::query(
        "SELECT ev.id, ev.status, ev.manifest_sha256, ev.runtime_ref, ev.probe_results, ev.manifest
         FROM training_engines te JOIN engine_versions ev ON ev.engine_id = te.id
         WHERE te.listed AND te.name = $1 AND ev.version = $2",
    )
    .bind(&eng)
    .bind(&ver)
    .fetch_optional(c.db.pool())
    .await
    .map_err(internal)?
    .ok_or_else(|| missing("engine version"))?;
    let id: Uuid = row.try_get("id").map_err(internal)?;
    let algorithms = sqlx::query(
        "SELECT algorithm_id, display_name, summary, maturity, fidelity, verified_hw
         FROM algorithm_definitions WHERE engine_version_id = $1 ORDER BY algorithm_id",
    )
    .bind(id)
    .fetch_all(c.db.pool())
    .await
    .map_err(internal)?
    .into_iter()
    .map(|r| {
        Ok(EngineAlgorithm {
            id: r.try_get("algorithm_id")?,
            display_name: r.try_get("display_name")?,
            summary: r.try_get("summary")?,
            maturity: r.try_get("maturity")?,
            fidelity: r.try_get("fidelity")?,
            verified_hw: r.try_get("verified_hw")?,
        })
    })
    .collect::<Result<Vec<_>, sqlx::Error>>()
    .map_err(internal)?;
    Ok(Json(EngineVersionDetail {
        name: eng,
        version: ver,
        status: row.try_get("status").map_err(internal)?,
        manifest_sha256: row.try_get("manifest_sha256").map_err(internal)?,
        runtime_ref: row.try_get("runtime_ref").map_err(internal)?,
        probe_results: serde_json::to_value(
            hardware_status(&row.try_get("probe_results").map_err(internal)?)
                .into_iter()
                .map(|(class, status)| (class, serde_json::json!({"status": status})))
                .collect::<BTreeMap<_, _>>(),
        )
        .map_err(internal)?,
        manifest: row.try_get("manifest").map_err(internal)?,
        algorithms,
    }))
}

#[utoipa::path(get, path = "/capabilities", tag = "engines", operation_id = "get_capabilities",
    params(CapabilitiesQuery),
    responses((status = 200, description = "What can run.", body = CapabilityReport),
              (status = 403, description = "The caller cannot read the requested compute or records.", body = crate::error::Problem),
              (status = 404, description = "No visible pool or project has this id.", body = crate::error::Problem),
              (status = 422, description = "An id can't be read.", body = crate::error::Problem)))]
async fn get_capabilities(
    State(s): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Query(q): Query<CapabilitiesQuery>,
) -> Result<Json<CapabilityReport>, ApiError> {
    let ctx = match q.project.as_deref() {
        Some(project) => ctx.with_project(
            ProjectId::from_str(project)
                .map_err(|_| missing("project"))?
                .to_string(),
        ),
        None => ctx,
    };
    read(&s, &ctx, None).await?;
    let org = caller_org(&ctx)?;
    let model = q
        .model_version
        .as_deref()
        .map(|m| {
            ModelVersionId::from_str(m)
                .map_err(|_| field("/model_version", "Use a model version id like `mv_…`."))
        })
        .transpose()?;
    let dataset = q
        .dataset_version
        .as_deref()
        .map(|d| {
            DatasetVersionId::from_str(d)
                .map_err(|_| field("/dataset_version", "Use a dataset version id like `dsv_…`."))
        })
        .transpose()?;
    let (pool, pool_owner) = match q.pool.as_deref() {
        None => {
            s.authorize(&ctx, "compute.read", &Resource::kind("pool"))
                .await?;
            (None, None)
        }
        Some(p) => {
            let gone = || ApiError::new(ErrorCode::NotFound, "No pool with this id exists.");
            let id = PoolId::from_str(p).map_err(|_| gone())?;
            let owner: Option<Option<Uuid>> =
                sqlx::query_scalar("SELECT org_id FROM compute_pools WHERE id = $1")
                    .bind(id.as_uuid())
                    .fetch_optional(c.db.pool())
                    .await
                    .map_err(internal)?;
            let owner = owner.ok_or_else(gone)?;
            let resource = Resource {
                id: Some(id.to_string()),
                org: owner.map(|o| OrgId::from_uuid(o).to_string()),
                ..Resource::kind("pool")
            };
            s.authorize(&ctx, "compute.read", &resource).await?;
            (Some(id.as_uuid()), owner)
        }
    };
    let scope = match q.project.as_deref() {
        Some(raw) => {
            let id = ProjectId::from_str(raw).map_err(|_| missing("project"))?;
            let owner: Option<Uuid> = sqlx::query_scalar(
                "SELECT org_id FROM projects WHERE id = $1 AND deleted_at IS NULL",
            )
            .bind(id.as_uuid())
            .fetch_optional(c.db.pool())
            .await
            .map_err(internal)?;
            let owner = owner.ok_or_else(|| missing("project"))?;
            let resource = Resource {
                id: Some(id.to_string()),
                org: Some(OrgId::from_uuid(owner).to_string()),
                project: Some(id.to_string()),
                ..Resource::kind("project")
            };
            if model.is_some() {
                s.authorize(&ctx, "models.read", &resource).await?;
            }
            if dataset.is_some() {
                s.authorize(&ctx, "datasets.read", &resource).await?;
            }
            if model.is_none() && dataset.is_none() {
                s.authorize(&ctx, "engines.read", &resource).await?;
            }
            RecordScope {
                org_id: owner,
                project_id: id.as_uuid(),
            }
        }
        None if model.is_some() || dataset.is_some() => {
            return Err(field(
                "/project",
                "Name the project these versions are for.",
            ));
        }
        None => RecordScope {
            org_id: org.unwrap_or_default(),
            project_id: Uuid::nil(),
        },
    };
    let model = match model {
        None => Lookup::NotGiven,
        Some(m) => match c.records.model(&scope, m).await.map_err(internal)? {
            Some(f) => Lookup::Found(f),
            None => Lookup::Missing,
        },
    };
    let datasets = match dataset {
        None => vec![],
        Some(d) => vec![
            match c.records.dataset(&scope, d).await.map_err(internal)? {
                Some(f) => Lookup::Found(f),
                None => Lookup::Missing,
            },
        ],
    };
    let org = q.project.as_ref().map_or(org, |_| Some(scope.org_id));
    if q.project.is_some() && pool_owner.is_some() && pool_owner != org {
        return Err(missing("pool"));
    }
    let machines = store::machines(&c.db, pool_owner.or(org), pool)
        .await
        .map_err(internal)?;
    let available: BTreeSet<_> = machines.iter().flat_map(|m| m.classes()).collect();
    let candidates = store::candidates(&c.db, None).await.map_err(internal)?;
    let mut items = Vec::new();
    for cand in &candidates {
        let caps = &cand.manifest.spec.capabilities;
        for algo in &cand.manifest.spec.algorithms {
            let req = Request {
                algorithm: algo.id.clone(),
                config: None,
                model: model.clone(),
                datasets: datasets.clone(),
                // A listing asks what could run; attaching a reward is the next step.
                reward_attached: true,
                hardware: HardwareRequest {
                    wanted: Wanted::Any,
                    gpus: 0,
                    nodes: 1,
                    gpu_mem_gib: None,
                },
                available: available.clone(),
                machines: Some(machines.clone()),
                resources: None,
                model_signals: BTreeMap::new(),
                rollout: false,
            };
            let problems = matcher::check(
                Engine {
                    manifest: &cand.manifest,
                    schemas: &cand.schemas,
                    enabled: &cand.enabled,
                },
                &req,
            );
            let engine = Engine {
                manifest: &cand.manifest,
                schemas: &cand.schemas,
                enabled: &cand.enabled,
            };
            let cuda = matcher::offers_qlora(engine, &req);
            let updates: Vec<String> = algo
                .supports
                .update_strategies
                .as_ref()
                .unwrap_or(&caps.update_strategies)
                .iter()
                .filter(|u| cuda || u.as_str() != "qlora")
                .cloned()
                .collect();
            items.push(CapabilityItem {
                engine: cand.name.clone(),
                version: cand.version.clone(),
                algorithm: algo.id.clone(),
                display_name: algo.display_name.clone(),
                maturity: algo.maturity.clone(),
                runnable: problems.is_empty(),
                update_strategies: updates,
                precision: caps.precision.clone(),
                enabled_hardware: cand.enabled.iter().map(|h| h.as_str().to_owned()).collect(),
                problems: problems
                    .into_iter()
                    .map(|p| {
                        let r = super::validator::rejection(p);
                        CapabilityProblem {
                            code: r.code,
                            path: r.path,
                            message: r.message,
                            hint: r.hint,
                        }
                    })
                    .collect(),
            });
        }
    }
    Ok(Json(CapabilityReport {
        hardware: available
            .iter()
            .map(|h| h.as_str().to_owned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        items,
    }))
}

#[utoipa::path(get, path = "/algorithms/{algo}/schema", tag = "engines",
    operation_id = "get_algorithm_schema",
    params(("algo" = String, Path, description = "Method id, like `sft`."), AlgorithmSchemaQuery),
    responses((status = 200, description = "The method's form.", body = AlgorithmSchemaDoc),
              (status = 404, description = "No live engine version has this method.", body = crate::error::Problem)))]
async fn get_algorithm_schema(
    State(s): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(algo): Path<String>,
    Query(q): Query<AlgorithmSchemaQuery>,
) -> Result<Json<AlgorithmSchemaDoc>, ApiError> {
    read(&s, &ctx, None).await?;
    let (name, version) = match q.engine_version.as_deref() {
        None => (None, None),
        Some(ev) => match ev.split_once('@') {
            Some((n, v)) if !n.is_empty() && !v.is_empty() => {
                (Some(n.to_owned()), Some(v.to_owned()))
            }
            _ => {
                return Err(field(
                    "/engine_version",
                    "Use the engine name and version joined by @, like `trl@1.0.0`.",
                ))
            }
        },
    };
    let row = sqlx::query(
        "SELECT te.name, ev.version, ad.display_name, ad.summary, ad.maturity, ad.fidelity,
                ad.fidelity_note, ad.switch, ad.defaults, ad.config_schema, ad.ui_schema, ad.presets
         FROM algorithm_definitions ad
         JOIN engine_versions ev ON ev.id = ad.engine_version_id
         JOIN training_engines te ON te.id = ev.engine_id
         WHERE te.listed AND ev.status IN ('PROBING', 'ENABLED') AND ad.algorithm_id = $1
           AND ($2::text IS NULL OR te.name = $2) AND ($3::text IS NULL OR ev.version = $3)
         ORDER BY ev.status = 'ENABLED' DESC, ev.id DESC LIMIT 1",
    )
    .bind(&algo)
    .bind(&name)
    .bind(&version)
    .fetch_optional(c.db.pool())
    .await
    .map_err(internal)?
    .ok_or_else(|| missing("method in a live engine version"))?;
    let get = |k: &str| row.try_get::<Value, _>(k).map_err(internal);
    Ok(Json(AlgorithmSchemaDoc {
        engine: row.try_get("name").map_err(internal)?,
        version: row.try_get("version").map_err(internal)?,
        algorithm: algo,
        display_name: row.try_get("display_name").map_err(internal)?,
        summary: row.try_get("summary").map_err(internal)?,
        maturity: row.try_get("maturity").map_err(internal)?,
        fidelity: row.try_get("fidelity").map_err(internal)?,
        fidelity_note: row.try_get("fidelity_note").map_err(internal)?,
        switch: get("switch")?,
        defaults: get("defaults")?,
        config_schema: row.try_get("config_schema").map_err(internal)?,
        ui_schema: row.try_get("ui_schema").map_err(internal)?,
        presets: get("presets")?,
    }))
}

//! `POST /projects/{prj}/runs:validate` and `POST /runs:preflight`.
//!
//! Validate is a dry run of submit with no side effects the user sees (no run, no job in
//! the project's runs): the same resolution, the L0 validator chain, the L1 probe on a
//! connected machine (60 s), the estimate and the Guardian preflight.

use super::codes;
use super::probe::{self, NewProbe, Outcome, ProbeSpec};
use super::resolve::{self, Resolved};
use super::{policy, Ctx};
use crate::app::AppState;
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::modules::estimates::{self, RunEstimate, Verdict};
use crate::modules::jobs::{scope, JobSpec, JobsError, Rejection, ResolveError, Validation};
use crate::modules::preflight::{self, PreflightInput, RunPreflight};
use crate::modules::runs::api::{default_resources, RunSubmission};
use crate::seams::RequestContext;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::{Extension, Json};
use mrl_domain::ids::{NodeId, ProjectId, RunId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

/// How serious an issue is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum IssueSeverity {
    /// The run can't start like this.
    Error,
    /// The run can start; read this first.
    Warning,
    /// Good to know.
    Info,
}

/// One finding (Doc 09 §6.5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ValidateIssue {
    /// `error`, `warning` or `info`.
    pub severity: IssueSeverity,
    /// Guidance code, like `MRL-CAP-250`.
    pub code: String,
    /// JSON Pointer into the submission, like `/config/engine/optim`.
    pub path: Option<String>,
    /// What was found, in plain words.
    pub message: String,
    /// What to do.
    pub hint: String,
}

impl ValidateIssue {
    fn of(r: Rejection, severity: IssueSeverity) -> Self {
        Self {
            severity,
            code: r.code,
            path: r.path,
            message: r.message,
            hint: r.hint,
        }
    }

    fn new(
        severity: IssueSeverity,
        code: mrl_domain::errors::ErrorCode,
        path: Option<&str>,
        message: impl Into<String>,
    ) -> Self {
        Self::of(Rejection::new(code, path, message), severity)
    }
}

/// `POST /projects/{prj}/runs:validate` answer (Doc 09 §6.5).
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct ValidateResponse {
    /// True when no issue is an error: submitting this would queue the run.
    pub ok: bool,
    /// Every finding: the app's checks (L0), the machine's (L1) and the preflight.
    pub issues: Vec<ValidateIssue>,
    /// The config the run would get (preset, settings, defaults, and any safety settings
    /// the preflight applied). Submit this to get the same run.
    #[schema(value_type = Object)]
    pub resolved_config: Value,
    /// Memory estimate with its verdict and confidence.
    pub estimate: RunEstimate,
    /// The Guardian preflight; absent when the settings have errors.
    pub preflight: Option<RunPreflight>,
    /// The engine version the run would use (`engine_versions.id`).
    pub engine_version_id: Option<String>,
    /// The preset applied, if any.
    pub preset: Option<String>,
    /// How far the checks went: `l0` (the app's) or `l1` (also on a machine).
    pub checked: String,
}

/// `POST /runs:preflight` body.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PreflightRequest {
    /// Project, `prj_…`.
    pub project_id: String,
    /// The run, as for submit.
    #[schema(value_type = Object)]
    pub run: Value,
}

fn bad_body(detail: impl Into<String>) -> JobsError {
    ApiError::new(ErrorCode::BadBody, detail).into()
}

fn read_submission(v: &Value) -> Result<RunSubmission, JobsError> {
    scope::no_nul(v)?;
    serde_json::from_value(v.clone())
        .map_err(|e| bad_body(format!("The run could not be read: {e}.")))
}

fn ids<T: std::fmt::Display>(v: &[T]) -> Vec<String> {
    v.iter().map(ToString::to_string).collect()
}

/// Runs every check for `sub` in `(org, project)` on behalf of `who`.
pub async fn check(
    state: &AppState,
    ctx: &Ctx,
    req: &RequestContext,
    org: Uuid,
    project: Uuid,
    sub: &RunSubmission,
) -> Result<ValidateResponse, JobsError> {
    if let Some(checkpoint) = sub.parent_checkpoint_id {
        let source: Option<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT r.id, r.project_id FROM checkpoints c JOIN runs r ON r.id = c.run_id
             WHERE c.id = $1 AND c.org_id = $2 AND r.org_id = $2",
        )
        .bind(checkpoint.as_uuid())
        .bind(org)
        .fetch_optional(ctx.db.pool())
        .await?;
        let (run, source_project) = source.ok_or_else(|| JobsError::not_found("checkpoint"))?;
        let resource = scope::resource(
            "run",
            Some(RunId::from_uuid(run).to_string()),
            org,
            source_project,
        );
        scope::authorize(state, req, "checkpoints.read", &resource)
            .await
            .map_err(|err| match err {
                JobsError::Api(a) if a.code == ErrorCode::NotFound => {
                    JobsError::not_found("checkpoint")
                }
                other => other,
            })?;
    }
    let mut issues = Vec::new();
    let resources = sub.resources.clone().unwrap_or_else(default_resources);
    let unchecked = |issues: Vec<ValidateIssue>, config: Value, why: &str| ValidateResponse {
        ok: false,
        issues,
        resolved_config: config,
        estimate: RunEstimate::unknown(why),
        preflight: None,
        engine_version_id: None,
        preset: None,
        checked: "l0".into(),
    };
    let mut spec = JobSpec {
        org_id: org,
        project_id: project,
        kind: serde_json::to_value(sub.kind)
            .ok()
            .and_then(|k| k.as_str().map(str::to_owned))
            .unwrap_or_else(|| "TRAIN".into()),
        algorithm_id: sub.algorithm_id.clone(),
        base_model_version_id: sub.base_model_version_id.as_uuid(),
        reward_version_id: sub.reward_version_id.map(|r| r.as_uuid()),
        dataset_version_ids: sub
            .dataset_version_ids
            .iter()
            .map(|d| d.as_uuid())
            .collect(),
        resolved: sub.config.clone(),
        resources: resources.clone(),
        max_runtime_seconds: sub.max_runtime_seconds,
    };
    let picker = Some((ctx.capability.as_ref(), &spec));
    let Resolved {
        config,
        method,
        preset,
    } = match resolve::resolve(&ctx.db, Some(&sub.algorithm_id), &sub.config, picker).await {
        Ok(r) => r,
        Err(ResolveError::Invalid(fields)) => {
            let issues = fields
                .into_iter()
                .map(|f| ValidateIssue {
                    severity: IssueSeverity::Error,
                    code: f.code,
                    path: Some(f.path),
                    message: f.message,
                    hint: f.hint,
                })
                .collect();
            return Ok(unchecked(
                issues,
                sub.config.clone(),
                "Fix the settings first; then the memory can be estimated.",
            ));
        }
        Err(ResolveError::Seam(s)) => return Err(JobsError::Seam(s)),
    };
    spec.resolved = config.clone();
    let engine = match ctx.chains.validate(&spec).await? {
        Validation::Accepted(engine) => engine,
        Validation::Rejected(problems) => {
            // The submit-time preflight reads an older estimate; the one below decides.
            issues.extend(
                problems
                    .into_iter()
                    .filter(|p| p.code != codes::DOES_NOT_FIT.to_string())
                    .map(|p| ValidateIssue::of(p, IssueSeverity::Error)),
            );
            None
        }
    };
    let engine = engine.or(method.as_ref().map(|m| m.engine_version_id));
    if let (Some(ckpt), Some(engine)) = (sub.parent_checkpoint_id, engine) {
        if let Some(why) =
            policy::resume_problem(&ctx.db, org, ckpt.as_uuid(), engine, &sub.algorithm_id).await?
        {
            issues.push(ValidateIssue::new(
                IssueSeverity::Error,
                codes::RESUME_INCOMPATIBLE,
                Some("/parent_checkpoint_id"),
                why,
            ));
        }
    }
    if issues.iter().any(|i| i.severity == IssueSeverity::Error) {
        let mut out = unchecked(
            issues,
            config,
            "Fix the problems above first; then the memory can be estimated.",
        );
        out.preset = preset;
        out.engine_version_id = engine.map(|e| e.to_string());
        return Ok(out);
    }
    // L1: a dry run on a connected machine.
    let mut checked = "l0";
    let mut ok = true;
    let mut estimate = RunEstimate::unknown("No machine checked these settings.");
    let all = preflight::machines(&ctx.db, org, None).await?;
    let eligible: Vec<NodeId> = all
        .iter()
        .map(|m| NodeId::from_uuid(m.node_id))
        .filter(|node| {
            resources
                .roles
                .iter()
                .all(|r| r.node_ids.is_empty() || r.node_ids.contains(node))
        })
        .collect();
    match (&method, engine) {
        (Some(m), Some(_)) if !m.has_l1() => {
            issues.push(ValidateIssue::new(
                IssueSeverity::Info,
                codes::NO_L1,
                None,
                format!(
                    "{} checks its settings only when the run starts, so these were checked by the app alone.",
                    m.engine_ref()
                ),
            ));
        }
        (Some(_), Some(_)) if eligible.is_empty() => {
            issues.push(ValidateIssue::new(
                IssueSeverity::Info,
                codes::CHECKED_WITHOUT_MACHINE,
                None,
                "No machine is connected, so the settings were checked by the app alone and the memory is not estimated.",
            ));
        }
        (Some(m), Some(engine)) => {
            let inputs_sha256 = estimates::inputs_sha256(&spec);
            let spec = ProbeSpec {
                algorithm: sub.algorithm_id.clone(),
                base_model_version_id: sub.base_model_version_id.to_string(),
                dataset_version_ids: ids(&sub.dataset_version_ids),
                reward_version_id: sub.reward_version_id.map(|r| r.to_string()),
                target_resources: Some(resources.clone()),
                seeds: sub.seeds.clone().unwrap_or_else(|| json!({"global": 0})),
                resolved: config.clone(),
            };
            let budget = Duration::from_secs(ctx.config.probe_timeout_seconds);
            let (id, job) = probe::start(
                &ctx.db,
                &NewProbe {
                    org,
                    project,
                    created_by: &req.principal.id,
                    spec: &spec,
                    engine_version_id: engine,
                    estimator_version: m.engine_ref(),
                    calibration_key: estimates::calibration_key(
                        &m.engine_ref(),
                        &sub.algorithm_id,
                        &config,
                        "unknown",
                    ),
                    inputs_sha256,
                    node_ids: eligible,
                    timeout_seconds: budget.as_secs(),
                    max_runtime_seconds: i64::try_from(budget.as_secs() * 2).unwrap_or(120),
                },
            )
            .await?;
            match probe::wait(&ctx.db, id, job, budget).await? {
                Outcome::Answered { ok: worker_ok, issues: found } => {
                    checked = "l1";
                    ok = worker_ok;
                    issues.extend(
                        found
                            .into_iter()
                            .filter_map(|i| serde_json::from_value::<ValidateIssue>(i).ok()),
                    );
                    if !worker_ok && !issues.iter().any(|i| i.severity == IssueSeverity::Error) {
                        issues.push(ValidateIssue::new(
                            IssueSeverity::Error,
                            crate::modules::jobs::codes::FIELD_VALUE,
                            None,
                            "The machine could not check these settings. Check the worker environment and try again.",
                        ));
                    }
                    let row = if worker_ok { estimates::ready(&ctx.db, id).await? } else { None };
                    if let Some(row) = row {
                        estimate = RunEstimate::of(&row);
                    }
                }
                Outcome::Failed(why) => issues.push(ValidateIssue::new(
                    IssueSeverity::Info,
                    codes::CHECKED_WITHOUT_MACHINE,
                    None,
                    format!("The check on a machine did not finish ({why}), so the memory is not estimated."),
                )),
                Outcome::TimedOut => issues.push(ValidateIssue::new(
                    IssueSeverity::Info,
                    codes::CHECKED_WITHOUT_MACHINE,
                    None,
                    format!(
                        "No machine answered within {} seconds, so the settings were checked by the app alone and the memory is not estimated.",
                        budget.as_secs()
                    ),
                )),
            }
        }
        _ => {}
    }
    // The Guardian preflight.
    let input = PreflightInput {
        resources: &resources,
        estimate: estimate.per_gpu_memory_gib,
        config: &config,
        schema: method.as_ref().and_then(|m| m.schema.as_ref()),
        supports_qlora: method.as_ref().is_some_and(resolve::Method::supports_qlora),
        probe_steps: ctx.config.probe_steps,
    };
    let (mut pre, mut adjusted) = preflight::decide(&input, &all, &ctx.config.headroom);
    if !pre.changes.is_empty() {
        let mut adjusted_spec = spec;
        adjusted_spec.resolved = adjusted.clone();
        if matches!(
            ctx.chains.validate(&adjusted_spec).await?,
            Validation::Rejected(_)
        ) {
            adjusted = config;
            pre.changes.clear();
            pre.message = "The fit is tight. The automatic changes did not pass validation.".into();
        } else {
            estimate = RunEstimate::unknown(
                "The machine estimated the settings before the safety changes.",
            );
        }
    }
    if pre.changes.is_empty() {
        estimate.verdict = pre.verdict;
    }
    match pre.verdict {
        Verdict::DoesNotFit => {
            let mut issue = ValidateIssue::new(
                IssueSeverity::Error,
                codes::DOES_NOT_FIT,
                Some("/resources"),
                pre.message.clone(),
            );
            if !pre.alternatives.is_empty() {
                issue.hint = pre
                    .alternatives
                    .iter()
                    .map(|a| a.message.as_str())
                    .collect::<Vec<_>>()
                    .join(" ");
            }
            issues.push(issue);
        }
        Verdict::Tight => issues.push(ValidateIssue::new(
            IssueSeverity::Warning,
            codes::SAFETY_SETTINGS,
            None,
            pre.message.clone(),
        )),
        Verdict::Fits | Verdict::Unknown => {}
    }
    let ok = ok && !issues.iter().any(|i| i.severity == IssueSeverity::Error);
    Ok(ValidateResponse {
        ok,
        issues,
        resolved_config: adjusted,
        estimate,
        preflight: Some(pre),
        engine_version_id: engine.map(|e| e.to_string()),
        preset,
        checked: checked.into(),
    })
}

/// `POST /projects/{prj}/runs:validate`.
#[utoipa::path(post, path = "/projects/{prj}/runs:validate", operation_id = "validate_run",
    tag = "runs",
    params(("prj" = String, Path, description = "Project id, `prj_…`.")),
    request_body = RunSubmission,
    responses((status = 200, description = "Every finding, the resolved config, the estimate and the preflight.", body = ValidateResponse),
              (status = 400, description = "An unreadable body.", body = crate::error::Problem),
              (status = 404, description = "No such project, or hidden from you.", body = crate::error::Problem)))]
pub async fn validate(
    State(state): State<AppState>,
    Extension(ctx): Extension<Ctx>,
    req: RequestContext,
    Path(prj): Path<String>,
    body: Bytes,
) -> Result<Json<ValidateResponse>, JobsError> {
    let (org, project) = crate::modules::runs::experiments::project_scope(
        &state,
        &req,
        &ctx.projects,
        &prj,
        "runs.submit",
    )
    .await?;
    let v: Value = serde_json::from_slice(&body)
        .map_err(|e| bad_body(format!("The body is not JSON: {e}.")))?;
    let sub = read_submission(&v)?;
    Ok(Json(check(&state, &ctx, &req, org, project, &sub).await?))
}

/// `POST /runs:preflight`: will this run fit the connected machines?
#[utoipa::path(post, path = "/runs:preflight", operation_id = "preflight_run", tag = "runs",
    request_body = PreflightRequest,
    responses((status = 200, description = "The verdict per machine, safety settings or alternatives.", body = RunPreflight),
              (status = 404, description = "No such project, or hidden from you.", body = crate::error::Problem),
              (status = 422, description = "The run's settings have problems (see `runs:validate`).", body = crate::error::Problem)))]
pub async fn preflight_route(
    State(state): State<AppState>,
    Extension(ctx): Extension<Ctx>,
    req: RequestContext,
    body: Bytes,
) -> Result<Json<RunPreflight>, JobsError> {
    let v: Value = serde_json::from_slice(&body)
        .map_err(|e| bad_body(format!("The body is not JSON: {e}.")))?;
    scope::no_nul(&v)?;
    let r: PreflightRequest = serde_json::from_value(v)
        .map_err(|e| bad_body(format!("The request could not be read: {e}.")))?;
    let _: ProjectId = scope::parse_id(&r.project_id, "project")?;
    let (org, project) = crate::modules::runs::experiments::project_scope(
        &state,
        &req,
        &ctx.projects,
        &r.project_id,
        "runs.submit",
    )
    .await?;
    let sub = read_submission(&r.run)?;
    let out = check(&state, &ctx, &req, org, project, &sub).await?;
    match out.preflight {
        Some(p)
            if out.ok
                || (p.verdict == Verdict::DoesNotFit
                    && out
                        .issues
                        .iter()
                        .filter(|i| i.severity == IssueSeverity::Error)
                        .all(|i| i.code == codes::DOES_NOT_FIT.to_string())) =>
        {
            Ok(Json(p))
        }
        _ => Err(ApiError::validation(
            out.issues
                .into_iter()
                .filter(|i| i.severity == IssueSeverity::Error)
                .map(|i| FieldError {
                    path: i.path.unwrap_or_else(|| "/run".into()),
                    code: i.code,
                    message: i.message,
                    hint: i.hint,
                })
                .collect(),
        )
        .into()),
    }
}

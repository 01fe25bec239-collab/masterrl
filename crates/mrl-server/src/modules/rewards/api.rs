//! REST endpoints under `/api/v1` (Doc 09 §6.3 Rewards). Every route authorizes on the
//! reward's org and project: other orgs' and missing projects or rewards answer 404.
//!
//! | Route | Action |
//! |---|---|
//! | `GET /projects/{prj}/rewards` | `rewards.read` |
//! | `POST /projects/{prj}/rewards` (`{name, description?, version?}`) | `rewards.write` |
//! | `GET /rewards/{rwd}` | `rewards.read` |
//! | `POST /rewards/{rwd}/versions` | `rewards.write` |
//! | `GET /reward-versions/{rwdv}` | `rewards.read` |
//! | `POST /reward-versions/{rwdv}:publish` | `rewards.write` |
//! | `POST /reward-versions/{rwdv}:deprecate` | `rewards.write` |
//! | `GET /reward-components` | `rewards.read` |
//!
//! The checks (`:test`, `:preview`, `:regression`, `POST /rewards:compare` and the reports
//! list) are in [`super::check_api`].

use super::hash::{bundle_sha256, HashInputs, CONTENT_FORMAT};
use super::policy::{self, CaseSample, Expect};
use super::store::{self, CaseRow, DefinitionRow, NewVersion, VersionRow};
use super::{codes, Catalog, ComponentRow, RewardsConfig, RewardsError};
use crate::app::AppState;
use crate::db::Db;
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::http::pagination::{Page, PageQuery};
use crate::modules::artifacts::ArtifactStore;
use crate::modules::jobs::{scope, JobsError, Projects};
use crate::seams::{RequestContext, Resource};
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use mrl_domain::ids::{ArtifactId, ProjectId, RewardId, RewardVersionId};
use mrl_domain::state::RewardVersionEvent;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use time::OffsetDateTime;
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

/// `POST /projects/{prj}/rewards`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = RewardCreateRequest)]
pub struct CreateReward {
    /// Unique in the project: lowercase letters, digits, `.`, `_`, `-` (1–100).
    pub name: String,
    /// What it rewards.
    #[serde(default)]
    pub description: String,
    /// The first version, if any.
    pub version: Option<NewVersionBody>,
}

/// `POST /rewards/{rwd}/versions` (and `version` of a new reward).
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = RewardVersionCreateRequest)]
pub struct NewVersionBody {
    /// RewardPolicy v1 (`apiVersion: mrl.dev/reward/v1`).
    #[schema(value_type = Object)]
    pub policy: Value,
    /// `art_…`: a committed artifact with the code bundle (`components.yaml` and `.py`
    /// files at its top level). Needed for `org/` and `hub/` components.
    #[serde(default)]
    pub code_artifact_id: Option<String>,
    /// Test cases: `{name, sample: {prompt, completion, reference?, completion_token_count?,
    /// finish_reason?}, expect: {total?, components?, flags?, absent_flags?, resolution?}}`.
    #[serde(default)]
    #[schema(value_type = Vec<Object>)]
    pub test_cases: Vec<Value>,
    /// Notes shown with the version.
    #[serde(default)]
    pub notes: String,
}

/// A version in lists.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[schema(as = RewardVersionSummary)]
pub struct VersionSummary {
    /// `rwdv_…`.
    pub id: String,
    /// 1, 2, …
    pub version_no: i32,
    /// DRAFT, TESTING, PUBLISHED, FAILED_TESTS or DEPRECATED.
    pub state: String,
    /// Content hash.
    pub content_sha256: String,
    /// `standard` or `code_exec`.
    pub sandbox_tier: String,
    /// When it was made.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
}

/// A reward definition.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[schema(as = RewardDefinition)]
pub struct RewardView {
    /// `rwd_…`.
    pub id: String,
    /// `prj_…`.
    pub project_id: String,
    /// Name.
    pub name: String,
    /// Description.
    pub description: String,
    /// Who made it.
    pub created_by: String,
    /// When.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
    /// Versions, newest first.
    pub versions: Vec<VersionSummary>,
}

/// Code bundle of a version.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[schema(as = RewardCodeBundle)]
pub struct CodeView {
    /// `art_…`.
    pub artifact_id: String,
    /// sha256 over its `file <path> <sha256>` lines.
    pub bundle_sha256: String,
    /// `[{path, sha256, size}]`.
    #[schema(value_type = Vec<Object>)]
    pub files: Value,
}

/// One test case.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[schema(as = RewardTestCase)]
pub struct CaseView {
    /// Order.
    pub position: i32,
    /// Name.
    pub name: String,
    /// The sample.
    pub sample: CaseSample,
    /// What must come out.
    pub expect: Expect,
    /// The latest test run's outcome for it.
    #[schema(value_type = Option<Object>)]
    pub last_result: Option<Value>,
}

/// Whether `:publish` would work, and why not.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[schema(as = RewardPublishCheck)]
pub struct Publishable {
    /// True when nothing blocks it.
    pub ok: bool,
    /// What blocks it.
    pub reasons: Vec<String>,
}

/// A reward version.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[schema(as = RewardVersion)]
pub struct VersionView {
    /// `rwdv_…`.
    pub id: String,
    /// `rwd_…`.
    pub reward_id: String,
    /// `prj_…`.
    pub project_id: String,
    /// 1, 2, …
    pub version_no: i32,
    /// State.
    pub state: String,
    /// sha256 of the canonical JSON of `hash_inputs`.
    pub content_sha256: String,
    /// Exactly what the hash covers.
    #[schema(value_type = Object)]
    pub hash_inputs: Value,
    /// `standard` or `code_exec`.
    pub sandbox_tier: String,
    /// The policy.
    #[schema(value_type = Object)]
    pub policy: Value,
    /// The code bundle, if any.
    pub code: Option<CodeView>,
    /// Model versions the policy reads.
    pub model_refs: Vec<String>,
    /// Notes.
    pub notes: String,
    /// Last test run's summary.
    #[schema(value_type = Option<Object>)]
    pub test_summary: Option<Value>,
    /// Regression report against the previous published version.
    #[schema(value_type = Option<Object>)]
    pub regression: Option<Value>,
    /// The bundle hash the sandbox reported in the passing test run.
    pub exec_sha256: Option<String>,
    /// Whether it can be published now.
    pub publishable: Publishable,
    /// Test cases.
    pub test_cases: Vec<CaseView>,
    /// Who made it.
    pub created_by: String,
    /// When.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
    /// When it was published.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub published_at: Option<OffsetDateTime>,
    /// When it was deprecated.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub deprecated_at: Option<OffsetDateTime>,
}

/// A component with whether it can be used now.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[schema(as = RewardComponentInfo)]
pub struct ComponentInfo {
    /// The component.
    #[serde(flatten)]
    pub component: ComponentRow,
    /// True unless it is no longer shipped by this server.
    pub listed: bool,
}

/// `GET /reward-components`.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[schema(as = RewardComponentList)]
pub struct ComponentList {
    /// The reward SDK contract.
    pub sdk_contract: String,
    /// Identity of the executor environment (part of every content hash).
    pub executor_sha256: String,
    /// Components, by reference.
    pub items: Vec<ComponentInfo>,
}

#[derive(OpenApi)]
#[openapi(
    paths(list_rewards, create_reward, get_reward, create_version, get_version, publish_version,
          deprecate_version, list_components, super::check_api::test_version,
          super::check_api::preview_version, super::check_api::regression_version,
          super::check_api::compare_versions, super::check_api::list_reports),
    components(schemas(CreateReward, NewVersionBody, VersionSummary, RewardView, CodeView, CaseView,
        Publishable, VersionView, ComponentInfo, ComponentList, ComponentRow, CaseSample, Expect,
        super::check_api::PreviewBody, super::check_api::CompareBody,
        super::check_api::ReportView)),
    tags((name = "rewards", description = "Reward Studio: reward definitions, versions, tests and publishing."))
)]
struct Doc;

/// The OpenAPI fragment.
pub fn openapi() -> utoipa::openapi::OpenApi {
    Doc::openapi()
}

/// Shared state of the routes.
#[derive(Clone)]
pub struct Ctx {
    /// The database.
    pub db: Db,
    /// Settings.
    pub config: RewardsConfig,
    /// The component list.
    pub catalog: Arc<Catalog>,
    /// Which org owns a project.
    pub projects: Projects,
    /// Artifacts (code bundles).
    pub store: Arc<ArtifactStore>,
}

/// The routes.
pub fn routes(
    db: Db,
    config: RewardsConfig,
    catalog: Arc<Catalog>,
    store: Arc<ArtifactStore>,
) -> Router<AppState> {
    let projects = Projects::registered(&db);
    Router::new()
        .route(
            "/projects/{prj}/rewards",
            get(list_rewards).post(create_reward),
        )
        .route("/rewards/{rwd}", get(get_reward))
        .route(
            "/rewards/{rwd}/versions",
            axum::routing::post(create_version),
        )
        .route(
            "/reward-versions/{seg}",
            get(get_version).post(version_action),
        )
        .route(
            "/reward-versions/{rwdv}/reports",
            get(super::check_api::list_reports),
        )
        .route(
            "/rewards:compare",
            axum::routing::post(super::check_api::compare_versions),
        )
        .route("/reward-components", get(list_components))
        .layer(axum::middleware::from_fn(super::render))
        .layer(axum::middleware::from_fn(
            crate::modules::jobs::error::render,
        ))
        .layer(Extension(Ctx {
            db,
            config,
            catalog,
            projects,
            store,
        }))
}

/// The JSON body (no NUL characters), or 422.
pub(super) fn body<T: for<'de> Deserialize<'de>>(bytes: &Bytes) -> Result<T, RewardsError> {
    let raw: Value = serde_json::from_slice(bytes)
        .map_err(|e| bad_body(format!("The body is not JSON: {e}.")))?;
    scope::no_nul(&raw)?;
    serde_json::from_value(raw).map_err(|e| bad_body(format!("The body could not be read: {e}.")))
}

fn bad_body(message: String) -> RewardsError {
    ApiError::validation(vec![FieldError::new(
        "",
        ErrorCode::FieldValue,
        message,
        "Fix the request body and send it again.",
    )])
    .into()
}

fn rid(id: Uuid) -> String {
    RewardId::from_uuid(id).to_string()
}

/// `rwdv_…` of `id`.
pub(super) fn vid(id: Uuid) -> String {
    RewardVersionId::from_uuid(id).to_string()
}

fn summary(v: &VersionRow) -> VersionSummary {
    VersionSummary {
        id: vid(v.id),
        version_no: v.version_no,
        state: v.state.clone(),
        content_sha256: v.content_sha256.clone(),
        sandbox_tier: v.sandbox_tier.clone(),
        created_at: v.created_at,
    }
}

fn reward_view(d: &DefinitionRow, versions: &[VersionRow]) -> RewardView {
    RewardView {
        id: rid(d.id),
        project_id: ProjectId::from_uuid(d.project_id).to_string(),
        name: d.name.clone(),
        description: d.description.clone(),
        created_by: d.created_by.clone(),
        created_at: d.created_at,
        versions: versions.iter().map(summary).collect(),
    }
}

/// The full view of `v` (`previous`: the newest other PUBLISHED version of its reward).
pub fn version_view(
    v: &VersionRow,
    cases: &[CaseRow],
    previous: Option<&VersionRow>,
) -> VersionView {
    let reasons = store::publish_blockers(v, previous);
    VersionView {
        id: vid(v.id),
        reward_id: rid(v.reward_id),
        project_id: ProjectId::from_uuid(v.project_id).to_string(),
        version_no: v.version_no,
        state: v.state.clone(),
        content_sha256: v.content_sha256.clone(),
        hash_inputs: v.hash_inputs.clone(),
        sandbox_tier: v.sandbox_tier.clone(),
        policy: v.policy.clone(),
        code: v
            .code_artifact_id
            .zip(v.code_bundle_sha256.clone())
            .map(|(a, b)| CodeView {
                artifact_id: ArtifactId::from_uuid(a).to_string(),
                bundle_sha256: b,
                files: v.code_files.clone(),
            }),
        model_refs: v
            .model_refs
            .iter()
            .map(|m| mrl_domain::ids::ModelVersionId::from_uuid(*m).to_string())
            .collect(),
        notes: v.notes.clone(),
        test_summary: v.test_summary.clone(),
        regression: v.regression.clone(),
        exec_sha256: v.exec_sha256.clone(),
        publishable: Publishable {
            ok: reasons.is_empty(),
            reasons,
        },
        test_cases: cases
            .iter()
            .filter_map(|c| {
                Some(CaseView {
                    position: c.position,
                    name: c.name.clone(),
                    sample: serde_json::from_value(c.sample.clone()).ok()?,
                    expect: serde_json::from_value(c.expect.clone()).ok()?,
                    last_result: c.last_result.clone(),
                })
            })
            .collect(),
        created_by: v.created_by.clone(),
        created_at: v.created_at,
        published_at: v.published_at,
        deprecated_at: v.deprecated_at,
    }
}

/// The project's org, authorized for `action` (missing or hidden project: 404).
async fn project_scope(
    state: &AppState,
    ctx: &RequestContext,
    c: &Ctx,
    prj: &str,
    action: &str,
) -> Result<(Uuid, Uuid), RewardsError> {
    let project: ProjectId = scope::parse_id(prj, "project")?;
    let org = c.projects.org_for(ctx, project).await?;
    let res = scope::resource("reward", None, org, project.as_uuid());
    scope::authorize(state, ctx, action, &res).await?;
    Ok((org, project.as_uuid()))
}

/// The definition, authorized for `action` (missing or hidden: 404).
async fn read_definition(
    state: &AppState,
    ctx: &RequestContext,
    c: &Ctx,
    rwd: &str,
    action: &str,
) -> Result<DefinitionRow, RewardsError> {
    let id: RewardId = scope::parse_id(rwd, "reward")?;
    let d = store::definition(&c.db, id.as_uuid())
        .await?
        .ok_or_else(|| JobsError::not_found("reward"))?;
    let res = scope::resource("reward", Some(rid(d.id)), d.org_id, d.project_id);
    scope::authorize(state, ctx, action, &res).await?;
    Ok(d)
}

/// The version, authorized for `action` on its reward (missing or hidden: 404).
pub(super) async fn read_version(
    state: &AppState,
    ctx: &RequestContext,
    c: &Ctx,
    rwdv: &str,
    action: &str,
) -> Result<VersionRow, RewardsError> {
    let id: RewardVersionId = scope::parse_id(rwdv, "reward version")?;
    let v = store::version(&c.db, id.as_uuid())
        .await?
        .ok_or_else(|| JobsError::not_found("reward version"))?;
    let res = scope::resource("reward", Some(rid(v.reward_id)), v.org_id, v.project_id);
    scope::authorize(state, ctx, action, &res)
        .await
        .map_err(|e| match e {
            JobsError::Api(a) if a.code == ErrorCode::NotFound => {
                JobsError::not_found("reward version")
            }
            other => other,
        })?;
    Ok(v)
}

/// Gets an artifact after applying the shared authorization response and project scope.
pub(super) async fn project_artifact(
    state: &AppState,
    ctx: &RequestContext,
    c: &Ctx,
    (org, project): (Uuid, Uuid),
    id: ArtifactId,
) -> Result<crate::modules::artifacts::Artifact, RewardsError> {
    let org_id = scope::org_text(org);
    let storage_org = org_id
        .as_deref()
        .unwrap_or(crate::modules::artifacts::LOCAL_ORG);
    let artifact = c
        .store
        .get(storage_org, id.as_uuid())
        .await
        .map_err(|e| RewardsError::from(crate::seams::SeamError::new("artifacts", e.to_string())))?
        .ok_or_else(|| JobsError::not_found("artifact"))?;
    let mut resource = artifact.resource(storage_org);
    resource.org = org_id;
    if artifact
        .project_id
        .as_deref()
        .is_some_and(|p| p != ProjectId::from_uuid(project).to_string())
    {
        return Err(JobsError::not_found("artifact").into());
    }
    scope::authorize(state, ctx, "lineage.read", &resource).await?;
    Ok(artifact)
}

/// Checks the code bundle artifact the caller names: visible to them (P19's `lineage.read`
/// on it), committed, in the same project (or org-wide), and only top-level `.py` files plus
/// `components.yaml`, within the size limits. Returns `(id, bundle hash, files)`.
async fn code_bundle(
    state: &AppState,
    ctx: &RequestContext,
    c: &Ctx,
    org: Uuid,
    project: Uuid,
    raw: &str,
) -> Result<(Uuid, String, Value), RewardsError> {
    let field = |message: String| {
        RewardsError::fields(vec![FieldError {
            path: "/code_artifact_id".into(),
            code: codes::BUNDLE_INVALID.to_string(),
            message,
            hint: "Upload the bundle as an artifact of this project, then name it here.".into(),
        }])
    };
    let id: ArtifactId = raw
        .parse()
        .map_err(|_| field("This is not an artifact id (art_…).".into()))?;
    let a = project_artifact(state, ctx, c, (org, project), id).await?;
    if a.state != "COMMITTED" {
        return Err(field(format!(
            "The artifact is {}; use a committed one.",
            a.state
        )));
    }
    let files = a
        .manifest
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if files.is_empty() || files.len() > c.config.max_code_files as usize {
        return Err(field(format!(
            "A code bundle has 1–{} files (this one has {}).",
            c.config.max_code_files,
            files.len()
        )));
    }
    let mut total: u64 = 0;
    let mut listed = Vec::new();
    let mut has_index = false;
    for f in &files {
        let path = f.get("path").and_then(Value::as_str).unwrap_or_default();
        let sha = f.get("sha256").and_then(Value::as_str).unwrap_or_default();
        let size = f.get("size").and_then(Value::as_u64).unwrap_or(0);
        let top_level_py = !path.contains('/')
            && path.ends_with(".py")
            && path
                .trim_end_matches(".py")
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            && !path.starts_with(|ch: char| ch.is_ascii_digit());
        has_index |= path == "components.yaml";
        if !(top_level_py || path == "components.yaml") {
            return Err(field(format!(
                "`{path}` can't be in a code bundle: use components.yaml and top-level Python files named like `my_reward.py`."
            )));
        }
        total = total.saturating_add(size);
        listed.push((path.to_owned(), sha.to_owned(), size));
    }
    if !has_index {
        return Err(field(
            "The bundle needs a components.yaml that lists its components.".into(),
        ));
    }
    if total > c.config.max_code_bytes {
        return Err(field(format!(
            "The bundle is {total} bytes; the limit is {} bytes.",
            c.config.max_code_bytes
        )));
    }
    let bundle = bundle_sha256(listed.iter().map(|(p, s, _)| (p.as_str(), s.as_str())));
    listed.sort();
    let files = json!(listed
        .iter()
        .map(|(p, s, n)| json!({"path": p, "sha256": s, "size": n}))
        .collect::<Vec<_>>());
    Ok((id.as_uuid(), bundle, files))
}

/// Checks a version body and builds what is stored.
async fn new_version(
    state: &AppState,
    ctx: &RequestContext,
    c: &Ctx,
    (org, project): (Uuid, Uuid),
    b: NewVersionBody,
) -> Result<NewVersion, RewardsError> {
    if b.notes.chars().count() > 2000 {
        return Err(bad_body("notes can be at most 2000 characters.".into()));
    }
    let code = match b.code_artifact_id.as_deref() {
        Some(raw) => Some(code_bundle(state, ctx, c, org, project, raw).await?),
        None => None,
    };
    let checked = policy::check(
        &b.policy,
        &c.catalog,
        code.as_ref().map(|(_, h, _)| h.as_str()),
    )
    .map_err(RewardsError::fields)?;
    let cases = policy::parse_cases(
        &b.test_cases,
        &checked.component_ids,
        c.config.max_test_cases as usize,
    )
    .map_err(RewardsError::fields)?;
    let inputs = HashInputs {
        format: CONTENT_FORMAT.into(),
        sdk_contract: c.catalog.sdk_contract.clone(),
        policy: b.policy.clone(),
        test_cases: serde_json::to_value(&cases)
            .map_err(|e| bad_body(format!("The test cases could not be saved: {e}.")))?,
        components: checked.components.clone(),
        code_bundle_sha256: code.as_ref().map(|(_, h, _)| h.clone()),
        executor_sha256: c.catalog.executor_sha256.clone(),
        model_refs: checked.model_refs.clone(),
        judge_templates: Vec::new(),
    };
    let model_refs = checked
        .model_refs
        .iter()
        .filter_map(|m| m.parse::<mrl_domain::ids::ModelVersionId>().ok())
        .map(|m| m.as_uuid())
        .collect();
    Ok(NewVersion {
        policy: b.policy,
        inputs,
        sandbox_tier: checked.sandbox_tier,
        code,
        model_refs,
        notes: b.notes,
        cases,
    })
}

fn check_name(name: &str) -> Result<(), RewardsError> {
    let ok = (1..=100).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
        && name
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphanumeric())
        && name
            .bytes()
            .last()
            .is_some_and(|b| b.is_ascii_alphanumeric());
    if ok {
        Ok(())
    } else {
        Err(ApiError::validation(vec![FieldError::new(
            "/name",
            ErrorCode::FieldValue,
            "Use 1–100 lowercase letters, digits, `.`, `_` or `-`, starting and ending with a letter or digit.",
            "For example: math-correctness.",
        )])
        .into())
    }
}

#[utoipa::path(get, path = "/projects/{prj}/rewards", tag = "rewards", operation_id = "list_rewards",
    params(("prj" = String, Path, description = "Project id, `prj_…`."), PageQuery),
    responses((status = 200, description = "The project's rewards, newest first.", body = Page<RewardView>),
              (status = 404, description = "No such project.", body = crate::error::Problem)))]
async fn list_rewards(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(prj): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<RewardView>>, RewardsError> {
    let (org, project) = project_scope(&state, &ctx, &c, &prj, "rewards.read").await?;
    let page = q.resolve()?;
    let after = scope::after::<RewardId>(&page, |r| r.as_uuid())?;
    let defs = sqlx::query_as::<_, DefinitionRow>(
        "SELECT id, org_id, project_id, name, description, next_version, created_by, created_at
         FROM reward_definitions WHERE org_id = $1 AND project_id = $2
           AND ($3::uuid IS NULL OR id < $3) ORDER BY id DESC LIMIT $4",
    )
    .bind(org)
    .bind(project)
    .bind(after)
    .bind(page.fetch() as i64)
    .fetch_all(c.db.pool())
    .await?;
    let (defs, next_cursor) = scope::cut(defs, &page, |d| rid(d.id));
    let mut items = Vec::with_capacity(defs.len());
    for d in &defs {
        let versions = store::versions_of(&c.db, d.id).await?;
        items.push(reward_view(d, &versions));
    }
    Ok(Json(Page { items, next_cursor }))
}

#[utoipa::path(post, path = "/projects/{prj}/rewards", tag = "rewards", operation_id = "create_reward",
    params(("prj" = String, Path, description = "Project id, `prj_…`.")),
    request_body = CreateReward,
    responses((status = 201, description = "The reward (with its first version when one was given).", body = RewardView),
              (status = 404, description = "No such project.", body = crate::error::Problem),
              (status = 409, description = "The name is taken (MRL-CONFLICT-290).", body = crate::error::Problem),
              (status = 422, description = "The policy or test cases need a change (MRL-RWD-*, MRL-VAL-290).", body = crate::error::Problem)))]
async fn create_reward(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(prj): Path<String>,
    raw: Bytes,
) -> Result<Response, RewardsError> {
    let scope_ids = project_scope(&state, &ctx, &c, &prj, "rewards.write").await?;
    let b: CreateReward = body(&raw)?;
    check_name(&b.name)?;
    if b.description.chars().count() > 2000 {
        return Err(bad_body(
            "description can be at most 2000 characters.".into(),
        ));
    }
    let first = match b.version {
        Some(v) => Some(new_version(&state, &ctx, &c, scope_ids, v).await?),
        None => None,
    };
    let actor = ctx.principal.id.clone();
    let mut tx = c.db.begin().await?;
    let def = store::create_definition(
        &mut tx,
        scope_ids.0,
        scope_ids.1,
        &b.name,
        &b.description,
        &actor,
    )
    .await?;
    scope::audit(
        &mut tx,
        &state,
        &ctx,
        scope_ids,
        "rewards.create",
        rid(def.id),
    )
    .await?;
    let mut versions = Vec::new();
    if let Some(new) = first {
        let (v, _) = store::create_version(&mut tx, &def, new, &actor).await?;
        scope::audit(
            &mut tx,
            &state,
            &ctx,
            scope_ids,
            "rewards.version_create",
            vid(v.id),
        )
        .await?;
        versions.push(v);
    }
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok((StatusCode::CREATED, Json(reward_view(&def, &versions))).into_response())
}

#[utoipa::path(get, path = "/rewards/{rwd}", tag = "rewards", operation_id = "get_reward",
    params(("rwd" = String, Path, description = "Reward id, `rwd_…`.")),
    responses((status = 200, description = "The reward and its versions.", body = RewardView),
              (status = 404, description = "No such reward.", body = crate::error::Problem)))]
async fn get_reward(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(rwd): Path<String>,
) -> Result<Json<RewardView>, RewardsError> {
    let d = read_definition(&state, &ctx, &c, &rwd, "rewards.read").await?;
    let versions = store::versions_of(&c.db, d.id).await?;
    Ok(Json(reward_view(&d, &versions)))
}

#[utoipa::path(post, path = "/rewards/{rwd}/versions", tag = "rewards", operation_id = "create_reward_version",
    params(("rwd" = String, Path, description = "Reward id, `rwd_…`.")),
    request_body = NewVersionBody,
    responses((status = 201, description = "A new DRAFT version.", body = VersionView),
              (status = 200, description = "A version with the same content already exists; it is returned.", body = VersionView),
              (status = 404, description = "No such reward.", body = crate::error::Problem),
              (status = 422, description = "The policy or test cases need a change (MRL-RWD-*, MRL-VAL-290).", body = crate::error::Problem)))]
async fn create_version(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(rwd): Path<String>,
    raw: Bytes,
) -> Result<Response, RewardsError> {
    let d = read_definition(&state, &ctx, &c, &rwd, "rewards.write").await?;
    let b: NewVersionBody = body(&raw)?;
    let scope_ids = (d.org_id, d.project_id);
    let new = new_version(&state, &ctx, &c, scope_ids, b).await?;
    let mut tx = c.db.begin().await?;
    let def = store::lock_definition(&mut tx, d.id)
        .await?
        .ok_or_else(|| JobsError::not_found("reward"))?;
    let (v, created) = store::create_version(&mut tx, &def, new, &ctx.principal.id).await?;
    if created {
        scope::audit(
            &mut tx,
            &state,
            &ctx,
            scope_ids,
            "rewards.version_create",
            vid(v.id),
        )
        .await?;
    }
    let previous = store::latest_published(&mut tx, v.reward_id, v.id).await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    let cases = store::cases(&c.db, v.id).await?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(version_view(&v, &cases, previous.as_ref()))).into_response())
}

async fn view_of(c: &Ctx, v: &VersionRow) -> Result<VersionView, RewardsError> {
    let cases = store::cases(&c.db, v.id).await?;
    let mut tx = c.db.begin().await?;
    let previous = store::latest_published(&mut tx, v.reward_id, v.id).await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(version_view(v, &cases, previous.as_ref()))
}

#[utoipa::path(get, path = "/reward-versions/{rwdv}", tag = "rewards", operation_id = "get_reward_version",
    params(("rwdv" = String, Path, description = "Reward version id, `rwdv_…`.")),
    responses((status = 200, description = "The version with its test cases and results.", body = VersionView),
              (status = 404, description = "No such reward version.", body = crate::error::Problem)))]
async fn get_version(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(seg): Path<String>,
) -> Result<Json<VersionView>, RewardsError> {
    let v = read_version(&state, &ctx, &c, &seg, "rewards.read").await?;
    Ok(Json(view_of(&c, &v).await?))
}

/// `POST /reward-versions/{rwdv}:<action>` (axum sees one segment).
async fn version_action(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(seg): Path<String>,
    headers: HeaderMap,
    raw: Bytes,
) -> Result<Response, RewardsError> {
    use super::check_api as checks;
    let (s, e) = (State(state), Extension(c));
    match seg.split_once(':') {
        Some((id, "publish")) => Ok(publish_version(s, e, ctx, Path(id.to_owned()))
            .await?
            .into_response()),
        Some((id, "deprecate")) => Ok(deprecate_version(s, e, ctx, Path(id.to_owned()))
            .await?
            .into_response()),
        Some((id, "test")) => checks::test_version(s, e, ctx, headers, Path(id.to_owned())).await,
        Some((id, "preview")) => {
            checks::preview_version(s, e, ctx, headers, Path(id.to_owned()), raw).await
        }
        Some((id, "regression")) => {
            checks::regression_version(s, e, ctx, headers, Path(id.to_owned())).await
        }
        _ => Err(ApiError::new(ErrorCode::RouteNotFound, "No endpoint has this path.").into()),
    }
}

#[utoipa::path(post, path = "/reward-versions/{rwdv}:publish", tag = "rewards", operation_id = "publish_reward_version",
    params(("rwdv" = String, Path, description = "Reward version id, `rwdv_…`.")),
    responses((status = 200, description = "Published (or already published).", body = VersionView),
              (status = 404, description = "No such reward version.", body = crate::error::Problem),
              (status = 409, description = "Its checks haven't passed (MRL-STATE-291) or its state doesn't allow it (MRL-STATE-290).", body = crate::error::Problem)))]
async fn publish_version(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(rwdv): Path<String>,
) -> Result<Json<VersionView>, RewardsError> {
    let found = read_version(&state, &ctx, &c, &rwdv, "rewards.write").await?;
    let mut tx = c.db.begin().await?;
    store::lock_definition(&mut tx, found.reward_id).await?;
    let v = store::lock_version(&mut tx, found.id)
        .await?
        .ok_or_else(|| JobsError::not_found("reward version"))?;
    if v.state == "PUBLISHED" {
        tx.commit().await.map_err(crate::db::DbError::from)?;
        return Ok(Json(view_of(&c, &v).await?));
    }
    let previous = store::latest_published(&mut tx, v.reward_id, v.id).await?;
    let blockers = store::publish_blockers(&v, previous.as_ref());
    if !blockers.is_empty() {
        let code = if v.state == "DEPRECATED" {
            codes::ILLEGAL_STEP
        } else {
            codes::NOT_PUBLISHABLE
        };
        return Err(RewardsError::problem(
            code,
            StatusCode::CONFLICT,
            blockers.join(" "),
        ));
    }
    store::freeze_golden_samples(&mut tx, &v).await?;
    let v = store::step(&mut tx, &v, RewardVersionEvent::Publish, json!({})).await?;
    scope::audit(
        &mut tx,
        &state,
        &ctx,
        (v.org_id, v.project_id),
        "rewards.publish",
        vid(v.id),
    )
    .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(Json(view_of(&c, &v).await?))
}

#[utoipa::path(post, path = "/reward-versions/{rwdv}:deprecate", tag = "rewards", operation_id = "deprecate_reward_version",
    params(("rwdv" = String, Path, description = "Reward version id, `rwdv_…`.")),
    responses((status = 200, description = "Deprecated (or already deprecated): new runs can't use it.", body = VersionView),
              (status = 404, description = "No such reward version.", body = crate::error::Problem),
              (status = 409, description = "Only a published version can be deprecated (MRL-STATE-290).", body = crate::error::Problem)))]
async fn deprecate_version(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(rwdv): Path<String>,
) -> Result<Json<VersionView>, RewardsError> {
    let found = read_version(&state, &ctx, &c, &rwdv, "rewards.write").await?;
    let mut tx = c.db.begin().await?;
    let v = store::lock_version(&mut tx, found.id)
        .await?
        .ok_or_else(|| JobsError::not_found("reward version"))?;
    if v.state == "DEPRECATED" {
        tx.commit().await.map_err(crate::db::DbError::from)?;
        return Ok(Json(view_of(&c, &v).await?));
    }
    let v = store::step(&mut tx, &v, RewardVersionEvent::Deprecate, json!({})).await?;
    scope::audit(
        &mut tx,
        &state,
        &ctx,
        (v.org_id, v.project_id),
        "rewards.deprecate",
        vid(v.id),
    )
    .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(Json(view_of(&c, &v).await?))
}

#[utoipa::path(get, path = "/reward-components", tag = "rewards", operation_id = "list_reward_components",
    responses((status = 200, description = "Components the reward sandbox ships.", body = ComponentList)))]
async fn list_components(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
) -> Result<Json<ComponentList>, RewardsError> {
    let res = Resource {
        org: ctx.org.clone(),
        ..Resource::kind("reward_component")
    };
    state.authorize(&ctx, "rewards.read", &res).await?;
    let listed = store::listed_components(&c.db).await?;
    Ok(Json(ComponentList {
        sdk_contract: c.catalog.sdk_contract.clone(),
        executor_sha256: c.catalog.executor_sha256.clone(),
        items: c
            .catalog
            .components
            .iter()
            .map(|row| ComponentInfo {
                component: row.clone(),
                listed: listed.contains(&row.reference),
            })
            .collect(),
    }))
}

//! `/projects/{prj}/models`, `/models/{mdl}` and `/model-versions/{mv}`.
//!
//! Every route authorizes on the model's org and project: a hidden, foreign or missing model
//! or version is the same 404. Reads need `models.read`; creating models and acknowledging
//! licenses need `models.import`; approving a version's custom code needs `models.import`
//! and `promotions.approve` (maintainers and owners). Changes are audited in their
//! transaction.

use super::store::{self, hex, model_cols, version_cols, ModelRow, VersionRow};
use super::{codes, gates, Models};
use crate::app::AppState;
use crate::error::{ApiError, ErrorCode};
use crate::http::pagination::{Page, PageQuery};
use crate::modules::jobs::{scope, JobsError};
use crate::modules::runs::experiments::project_scope;
use crate::seams::RequestContext;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use mrl_domain::ids::{JobId, ModelId, ModelVersionId, ProjectId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::OffsetDateTime;
use uuid::Uuid;

/// A model as the API shows it.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ModelInfo {
    /// `mdl_…`.
    pub id: String,
    /// `prj_…`.
    pub project_id: String,
    /// Unique name in the project.
    pub name: String,
    /// Family of the newest READY version.
    pub family: Option<String>,
    /// Who created it.
    pub created_by: String,
    /// When.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
}

fn model_view(m: &ModelRow) -> ModelInfo {
    ModelInfo {
        id: ModelId::from_uuid(m.id).to_string(),
        project_id: ProjectId::from_uuid(m.project_id).to_string(),
        name: m.name.clone(),
        family: m.family.clone(),
        created_by: m.created_by.clone(),
        created_at: m.created_at,
    }
}

/// `POST /projects/{prj}/models` body.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NewModel {
    /// 1–128 letters, digits, dots, dashes or underscores.
    pub name: String,
}

/// A version's license and whether its project accepted it.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ModelLicense {
    /// SPDX id, like `apache-2.0`.
    pub spdx: Option<String>,
    /// `spdx`, `custom` or `unknown`.
    pub kind: Option<String>,
    /// sha256 of the license text; `:acknowledge-license` names it.
    pub sha256: Option<String>,
    /// The project accepted exactly this text.
    pub acknowledged: bool,
}

/// One file of a version.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ModelFileInfo {
    /// Path inside the model folder.
    pub path: String,
    /// `weights`, `config`, `tokenizer`, `adapter`, `code` or `other`.
    pub role: String,
    /// `safetensors`, `gguf`, `json`, `txt` or `other`.
    pub format: String,
    /// Bytes.
    pub size: i64,
    /// sha256 (hex).
    pub sha256: String,
}

/// A model version as the API shows it.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ModelVersionInfo {
    /// `mv_…`.
    pub id: String,
    /// `mdl_…`.
    pub model_id: String,
    /// 1, 2, … within the model.
    pub seq: i32,
    /// `IMPORTING`, `READY`, `INVALID` or `ARCHIVED`.
    pub state: String,
    /// `EXPERIMENTAL`, `CANDIDATE`, `APPROVED` or `ARCHIVED`.
    pub stage: String,
    /// `full_weights`, `adapter` or `inference_only` (once inspected).
    pub kind: Option<String>,
    /// `hub`, `upload`, `node_path`, `checkpoint` or `merge`.
    pub source_kind: String,
    /// Where it came from (`commit` is the pinned Hub revision).
    #[schema(value_type = Object)]
    pub source_ref: serde_json::Value,
    /// The revision approvals name.
    pub revision: Option<String>,
    /// The base of an adapter or merge.
    pub base_model_version_id: Option<String>,
    /// `decoder`, `decoder-moe`, `encoder`, `encoder-decoder`, `vlm` or `unknown`.
    pub family: Option<String>,
    /// `architectures[0]`.
    pub architecture: Option<String>,
    /// `model_type`.
    pub model_type: Option<String>,
    /// Parameters.
    pub param_count: Option<i64>,
    /// Weight dtypes.
    pub dtypes: Vec<String>,
    /// Context length.
    pub context_length: Option<i32>,
    /// Quantization, if any.
    pub quantization: Option<String>,
    /// sha256 of the default chat template.
    pub chat_template_sha256: Option<String>,
    /// The hash runs record as `base_model_sha256`.
    pub weights_sha256: Option<String>,
    /// Ships its own Python code.
    pub requires_remote_code: bool,
    /// The code is approved for this revision (or none is needed).
    pub remote_code_approved: bool,
    /// Who approved it.
    pub remote_code_approved_by: Option<String>,
    /// License.
    pub license: ModelLicense,
    /// Per engine version and method: `{ok, problems[]}`.
    #[schema(value_type = Object)]
    pub compatibility: serde_json::Value,
    /// The inspection report (P15), once inspected.
    #[schema(value_type = Option<Object>)]
    pub inspection: Option<serde_json::Value>,
    /// Why it is INVALID: `{code, message}`.
    #[schema(value_type = Option<Object>)]
    pub invalid_reason: Option<serde_json::Value>,
    /// `job_…` that inspects or builds it.
    pub job_id: Option<String>,
    /// Who created it.
    pub created_by: String,
    /// When.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
    /// When it became READY.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub ready_at: Option<OffsetDateTime>,
    /// Its files (only on `GET /model-versions/{mv}`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<ModelFileInfo>>,
}

/// The API view of `v`; `acknowledged` is the project's accepted license hash.
pub fn version_view(
    v: &VersionRow,
    acknowledged: Option<&[u8]>,
    files: Option<&[store::FileRow]>,
) -> ModelVersionInfo {
    ModelVersionInfo {
        id: ModelVersionId::from_uuid(v.id).to_string(),
        model_id: ModelId::from_uuid(v.model_id).to_string(),
        seq: v.seq,
        state: v.state.clone(),
        stage: v.stage.clone(),
        kind: v.kind.clone(),
        source_kind: v.source_kind.clone(),
        source_ref: v.source_ref.clone(),
        revision: v.revision(),
        base_model_version_id: v
            .base_model_version_id
            .map(|b| ModelVersionId::from_uuid(b).to_string()),
        family: v.family.clone(),
        architecture: v.architecture.clone(),
        model_type: v.model_type.clone(),
        param_count: v.param_count,
        dtypes: v.dtypes.clone(),
        context_length: v.context_length,
        quantization: v.quantization.clone(),
        chat_template_sha256: v.chat_template_sha256.as_deref().map(hex),
        weights_sha256: v.weights_sha256.as_deref().map(hex),
        requires_remote_code: v.requires_remote_code,
        remote_code_approved: v.code_approved(),
        remote_code_approved_by: v.remote_code_approved_by.clone(),
        license: ModelLicense {
            spdx: v.license_spdx.clone(),
            kind: v.license_kind.clone(),
            sha256: v.license_sha256.as_deref().map(hex),
            acknowledged: v.license_sha256.is_some() && acknowledged == v.license_sha256.as_deref(),
        },
        compatibility: v.compatibility.clone(),
        inspection: v.inspection.clone(),
        invalid_reason: v.invalid_reason.clone(),
        job_id: v.job_id.map(|j| JobId::from_uuid(j).to_string()),
        created_by: v.created_by.clone(),
        created_at: v.created_at,
        ready_at: v.ready_at,
        files: files.map(|f| {
            f.iter()
                .map(|f| ModelFileInfo {
                    path: f.path.clone(),
                    role: f.role.clone(),
                    format: f.format.clone(),
                    size: f.size_bytes,
                    sha256: hex(&f.sha256),
                })
                .collect()
        }),
    }
}

fn check_name(name: &str) -> Result<(), JobsError> {
    let ok = (1..=128).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(ApiError::validation(vec![crate::error::FieldError::new(
            "/name",
            ErrorCode::FieldValue,
            "Names have 1 to 128 letters, digits, dots, dashes or underscores.",
            "Change the name and send the request again.",
        )])
        .into())
    }
}

/// The model `id`, authorized for `action` on its org and project (missing or hidden: 404).
pub async fn authorized_model(
    state: &AppState,
    models: &Models,
    ctx: &RequestContext,
    id: Uuid,
    action: &str,
) -> Result<ModelRow, JobsError> {
    let m = store::model(&models.db, id)
        .await?
        .ok_or_else(|| JobsError::not_found("model"))?;
    let res = scope::resource(
        "model",
        Some(ModelId::from_uuid(id).to_string()),
        m.org_id,
        m.project_id,
    );
    let ctx = ctx
        .clone()
        .with_project(ProjectId::from_uuid(m.project_id).to_string());
    scope::authorize(state, &ctx, action, &res)
        .await
        .map_err(|e| hide(e, "model"))?;
    Ok(m)
}

/// The version `mv`, authorized for each of `actions` on its model (missing or hidden: 404).
pub async fn authorized_version(
    state: &AppState,
    models: &Models,
    ctx: &RequestContext,
    mv: &str,
    actions: &[&str],
) -> Result<VersionRow, JobsError> {
    let id: ModelVersionId = scope::parse_id(mv, "model version")?;
    let v = store::version(&models.db, id.as_uuid())
        .await?
        .ok_or_else(|| JobsError::not_found("model version"))?;
    for action in actions {
        authorized_model(state, models, ctx, v.model_id, action)
            .await
            .map_err(|e| hide(e, "model version"))?;
    }
    Ok(v)
}

fn hide(e: JobsError, kind: &str) -> JobsError {
    match e {
        JobsError::Api(a) if a.code == ErrorCode::NotFound => JobsError::not_found(kind),
        other => other,
    }
}

/// `GET /projects/{prj}/models`.
#[utoipa::path(get, path = "/projects/{prj}/models", operation_id = "list_models", tag = "models",
    params(("prj" = String, Path, description = "Project id, `prj_…`."), PageQuery),
    responses((status = 200, description = "The project's models, newest first.", body = Page<ModelInfo>),
              (status = 404, description = "No such project.", body = crate::error::Problem)))]
pub async fn list_models(
    State(state): State<AppState>,
    Extension(models): Extension<Arc<Models>>,
    ctx: RequestContext,
    Path(prj): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<ModelInfo>>, JobsError> {
    let (org, project) = project_scope(&state, &ctx, &models.projects, &prj, "models.read").await?;
    let page = q.resolve()?;
    let after = scope::after::<ModelId>(&page, |m| m.as_uuid())?;
    let rows: Vec<ModelRow> = sqlx::query_as(concat!(
        "SELECT ",
        model_cols!(),
        " FROM models WHERE org_id = $1 AND project_id = $2
           AND ($3::uuid IS NULL OR id < $3) ORDER BY id DESC LIMIT $4"
    ))
    .bind(org)
    .bind(project)
    .bind(after)
    .bind(page.fetch() as i64)
    .fetch_all(models.db.pool())
    .await
    .map_err(crate::db::DbError::from)?;
    let (rows, next) = scope::cut(rows, &page, |m| ModelId::from_uuid(m.id).to_string());
    Ok(Json(Page {
        items: rows.iter().map(model_view).collect(),
        next_cursor: next,
    }))
}

/// `POST /projects/{prj}/models`: a model with no versions yet (or the existing one of that
/// name).
#[utoipa::path(post, path = "/projects/{prj}/models", operation_id = "create_model", tag = "models",
    params(("prj" = String, Path, description = "Project id, `prj_…`.")),
    request_body = NewModel,
    responses((status = 201, description = "The model (also when it already existed).", body = ModelInfo),
              (status = 404, description = "No such project.", body = crate::error::Problem)))]
pub async fn create_model(
    State(state): State<AppState>,
    Extension(models): Extension<Arc<Models>>,
    ctx: RequestContext,
    Path(prj): Path<String>,
    Json(body): Json<NewModel>,
) -> Result<(StatusCode, Json<ModelInfo>), JobsError> {
    check_name(&body.name)?;
    let (org, project) =
        project_scope(&state, &ctx, &models.projects, &prj, "models.import").await?;
    let mut tx = models.db.begin().await?;
    let m =
        store::get_or_create_model(&mut tx, org, project, &body.name, &ctx.principal.id).await?;
    scope::audit(
        &mut tx,
        &state,
        &ctx,
        (org, project),
        "models.create",
        ModelId::from_uuid(m.id).to_string(),
    )
    .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok((StatusCode::CREATED, Json(model_view(&m))))
}

/// `GET /models/{mdl}`.
#[utoipa::path(get, path = "/models/{mdl}", operation_id = "get_model", tag = "models",
    params(("mdl" = String, Path, description = "Model id, `mdl_…`.")),
    responses((status = 200, description = "The model.", body = ModelInfo),
              (status = 404, description = "No such model.", body = crate::error::Problem)))]
pub async fn get_model(
    State(state): State<AppState>,
    Extension(models): Extension<Arc<Models>>,
    ctx: RequestContext,
    Path(mdl): Path<String>,
) -> Result<Json<ModelInfo>, JobsError> {
    let id: ModelId = scope::parse_id(&mdl, "model")?;
    let m = authorized_model(&state, &models, &ctx, id.as_uuid(), "models.read").await?;
    Ok(Json(model_view(&m)))
}

/// `GET /models/{mdl}/versions`.
#[utoipa::path(get, path = "/models/{mdl}/versions", operation_id = "list_model_versions", tag = "models",
    params(("mdl" = String, Path, description = "Model id, `mdl_…`."), PageQuery),
    responses((status = 200, description = "The model's versions, newest first (no files).", body = Page<ModelVersionInfo>),
              (status = 404, description = "No such model.", body = crate::error::Problem)))]
pub async fn list_versions(
    State(state): State<AppState>,
    Extension(models): Extension<Arc<Models>>,
    ctx: RequestContext,
    Path(mdl): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<ModelVersionInfo>>, JobsError> {
    let id: ModelId = scope::parse_id(&mdl, "model")?;
    let m = authorized_model(&state, &models, &ctx, id.as_uuid(), "models.read").await?;
    let page = q.resolve()?;
    let after = scope::after::<ModelVersionId>(&page, |m| m.as_uuid())?;
    let rows: Vec<VersionRow> = sqlx::query_as(concat!(
        "SELECT ",
        version_cols!(),
        " FROM model_versions WHERE model_id = $1
           AND ($2::uuid IS NULL OR seq < (SELECT seq FROM model_versions WHERE id = $2))
         ORDER BY seq DESC LIMIT $3"
    ))
    .bind(m.id)
    .bind(after)
    .bind(page.fetch() as i64)
    .fetch_all(models.db.pool())
    .await
    .map_err(crate::db::DbError::from)?;
    let ack = store::acknowledged(&models.db, m.project_id, m.id).await?;
    let (rows, next) = scope::cut(rows, &page, |v| ModelVersionId::from_uuid(v.id).to_string());
    Ok(Json(Page {
        items: rows
            .iter()
            .map(|v| version_view(v, ack.as_deref(), None))
            .collect(),
        next_cursor: next,
    }))
}

/// `GET /model-versions/{mv}`: the version with its files and compatibility.
#[utoipa::path(get, path = "/model-versions/{mv}", operation_id = "get_model_version", tag = "models",
    params(("mv" = String, Path, description = "Model version id, `mv_…`.")),
    responses((status = 200, description = "The version with its files.", body = ModelVersionInfo),
              (status = 404, description = "No such model version.", body = crate::error::Problem)))]
pub async fn get_version(
    State(state): State<AppState>,
    Extension(models): Extension<Arc<Models>>,
    ctx: RequestContext,
    Path(mv): Path<String>,
) -> Result<Json<ModelVersionInfo>, JobsError> {
    let v = authorized_version(&state, &models, &ctx, &mv, &["models.read"]).await?;
    let files = store::files(&models.db, v.id).await?;
    let ack = store::acknowledged(&models.db, v.project_id, v.model_id).await?;
    Ok(Json(version_view(&v, ack.as_deref(), Some(&files))))
}

/// Body of the `/model-versions/{mv}:…` actions.
#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelVersionAction {
    /// `:approve-remote-code`: the revision being approved (the version's `revision`).
    #[serde(default)]
    pub revision: Option<String>,
    /// `:acknowledge-license`: the license text hash being accepted (`license.sha256`).
    #[serde(default)]
    pub license_sha256: Option<String>,
}

/// `POST /model-versions/{mv}:…`: axum sees `/model-versions/{mv}` with the action in the
/// segment; [`approve`] and [`acknowledge`] do the work.
pub async fn version_action(
    State(state): State<AppState>,
    Extension(models): Extension<Arc<Models>>,
    ctx: RequestContext,
    Path(segment): Path<String>,
    body: Option<Json<ModelVersionAction>>,
) -> Result<Json<ModelVersionInfo>, JobsError> {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let (mv, action) = match segment.split_once(':') {
        Some((mv, a)) => (mv, a),
        None => {
            return Err(
                ApiError::new(ErrorCode::RouteNotFound, "No endpoint has this path.").into(),
            )
        }
    };
    match action {
        "approve-remote-code" => approve(&state, &models, &ctx, mv, &body).await,
        "acknowledge-license" => acknowledge(&state, &models, &ctx, mv, &body).await,
        _ => Err(ApiError::new(ErrorCode::RouteNotFound, "No endpoint has this path.").into()),
    }
}

fn conflict(code: mrl_domain::errors::ErrorCode, message: impl Into<String>) -> JobsError {
    JobsError::domain(code, StatusCode::CONFLICT, message)
}

fn ready(v: &VersionRow) -> Result<(), JobsError> {
    if v.state == "READY" {
        Ok(())
    } else {
        Err(conflict(
            codes::NOT_READY,
            format!("This model version is {}, not READY.", v.state),
        ))
    }
}

/// `POST /model-versions/{mv}:approve-remote-code`: the version's custom code may run, for
/// this exact revision.
#[utoipa::path(post, path = "/model-versions/{mv}:approve-remote-code", operation_id = "approve_model_remote_code", tag = "models",
    params(("mv" = String, Path, description = "Model version id, `mv_…`.")),
    request_body = ModelVersionAction,
    responses((status = 200, description = "The version afterwards.", body = ModelVersionInfo),
              (status = 409, description = "Not READY (MRL-MODEL-263), or another revision (MRL-MODEL-266).", body = crate::error::Problem)))]
pub async fn approve(
    state: &AppState,
    models: &Models,
    ctx: &RequestContext,
    mv: &str,
    body: &ModelVersionAction,
) -> Result<Json<ModelVersionInfo>, JobsError> {
    let v = authorized_version(
        state,
        models,
        ctx,
        mv,
        &["models.import", "promotions.approve"],
    )
    .await?;
    ready(&v)?;
    let revision = v.revision().unwrap_or_default();
    if body.revision.as_deref() != Some(revision.as_str()) {
        return Err(conflict(
            codes::STALE_CONFIRMATION,
            format!("Approve the revision this version holds: {revision}."),
        ));
    }
    let mut after = v.clone();
    after.remote_code_approved_for = Some(revision.clone());
    let compatibility = gates::compatibility(&models.db, &gates::facts(&after)).await?;
    let mut tx = models.db.begin().await?;
    let v = store::lock_version(&mut tx, v.id)
        .await?
        .ok_or_else(|| JobsError::not_found("model version"))?;
    if v.requires_remote_code && !v.code_approved() {
        sqlx::query(
            "UPDATE model_versions SET remote_code_approved_by = $2, remote_code_approved_at = now(),
                    remote_code_approved_for = $3, compatibility = $4 WHERE id = $1",
        )
        .bind(v.id)
        .bind(&ctx.principal.id)
        .bind(&revision)
        .bind(&compatibility)
        .execute(&mut *tx)
        .await
        .map_err(crate::db::DbError::from)?;
        scope::audit(
            &mut tx,
            state,
            ctx,
            (v.org_id, v.project_id),
            "models.approve_remote_code",
            format!("{}@{revision}", ModelVersionId::from_uuid(v.id)),
        )
        .await?;
    }
    tx.commit().await.map_err(crate::db::DbError::from)?;
    respond(models, v.id).await
}

/// `POST /model-versions/{mv}:acknowledge-license`: the project accepts this license text.
#[utoipa::path(post, path = "/model-versions/{mv}:acknowledge-license", operation_id = "acknowledge_model_license", tag = "models",
    params(("mv" = String, Path, description = "Model version id, `mv_…`.")),
    request_body = ModelVersionAction,
    responses((status = 200, description = "The version afterwards.", body = ModelVersionInfo),
              (status = 409, description = "Not READY (MRL-MODEL-263), or another license text (MRL-MODEL-266).", body = crate::error::Problem)))]
pub async fn acknowledge(
    state: &AppState,
    models: &Models,
    ctx: &RequestContext,
    mv: &str,
    body: &ModelVersionAction,
) -> Result<Json<ModelVersionInfo>, JobsError> {
    let v = authorized_version(state, models, ctx, mv, &["models.import"]).await?;
    ready(&v)?;
    let Some(license) = v.license_sha256.clone() else {
        return Err(conflict(
            codes::NOT_READY,
            "This model version has no license record.",
        ));
    };
    if body
        .license_sha256
        .as_deref()
        .and_then(store::unhex)
        .as_ref()
        != Some(&license)
    {
        return Err(conflict(
            codes::STALE_CONFIRMATION,
            format!(
                "Accept the license text this version holds: {}.",
                hex(&license)
            ),
        ));
    }
    let mut tx = models.db.begin().await?;
    sqlx::query(
        "INSERT INTO license_acknowledgements (org_id, project_id, model_id, user_id, license_text_sha256)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (project_id, model_id) DO UPDATE
            SET user_id = EXCLUDED.user_id, license_text_sha256 = EXCLUDED.license_text_sha256, at = now()",
    )
    .bind(v.org_id)
    .bind(v.project_id)
    .bind(v.model_id)
    .bind(&ctx.principal.id)
    .bind(&license)
    .execute(&mut *tx)
    .await
    .map_err(crate::db::DbError::from)?;
    scope::audit(
        &mut tx,
        state,
        ctx,
        (v.org_id, v.project_id),
        "models.acknowledge_license",
        format!("{}@{}", ModelVersionId::from_uuid(v.id), hex(&license)),
    )
    .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    respond(models, v.id).await
}

async fn respond(models: &Models, id: Uuid) -> Result<Json<ModelVersionInfo>, JobsError> {
    let v = store::version(&models.db, id)
        .await?
        .ok_or_else(|| JobsError::not_found("model version"))?;
    let ack = store::acknowledged(&models.db, v.project_id, v.model_id).await?;
    Ok(Json(version_view(&v, ack.as_deref(), None)))
}

//! `/projects/{prj}/datasets`, `/datasets/{ds}` and `/dataset-versions/{dsv}` (Doc 09 §6.3).
//!
//! Every route authorizes on the dataset's org and project: a hidden, foreign or missing
//! dataset or version is the same 404. Reads need `datasets.read`; creating and changing
//! need `datasets.import`; exports need `datasets.export` and are refused for
//! `restricted-pii` datasets. Every change is audited in its own transaction. The server
//! reads only what the sandbox wrote (browse pages, hash lists, reports), never the data
//! files a user gave.

use super::codes;
use super::expr::Expr;
use super::store::{
    self, hex, not_ready, DatasetRow, FileRow, VersionRow, DATASET_COLS, VERSION_COLS,
};
use super::Datasets;
use crate::app::AppState;
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::http::pagination::{encode_cursor, Page, PageQuery};
use crate::modules::checkpoints::service::storage_org;
use crate::modules::jobs::{scope, JobsError};
use crate::modules::runs::experiments::project_scope;
use crate::seams::{RequestContext, Resource, SeamError};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use mrl_domain::ids::{DatasetId, DatasetVersionId, ProjectId};
use mrl_storage::{Store, TicketMethod};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use time::OffsetDateTime;
use uuid::Uuid;

/// Sensitivity labels (Doc 08 §2.7).
pub const SENSITIVITIES: [&str; 4] = ["public", "internal", "confidential", "restricted-pii"];
/// Permitted uses.
pub const USES: [&str; 3] = ["research", "commercial", "internal"];
/// Records per page by default, and at most.
const RECORDS_DEFAULT: usize = 20;
const RECORDS_MAX: usize = 100;
/// Browse pages one records request reads at most (a filter that matches little stops here
/// and hands back a cursor).
const SCAN_PAGES: usize = 20;
/// Largest browse page the server reads.
const MAX_PAGE_BYTES: i64 = 32 * 1024 * 1024;
/// Records shown side by side in a diff.
const DIFF_SAMPLES: usize = 5;
/// Example hashes in a diff.
const DIFF_EXAMPLES: usize = 20;

// ---- views ----

/// What a dataset's sensitivity allows (P22/P23 read `sample_logging`).
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct DatasetPolicy {
    /// Its files may be downloaded or exported.
    pub export_allowed: bool,
    /// It may be pushed to the Hugging Face Hub.
    pub hub_push_allowed: bool,
    /// Runs on it may log raw samples.
    pub sample_logging: bool,
}

impl DatasetPolicy {
    /// The policy of a sensitivity label: `restricted-pii` allows none of it.
    pub fn of(sensitivity: &str) -> Self {
        let open = sensitivity != "restricted-pii";
        Self {
            export_allowed: open,
            hub_push_allowed: open,
            sample_logging: open,
        }
    }
}

/// A dataset.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct DatasetInfo {
    /// `ds_…`.
    pub id: String,
    /// `prj_…`.
    pub project_id: String,
    /// Unique in the project.
    pub name: String,
    /// Short description.
    pub description: String,
    /// SPDX licence id, `custom` or `unknown`.
    pub license: String,
    /// `public`, `internal`, `confidential` or `restricted-pii`.
    pub sensitivity: String,
    /// `research`, `commercial`, `internal`.
    pub permitted_use: Vec<String>,
    /// Why the data may be used.
    pub consent_basis: Option<String>,
    /// Where the data came from.
    pub source_urls: Vec<String>,
    /// Dataset card (Markdown, shown sanitized).
    pub card_md: String,
    /// New versions of other datasets are checked for overlap with this one.
    pub eval_set: bool,
    /// What the sensitivity allows.
    pub policy: DatasetPolicy,
    /// Who made it.
    pub created_by: String,
    /// Created.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
    /// Last changed.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub updated_at: OffsetDateTime,
}

fn dataset_view(d: &DatasetRow) -> DatasetInfo {
    DatasetInfo {
        id: DatasetId::from_uuid(d.id).to_string(),
        project_id: ProjectId::from_uuid(d.project_id).to_string(),
        name: d.name.clone(),
        description: d.description.clone(),
        license: d.license_spdx.clone(),
        sensitivity: d.sensitivity.clone(),
        permitted_use: d.permitted_use.clone(),
        consent_basis: d.consent_basis.clone(),
        source_urls: d.source_urls.clone(),
        card_md: d.card_md.clone(),
        eval_set: d.eval_set,
        policy: DatasetPolicy::of(&d.sensitivity),
        created_by: d.created_by.clone(),
        created_at: d.created_at,
        updated_at: d.updated_at,
    }
}

/// One stored file of a version.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct DatasetFileInfo {
    /// Path inside the version.
    pub path: String,
    /// Split (shards).
    pub split: Option<String>,
    /// `shard`, `original` or `derived`.
    pub role: String,
    /// sha256 (hex).
    pub sha256: String,
    /// Size in bytes.
    pub size: i64,
    /// Rows (shards).
    pub rows: Option<i64>,
}

/// A dataset version.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct DatasetVersionInfo {
    /// `dsv_…`.
    pub id: String,
    /// `ds_…`.
    pub dataset_id: String,
    /// 1, 2, … within the dataset.
    pub seq: i32,
    /// Canonical schema id, like `sft.messages.v1`.
    pub schema_id: String,
    /// `INGESTING`, `VALIDATING`, `READY`, `INVALID` or `ARCHIVED`.
    pub state: String,
    /// Why it is INVALID: `{code, message}`.
    #[schema(value_type = Option<Object>)]
    pub state_reason: Option<Value>,
    /// `upload`, `hub`, `remote_ref` or `transform`.
    pub source_kind: String,
    /// Where it came from (uploads, Hub repo with its commit SHA, link, parent).
    #[schema(value_type = Object)]
    pub source_ref: Value,
    /// The version it was made from.
    pub parent_version_id: Option<String>,
    /// The transform that made it.
    #[schema(value_type = Option<Object>)]
    pub transform_spec: Option<Value>,
    /// Rows.
    pub row_count: Option<i64>,
    /// Bytes in its shards.
    pub byte_size: Option<i64>,
    /// sha256 over its sorted shard hashes (hex): the same records give the same value.
    pub content_sha256: Option<String>,
    /// Rows per split.
    #[schema(value_type = Option<Object>)]
    pub splits: Option<Value>,
    /// The job working on it (`job_…`), while it is being prepared.
    pub job_id: Option<String>,
    /// Its files (only on `GET /dataset-versions/{dsv}`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<DatasetFileInfo>>,
    /// Who made it.
    pub created_by: String,
    /// Created.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
    /// Became READY.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub ready_at: Option<OffsetDateTime>,
}

/// The API view of a version row.
pub fn version_view(v: &VersionRow, files: Option<&[FileRow]>) -> DatasetVersionInfo {
    DatasetVersionInfo {
        id: DatasetVersionId::from_uuid(v.id).to_string(),
        dataset_id: DatasetId::from_uuid(v.dataset_id).to_string(),
        seq: v.seq,
        schema_id: v.schema_id.clone(),
        state: v.state.clone(),
        state_reason: v.state_reason.clone(),
        source_kind: v.source_kind.clone(),
        source_ref: v.source_ref.clone(),
        parent_version_id: v
            .parent_version_id
            .map(|p| DatasetVersionId::from_uuid(p).to_string()),
        transform_spec: v.transform_spec.clone(),
        row_count: v.row_count,
        byte_size: v.byte_size,
        content_sha256: v.content_sha256.as_deref().map(hex),
        splits: v.splits.clone(),
        job_id: v
            .job_id
            .map(|j| mrl_domain::ids::JobId::from_uuid(j).to_string()),
        files: files.map(|fs| {
            fs.iter()
                .map(|f| DatasetFileInfo {
                    path: f.path.clone(),
                    split: f.split.clone(),
                    role: f.role.clone(),
                    sha256: hex(&f.sha256),
                    size: f.size_bytes,
                    rows: f.row_count,
                })
                .collect()
        }),
        created_by: v.created_by.clone(),
        created_at: v.created_at,
        ready_at: v.ready_at,
    }
}

// ---- bodies ----

/// `POST /projects/{prj}/datasets`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetCreate {
    /// Unique in the project (1–200 characters).
    pub name: String,
    /// Short description.
    #[serde(default)]
    pub description: Option<String>,
    /// SPDX licence id, `custom` or `unknown` (default).
    #[serde(default)]
    pub license: Option<String>,
    /// `public`, `internal` (default), `confidential` or `restricted-pii`.
    #[serde(default)]
    pub sensitivity: Option<String>,
    /// `research`, `commercial`, `internal`.
    #[serde(default)]
    pub permitted_use: Option<Vec<String>>,
    /// Why the data may be used.
    #[serde(default)]
    pub consent_basis: Option<String>,
    /// Where the data came from (http or https links).
    #[serde(default)]
    pub source_urls: Option<Vec<String>>,
    /// Dataset card (Markdown).
    #[serde(default)]
    pub card_md: Option<String>,
    /// Check new versions of other datasets for overlap with this one.
    #[serde(default)]
    pub eval_set: Option<bool>,
}

/// `PATCH /datasets/{ds}`: the fields to change (the name too).
#[derive(Debug, Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetPatch {
    /// New name.
    #[serde(default)]
    pub name: Option<String>,
    /// New description.
    #[serde(default)]
    pub description: Option<String>,
    /// New licence.
    #[serde(default)]
    pub license: Option<String>,
    /// New sensitivity.
    #[serde(default)]
    pub sensitivity: Option<String>,
    /// New permitted uses.
    #[serde(default)]
    pub permitted_use: Option<Vec<String>>,
    /// New consent basis.
    #[serde(default)]
    pub consent_basis: Option<String>,
    /// New source links.
    #[serde(default)]
    pub source_urls: Option<Vec<String>>,
    /// New card.
    #[serde(default)]
    pub card_md: Option<String>,
    /// Registered as an evaluation set.
    #[serde(default)]
    pub eval_set: Option<bool>,
}

fn field(path: &str, message: &str, hint: &str) -> FieldError {
    FieldError::new(path, ErrorCode::FieldValue, message, hint)
}

fn no_control(s: &str) -> bool {
    !s.chars()
        .any(|c| c.is_control() && c != '\n' && c != '\t' && c != '\r')
}

/// Checks the metadata fields of a create or patch body.
fn check_meta(p: &DatasetPatch) -> Result<(), JobsError> {
    let mut errors = Vec::new();
    if let Some(n) = &p.name {
        if n.trim().is_empty() || n.chars().count() > 200 || n.chars().any(char::is_control) {
            errors.push(field(
                "/name",
                "The name must be 1 to 200 characters, without control characters.",
                "Pick a shorter, plain name.",
            ));
        }
    }
    if p.description
        .as_ref()
        .is_some_and(|d| d.chars().count() > 4000 || !no_control(d))
    {
        errors.push(field(
            "/description",
            "The description must be at most 4000 characters.",
            "Shorten the description.",
        ));
    }
    if let Some(l) = &p.license {
        let ok = !l.is_empty()
            && l.len() <= 64
            && l.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+'));
        if !ok {
            errors.push(field(
                "/license",
                "The licence must be an SPDX id (like Apache-2.0), custom or unknown.",
                "See spdx.org/licenses for ids.",
            ));
        }
    }
    if p.sensitivity
        .as_deref()
        .is_some_and(|s| !SENSITIVITIES.contains(&s))
    {
        errors.push(field(
            "/sensitivity",
            "Sensitivity must be public, internal, confidential or restricted-pii.",
            "Pick one of the four labels.",
        ));
    }
    if let Some(uses) = &p.permitted_use {
        if uses.iter().any(|u| !USES.contains(&u.as_str())) {
            errors.push(field(
                "/permitted_use",
                "Permitted uses are research, commercial and internal.",
                "Remove other values.",
            ));
        }
    }
    if p.consent_basis
        .as_ref()
        .is_some_and(|c| c.chars().count() > 2000 || !no_control(c))
    {
        errors.push(field(
            "/consent_basis",
            "The consent basis must be at most 2000 characters.",
            "Shorten it.",
        ));
    }
    if let Some(urls) = &p.source_urls {
        let bad = urls.len() > 50
            || urls.iter().any(|u| {
                u.len() > 2048
                    || !(u.starts_with("https://") || u.starts_with("http://"))
                    || !no_control(u)
            });
        if bad {
            errors.push(field(
                "/source_urls",
                "Give at most 50 http or https links.",
                "Remove other links.",
            ));
        }
    }
    if p.card_md
        .as_ref()
        .is_some_and(|c| c.len() > 256 * 1024 || !no_control(c))
    {
        errors.push(field(
            "/card_md",
            "The card must be at most 256 KiB of text.",
            "Shorten the card.",
        ));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors).into())
    }
}

fn name_taken(e: JobsError) -> JobsError {
    if e.is_unique_violation("datasets_name") {
        JobsError::domain(
            codes::NAME_TAKEN,
            StatusCode::CONFLICT,
            "A dataset with this name already exists in the project.",
        )
    } else {
        e
    }
}

// ---- authorization ----

fn dataset_resource(org: Uuid, project: Uuid, id: Uuid) -> Resource {
    scope::resource(
        "dataset",
        Some(DatasetId::from_uuid(id).to_string()),
        org,
        project,
    )
}

/// Hide 404s as "no such `kind`".
fn hidden_as(kind: &'static str) -> impl Fn(JobsError) -> JobsError {
    move |e| match e {
        JobsError::Api(a) if a.code == ErrorCode::NotFound => JobsError::not_found(kind),
        other => other,
    }
}

/// The dataset `ds`, authorized for `action`.
pub async fn authorized_dataset(
    state: &AppState,
    db: &crate::db::Db,
    ctx: &RequestContext,
    ds: &str,
    action: &str,
) -> Result<DatasetRow, JobsError> {
    authorized_dataset_row(state, db, ctx, ds, action, false).await
}

async fn authorized_dataset_row(
    state: &AppState,
    db: &crate::db::Db,
    ctx: &RequestContext,
    ds: &str,
    action: &str,
    deleted_too: bool,
) -> Result<DatasetRow, JobsError> {
    let id: DatasetId = scope::parse_id(ds, "dataset")?;
    let row = store::dataset(db, id.as_uuid(), deleted_too)
        .await?
        .ok_or_else(|| JobsError::not_found("dataset"))?;
    let ctx = ctx
        .clone()
        .with_project(ProjectId::from_uuid(row.project_id).to_string());
    scope::authorize(
        state,
        &ctx,
        action,
        &dataset_resource(row.org_id, row.project_id, row.id),
    )
    .await
    .map_err(hidden_as("dataset"))?;
    Ok(row)
}

/// The version `dsv`, authorized for `action` on its dataset.
pub async fn authorized_version(
    state: &AppState,
    db: &crate::db::Db,
    ctx: &RequestContext,
    dsv: &str,
    action: &str,
) -> Result<VersionRow, JobsError> {
    let id: DatasetVersionId = scope::parse_id(dsv, "dataset version")?;
    let row = store::version(db, id.as_uuid())
        .await?
        .ok_or_else(|| JobsError::not_found("dataset version"))?;
    let ctx = ctx
        .clone()
        .with_project(ProjectId::from_uuid(row.project_id).to_string());
    scope::authorize(
        state,
        &ctx,
        action,
        &dataset_resource(row.org_id, row.project_id, row.dataset_id),
    )
    .await
    .map_err(hidden_as("dataset version"))?;
    Ok(row)
}

fn ready(v: &VersionRow) -> Result<(), JobsError> {
    if v.state == "READY" {
        Ok(())
    } else {
        Err(not_ready(&v.state))
    }
}

fn storage(e: impl std::fmt::Display) -> JobsError {
    JobsError::Seam(SeamError::new("storage", e.to_string()))
}

/// The org storage of `org` (P19's spelling).
pub async fn org_store(svc: &Datasets, org: Uuid) -> Result<Store, JobsError> {
    svc.store
        .org_store(&storage_org(org))
        .await
        .map_err(storage)
}

// ---- datasets ----

/// `GET /projects/{prj}/datasets`.
#[utoipa::path(get, path = "/projects/{prj}/datasets", operation_id = "list_datasets", tag = "datasets",
    params(("prj" = String, Path, description = "Project id, `prj_…`."), PageQuery),
    responses((status = 200, description = "The project's datasets, newest first.", body = Page<DatasetInfo>),
              (status = 404, description = "No such project.", body = crate::error::Problem)))]
pub async fn list_datasets(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path(prj): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<DatasetInfo>>, JobsError> {
    let (org, project) = project_scope(&state, &ctx, &svc.projects, &prj, "datasets.read").await?;
    let page = q.resolve()?;
    let after = scope::after::<DatasetId>(&page, |d| d.as_uuid())?;
    let rows = sqlx::query_as::<_, DatasetRow>(sqlx::AssertSqlSafe(format!(
        "SELECT {DATASET_COLS} FROM datasets d WHERE d.org_id = $1 AND d.project_id = $2 \
           AND d.deleted_at IS NULL AND ($3::uuid IS NULL OR d.id < $3) \
         ORDER BY d.id DESC LIMIT $4"
    )))
    .bind(org)
    .bind(project)
    .bind(after)
    .bind(page.fetch() as i64)
    .fetch_all(svc.db.pool())
    .await?;
    let (rows, next) = scope::cut(rows, &page, |r| DatasetId::from_uuid(r.id).to_string());
    Ok(Json(Page {
        items: rows.iter().map(dataset_view).collect(),
        next_cursor: next,
    }))
}

/// `POST /projects/{prj}/datasets`.
#[utoipa::path(post, path = "/projects/{prj}/datasets", operation_id = "create_dataset", tag = "datasets",
    params(("prj" = String, Path, description = "Project id, `prj_…`.")),
    request_body = DatasetCreate,
    responses((status = 201, description = "The new dataset.", body = DatasetInfo),
              (status = 409, description = "The name is taken (MRL-CONFLICT-270).", body = crate::error::Problem),
              (status = 422, description = "A field needs a change.", body = crate::error::Problem)))]
pub async fn create_dataset(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path(prj): Path<String>,
    headers: HeaderMap,
    Json(body): Json<DatasetCreate>,
) -> Result<Response, JobsError> {
    scope::no_nul(&serde_json::to_value(&body).unwrap_or_default())?;
    let meta = DatasetPatch {
        name: Some(body.name.clone()),
        description: body.description.clone(),
        license: body.license.clone(),
        sensitivity: body.sensitivity.clone(),
        permitted_use: body.permitted_use.clone(),
        consent_basis: body.consent_basis.clone(),
        source_urls: body.source_urls.clone(),
        card_md: body.card_md.clone(),
        eval_set: body.eval_set,
    };
    check_meta(&meta)?;
    let (org, project) =
        project_scope(&state, &ctx, &svc.projects, &prj, "datasets.import").await?;
    let id = state.seams.ids.new_uuid();
    let mut tx = svc.db.begin().await?;
    let idem = crate::modules::artifacts::Idempotency::of(&headers, &body);
    if let Some(key) = &idem {
        let inserted = sqlx::query(
            "INSERT INTO dataset_create_requests (org_id, project_id, actor, key, fingerprint)
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
        )
        .bind(org)
        .bind(project)
        .bind(&ctx.principal.id)
        .bind(&key.key)
        .bind(&key.request_sha256)
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() == 0 {
            let (fingerprint, response): (String, Value) = sqlx::query_as(
                "SELECT fingerprint, response FROM dataset_create_requests
                 WHERE org_id = $1 AND project_id = $2 AND actor = $3 AND key = $4",
            )
            .bind(org)
            .bind(project)
            .bind(&ctx.principal.id)
            .bind(&key.key)
            .fetch_one(&mut *tx)
            .await?;
            if fingerprint != key.request_sha256 {
                return Err(ApiError::new(
                    ErrorCode::IdempotencyMismatch,
                    "This key was already used with a different dataset request.",
                )
                .into());
            }
            let row_id = response
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| storage("the saved dataset response has no id"))?;
            let dataset = scope::parse_id::<DatasetId>(row_id, "dataset")?;
            let live = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM datasets WHERE id = $1 AND deleted_at IS NULL)",
            )
            .bind(dataset.as_uuid())
            .fetch_one(&mut *tx)
            .await?;
            if !live {
                return Err(JobsError::not_found("dataset"));
            }
            return Ok(created_response(row_id, response.clone()));
        }
    }
    let row = sqlx::query_as::<_, DatasetRow>(sqlx::AssertSqlSafe(format!(
        "INSERT INTO datasets AS d (id, org_id, project_id, name, description, license_spdx, \
           sensitivity, permitted_use, consent_basis, source_urls, card_md, eval_set, created_by) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING {DATASET_COLS}"
    )))
    .bind(id)
    .bind(org)
    .bind(project)
    .bind(body.name.trim())
    .bind(body.description.unwrap_or_default())
    .bind(body.license.unwrap_or_else(|| "unknown".into()))
    .bind(body.sensitivity.unwrap_or_else(|| "internal".into()))
    .bind(body.permitted_use.unwrap_or_default())
    .bind(body.consent_basis)
    .bind(body.source_urls.unwrap_or_default())
    .bind(body.card_md.unwrap_or_default())
    .bind(body.eval_set.unwrap_or(false))
    .bind(&ctx.principal.id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| name_taken(e.into()))?;
    scope::audit(
        &mut tx,
        &state,
        &ctx,
        (org, project),
        "datasets.create",
        DatasetId::from_uuid(id).to_string(),
    )
    .await?;
    let view = serde_json::to_value(dataset_view(&row)).map_err(storage)?;
    if let Some(key) = &idem {
        sqlx::query(
            "UPDATE dataset_create_requests SET response = $5
             WHERE org_id = $1 AND project_id = $2 AND actor = $3 AND key = $4",
        )
        .bind(org)
        .bind(project)
        .bind(&ctx.principal.id)
        .bind(&key.key)
        .bind(&view)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(created_response(
        &DatasetId::from_uuid(id).to_string(),
        view,
    ))
}

fn created_response(id: &str, view: Value) -> Response {
    (
        StatusCode::CREATED,
        [(
            axum::http::header::LOCATION,
            format!("/api/v1/datasets/{id}"),
        )],
        Json(view),
    )
        .into_response()
}

/// `GET /datasets/{ds}`.
#[utoipa::path(get, path = "/datasets/{ds}", operation_id = "get_dataset", tag = "datasets",
    params(("ds" = String, Path, description = "Dataset id, `ds_…`.")),
    responses((status = 200, description = "The dataset.", body = DatasetInfo),
              (status = 404, description = "No such dataset.", body = crate::error::Problem)))]
pub async fn get_dataset(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path(ds): Path<String>,
) -> Result<Json<DatasetInfo>, JobsError> {
    let row = authorized_dataset(&state, &svc.db, &ctx, &ds, "datasets.read").await?;
    Ok(Json(dataset_view(&row)))
}

/// `PATCH /datasets/{ds}`.
#[utoipa::path(patch, path = "/datasets/{ds}", operation_id = "update_dataset", tag = "datasets",
    params(("ds" = String, Path, description = "Dataset id, `ds_…`.")),
    request_body = DatasetPatch,
    responses((status = 200, description = "The dataset afterwards.", body = DatasetInfo),
              (status = 409, description = "The name is taken (MRL-CONFLICT-270).", body = crate::error::Problem),
              (status = 422, description = "A field needs a change.", body = crate::error::Problem)))]
pub async fn update_dataset(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path(ds): Path<String>,
    Json(body): Json<DatasetPatch>,
) -> Result<Json<DatasetInfo>, JobsError> {
    scope::no_nul(&serde_json::to_value(&body).unwrap_or_default())?;
    check_meta(&body)?;
    let found = authorized_dataset(&state, &svc.db, &ctx, &ds, "datasets.import").await?;
    let mut tx = svc.db.begin().await?;
    let row = sqlx::query_as::<_, DatasetRow>(sqlx::AssertSqlSafe(format!(
        "UPDATE datasets AS d SET name = COALESCE($2, name), description = COALESCE($3, description), \
           license_spdx = COALESCE($4, license_spdx), sensitivity = COALESCE($5, sensitivity), \
           permitted_use = COALESCE($6, permitted_use), consent_basis = COALESCE($7, consent_basis), \
           source_urls = COALESCE($8, source_urls), card_md = COALESCE($9, card_md), \
           eval_set = COALESCE($10, eval_set), version = version + 1, updated_at = now() \
         WHERE id = $1 AND deleted_at IS NULL RETURNING {DATASET_COLS}"
    )))
    .bind(found.id)
    .bind(body.name.as_deref().map(str::trim))
    .bind(body.description)
    .bind(body.license)
    .bind(body.sensitivity)
    .bind(body.permitted_use)
    .bind(body.consent_basis)
    .bind(body.source_urls)
    .bind(body.card_md)
    .bind(body.eval_set)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| name_taken(e.into()))?
    .ok_or_else(|| JobsError::not_found("dataset"))?;
    scope::audit(
        &mut tx,
        &state,
        &ctx,
        (row.org_id, row.project_id),
        "datasets.update",
        DatasetId::from_uuid(row.id).to_string(),
    )
    .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(Json(dataset_view(&row)))
}

/// `DELETE /datasets/{ds}`: the dataset and its versions disappear for everyone (runs that
/// already staged them keep their copies). Its name is free again.
#[utoipa::path(delete, path = "/datasets/{ds}", operation_id = "delete_dataset", tag = "datasets",
    params(("ds" = String, Path, description = "Dataset id, `ds_…`.")),
    responses((status = 204, description = "Deleted (also when it already was)."),
              (status = 404, description = "No such dataset.", body = crate::error::Problem)))]
pub async fn delete_dataset(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path(ds): Path<String>,
) -> Result<StatusCode, JobsError> {
    // A repeated delete (a retry whose answer was lost) is 204 again, for whoever may delete.
    let found = authorized_dataset_row(&state, &svc.db, &ctx, &ds, "datasets.import", true).await?;
    let mut tx = svc.db.begin().await?;
    let done = sqlx::query(
        "UPDATE datasets SET deleted_at = now(), version = version + 1 WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(found.id)
    .execute(&mut *tx)
    .await?;
    if done.rows_affected() == 0 {
        return Ok(StatusCode::NO_CONTENT);
    }
    scope::audit(
        &mut tx,
        &state,
        &ctx,
        (found.org_id, found.project_id),
        "datasets.delete",
        DatasetId::from_uuid(found.id).to_string(),
    )
    .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /datasets/{ds}/versions`.
#[utoipa::path(get, path = "/datasets/{ds}/versions", operation_id = "list_dataset_versions", tag = "datasets",
    params(("ds" = String, Path, description = "Dataset id, `ds_…`."), PageQuery),
    responses((status = 200, description = "Its versions, newest first.", body = Page<DatasetVersionInfo>),
              (status = 404, description = "No such dataset.", body = crate::error::Problem)))]
pub async fn list_versions(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path(ds): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<DatasetVersionInfo>>, JobsError> {
    let d = authorized_dataset(&state, &svc.db, &ctx, &ds, "datasets.read").await?;
    let page = q.resolve()?;
    let after = scope::after::<DatasetVersionId>(&page, |v| v.as_uuid())?;
    let rows = sqlx::query_as::<_, VersionRow>(sqlx::AssertSqlSafe(format!(
        "SELECT {VERSION_COLS} FROM dataset_versions v JOIN datasets d ON d.id = v.dataset_id \
          WHERE v.dataset_id = $1 AND ($2::uuid IS NULL OR v.seq < \
                (SELECT seq FROM dataset_versions WHERE id = $2 AND dataset_id = $1)) \
          ORDER BY v.seq DESC LIMIT $3"
    )))
    .bind(d.id)
    .bind(after)
    .bind(page.fetch() as i64)
    .fetch_all(svc.db.pool())
    .await?;
    let (rows, next) = scope::cut(rows, &page, |r| {
        DatasetVersionId::from_uuid(r.id).to_string()
    });
    Ok(Json(Page {
        items: rows.iter().map(|r| version_view(r, None)).collect(),
        next_cursor: next,
    }))
}

// ---- versions ----

/// `GET /dataset-versions/{dsv}`.
#[utoipa::path(get, path = "/dataset-versions/{dsv}", operation_id = "get_dataset_version", tag = "datasets",
    params(("dsv" = String, Path, description = "Dataset version id, `dsv_…`.")),
    responses((status = 200, description = "The version with its files.", body = DatasetVersionInfo),
              (status = 404, description = "No such version.", body = crate::error::Problem)))]
pub async fn get_version(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path(dsv): Path<String>,
) -> Result<Json<DatasetVersionInfo>, JobsError> {
    let v = authorized_version(&state, &svc.db, &ctx, &dsv, "datasets.read").await?;
    let files = store::files(&svc.db, v.id).await?;
    Ok(Json(version_view(&v, Some(&files))))
}

/// The validation report of a checked version.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct DatasetValidation {
    /// `dsv_…`.
    pub dataset_version_id: String,
    /// The version's state.
    pub state: String,
    /// Why it is INVALID.
    #[schema(value_type = Option<Object>)]
    pub state_reason: Option<Value>,
    /// Counts, defect codes, invalid examples (record, code, field, message), duplicates,
    /// leakage and which checks were full or sampled.
    #[schema(value_type = Option<Object>)]
    pub report: Option<Value>,
    /// What the check wrote about the files (records per file, skipped files, images,
    /// leakage by evaluation version).
    #[schema(value_type = Option<Object>)]
    pub summary: Option<Value>,
    /// Overlap with registered evaluation sets.
    pub contamination: Vec<DatasetContamination>,
}

/// Overlap of a version with an evaluation version.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct DatasetContamination {
    /// `dsv_…` of the evaluation set.
    pub eval_dataset_version_id: String,
    /// Share of this version's records that share a prompt with it (0–1).
    pub overlap_ratio: f64,
    /// How many.
    pub overlapping_rows: i64,
    /// `exact`.
    pub method: String,
}

/// `GET /dataset-versions/{dsv}/validation`.
#[utoipa::path(get, path = "/dataset-versions/{dsv}/validation", operation_id = "get_dataset_validation", tag = "datasets",
    params(("dsv" = String, Path, description = "Dataset version id, `dsv_…`.")),
    responses((status = 200, description = "The report (empty while the version is still being checked).", body = DatasetValidation),
              (status = 404, description = "No such version.", body = crate::error::Problem)))]
pub async fn get_validation(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path(dsv): Path<String>,
) -> Result<Json<DatasetValidation>, JobsError> {
    let v = authorized_version(&state, &svc.db, &ctx, &dsv, "datasets.read").await?;
    let row: (String, Option<Value>, Option<Value>, Option<Value>) = sqlx::query_as(
        "SELECT state, state_reason, validation, summary FROM dataset_versions WHERE id = $1",
    )
    .bind(v.id)
    .fetch_one(svc.db.pool())
    .await?;
    let (state_name, state_reason, mut report, mut summary) = row;
    let rows: Vec<(Uuid, f64, i64, String)> = sqlx::query_as(
        "SELECT c.eval_dataset_version_id, c.overlap_ratio::float8, c.overlapping_rows, c.method
         FROM eval_contamination c
         JOIN dataset_versions e ON e.id = c.eval_dataset_version_id AND e.org_id = c.org_id
         JOIN datasets d ON d.id = e.dataset_id AND d.org_id = e.org_id
         WHERE c.dataset_version_id = $1 AND c.org_id = $2 AND d.project_id = $3
           AND d.deleted_at IS NULL AND d.eval_set AND e.state = 'READY'
         ORDER BY c.eval_dataset_version_id",
    )
    .bind(v.id)
    .bind(v.org_id)
    .bind(v.project_id)
    .fetch_all(svc.db.pool())
    .await?;
    let visible: std::collections::HashSet<_> = rows
        .iter()
        .map(|r| DatasetVersionId::from_uuid(r.0).to_string())
        .collect();
    for doc in [&mut report, &mut summary].into_iter().flatten() {
        if let Some(leakage) = doc.get_mut("leakage").and_then(Value::as_object_mut) {
            leakage.retain(|label, _| visible.contains(label));
        }
    }
    Ok(Json(DatasetValidation {
        dataset_version_id: DatasetVersionId::from_uuid(v.id).to_string(),
        state: state_name,
        state_reason,
        report,
        summary,
        contamination: rows
            .into_iter()
            .map(|(e, r, n, m)| DatasetContamination {
                eval_dataset_version_id: DatasetVersionId::from_uuid(e).to_string(),
                overlap_ratio: r,
                overlapping_rows: n,
                method: m,
            })
            .collect(),
    }))
}

/// `GET /dataset-versions/{dsv}/stats`.
#[utoipa::path(get, path = "/dataset-versions/{dsv}/stats", operation_id = "get_dataset_stats", tag = "datasets",
    params(("dsv" = String, Path, description = "Dataset version id, `dsv_…`.")),
    responses((status = 200, description = "Field lengths, role counts and turns per record.", body = Object),
              (status = 409, description = "The version is not ready (MRL-DATA-002).", body = crate::error::Problem)))]
pub async fn get_stats(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path(dsv): Path<String>,
) -> Result<Json<Value>, JobsError> {
    let v = authorized_version(&state, &svc.db, &ctx, &dsv, "datasets.read").await?;
    ready(&v)?;
    let stats: Option<Value> =
        sqlx::query_scalar("SELECT stats FROM dataset_versions WHERE id = $1")
            .bind(v.id)
            .fetch_one(svc.db.pool())
            .await?;
    Ok(Json(stats.unwrap_or_default()))
}

// ---- records ----

/// `GET /dataset-versions/{dsv}/records` query.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct RecordsQuery {
    /// Records per page, 1–100 (default 20).
    pub limit: Option<String>,
    /// `next_cursor` from the previous page.
    pub cursor: Option<String>,
    /// A filter expression, like `len(completion) > 0 and lang == 'en'`.
    pub filter: Option<String>,
}

/// One record.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct DatasetRecord {
    /// Position in the version (0-based, in shard order).
    pub index: u64,
    /// The record. Show it as text only: it is untrusted.
    #[schema(value_type = Object)]
    pub record: Value,
}

/// A page of records.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct DatasetRecordsPage {
    /// Records that match.
    pub items: Vec<DatasetRecord>,
    /// Continue here; `null` at the end. A page may hold fewer than `limit` records when a
    /// filter matches few: follow the cursor to keep looking.
    pub next_cursor: Option<String>,
    /// Records looked at for this page.
    pub scanned: u64,
}

#[derive(Debug, Deserialize)]
struct BrowsePage {
    path: String,
    first: u64,
}

fn browse_pages(v: &VersionRow) -> Vec<BrowsePage> {
    v.summary
        .as_ref()
        .and_then(|s| s.get("browse"))
        .and_then(|b| serde_json::from_value::<Vec<BrowsePage>>(b.clone()).ok())
        .unwrap_or_default()
}

/// The records of one browse page, read from its checked blob.
async fn read_page(
    svc: &Datasets,
    st: &Store,
    v: &VersionRow,
    path: &str,
) -> Result<Vec<Value>, JobsError> {
    let f = store::file(&svc.db, v.id, path)
        .await?
        .ok_or_else(|| storage(format!("the browse page {path} is not recorded")))?;
    if f.size_bytes > MAX_PAGE_BYTES {
        return Err(storage(format!("the browse page {path} is too large")));
    }
    let bytes = st.get(&f.object_key).await.map_err(storage)?;
    if mrl_storage::sha256_hex(&bytes) != hex(&f.sha256) {
        return Err(storage(format!(
            "the browse page {path} does not match its hash"
        )));
    }
    bytes
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_slice(l).map_err(|e| storage(format!("{path}: {e}"))))
        .collect()
}

fn bad_cursor() -> JobsError {
    ApiError::new(
        ErrorCode::Pagination,
        "cursor must be the next_cursor value from an earlier page, unchanged.",
    )
    .into()
}

/// `GET /dataset-versions/{dsv}/records?cursor&limit&filter`.
#[utoipa::path(get, path = "/dataset-versions/{dsv}/records", operation_id = "list_dataset_records", tag = "datasets",
    params(("dsv" = String, Path, description = "Dataset version id, `dsv_…`."), RecordsQuery),
    responses((status = 200, description = "Records in order, filtered.", body = DatasetRecordsPage),
              (status = 400, description = "The filter can't be read (MRL-VAL-006) or the cursor is wrong.", body = crate::error::Problem),
              (status = 409, description = "The version is not ready (MRL-DATA-002).", body = crate::error::Problem)))]
pub async fn list_records(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path(dsv): Path<String>,
    Query(q): Query<RecordsQuery>,
) -> Result<Json<DatasetRecordsPage>, JobsError> {
    let limit = match q.limit.as_deref() {
        None => RECORDS_DEFAULT,
        Some(s) => s
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=RECORDS_MAX).contains(n))
            .ok_or_else(|| {
                JobsError::from(ApiError::new(
                    ErrorCode::Pagination,
                    "limit must be a whole number from 1 to 100.",
                ))
            })?,
    };
    let filter = q
        .filter
        .as_deref()
        .filter(|f| !f.trim().is_empty())
        .map(Expr::parse)
        .transpose()
        .map_err(|e| {
            JobsError::from(ApiError::new(
                ErrorCode::Filter,
                format!("The filter can't be read: {e}."),
            ))
        })?;
    let v = authorized_version(&state, &svc.db, &ctx, &dsv, "datasets.read").await?;
    ready(&v)?;
    let (mut page, mut line) = match q.cursor.as_deref() {
        None => (0usize, 0usize),
        Some(c) => {
            let pos = crate::http::pagination::decode_cursor(c)?;
            let (p, l) = pos.split_once('.').ok_or_else(bad_cursor)?;
            (
                p.parse().map_err(|_| bad_cursor())?,
                l.parse().map_err(|_| bad_cursor())?,
            )
        }
    };
    let pages = browse_pages(&v);
    let st = org_store(&svc, v.org_id).await?;
    let mut items = Vec::new();
    let mut scanned = 0u64;
    let mut read = 0usize;
    while page < pages.len() && items.len() < limit && read < SCAN_PAGES {
        let records = read_page(&svc, &st, &v, &pages[page].path).await?;
        read += 1;
        while line < records.len() && items.len() < limit {
            scanned += 1;
            if filter.as_ref().is_none_or(|f| f.matches(&records[line])) {
                items.push(DatasetRecord {
                    index: pages[page].first + line as u64,
                    record: records[line].clone(),
                });
            }
            line += 1;
        }
        if line >= records.len() {
            (page, line) = (page + 1, 0);
        }
    }
    let next_cursor = (page < pages.len()).then(|| encode_cursor(&format!("{page}.{line}")));
    Ok(Json(DatasetRecordsPage {
        items,
        next_cursor,
        scanned,
    }))
}

// ---- diff ----

/// `POST /dataset-versions/{dsv}:diff`.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetDiffRequest {
    /// The version to compare with (`dsv_…`): "added" means in it but not in this one.
    pub against: String,
}

/// Records that differ between two versions.
#[derive(Debug, Clone, Default, Serialize, utoipa::ToSchema)]
pub struct DatasetRecordDiff {
    /// In `against` only.
    pub added: u64,
    /// In this version only.
    pub removed: u64,
    /// In both.
    pub unchanged: u64,
    /// Record hashes (sha256 of the record as canonical JSON) of some added records.
    pub added_examples: Vec<String>,
    /// … of some removed records.
    pub removed_examples: Vec<String>,
}

/// How two versions differ (Doc 08 §2.6).
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct DatasetDiff {
    /// This version.
    pub base: DatasetVersionInfo,
    /// The other.
    pub against: DatasetVersionInfo,
    /// Both use the same schema.
    pub same_schema: bool,
    /// `against` rows minus these rows.
    pub row_delta: i64,
    /// Same content hash: the same records.
    pub same_content: bool,
    /// Record changes, by exact record hash.
    pub records: DatasetRecordDiff,
    /// The first records of this version.
    #[schema(value_type = Vec<Object>)]
    pub base_sample: Vec<Value>,
    /// The first records of `against`.
    #[schema(value_type = Vec<Object>)]
    pub against_sample: Vec<Value>,
}

/// Reads a stored sorted hash list in pieces (it can be large).
struct HashLines {
    st: Store,
    key: String,
    size: u64,
    pos: u64,
    buf: Vec<u8>,
    offset: usize,
}

impl HashLines {
    const PIECE: u64 = 4 * 1024 * 1024;

    async fn next(&mut self) -> Result<Option<String>, JobsError> {
        loop {
            if let Some(i) = self.buf[self.offset..].iter().position(|b| *b == b'\n') {
                let end = self.offset + i;
                let text = String::from_utf8_lossy(&self.buf[self.offset..end])
                    .trim_end_matches('\r')
                    .to_owned();
                self.offset = end + 1;
                if text.is_empty() {
                    continue;
                }
                return Ok(Some(text));
            }
            if self.pos >= self.size {
                return Ok(None);
            }
            let end = (self.pos + Self::PIECE).min(self.size);
            let piece = self
                .st
                .get_range(&self.key, self.pos..end)
                .await
                .map_err(storage)?;
            self.pos = end;
            self.buf.drain(..self.offset);
            self.offset = 0;
            self.buf.extend_from_slice(&piece);
            if self.pos >= self.size && !self.buf.ends_with(b"\n") {
                self.buf.push(b'\n');
            }
        }
    }
}

async fn hash_lines(svc: &Datasets, st: &Store, v: &VersionRow) -> Result<HashLines, JobsError> {
    let f = store::file(&svc.db, v.id, store::RECORD_HASHES)
        .await?
        .ok_or_else(|| storage("the version has no record hash list"))?;
    Ok(HashLines {
        st: st.clone(),
        key: f.object_key,
        size: u64::try_from(f.size_bytes).unwrap_or_default(),
        pos: 0,
        buf: Vec::new(),
        offset: 0,
    })
}

async fn sample(svc: &Datasets, st: &Store, v: &VersionRow) -> Result<Vec<Value>, JobsError> {
    match browse_pages(v).first() {
        Some(p) => Ok(read_page(svc, st, v, &p.path)
            .await?
            .into_iter()
            .take(DIFF_SAMPLES)
            .collect()),
        None => Ok(Vec::new()),
    }
}

/// `POST /dataset-versions/{dsv}:diff`, `:export` (axum sees `/dataset-versions/{dsv}` with the
/// action in the segment).
pub async fn version_action(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path(segment): Path<String>,
    body: axum::body::Bytes,
) -> Result<Response, JobsError> {
    let parse = |b: &[u8]| -> Result<Value, JobsError> {
        if b.is_empty() {
            return Ok(Value::Object(Default::default()));
        }
        serde_json::from_slice(b).map_err(|e| {
            ApiError::new(
                ErrorCode::BadBody,
                format!("The body is not valid JSON: {e}."),
            )
            .into()
        })
    };
    let json = parse(&body)?;
    if let Some(dsv) = segment.strip_suffix(":diff") {
        let req: DatasetDiffRequest = from_body(json)?;
        return Ok(Json(diff(&state, &svc, &ctx, dsv, &req).await?).into_response());
    }
    if let Some(dsv) = segment.strip_suffix(":export") {
        let req: DatasetExportRequest = from_body(json)?;
        return Ok(Json(export(&state, &svc, &ctx, dsv, &req).await?).into_response());
    }
    Err(ApiError::new(ErrorCode::RouteNotFound, "No endpoint has this path.").into())
}

fn from_body<T: serde::de::DeserializeOwned>(v: Value) -> Result<T, JobsError> {
    serde_json::from_value(v).map_err(|e| {
        ApiError::validation(vec![FieldError::new(
            "",
            ErrorCode::FieldValue,
            format!("The body doesn't fit this request: {e}."),
            "Check the field names and values.",
        )])
        .into()
    })
}

/// `:diff`.
#[utoipa::path(post, path = "/dataset-versions/{dsv}:diff", operation_id = "diff_dataset_versions", tag = "datasets",
    params(("dsv" = String, Path, description = "Dataset version id, `dsv_…`.")),
    request_body = DatasetDiffRequest,
    responses((status = 200, description = "Schema, rows and record changes, with samples.", body = DatasetDiff),
              (status = 409, description = "A version is not ready (MRL-DATA-002).", body = crate::error::Problem)))]
pub async fn diff(
    state: &AppState,
    svc: &Datasets,
    ctx: &RequestContext,
    dsv: &str,
    req: &DatasetDiffRequest,
) -> Result<DatasetDiff, JobsError> {
    let base = authorized_version(state, &svc.db, ctx, dsv, "datasets.read").await?;
    let other = authorized_version(state, &svc.db, ctx, &req.against, "datasets.read").await?;
    ready(&base)?;
    ready(&other)?;
    let st = org_store(svc, base.org_id).await?;
    let st_other = org_store(svc, other.org_id).await?;
    let (mut a, mut b) = (
        hash_lines(svc, &st, &base).await?,
        hash_lines(svc, &st_other, &other).await?,
    );
    let mut d = DatasetRecordDiff::default();
    let (mut x, mut y) = (a.next().await?, b.next().await?);
    loop {
        match (&x, &y) {
            (None, None) => break,
            (Some(p), Some(q)) if p == q => {
                d.unchanged += 1;
                (x, y) = (a.next().await?, b.next().await?);
            }
            (Some(p), Some(q)) if p < q => {
                d.removed += 1;
                if d.removed_examples.len() < DIFF_EXAMPLES {
                    d.removed_examples.push(p.clone());
                }
                x = a.next().await?;
            }
            (Some(p), None) => {
                d.removed += 1;
                if d.removed_examples.len() < DIFF_EXAMPLES {
                    d.removed_examples.push(p.clone());
                }
                x = a.next().await?;
            }
            (_, Some(q)) => {
                d.added += 1;
                if d.added_examples.len() < DIFF_EXAMPLES {
                    d.added_examples.push(q.clone());
                }
                y = b.next().await?;
            }
        }
    }
    Ok(DatasetDiff {
        same_schema: base.schema_id == other.schema_id,
        row_delta: other.row_count.unwrap_or(0) - base.row_count.unwrap_or(0),
        same_content: base.content_sha256 == other.content_sha256,
        records: d,
        base_sample: sample(svc, &st, &base).await?,
        against_sample: sample(svc, &st_other, &other).await?,
        base: version_view(&base, None),
        against: version_view(&other, None),
    })
}

// ---- export ----

/// `POST /dataset-versions/{dsv}:export`.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetExportRequest {
    /// `parquet`: the version's shards.
    pub format: String,
}

/// A file to download.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct DatasetExportFile {
    /// Path inside the export.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// sha256 (hex): check the download against it.
    pub sha256: String,
    /// Download URL, valid until `expires_at`.
    pub url: String,
    /// Headers required by the download URL.
    pub headers: BTreeMap<String, String>,
    /// When the link stops working.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub expires_at: OffsetDateTime,
}

/// An export.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct DatasetExport {
    /// The format asked for.
    pub format: String,
    /// The files, with download links.
    pub files: Vec<DatasetExportFile>,
}

/// Download links for the version's files in `format`. Refused (403 MRL-PERM-270) for a
/// `restricted-pii` dataset; audited as `datasets.export`.
#[utoipa::path(post, path = "/dataset-versions/{dsv}:export", operation_id = "export_dataset_version", tag = "datasets",
    params(("dsv" = String, Path, description = "Dataset version id, `dsv_…`.")),
    request_body = DatasetExportRequest,
    responses((status = 200, description = "Download links for every file.", body = DatasetExport),
              (status = 403, description = "The dataset is restricted personal data (MRL-PERM-270).", body = crate::error::Problem),
              (status = 409, description = "The version is not ready (MRL-DATA-002).", body = crate::error::Problem)))]
pub async fn export(
    state: &AppState,
    svc: &Datasets,
    ctx: &RequestContext,
    dsv: &str,
    req: &DatasetExportRequest,
) -> Result<DatasetExport, JobsError> {
    if req.format != "parquet" {
        return Err(ApiError::validation(vec![field(
            "/format",
            "The format must be parquet.",
            "Use parquet: the version's shards.",
        )])
        .into());
    }
    let v = authorized_version(state, &svc.db, ctx, dsv, "datasets.export").await?;
    export_allowed(&v)?;
    ready(&v)?;
    let files: Vec<FileRow> = store::files(&svc.db, v.id)
        .await?
        .into_iter()
        .filter(|f| f.role == "shard")
        .collect();
    let mut out = Vec::with_capacity(files.len());
    let mut ctx = ctx
        .clone()
        .with_project(ProjectId::from_uuid(v.project_id).to_string());
    ctx.org = Some(storage_org(v.org_id));
    for f in files {
        let size = u64::try_from(f.size_bytes).unwrap_or_default();
        let t = svc
            .store
            .ticket(&state.seams, &ctx, &f.object_key, TicketMethod::Get, size)
            .await
            .map_err(storage)?;
        out.push(DatasetExportFile {
            path: f.path,
            size,
            sha256: hex(&f.sha256),
            url: t.url,
            headers: t.headers,
            expires_at: t.expires_at,
        });
    }
    let mut tx = svc.db.begin().await?;
    scope::audit(
        &mut tx,
        state,
        &ctx,
        (v.org_id, v.project_id),
        "datasets.export",
        DatasetVersionId::from_uuid(v.id).to_string(),
    )
    .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(DatasetExport {
        format: req.format.clone(),
        files: out,
    })
}

/// 403 MRL-PERM-270 for `restricted-pii` datasets.
pub fn export_allowed(v: &VersionRow) -> Result<(), JobsError> {
    if DatasetPolicy::of(&v.sensitivity).export_allowed {
        return Ok(());
    }
    Err(JobsError::domain(
        codes::EXPORT_REFUSED,
        StatusCode::FORBIDDEN,
        "This dataset is marked restricted personal data, so its files can't be exported or pushed to the Hub.",
    ))
}

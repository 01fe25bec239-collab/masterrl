//! Experiments: named groups of runs inside a project.

use crate::app::AppState;
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::http::etag;
use crate::http::pagination::{Page, PageQuery};
use crate::modules::jobs::api::with_etag;
use crate::modules::jobs::scope::{self, resource};
use crate::modules::jobs::{codes, JobsError};
use crate::seams::RequestContext;
use crate::Db;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use mrl_domain::ids::{ExperimentId, ProjectId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

/// Name of the experiment a run joins when it names none.
pub const DEFAULT_EXPERIMENT: &str = "default";
const MAX_NAME: usize = 200;
const MAX_TAGS: usize = 50;

macro_rules! experiment_cols {
    () => {
        "id, org_id, project_id, name, description, tags, version, created_by, created_at, updated_at"
    };
}

/// One `experiments` row.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ExperimentRow {
    /// Id.
    pub id: Uuid,
    /// Org.
    pub org_id: Uuid,
    /// Project.
    pub project_id: Uuid,
    /// Unique in the project.
    pub name: String,
    /// Free text.
    pub description: Option<String>,
    /// Labels.
    pub tags: Vec<String>,
    /// Bumped by every change; the ETag.
    pub version: i64,
    /// Creator's principal id.
    pub created_by: String,
    /// Created.
    pub created_at: OffsetDateTime,
    /// Last changed.
    pub updated_at: OffsetDateTime,
}

/// An experiment, as the API shows it.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct Experiment {
    /// `exp_…`.
    pub id: String,
    /// `prj_…`.
    pub project_id: String,
    /// Unique in the project.
    pub name: String,
    /// Free text.
    pub description: Option<String>,
    /// Labels.
    pub tags: Vec<String>,
    /// Bumped by every change; also the `ETag`.
    pub version: i64,
    /// Creator's principal id.
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

impl ExperimentRow {
    fn view(&self) -> Experiment {
        Experiment {
            id: ExperimentId::from_uuid(self.id).to_string(),
            project_id: ProjectId::from_uuid(self.project_id).to_string(),
            name: self.name.clone(),
            description: self.description.clone(),
            tags: self.tags.clone(),
            version: self.version,
            created_by: self.created_by.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

/// `POST /projects/{prj}/experiments` body.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NewExperiment {
    /// 1–200 characters, unique in the project.
    pub name: String,
    /// Free text.
    #[serde(default)]
    pub description: Option<String>,
    /// Up to 50 labels.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// `PATCH /experiments/{exp}` body; absent fields stay as they are.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ExperimentPatch {
    /// New name.
    #[serde(default)]
    pub name: Option<String>,
    /// New description.
    #[serde(default)]
    pub description: Option<String>,
    /// New labels (replace the old ones).
    #[serde(default)]
    pub tags: Option<Vec<String>>,
}

fn check_fields(name: Option<&str>, tags: Option<&[String]>) -> Result<(), JobsError> {
    let mut errors = Vec::new();
    if let Some(n) = name {
        let len = n.trim().chars().count();
        if len == 0 || len > MAX_NAME {
            errors.push(FieldError::new(
                "/name",
                ErrorCode::FieldValue,
                "The name must have 1 to 200 characters.",
                "Use a short name like `lr-sweep`.",
            ));
        }
    }
    if tags.is_some_and(|t| t.len() > MAX_TAGS || t.iter().any(|x| x.is_empty() || x.len() > 64)) {
        errors.push(FieldError::new(
            "/tags",
            ErrorCode::FieldValue,
            "Use at most 50 tags of 1 to 64 characters each.",
            "Remove some tags or shorten them.",
        ));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors).into())
    }
}

fn name_taken(e: JobsError) -> JobsError {
    if e.is_unique_violation("experiments_org_id_project_id_name_key") {
        JobsError::domain(
            codes::EXPERIMENT_NAME_TAKEN,
            StatusCode::CONFLICT,
            "Another experiment in this project already has this name.",
        )
    } else {
        e
    }
}

/// Checks `action` on project `prj` and returns (org, project).
pub async fn project_scope(
    state: &AppState,
    ctx: &RequestContext,
    prj: &str,
    action: &str,
) -> Result<(Uuid, Uuid), JobsError> {
    let org = scope::tenant(ctx)?;
    let project: ProjectId = scope::parse_id(prj, "project")?;
    let res = resource("project", Some(project.to_string()), org, project.as_uuid());
    let ctx = ctx.clone().with_project(project.to_string());
    scope::authorize(state, &ctx, action, &res).await?;
    Ok((org, project.as_uuid()))
}

/// The id of experiment `name` in the project, created when missing.
pub async fn get_or_create(
    tx: &mut crate::Tx,
    org: Uuid,
    project: Uuid,
    name: &str,
    created_by: &str,
) -> Result<Uuid, JobsError> {
    sqlx::query(
        "INSERT INTO experiments (id, org_id, project_id, name, created_by) \
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT (org_id, project_id, name) DO NOTHING",
    )
    .bind(Uuid::now_v7())
    .bind(org)
    .bind(project)
    .bind(name)
    .bind(created_by)
    .execute(&mut **tx)
    .await?;
    Ok(sqlx::query_scalar(
        "SELECT id FROM experiments WHERE org_id = $1 AND project_id = $2 AND name = $3",
    )
    .bind(org)
    .bind(project)
    .bind(name)
    .fetch_one(&mut **tx)
    .await?)
}

/// `POST /projects/{prj}/experiments`
#[utoipa::path(post, path = "/projects/{prj}/experiments", operation_id = "create_experiment", tag = "runs",
    params(("prj" = String, Path, description = "Project id, `prj_…`.")),
    request_body = NewExperiment,
    responses((status = 201, description = "Created.", body = Experiment),
              (status = 409, description = "The name is taken.", body = crate::error::Problem)))]
pub async fn create(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Path(prj): Path<String>,
    Json(body): Json<NewExperiment>,
) -> Result<Response, JobsError> {
    let (org, project) = project_scope(&state, &ctx, &prj, "runs.submit").await?;
    check_fields(Some(&body.name), Some(&body.tags))?;
    let row = sqlx::query_as::<_, ExperimentRow>(concat!(
        "INSERT INTO experiments (id, org_id, project_id, name, description, tags, created_by) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING ",
        experiment_cols!()
    ))
    .bind(Uuid::now_v7())
    .bind(org)
    .bind(project)
    .bind(body.name.trim())
    .bind(&body.description)
    .bind(&body.tags)
    .bind(&ctx.principal.id)
    .fetch_one(db.pool())
    .await
    .map_err(JobsError::from)
    .map_err(name_taken)?;
    let id = ExperimentId::from_uuid(row.id).to_string();
    scope::audit(&state, &ctx, "experiments.create", id.clone()).await;
    let location = format!("/api/v1/experiments/{id}");
    Ok((
        StatusCode::CREATED,
        [
            (header::LOCATION, location),
            (header::ETAG, format!("\"{}\"", row.version)),
        ],
        Json(row.view()),
    )
        .into_response())
}

/// `GET /projects/{prj}/experiments`
#[utoipa::path(get, path = "/projects/{prj}/experiments", operation_id = "list_experiments", tag = "runs",
    params(("prj" = String, Path, description = "Project id, `prj_…`."), PageQuery),
    responses((status = 200, description = "A page of experiments, newest first.", body = Page<Experiment>)))]
pub async fn list(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Path(prj): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<Experiment>>, JobsError> {
    let (org, project) = project_scope(&state, &ctx, &prj, "runs.read").await?;
    let page = q.resolve()?;
    let after = scope::after::<ExperimentId>(&page, |e| e.as_uuid())?;
    let rows = sqlx::query_as::<_, ExperimentRow>(concat!(
        "SELECT ",
        experiment_cols!(),
        " FROM experiments WHERE org_id = $1 AND project_id = $2 \
           AND ($3::uuid IS NULL OR id < $3) ORDER BY id DESC LIMIT $4"
    ))
    .bind(org)
    .bind(project)
    .bind(after)
    .bind(i64::try_from(page.fetch()).unwrap_or(i64::MAX))
    .fetch_all(db.pool())
    .await?;
    let (rows, next_cursor) =
        scope::cut(rows, &page, |r| ExperimentId::from_uuid(r.id).to_string());
    Ok(Json(Page {
        items: rows.iter().map(ExperimentRow::view).collect(),
        next_cursor,
    }))
}

async fn read(
    state: &AppState,
    db: &Db,
    ctx: &RequestContext,
    exp: &str,
    action: &str,
) -> Result<ExperimentRow, JobsError> {
    let org = scope::tenant(ctx)?;
    let id: ExperimentId = scope::parse_id(exp, "experiment")?;
    let row = sqlx::query_as::<_, ExperimentRow>(concat!(
        "SELECT ",
        experiment_cols!(),
        " FROM experiments WHERE id = $1 AND org_id = $2"
    ))
    .bind(id.as_uuid())
    .bind(org)
    .fetch_optional(db.pool())
    .await?
    .ok_or_else(|| JobsError::not_found("experiment"))?;
    let res = resource(
        "experiment",
        Some(id.to_string()),
        row.org_id,
        row.project_id,
    );
    scope::authorize(state, ctx, "runs.read", &res).await?;
    if action != "runs.read" {
        scope::authorize(state, ctx, action, &res).await?;
    }
    Ok(row)
}

/// `GET /experiments/{exp}`
#[utoipa::path(get, path = "/experiments/{exp}", operation_id = "get_experiment", tag = "runs",
    params(("exp" = String, Path, description = "Experiment id, `exp_…`.")),
    responses((status = 200, description = "The experiment; `ETag` is its version.", body = Experiment),
              (status = 404, description = "No such experiment.", body = crate::error::Problem)))]
pub async fn get(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Path(exp): Path<String>,
) -> Result<Response, JobsError> {
    let row = read(&state, &db, &ctx, &exp, "runs.read").await?;
    Ok(with_etag(row.version, row.view()))
}

/// `PATCH /experiments/{exp}`. `If-Match` is optional.
#[utoipa::path(patch, path = "/experiments/{exp}", operation_id = "update_experiment", tag = "runs",
    params(("exp" = String, Path, description = "Experiment id, `exp_…`.")),
    request_body = ExperimentPatch,
    responses((status = 200, description = "The changed experiment.", body = Experiment),
              (status = 412, description = "`If-Match` is stale.", body = crate::error::Problem)))]
pub async fn patch(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Path(exp): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ExperimentPatch>,
) -> Result<Response, JobsError> {
    check_fields(body.name.as_deref(), body.tags.as_deref())?;
    let row = read(&state, &db, &ctx, &exp, "runs.submit").await?;
    etag::check_if_match(&headers, &etag::from_version(row.version), false)?;
    let next = sqlx::query_as::<_, ExperimentRow>(concat!(
        "UPDATE experiments SET name = COALESCE($3, name), \
           description = COALESCE($4, description), tags = COALESCE($5, tags), \
           version = version + 1, updated_at = now() \
         WHERE id = $1 AND org_id = $2 AND version = $6 RETURNING ",
        experiment_cols!()
    ))
    .bind(row.id)
    .bind(row.org_id)
    .bind(body.name.as_deref().map(str::trim))
    .bind(&body.description)
    .bind(&body.tags)
    .bind(row.version)
    .fetch_optional(db.pool())
    .await
    .map_err(JobsError::from)
    .map_err(name_taken)?
    .ok_or_else(|| JobsError::changed("experiment"))?;
    scope::audit(&state, &ctx, "experiments.update", exp).await;
    Ok(with_etag(next.version, next.view()))
}

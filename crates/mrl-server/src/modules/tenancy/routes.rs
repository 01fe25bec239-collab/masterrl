//! Tenancy endpoints (Doc 09 §6.3): `GET|POST /orgs`, `GET|POST /orgs/{org}/workspaces`,
//! `GET|POST /workspaces/{ws}/projects`. Bodies `{slug, name}`; lists are cursor pages.
//!
//! RBAC: listing your orgs needs only a sign-in; creating an org needs `admin.manage`
//! (the platform owner); reading a level needs `tenancy.read` and creating inside it
//! `tenancy.manage_projects` (owner or maintainer of the parent). Another org's ids → 404.
//! Members endpoints are P43's.

use super::{acting_user, bind_role, Scope};
use crate::app::AppState;
use crate::db::Db;
use crate::error::{ApiError, ErrorCode, FieldError, Problem, ERROR_TYPE_BASE, PROBLEM_JSON};
use crate::http::pagination::{Page, PageQuery, PageRequest};
use crate::modules::audit;
use crate::modules::auth::{commit, db_error};
use crate::seams::{AuditOutcome, RequestContext, Resource};
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use mrl_domain::ids::{OrgId, ProjectId, WorkspaceId};
use mrl_domain::rbac::Role;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use time::OffsetDateTime;
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

/// Code: the slug is taken at this level.
pub const SLUG_TAKEN: &str = "MRL-CONFLICT-160";

pub(super) fn router(db: Db) -> Router<AppState> {
    Router::new()
        .route("/orgs", get(list_orgs).post(create_org))
        .route(
            "/orgs/{org}/workspaces",
            get(list_workspaces).post(create_workspace),
        )
        .route(
            "/workspaces/{ws}/projects",
            get(list_projects).post(create_project),
        )
        .layer(Extension(db))
}

pub(super) fn doc() -> utoipa::openapi::OpenApi {
    TenancyDoc::openapi()
}

#[derive(OpenApi)]
#[openapi(
    paths(list_orgs, create_org, list_workspaces, create_workspace, list_projects, create_project),
    components(schemas(NewItem, Org, Workspace, Project)),
    tags((name = "tenancy", description = "Orgs, workspaces and projects."))
)]
struct TenancyDoc;

/// A new org, workspace or project.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NewItem {
    /// Short name used in addresses: 1–63 lower-case letters, digits and `-`, starting with a letter or digit.
    pub slug: String,
    /// Display name, 1–100 characters.
    pub name: String,
}

/// An org.
#[derive(Debug, Serialize, ToSchema)]
pub struct Org {
    /// `org_…`.
    pub id: String,
    /// Slug, unique on this core.
    pub slug: String,
    /// Name.
    pub name: String,
    /// `personal` or `team`.
    pub kind: String,
    /// Created.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
}

/// A workspace.
#[derive(Debug, Serialize, ToSchema)]
pub struct Workspace {
    /// `ws_…`.
    pub id: String,
    /// `org_…`.
    pub org_id: String,
    /// Slug, unique in the org.
    pub slug: String,
    /// Name.
    pub name: String,
    /// Created.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
}

/// A project.
#[derive(Debug, Serialize, ToSchema)]
pub struct Project {
    /// `prj_…`.
    pub id: String,
    /// `org_…`.
    pub org_id: String,
    /// `ws_…`.
    pub workspace_id: String,
    /// Slug, unique in the workspace.
    pub slug: String,
    /// Name.
    pub name: String,
    /// Created.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
}

fn check(item: &NewItem) -> Result<(), ApiError> {
    let s = &item.slug;
    let slug_ok = (1..=63).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !s.starts_with('-');
    let mut errors = Vec::new();
    if !slug_ok {
        errors.push(FieldError::new(
            "/slug",
            ErrorCode::FieldValue,
            "slug must be 1–63 lower-case letters, digits and '-', starting with a letter or digit",
            "Use something like vision-lab.",
        ));
    }
    let n = item.name.trim().chars().count();
    if !(1..=100).contains(&n) {
        errors.push(FieldError::new(
            "/name",
            ErrorCode::FieldValue,
            "name must have 1–100 characters",
            "Use a short, readable name.",
        ));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::validation(errors))
    }
}

/// 409 with MRL-CONFLICT-160. P07's `ApiError` has no 409 conflict code, so the problem
/// document is built here, title from the code registry.
fn slug_taken(slug: &str, level: &str, uri: &OriginalUri, ctx: &RequestContext) -> Response {
    let title = mrl_domain::errors::ErrorCode::from_str(SLUG_TAKEN)
        .ok()
        .and_then(|c| c.title())
        .unwrap_or("This short name is already used here.");
    let problem = Problem {
        r#type: format!("{ERROR_TYPE_BASE}{SLUG_TAKEN}"),
        title: title.to_owned(),
        status: 409,
        detail: format!(
            "{SLUG_TAKEN}: {level} already has something named {slug:?}; pick another slug."
        ),
        instance: uri.path().to_owned(),
        code: SLUG_TAKEN.to_owned(),
        errors: Vec::new(),
        trace_id: ctx.trace_id.clone(),
    };
    let body = serde_json::to_vec(&problem).unwrap_or_default();
    (
        StatusCode::CONFLICT,
        [(header::CONTENT_TYPE, PROBLEM_JSON)],
        body,
    )
        .into_response()
}

fn missing(what: &str) -> ApiError {
    ApiError::new(
        ErrorCode::NotFound,
        format!("No {what} with this id exists."),
    )
}

fn after_uuid(page: &PageRequest) -> Result<Option<Uuid>, ApiError> {
    page.after
        .as_deref()
        .map(|a| {
            Uuid::parse_str(a).map_err(|_| {
                ApiError::new(
                    ErrorCode::Pagination,
                    "cursor must be the next_cursor value from an earlier page, unchanged.",
                )
            })
        })
        .transpose()
}

fn limit(page: &PageRequest) -> i64 {
    i64::try_from(page.fetch()).unwrap_or(201)
}

async fn audited(
    state: &AppState,
    tx: &mut crate::db::Tx,
    ctx: &RequestContext,
    org: Option<&str>,
    target: &str,
    step: &str,
) -> Result<(), ApiError> {
    let event = audit::event(
        state.seams.clock.now(),
        &ctx.principal.id,
        org,
        "tenancy.manage_projects",
        target,
        AuditOutcome::Success,
        &ctx.trace_id,
    );
    audit::write(tx, &event, &serde_json::json!({ "step": step }))
        .await
        .map_err(db_error)
}

type OrgRow = (Uuid, String, String, String, OffsetDateTime);

fn org_of(r: OrgRow) -> Org {
    Org {
        id: OrgId::from_uuid(r.0).to_string(),
        slug: r.1,
        name: r.2,
        kind: r.3,
        created_at: r.4,
    }
}

#[utoipa::path(get, path = "/orgs", tag = "tenancy", params(PageQuery),
    responses((status = 200, description = "Orgs you hold a role in.", body = Page<Org>)))]
async fn list_orgs(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<Org>>, ApiError> {
    state
        .authorize(&ctx, "auth.session", &Resource::kind("org"))
        .await?;
    let page = q.resolve()?;
    let user = acting_user(&db, &ctx.principal).await.map_err(db_error)?;
    let rows: Vec<OrgRow> = sqlx::query_as(
        "SELECT o.id, o.slug, o.name, o.kind, o.created_at FROM organizations o \
         WHERE EXISTS (SELECT 1 FROM role_bindings rb WHERE rb.org_id = o.id AND rb.principal_kind = 'user' AND rb.principal_id = $1) \
           AND ($2::uuid IS NULL OR o.id > $2) ORDER BY o.id LIMIT $3",
    )
    .bind(user.map(|u| u.as_uuid()))
    .bind(after_uuid(&page)?)
    .bind(limit(&page))
    .fetch_all(db.pool())
    .await
    .map_err(|e| db_error(e.into()))?;
    Ok(Json(Page::from_overfetch(
        rows.into_iter().map(org_of).collect(),
        &page,
        |o| {
            OrgId::from_str(&o.id)
                .map(|i| i.as_uuid().to_string())
                .unwrap_or_default()
        },
    )))
}

#[utoipa::path(post, path = "/orgs", tag = "tenancy", request_body = NewItem,
    responses((status = 201, body = Org), (status = 403, body = crate::error::Problem), (status = 409, body = crate::error::Problem)))]
async fn create_org(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    uri: OriginalUri,
    Json(item): Json<NewItem>,
) -> Result<Response, ApiError> {
    let resource = Resource {
        org: ctx.org.clone(),
        ..Resource::kind("org")
    };
    state.authorize(&ctx, "admin.manage", &resource).await?;
    check(&item)?;
    let user = acting_user(&db, &ctx.principal)
        .await
        .map_err(db_error)?
        .ok_or_else(|| {
            ApiError::new(
                ErrorCode::Unauthenticated,
                "This sign-in has no account behind it.",
            )
        })?;
    let id = OrgId::new();
    let mut tx = db.begin().await.map_err(db_error)?;
    let created: Option<OffsetDateTime> = sqlx::query_scalar(
        "INSERT INTO organizations (id, slug, name, kind) VALUES ($1, $2, $3, 'team') ON CONFLICT DO NOTHING RETURNING created_at",
    )
    .bind(id.as_uuid())
    .bind(&item.slug)
    .bind(item.name.trim())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| db_error(e.into()))?;
    let Some(created_at) = created else {
        return Ok(slug_taken(&item.slug, "This server", &uri, &ctx));
    };
    // The creator owns the new org.
    bind_role(&mut tx, id, user, Role::Owner, Scope::Org)
        .await
        .map_err(db_error)?;
    audited(
        &state,
        &mut tx,
        &ctx,
        Some(&id.to_string()),
        &id.to_string(),
        "org_created",
    )
    .await?;
    commit(tx).await?;
    let org = Org {
        id: id.to_string(),
        slug: item.slug,
        name: item.name.trim().to_owned(),
        kind: "team".into(),
        created_at,
    };
    Ok((StatusCode::CREATED, Json(org)).into_response())
}

/// The org, if it exists (any caller may learn nothing more than 404 from a bad id).
async fn find_org(db: &Db, raw: &str) -> Result<OrgId, ApiError> {
    let id = OrgId::from_str(raw).map_err(|_| missing("org"))?;
    let found: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM organizations WHERE id = $1)")
            .bind(id.as_uuid())
            .fetch_one(db.pool())
            .await
            .map_err(|e| db_error(e.into()))?;
    found.then_some(id).ok_or_else(|| missing("org"))
}

type WsRow = (Uuid, Uuid, String, String, OffsetDateTime);

fn ws_of(r: WsRow) -> Workspace {
    Workspace {
        id: WorkspaceId::from_uuid(r.0).to_string(),
        org_id: OrgId::from_uuid(r.1).to_string(),
        slug: r.2,
        name: r.3,
        created_at: r.4,
    }
}

#[utoipa::path(get, path = "/orgs/{org}/workspaces", tag = "tenancy", params(("org" = String, Path, description = "`org_…`"), PageQuery),
    responses((status = 200, body = Page<Workspace>), (status = 404, body = crate::error::Problem)))]
async fn list_workspaces(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Path(org): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<Workspace>>, ApiError> {
    let page = q.resolve()?;
    let org = find_org(&db, &org).await?;
    let resource = Resource {
        id: Some(org.to_string()),
        org: Some(org.to_string()),
        ..Resource::kind("org")
    };
    state.authorize(&ctx, "tenancy.read", &resource).await?;
    let rows: Vec<WsRow> = sqlx::query_as(
        "SELECT id, org_id, slug, name, created_at FROM workspaces \
         WHERE org_id = $1 AND deleted_at IS NULL AND ($2::uuid IS NULL OR id > $2) ORDER BY id LIMIT $3",
    )
    .bind(org.as_uuid())
    .bind(after_uuid(&page)?)
    .bind(limit(&page))
    .fetch_all(db.pool())
    .await
    .map_err(|e| db_error(e.into()))?;
    Ok(Json(Page::from_overfetch(
        rows.into_iter().map(ws_of).collect(),
        &page,
        |w| {
            WorkspaceId::from_str(&w.id)
                .map(|i| i.as_uuid().to_string())
                .unwrap_or_default()
        },
    )))
}

#[utoipa::path(post, path = "/orgs/{org}/workspaces", tag = "tenancy", params(("org" = String, Path, description = "`org_…`")), request_body = NewItem,
    responses((status = 201, body = Workspace), (status = 403, body = crate::error::Problem), (status = 404, body = crate::error::Problem), (status = 409, body = crate::error::Problem)))]
async fn create_workspace(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    uri: OriginalUri,
    Path(org): Path<String>,
    Json(item): Json<NewItem>,
) -> Result<Response, ApiError> {
    let org = find_org(&db, &org).await?;
    let resource = Resource {
        id: Some(org.to_string()),
        org: Some(org.to_string()),
        ..Resource::kind("org")
    };
    state
        .authorize(&ctx, "tenancy.manage_projects", &resource)
        .await?;
    check(&item)?;
    let id = WorkspaceId::new();
    let mut tx = db.begin().await.map_err(db_error)?;
    let created: Option<OffsetDateTime> = sqlx::query_scalar(
        "INSERT INTO workspaces (id, org_id, slug, name) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING RETURNING created_at",
    )
    .bind(id.as_uuid())
    .bind(org.as_uuid())
    .bind(&item.slug)
    .bind(item.name.trim())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| db_error(e.into()))?;
    let Some(created_at) = created else {
        return Ok(slug_taken(&item.slug, "This org", &uri, &ctx));
    };
    audited(
        &state,
        &mut tx,
        &ctx,
        Some(&org.to_string()),
        &id.to_string(),
        "workspace_created",
    )
    .await?;
    commit(tx).await?;
    let ws = Workspace {
        id: id.to_string(),
        org_id: org.to_string(),
        slug: item.slug,
        name: item.name.trim().to_owned(),
        created_at,
    };
    Ok((StatusCode::CREATED, Json(ws)).into_response())
}

/// The workspace and its org, if it exists.
async fn find_workspace(db: &Db, raw: &str) -> Result<(WorkspaceId, OrgId), ApiError> {
    let id = WorkspaceId::from_str(raw).map_err(|_| missing("workspace"))?;
    let org: Option<Uuid> =
        sqlx::query_scalar("SELECT org_id FROM workspaces WHERE id = $1 AND deleted_at IS NULL")
            .bind(id.as_uuid())
            .fetch_optional(db.pool())
            .await
            .map_err(|e| db_error(e.into()))?;
    org.map(|o| (id, OrgId::from_uuid(o)))
        .ok_or_else(|| missing("workspace"))
}

fn ws_resource(ws: WorkspaceId, org: OrgId) -> Resource {
    Resource {
        id: Some(ws.to_string()),
        org: Some(org.to_string()),
        ..Resource::kind("workspace")
    }
}

type PrjRow = (Uuid, Uuid, Uuid, String, String, OffsetDateTime);

fn prj_of(r: PrjRow) -> Project {
    Project {
        id: ProjectId::from_uuid(r.0).to_string(),
        org_id: OrgId::from_uuid(r.1).to_string(),
        workspace_id: WorkspaceId::from_uuid(r.2).to_string(),
        slug: r.3,
        name: r.4,
        created_at: r.5,
    }
}

#[utoipa::path(get, path = "/workspaces/{ws}/projects", tag = "tenancy", params(("ws" = String, Path, description = "`ws_…`"), PageQuery),
    responses((status = 200, body = Page<Project>), (status = 404, body = crate::error::Problem)))]
async fn list_projects(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    Path(ws): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<Project>>, ApiError> {
    let page = q.resolve()?;
    let (ws, org) = find_workspace(&db, &ws).await?;
    state
        .authorize(&ctx, "tenancy.read", &ws_resource(ws, org))
        .await?;
    let rows: Vec<PrjRow> = sqlx::query_as(
        "SELECT id, org_id, workspace_id, slug, name, created_at FROM projects \
         WHERE workspace_id = $1 AND deleted_at IS NULL AND ($2::uuid IS NULL OR id > $2) ORDER BY id LIMIT $3",
    )
    .bind(ws.as_uuid())
    .bind(after_uuid(&page)?)
    .bind(limit(&page))
    .fetch_all(db.pool())
    .await
    .map_err(|e| db_error(e.into()))?;
    Ok(Json(Page::from_overfetch(
        rows.into_iter().map(prj_of).collect(),
        &page,
        |p| {
            ProjectId::from_str(&p.id)
                .map(|i| i.as_uuid().to_string())
                .unwrap_or_default()
        },
    )))
}

#[utoipa::path(post, path = "/workspaces/{ws}/projects", tag = "tenancy", params(("ws" = String, Path, description = "`ws_…`")), request_body = NewItem,
    responses((status = 201, body = Project), (status = 403, body = crate::error::Problem), (status = 404, body = crate::error::Problem), (status = 409, body = crate::error::Problem)))]
async fn create_project(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    ctx: RequestContext,
    uri: OriginalUri,
    Path(ws): Path<String>,
    Json(item): Json<NewItem>,
) -> Result<Response, ApiError> {
    let (ws, org) = find_workspace(&db, &ws).await?;
    state
        .authorize(&ctx, "tenancy.manage_projects", &ws_resource(ws, org))
        .await?;
    check(&item)?;
    let id = ProjectId::new();
    let mut tx = db.begin().await.map_err(db_error)?;
    let created: Option<OffsetDateTime> = sqlx::query_scalar(
        "INSERT INTO projects (id, org_id, workspace_id, slug, name) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING RETURNING created_at",
    )
    .bind(id.as_uuid())
    .bind(org.as_uuid())
    .bind(ws.as_uuid())
    .bind(&item.slug)
    .bind(item.name.trim())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| db_error(e.into()))?;
    let Some(created_at) = created else {
        return Ok(slug_taken(&item.slug, "This workspace", &uri, &ctx));
    };
    audited(
        &state,
        &mut tx,
        &ctx,
        Some(&org.to_string()),
        &id.to_string(),
        "project_created",
    )
    .await?;
    commit(tx).await?;
    let p = Project {
        id: id.to_string(),
        org_id: org.to_string(),
        workspace_id: ws.to_string(),
        slug: item.slug,
        name: item.name.trim().to_owned(),
        created_at,
    };
    Ok((StatusCode::CREATED, Json(p)).into_response())
}

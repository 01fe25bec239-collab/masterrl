//! Secret references (P19). PostgreSQL keeps only metadata; values go through the
//! `SecretStore` seam (P07; P16's OS keychain in the desktop app) and are never returned,
//! logged, put in metrics or synced.
//!
//! - `POST /secrets {name, kind, allowed_uses, env_var?}`: a reference. With `env_var`, the
//!   value is read from that environment variable of the server when resolved. Solo only:
//!   on a team server the environment holds the server's own credentials, so tenants may
//!   never point at it.
//! - `PUT /secrets/{sec}/value {value}`: write-only; the answer is `204` with no body.
//! - `DELETE /secrets/{sec}`: marks the secret as being deleted, removes the value from the
//!   store, then the metadata and grants. A failure leaves it marked (rotation and resolution
//!   refuse it, MRL-CONFLICT-192); deleting again finishes. Rotation holds the row lock while
//!   it writes, so the two never interleave, even across server replicas.
//! - [`SecretResolver::grant`]: lets a run receive the secret (needs the `run` use). There is
//!   no public endpoint: the runs module (P17) calls it in the transaction where it checked
//!   the run and the caller's permission, since only it knows whether a run id is real.
//! - [`SecretResolver`]: the only way to read a value; it enforces `allowed_uses`.
//!   P20 delivers granted secrets to agents with [`SecretResolver::resolve_for_run`].
//!
//! Reading one secret and listing secrets check the same resource: the secret with the
//! project recorded on it.

use crate::app::AppState;
use crate::config::Mode;
use crate::db::{Db, Tx};
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::modules::artifacts::{
    check_name, codes, field, not_found, org_of, owned, resource, ArtifactStore, Error,
};
use crate::seams::{
    AuditEvent, AuditOutcome, Decision, RequestContext, Resource, Secret, SecretStore,
};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use mrl_domain::ids::{RunId, SecretId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::OffsetDateTime;
use uuid::Uuid;

/// Kinds a secret can have.
pub const KINDS: [&str; 5] = ["hf_token", "cloud_credentials", "s3", "oidc", "generic"];
/// The use a run grant needs.
pub const RUN_USE: &str = "run";
/// Largest value, in bytes.
pub const MAX_VALUE: usize = 64 * 1024;
/// The action that reads secret metadata.
const READ: &str = "tenancy.read";
/// The action that changes secrets.
const MANAGE: &str = "secrets.manage";

/// `POST /secrets` body.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NewSecret {
    /// Unique name in the org (or project).
    pub name: String,
    /// `hf_token`, `cloud_credentials`, `s3`, `oidc` or `generic`.
    pub kind: String,
    /// What the secret may be used for, like `run` or `model_download`.
    pub allowed_uses: Vec<String>,
    /// Read the value from this environment variable of the server instead of storing it.
    #[serde(default)]
    pub env_var: Option<String>,
    /// Project, if the secret belongs to one.
    #[serde(default)]
    pub project_id: Option<String>,
}

/// A secret reference. Never has the value.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct SecretMeta {
    /// `sec_…`.
    pub id: String,
    /// Name.
    pub name: String,
    /// Kind.
    pub kind: String,
    /// `secret_store` or `env`.
    pub backend: String,
    /// The environment variable, for `env` secrets.
    pub env_var: Option<String>,
    /// Allowed uses.
    pub allowed_uses: Vec<String>,
    /// Project.
    pub project_id: Option<String>,
    /// Who made it.
    pub created_by: String,
    /// When it was made.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
    /// When its value was last saved; `null` if never.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = String, format = DateTime)]
    pub rotated_at: Option<OffsetDateTime>,
}

/// A secret a run may receive, with its value. `Debug` hides the value.
#[derive(Debug, Clone)]
pub struct GrantedSecret {
    /// The secret's id.
    pub id: SecretId,
    /// Its name.
    pub name: String,
    /// `env` or `file`.
    pub mount: String,
    /// The value.
    pub value: Secret,
}

#[derive(sqlx::FromRow)]
struct Row {
    id: Uuid,
    project_id: Option<String>,
    name: String,
    backend: String,
    #[sqlx(rename = "ref")]
    reference: String,
    kind: String,
    allowed_uses: Vec<String>,
    created_by: String,
    created_at: OffsetDateTime,
    rotated_at: Option<OffsetDateTime>,
    deleting: bool,
}

impl Row {
    /// What every check of this secret is authorized against.
    fn resource(&self, org: &str) -> Resource {
        owned(
            "secret",
            org,
            SecretId::from_uuid(self.id).to_string(),
            self.project_id.clone(),
        )
    }
}

macro_rules! columns {
    () => {
        "id, project_id, name, backend, ref, kind, allowed_uses, created_by, created_at, rotated_at, deleting"
    };
}

impl From<Row> for SecretMeta {
    fn from(r: Row) -> Self {
        Self {
            id: SecretId::from_uuid(r.id).to_string(),
            env_var: (r.backend == "env").then_some(r.reference),
            name: r.name,
            kind: r.kind,
            backend: r.backend,
            allowed_uses: r.allowed_uses,
            project_id: r.project_id,
            created_by: r.created_by,
            created_at: r.created_at,
            rotated_at: r.rotated_at,
        }
    }
}

/// Reads secret values, enforcing each secret's `allowed_uses`. Values come only from the
/// `SecretStore` seam or, for `env` secrets, the server's environment.
#[derive(Clone)]
pub struct SecretResolver {
    db: Db,
    mode: Mode,
}

impl SecretResolver {
    /// A resolver over this database for a server in `mode`. Only solo servers resolve
    /// `env` secrets.
    pub fn new(db: Db, mode: Mode) -> Self {
        Self { db, mode }
    }

    /// The secret, also while it is being deleted.
    async fn row_any(&self, org: &str, id: Uuid) -> Result<Row, Error> {
        sqlx::query_as(concat!(
            "SELECT ",
            columns!(),
            " FROM secrets_metadata WHERE org_id = $1 AND id = $2"
        ))
        .bind(org)
        .bind(id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| not_found("secret"))
    }

    /// The secret, unless it is being deleted.
    async fn row(&self, org: &str, id: Uuid) -> Result<Row, Error> {
        let row = self.row_any(org, id).await?;
        if row.deleting {
            return Err(deleting(&row));
        }
        Ok(row)
    }

    /// Lets `run` receive `secret` of `org`, mounted as `env` or `file`. Call it inside the
    /// transaction where the run's owner checked that `run` is a real run of `org` and that
    /// the caller may attach secrets to it. The secret must allow the `run` use and not be
    /// being deleted (its row is locked until `tx` ends, so a delete waits).
    pub async fn grant(
        &self,
        tx: &mut Tx,
        org: &str,
        secret: SecretId,
        run: RunId,
        mount: &str,
    ) -> Result<(), Error> {
        if !matches!(mount, "env" | "file") {
            return Err(field("/mount", "Use env or file."));
        }
        let row: Row = sqlx::query_as(concat!(
            "SELECT ",
            columns!(),
            " FROM secrets_metadata WHERE org_id = $1 AND id = $2 FOR SHARE"
        ))
        .bind(org)
        .bind(secret.as_uuid())
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| not_found("secret"))?;
        if row.deleting {
            return Err(deleting(&row));
        }
        if !row.allowed_uses.iter().any(|u| u == RUN_USE) {
            return Err(Error::coded(
                codes::SECRET_USE,
                StatusCode::FORBIDDEN,
                format!("The secret {:?} does not allow the use \"run\".", row.name),
            ));
        }
        sqlx::query(
            "INSERT INTO run_secret_grants (run_id, secret_id, mount) VALUES ($1, $2, $3)
             ON CONFLICT (run_id, secret_id) DO UPDATE SET mount = EXCLUDED.mount",
        )
        .bind(run.as_uuid())
        .bind(secret.as_uuid())
        .bind(mount)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// The value of `secret` in `org` for `use_` (MRL-PERM-190 when the secret does not allow it).
    pub async fn resolve(
        &self,
        store: Option<&Arc<dyn SecretStore>>,
        org: &str,
        secret: SecretId,
        use_: &str,
    ) -> Result<Secret, Error> {
        let row = self.row(org, secret.as_uuid()).await?;
        value(store, &row, use_, self.mode).await
    }

    /// Every secret granted to `run` in `org`, with its value, for delivery to the run's agent.
    /// Each must allow `run`.
    pub async fn resolve_for_run(
        &self,
        store: Option<&Arc<dyn SecretStore>>,
        org: &str,
        run: RunId,
    ) -> Result<Vec<GrantedSecret>, Error> {
        let rows: Vec<(String, Row)> = sqlx::query_as::<_, GrantRow>(
            "SELECT g.mount, s.id, s.project_id, s.name, s.backend, s.ref, s.kind, s.allowed_uses,
                    s.created_by, s.created_at, s.rotated_at, s.deleting
             FROM run_secret_grants g JOIN secrets_metadata s ON s.id = g.secret_id
             WHERE s.org_id = $1 AND g.run_id = $2 AND NOT s.deleting ORDER BY s.name",
        )
        .bind(org)
        .bind(run.as_uuid())
        .fetch_all(self.db.pool())
        .await?
        .into_iter()
        .map(|g| (g.mount, g.row))
        .collect();
        let mut out = Vec::with_capacity(rows.len());
        for (mount, row) in rows {
            out.push(GrantedSecret {
                value: value(store, &row, RUN_USE, self.mode).await?,
                id: SecretId::from_uuid(row.id),
                name: row.name,
                mount,
            });
        }
        Ok(out)
    }
}

#[derive(sqlx::FromRow)]
struct GrantRow {
    mount: String,
    #[sqlx(flatten)]
    row: Row,
}

async fn value(
    store: Option<&Arc<dyn SecretStore>>,
    row: &Row,
    use_: &str,
    mode: Mode,
) -> Result<Secret, Error> {
    if !row.allowed_uses.iter().any(|u| u == use_) {
        return Err(Error::coded(
            codes::SECRET_USE,
            StatusCode::FORBIDDEN,
            format!("The secret {:?} does not allow the use {use_:?}.", row.name),
        ));
    }
    let empty = || {
        Error::coded(
            codes::SECRET_EMPTY,
            StatusCode::CONFLICT,
            format!("The secret {:?} has no value yet.", row.name),
        )
    };
    if row.backend == "env" {
        if mode != Mode::Solo {
            return Err(env_refused());
        }
        return std::env::var(&row.reference)
            .map(Secret::new)
            .map_err(|_| empty());
    }
    store_of(store)?
        .get(&row.reference)
        .await?
        .ok_or_else(empty)
}

fn deleting(row: &Row) -> Error {
    Error::coded(
        codes::SECRET_DELETING,
        StatusCode::CONFLICT,
        format!(
            "The secret {:?} is being deleted. Delete it again to finish.",
            row.name
        ),
    )
}

fn env_refused() -> Error {
    ApiError::new(
        ErrorCode::Forbidden,
        "Secrets read from environment variables work only on a solo server.",
    )
    .into()
}

fn store_of(store: Option<&Arc<dyn SecretStore>>) -> Result<&Arc<dyn SecretStore>, Error> {
    store.ok_or_else(|| {
        Error::coded(
            codes::NO_SECRET_STORE,
            StatusCode::SERVICE_UNAVAILABLE,
            "This server has no secret store, so it can't keep secret values.",
        )
    })
}

fn parse_id(s: &str) -> Result<Uuid, Error> {
    s.parse::<SecretId>()
        .map(|i| i.as_uuid())
        .map_err(|_| not_found("secret"))
}

async fn audit(
    s: &AppState,
    ctx: &RequestContext,
    org: &str,
    action: &str,
    target: String,
) -> Result<(), Error> {
    s.seams
        .audit
        .record(AuditEvent {
            at: s.seams.clock.now(),
            actor: ctx.principal.id.clone(),
            org: Some(org.to_owned()),
            project: ctx.project.clone(),
            action: action.to_owned(),
            target,
            outcome: AuditOutcome::Success,
            trace_id: ctx.trace_id.clone(),
        })
        .await?;
    Ok(())
}

fn check_new(new: &NewSecret) -> Result<Vec<String>, Error> {
    let mut errors = Vec::new();
    let bad = |path: &str, msg: &str| {
        FieldError::new(
            path,
            ErrorCode::FieldValue,
            msg,
            "Change this field and send the request again.",
        )
    };
    if check_name(&new.name, "/name").is_err() {
        errors.push(bad(
            "/name",
            "Names have 1 to 128 letters, digits, dots, dashes or underscores.",
        ));
    }
    if !KINDS.contains(&new.kind.as_str()) {
        errors.push(bad(
            "/kind",
            "Use hf_token, cloud_credentials, s3, oidc or generic.",
        ));
    }
    let use_ok = |u: &String| {
        (1..=64).contains(&u.len())
            && u.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    };
    if new.allowed_uses.is_empty()
        || new.allowed_uses.len() > 32
        || !new.allowed_uses.iter().all(use_ok)
    {
        errors.push(bad(
            "/allowed_uses",
            "List 1 to 32 uses, each made of lowercase letters, digits and underscores.",
        ));
    }
    if let Some(v) = &new.env_var {
        let ok = (1..=128).contains(&v.len())
            && !v.as_bytes()[0].is_ascii_digit()
            && v.bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
        if !ok {
            errors.push(bad(
                "/env_var",
                "Use capital letters, digits and underscores, like HF_TOKEN.",
            ));
        }
    }
    if !errors.is_empty() {
        return Err(ApiError::validation(errors).into());
    }
    let mut uses = new.allowed_uses.clone();
    uses.sort();
    uses.dedup();
    Ok(uses)
}

/// Creates a secret reference (no value).
#[utoipa::path(post, path = "/secrets", tag = "secrets", request_body = NewSecret,
    responses((status = 201, body = SecretMeta), (status = 409, body = crate::error::Problem)))]
pub(crate) async fn create(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Json(new): Json<NewSecret>,
) -> Result<(StatusCode, Json<SecretMeta>), Error> {
    s.authorize(&ctx, MANAGE, &resource(&ctx, "secret", None))
        .await?;
    let uses = check_new(&new)?;
    if new.env_var.is_some() && s.config.server.mode != Mode::Solo {
        return Err(env_refused());
    }
    let org = org_of(&ctx)?;
    let id = s.seams.ids.new_uuid();
    let (backend, reference) = match &new.env_var {
        Some(v) => ("env", v.clone()),
        None => (
            "secret_store",
            format!("mrl.secret.{org}.{}", SecretId::from_uuid(id)),
        ),
    };
    let row: Result<Row, sqlx::Error> = sqlx::query_as(concat!(
        "INSERT INTO secrets_metadata (id, org_id, project_id, name, backend, ref, kind, allowed_uses, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING ",
        columns!()
    ))
    .bind(id)
    .bind(&org)
    .bind(&new.project_id)
    .bind(&new.name)
    .bind(backend)
    .bind(&reference)
    .bind(&new.kind)
    .bind(&uses)
    .bind(&ctx.principal.id)
    .fetch_one(store.db().pool())
    .await;
    let row = match row {
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23505") => {
            return Err(Error::coded(
                codes::NAME_TAKEN,
                StatusCode::CONFLICT,
                format!("A secret named {:?} already exists here.", new.name),
            ))
        }
        other => other?,
    };
    audit(
        &s,
        &ctx,
        &org,
        "secrets.create",
        SecretId::from_uuid(id).to_string(),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(row.into())))
}

/// Lists secret references.
#[utoipa::path(get, path = "/secrets", tag = "secrets",
    responses((status = 200, body = Object, description = "`{items: [SecretMeta]}`")))]
pub(crate) async fn list(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
) -> Result<Json<serde_json::Value>, Error> {
    s.authorize(&ctx, READ, &resource(&ctx, "secret", None))
        .await?;
    let org = org_of(&ctx)?;
    // ponytail: no paging; an org has few secrets. Add PageQuery when that changes.
    let rows: Vec<Row> = sqlx::query_as(concat!(
        "SELECT ",
        columns!(),
        " FROM secrets_metadata WHERE org_id = $1 AND NOT deleting ORDER BY name, id"
    ))
    .bind(&org)
    .fetch_all(store.db().pool())
    .await?;
    // Each entry is checked like a GET of it.
    let mut items: Vec<SecretMeta> = Vec::with_capacity(rows.len());
    for row in rows {
        if s.seams
            .authorizer
            .authorize(&ctx, READ, &row.resource(&org))
            .await?
            == Decision::Allow
        {
            items.push(row.into());
        }
    }
    Ok(Json(serde_json::json!({ "items": items })))
}

/// Reads one secret reference.
#[utoipa::path(get, path = "/secrets/{sec}", tag = "secrets", params(("sec" = String, Path, description = "`sec_…`")),
    responses((status = 200, body = SecretMeta), (status = 404, body = crate::error::Problem)))]
pub(crate) async fn get(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Path(sec): Path<String>,
) -> Result<Json<SecretMeta>, Error> {
    let id = parse_id(&sec)?;
    let org = org_of(&ctx)?;
    let row = SecretResolver::new(store.db().clone(), s.config.server.mode)
        .row_any(&org, id)
        .await?;
    s.authorize(&ctx, READ, &row.resource(&org)).await?;
    if row.deleting {
        return Err(deleting(&row));
    }
    Ok(Json(row.into()))
}

/// Saves a secret's value. Write-only: the answer has no body.
#[utoipa::path(put, path = "/secrets/{sec}/value", tag = "secrets", params(("sec" = String, Path, description = "`sec_…`")),
    request_body(content = Object, description = "`{\"value\": \"…\"}`"),
    responses((status = 204, description = "Saved."), (status = 503, body = crate::error::Problem)))]
pub(crate) async fn set_value(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Path(sec): Path<String>,
    body: Bytes,
) -> Result<StatusCode, Error> {
    let id = parse_id(&sec)?;
    let org = org_of(&ctx)?;
    let row = SecretResolver::new(store.db().clone(), s.config.server.mode)
        .row_any(&org, id)
        .await?;
    s.authorize(&ctx, MANAGE, &row.resource(&org)).await?;
    // Parsed by hand so no error message can echo the value.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Value {
        value: String,
    }
    let value = serde_json::from_slice::<Value>(&body)
        .map_err(|_| {
            ApiError::new(
                ErrorCode::BadBody,
                "The body must be JSON like {\"value\": \"…\"}.",
            )
        })?
        .value;
    if value.is_empty() || value.len() > MAX_VALUE {
        return Err(field("/value", "The value needs 1 byte to 64 KiB."));
    }
    if row.backend == "env" {
        return Err(field(
            "/value",
            "This secret reads an environment variable; set that variable on the server instead.",
        ));
    }
    let secrets = store_of(s.seams.secrets.as_ref())?;
    // The row stays locked while the value is written: a delete waits for it, and a
    // rotation that starts after a delete began is refused.
    let mut tx = store.db().pool().begin().await?;
    let locked: Option<bool> =
        sqlx::query_scalar("SELECT deleting FROM secrets_metadata WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
    match locked {
        None => return Err(not_found("secret")),
        Some(true) => return Err(deleting(&row)),
        Some(false) => {}
    }
    secrets.set(&row.reference, Secret::new(value)).await?;
    sqlx::query("UPDATE secrets_metadata SET rotated_at = now() WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    audit(&s, &ctx, &org, "secrets.set_value", sec).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Deletes a secret reference, its value and its grants.
#[utoipa::path(delete, path = "/secrets/{sec}", tag = "secrets", params(("sec" = String, Path, description = "`sec_…`")),
    responses((status = 204, description = "Deleted."), (status = 404, body = crate::error::Problem)))]
pub(crate) async fn delete(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Path(sec): Path<String>,
) -> Result<StatusCode, Error> {
    let id = parse_id(&sec)?;
    let org = org_of(&ctx)?;
    let row = SecretResolver::new(store.db().clone(), s.config.server.mode)
        .row_any(&org, id)
        .await?;
    s.authorize(&ctx, MANAGE, &row.resource(&org)).await?;
    // 1. Mark it. This waits for a rotation in progress (it holds the row lock) and makes
    //    later rotations, grants and resolutions refuse it.
    sqlx::query("UPDATE secrets_metadata SET deleting = true WHERE org_id = $1 AND id = $2")
        .bind(&org)
        .bind(id)
        .execute(store.db().pool())
        .await?;
    // 2. Remove the value and make sure it is gone. A store error keeps the marked row, so
    //    the reference is not lost; deleting again carries on from here.
    if row.backend == "secret_store" {
        if let Some(st) = s.seams.secrets.as_ref() {
            st.delete(&row.reference).await?;
            if st.get(&row.reference).await?.is_some() {
                return Err(ApiError::new(
                    ErrorCode::Unavailable,
                    "The secret store still has the value. Delete the secret again.",
                )
                .into());
            }
        }
    }
    // 3. The metadata, and with it the grants.
    sqlx::query("DELETE FROM secrets_metadata WHERE org_id = $1 AND id = $2")
        .bind(&org)
        .bind(id)
        .execute(store.db().pool())
        .await?;
    audit(&s, &ctx, &org, "secrets.delete", sec).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(create, list, get, set_value, delete),
    components(schemas(NewSecret, SecretMeta)),
    tags((name = "secrets", description = "Secret references. Values are write-only."))
)]
pub(crate) struct Doc;

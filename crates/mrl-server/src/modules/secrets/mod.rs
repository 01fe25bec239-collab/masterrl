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
//!   store, then the metadata and grants. A failure, or a server running without its store,
//!   leaves it marked (rotation and resolution refuse it, MRL-CONFLICT-192); deleting again
//!   finishes. Rotation holds the row lock while
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
    allow, check_name, codes, destination, field, is_idempotency_conflict, not_found, org_of,
    owned, resource, ArtifactStore, Created, Error, Idempotency,
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
pub(crate) const READ: &str = "tenancy.read";
/// The action that changes secrets.
const MANAGE: &str = "secrets.manage";

/// `POST /secrets` body.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
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

    /// Deletes secret `id` of `org`: marks it (which waits for a rotation in progress, since
    /// that holds the row lock, and makes later rotations, grants and resolutions refuse
    /// it), removes the value from `store` and checks it is gone, then deletes the metadata
    /// and grants. A stored secret needs the store: without one, or when it fails, the
    /// marked row stays and the error is retryable (503); deleting again carries on.
    pub async fn delete(
        &self,
        store: Option<&Arc<dyn SecretStore>>,
        org: &str,
        id: Uuid,
    ) -> Result<(), Error> {
        self.delete_with_audit(store, org, id, None).await
    }

    async fn delete_with_audit(
        &self,
        store: Option<&Arc<dyn SecretStore>>,
        org: &str,
        id: Uuid,
        audit_id: Option<Uuid>,
    ) -> Result<(), Error> {
        let mut lock = SecretLock::take(&self.db, id).await?;
        let row: Option<(String, String, Option<String>)> = sqlx::query_as(
            "UPDATE secrets_metadata SET deleting = true WHERE org_id = $1 AND id = $2
             RETURNING backend, ref, rotation_ref",
        )
        .bind(org)
        .bind(id)
        .fetch_optional(lock.connection())
        .await?;
        let Some((backend, reference, extra)) = row else {
            return Err(not_found("secret"));
        };
        if backend == "secret_store" {
            let st = store_of(store)?;
            remove_value(st.as_ref(), &reference).await?;
            if let Some(extra) = extra {
                remove_value(st.as_ref(), &extra).await?;
            }
        }
        let mut tx = sqlx::Connection::begin(lock.connection()).await?;
        sqlx::query("DELETE FROM secrets_metadata WHERE org_id = $1 AND id = $2")
            .bind(org)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        finish_audit(&mut tx, audit_id, "success").await?;
        tx.commit().await?;
        Ok(())
    }

    /// Lets `run` receive `secret` of `org`, mounted as `env` or `file`. Call it inside the
    /// transaction where the run's owner checked that `run` is a real run of `org` and that
    /// the caller may attach secrets to it. The secret must allow the `run` use and not be
    /// being deleted (its row is locked until `tx` ends, so a delete waits).
    #[allow(clippy::too_many_arguments)]
    pub async fn grant(
        &self,
        seams: &crate::seams::Seams,
        ctx: &RequestContext,
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
        // The caller must be able to manage this very secret (with its project), not only
        // the run.
        allow(seams, ctx, MANAGE, &row.resource(org)).await?;
        write_grant(tx, run, &row, mount).await
    }

    /// Authorizes a future run grant before the caller opens its write transaction.
    pub(crate) async fn prepare_grant(
        &self,
        seams: &crate::seams::Seams,
        ctx: &RequestContext,
        org: &str,
        secret: SecretId,
        mount: &str,
    ) -> Result<AuthorizedGrant, Error> {
        let row: Row = sqlx::query_as(concat!(
            "SELECT ",
            columns!(),
            " FROM secrets_metadata WHERE org_id = $1 AND id = $2"
        ))
        .bind(org)
        .bind(secret.as_uuid())
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| not_found("secret"))?;
        allow(seams, ctx, MANAGE, &row.resource(org)).await?;
        Ok(AuthorizedGrant {
            org: org.to_owned(),
            secret,
            mount: mount.to_owned(),
        })
    }

    /// The value of `secret` in `org` for `use_` (MRL-PERM-190 when the secret does not allow it).
    pub async fn resolve(
        &self,
        store: Option<&Arc<dyn SecretStore>>,
        org: &str,
        secret: SecretId,
        use_: &str,
    ) -> Result<Secret, Error> {
        let mut tx = self.db.pool().begin().await?;
        let row: Row = sqlx::query_as(concat!(
            "SELECT ",
            columns!(),
            " FROM secrets_metadata WHERE org_id = $1 AND id = $2 FOR SHARE"
        ))
        .bind(org)
        .bind(secret.as_uuid())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| not_found("secret"))?;
        if row.deleting {
            return Err(deleting(&row));
        }
        let value = value(store, &row, use_, self.mode).await?;
        tx.commit().await?;
        Ok(value)
    }

    /// Every secret granted to `run` in `org`, with its value, for delivery to the run's agent.
    /// Each must allow `run`.
    pub async fn resolve_for_run(
        &self,
        store: Option<&Arc<dyn SecretStore>>,
        org: &str,
        run: RunId,
    ) -> Result<Vec<GrantedSecret>, Error> {
        let mut tx = self.db.pool().begin().await?;
        let rows: Vec<(String, Row)> = sqlx::query_as::<_, GrantRow>(
            "SELECT g.mount, s.id, s.project_id, s.name, s.backend, s.ref, s.kind, s.allowed_uses,
                    s.created_by, s.created_at, s.rotated_at, s.deleting
             FROM run_secret_grants g JOIN secrets_metadata s ON s.id = g.secret_id
             WHERE s.org_id = $1 AND g.run_id = $2 AND NOT s.deleting ORDER BY s.name FOR SHARE OF s",
        )
        .bind(org)
        .bind(run.as_uuid())
        .fetch_all(&mut *tx)
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
        tx.commit().await?;
        Ok(out)
    }
}

/// Permission to attach one secret, checked against its immutable org and project.
pub(crate) struct AuthorizedGrant {
    org: String,
    secret: SecretId,
    mount: String,
}

impl AuthorizedGrant {
    /// Locks and rechecks the secret inside the run transaction before recording the grant.
    pub(crate) async fn grant(self, tx: &mut Tx, run: RunId) -> Result<(), Error> {
        let row: Row = sqlx::query_as(concat!(
            "SELECT ",
            columns!(),
            " FROM secrets_metadata WHERE org_id = $1 AND id = $2 FOR SHARE"
        ))
        .bind(&self.org)
        .bind(self.secret.as_uuid())
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| not_found("secret"))?;
        write_grant(tx, run, &row, &self.mount).await
    }
}

async fn write_grant(tx: &mut Tx, run: RunId, row: &Row, mount: &str) -> Result<(), Error> {
    if row.deleting {
        return Err(deleting(row));
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
    .bind(row.id)
    .bind(mount)
    .execute(&mut **tx)
    .await?;
    Ok(())
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

/// A PostgreSQL session lock on one secret, held on a connection of its own. Rotation and
/// deletion take it, so they never interleave, even across server replicas. The connection
/// is closed instead of reused when this drops, which releases the lock however the
/// request ends (finished, failed or cancelled).
struct SecretLock(sqlx::pool::PoolConnection<sqlx::Postgres>);

impl SecretLock {
    fn connection(&mut self) -> &mut sqlx::PgConnection {
        &mut self.0
    }

    async fn take(db: &Db, id: Uuid) -> Result<Self, Error> {
        let mut conn = db.pool().acquire().await?;
        conn.close_on_drop();
        sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 0))")
            .bind(format!("mrl.secret:{id}"))
            .execute(&mut *conn)
            .await?;
        Ok(Self(conn))
    }
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

/// The attempt event is required before mutation. Its action names the attempt, so a
/// process stop at this point cannot leave a false record of a completed change. The event
/// and the outcome row commit together (`record_in`: the audit sink stores successful
/// changes only inside the transaction that makes them).
async fn audit_attempt(
    db: &Db,
    s: &AppState,
    ctx: &RequestContext,
    org: &str,
    action: &str,
    target: &str,
) -> Result<Uuid, Error> {
    let mut tx = db.begin().await?;
    let event = AuditEvent {
        at: s.seams.clock.now(),
        actor: ctx.principal.id.clone(),
        org: Some(org.to_owned()),
        project: ctx.project.clone(),
        action: format!("{action}.attempt"),
        target: target.to_owned(),
        outcome: AuditOutcome::Success,
        trace_id: ctx.trace_id.clone(),
    };
    s.seams.audit.record_in(&mut tx, event).await?;
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO secret_mutation_audit (id, org_id, actor, project_id, action, target, trace_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(id)
    .bind(org)
    .bind(&ctx.principal.id)
    .bind(&ctx.project)
    .bind(action)
    .bind(target)
    .bind(&ctx.trace_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(id)
}

async fn finish_audit(
    conn: &mut sqlx::PgConnection,
    id: Option<Uuid>,
    outcome: &str,
) -> Result<(), Error> {
    if let Some(id) = id {
        sqlx::query(
            "UPDATE secret_mutation_audit SET outcome = $2, finished_at = clock_timestamp()
             WHERE id = $1 AND outcome IS NULL",
        )
        .bind(id)
        .bind(outcome)
        .execute(conn)
        .await?;
    }
    Ok(())
}

#[derive(sqlx::FromRow)]
struct AuditRow {
    finished_at: OffsetDateTime,
    org_id: String,
    actor: String,
    project_id: Option<String>,
    action: String,
    target: String,
    trace_id: String,
    outcome: String,
}

/// Forward the durable outcome to the configured sink, in one transaction with it.
async fn audit_outcome(db: &Db, s: &AppState, id: Uuid, failed: bool) {
    let forwarded: Result<bool, Error> = async {
        let mut tx = db.begin().await?;
        if failed {
            finish_audit(&mut tx, Some(id), "failed").await?;
        }
        let row: Option<AuditRow> = sqlx::query_as(
            "SELECT finished_at, org_id, actor, project_id, action, target, trace_id, outcome
             FROM secret_mutation_audit WHERE id = $1 AND outcome IS NOT NULL",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        s.seams.audit.record_in(&mut tx, outcome_event(row)).await?;
        tx.commit().await?;
        Ok(true)
    }
    .await;
    match forwarded {
        Ok(true) => {}
        Ok(false) => tracing::warn!(audit_id = %id, "could not read the secret change outcome"),
        Err(_) => tracing::warn!(audit_id = %id, "could not forward the secret change outcome"),
    }
}

/// The audit event for a finished secret change.
fn outcome_event(row: AuditRow) -> AuditEvent {
    AuditEvent {
        at: row.finished_at,
        org: Some(row.org_id),
        actor: row.actor,
        project: row.project_id,
        action: row.action,
        target: row.target,
        trace_id: row.trace_id,
        outcome: if row.outcome == "success" {
            AuditOutcome::Success
        } else {
            AuditOutcome::Failed
        },
    }
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
#[utoipa::path(post, path = "/secrets", tag = "secrets", operation_id = "create_secret", request_body = NewSecret,
    responses((status = 201, body = SecretMeta), (status = 409, body = crate::error::Problem)))]
pub(crate) async fn create(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    headers: axum::http::HeaderMap,
    Json(new): Json<NewSecret>,
) -> Result<(StatusCode, Json<SecretMeta>), Error> {
    let org = org_of(&ctx)?;
    s.authorize(
        &ctx,
        MANAGE,
        &destination("secret", &org, new.project_id.clone()),
    )
    .await?;
    let uses = check_new(&new)?;
    if new.env_var.is_some() && s.config.server.mode != Mode::Solo {
        return Err(env_refused());
    }
    // A retry with the same Idempotency-Key (authorized afresh above) gets the same secret.
    let idem = Idempotency::of(&headers, &new);
    let resolver = SecretResolver::new(store.db().clone(), s.config.server.mode);
    let earlier = || async {
        match &idem {
            Some(i) => match i
                .earlier(store.db(), Created::Secret, &org, &ctx.principal.id)
                .await?
            {
                Some(id) => {
                    let row = resolver.row_any(&org, id).await?;
                    s.authorize(&ctx, READ, &row.resource(&org)).await?;
                    if row.deleting {
                        return Err(deleting(&row));
                    }
                    Ok(Some(row))
                }
                None => Ok(None),
            },
            None => Ok::<_, Error>(None),
        }
    };
    if let Some(row) = earlier().await? {
        return Ok((StatusCode::CREATED, Json(row.into())));
    }
    let id = s.seams.ids.new_uuid();
    let (backend, reference) = match &new.env_var {
        Some(v) => ("env", v.clone()),
        None => (
            "secret_store",
            format!("mrl.secret.{org}.{}", SecretId::from_uuid(id)),
        ),
    };
    let target = SecretId::from_uuid(id).to_string();
    let audit_id = audit_attempt(store.db(), &s, &ctx, &org, "secrets.create", &target).await?;
    let mut tx = store.db().pool().begin().await?;
    let row: Result<Row, sqlx::Error> = sqlx::query_as(concat!(
        "INSERT INTO secrets_metadata (id, org_id, project_id, name, backend, ref, kind, allowed_uses, created_by, idempotency_key, request_sha256)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING ",
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
    .bind(idem.as_ref().map(|i| &i.key))
    .bind(idem.as_ref().map(|i| &i.request_sha256))
    .fetch_one(&mut *tx)
    .await;
    let row = match row {
        Ok(row) => {
            finish_audit(&mut tx, Some(audit_id), "success").await?;
            tx.commit().await?;
            audit_outcome(store.db(), &s, audit_id, false).await;
            row
        }
        Err(e) => {
            tx.rollback().await?;
            audit_outcome(store.db(), &s, audit_id, true).await;
            if is_idempotency_conflict(&e) {
                earlier().await?.ok_or_else(|| not_found("secret"))?
            } else if matches!(&e, sqlx::Error::Database(d) if d.code().as_deref() == Some("23505"))
            {
                return Err(Error::coded(
                    codes::NAME_TAKEN,
                    StatusCode::CONFLICT,
                    format!("A secret named {:?} already exists here.", new.name),
                ));
            } else {
                return Err(e.into());
            }
        }
    };
    Ok((StatusCode::CREATED, Json(row.into())))
}

/// Lists secret references.
#[utoipa::path(get, path = "/secrets", tag = "secrets", operation_id = "list_secrets",
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
#[utoipa::path(get, path = "/secrets/{sec}", tag = "secrets", operation_id = "get_secret", params(("sec" = String, Path, description = "`sec_…`")),
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
    let audit_id = audit_attempt(store.db(), &s, &ctx, &org, "secrets.set_value", &sec).await?;
    let result = rotate(
        store.db(),
        &row,
        secrets.as_ref(),
        Secret::new(value),
        audit_id,
    )
    .await;
    audit_outcome(store.db(), &s, audit_id, result.is_err()).await;
    result?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_value(store: &dyn SecretStore, reference: &str) -> Result<(), Error> {
    store.delete(reference).await?;
    if store.get(reference).await?.is_some() {
        return Err(ApiError::new(
            ErrorCode::Unavailable,
            "The secret store still has the old value. Try again.",
        )
        .into());
    }
    Ok(())
}

async fn rotate(
    db: &Db,
    row: &Row,
    store: &dyn SecretStore,
    value: Secret,
    audit_id: Uuid,
) -> Result<(), Error> {
    let mut lock = SecretLock::take(db, row.id).await?;
    let (deleting_now, previous): (bool, Option<String>) =
        sqlx::query_as("SELECT deleting, rotation_ref FROM secrets_metadata WHERE id = $1")
            .bind(row.id)
            .fetch_optional(lock.connection())
            .await?
            .ok_or_else(|| not_found("secret"))?;
    if deleting_now {
        return Err(deleting(row));
    }
    if let Some(previous) = previous {
        remove_value(store, &previous).await?;
    }

    // The old reference stays readable until the new value is saved. Either side of a
    // crash leaves the extra reference recorded for the next rotation or deletion.
    let candidate = format!("mrl.secret.rotation.{}", Uuid::new_v4().simple());
    sqlx::query("UPDATE secrets_metadata SET rotation_ref = $2 WHERE id = $1")
        .bind(row.id)
        .bind(&candidate)
        .execute(lock.connection())
        .await?;
    store.set(&candidate, value).await?;
    let mut tx = sqlx::Connection::begin(lock.connection()).await?;
    let retired: String = sqlx::query_scalar(
        "UPDATE secrets_metadata SET ref = rotation_ref, rotation_ref = ref, rotated_at = clock_timestamp()
         WHERE id = $1 RETURNING rotation_ref",
    )
    .bind(row.id)
    .fetch_one(&mut *tx)
    .await?;
    finish_audit(&mut tx, Some(audit_id), "success").await?;
    tx.commit().await?;
    if let Err(e) = remove_value(store, &retired).await {
        tracing::warn!(code = e.code(), "could not remove a retired secret value");
    } else if let Err(e) =
        sqlx::query("UPDATE secrets_metadata SET rotation_ref = NULL WHERE id = $1")
            .bind(row.id)
            .execute(lock.connection())
            .await
    {
        tracing::warn!(error = %e, "could not clear a retired secret reference");
    }
    Ok(())
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
    let audit_id = audit_attempt(store.db(), &s, &ctx, &org, "secrets.delete", &sec).await?;
    let result = SecretResolver::new(store.db().clone(), s.config.server.mode)
        .delete_with_audit(s.seams.secrets.as_ref(), &org, id, Some(audit_id))
        .await;
    audit_outcome(store.db(), &s, audit_id, result.is_err()).await;
    result?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(create, list, get, set_value, delete),
    components(schemas(NewSecret, SecretMeta)),
    tags((name = "secrets", description = "Secret references. Values are write-only."))
)]
pub(crate) struct Doc;

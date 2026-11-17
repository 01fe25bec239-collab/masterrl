//! The `artifacts` module (P19): object storage for the server. It serves uploads
//! (`modules::uploads`), the generic artifact registry, the server-proxied transfer endpoint,
//! lineage (`modules::lineage`) and secret references (`modules::secrets`). All of them share
//! one [`ArtifactStore`], so tickets one part issues verify everywhere.
//!
//! Storage comes from `[object_store]`: `file:///path` (local folder) or `s3://bucket`
//! (credentials from the standard `AWS_*` variables; `endpoint` for SeaweedFS or MinIO).
//! Unset, it is the folder `objects` next to the bundled database's data folder.
//!
//! ```text
//! let store = module.store();                                    // Arc<ArtifactStore>
//! let t = store.ticket(&state, &ctx, &key, TicketMethod::Get, max).await?;  // audited
//! let art = store.register(&state, &ctx, new_artifact).await?;   // checks every object
//! ```

mod error;
mod transfer;

pub use error::{codes, render, Error};
pub use transfer::SHA256_HEADER;

use crate::app::{AppState, ServerModule, Task};
use crate::config::{Config, Role};
use crate::db::{Db, LeaderElection};
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::http::pagination::{Page, PageQuery};
use crate::modules::lineage::{LineageWriter, NodeRef, Relation};
use crate::seams::{
    AuditEvent, AuditOutcome, PrincipalKind, RequestContext, Resource, SecretStore,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::middleware::from_fn;
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use mrl_domain::ids::ArtifactId;
use mrl_storage::{
    verify_ticket, ArtifactManifest, Backend, BackendConfig, S3Config, StorageError, Store,
    TicketClaims, TicketKey, TicketMethod, TransferTicket,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::OnceCell;
use utoipa::OpenApi;
use uuid::Uuid;

/// The org of a solo server, which has no org of its own.
pub const LOCAL_ORG: &str = "local";
/// Lifetime of every ticket this module issues.
pub const TICKET_TTL: Duration = Duration::from_secs(10 * 60);
/// Name of the ticket signing key in the `SecretStore`.
pub const TICKET_KEY_NAME: &str = "mrl.transfer_ticket_key";
/// How often the cleanup job runs.
const SWEEP_EVERY: Duration = Duration::from_secs(3600);

/// The module: routes, the cleanup job and the shared [`ArtifactStore`].
pub struct ArtifactsModule {
    store: Arc<ArtifactStore>,
}

impl ArtifactsModule {
    /// The module for this database and these settings. Connects to storage on first use.
    pub fn new(db: Db, config: &Config) -> Self {
        Self {
            store: Arc::new(ArtifactStore::new(db, backend_config(config))),
        }
    }

    /// The module with an explicit backend (tests and embedders).
    pub fn with_backend(db: Db, backend: BackendConfig) -> Self {
        Self {
            store: Arc::new(ArtifactStore::new(db, Ok(backend))),
        }
    }

    /// The shared store, for other modules.
    pub fn store(&self) -> Arc<ArtifactStore> {
        self.store.clone()
    }

    /// The secret resolver, for other modules (P20 delivers granted secrets with it).
    pub fn secrets(&self) -> crate::modules::secrets::SecretResolver {
        crate::modules::secrets::SecretResolver::new(self.store.db.clone())
    }
}

impl ServerModule for ArtifactsModule {
    fn name(&self) -> &'static str {
        "artifacts"
    }

    fn routes(&self) -> Router<AppState> {
        use crate::modules::{lineage, secrets, uploads};
        Router::new()
            .route("/uploads", post(uploads::create))
            .route("/uploads/{up}", get(uploads::get).post(uploads::action))
            .route("/uploads/{up}/parts/{n}", put(uploads::put_part))
            .route("/artifacts", get(list).post(register))
            .route("/artifacts/{art}", get(get_one).post(action))
            .route("/lineage/{id}", get(lineage::query))
            .route("/secrets", get(secrets::list).post(secrets::create))
            .route("/secrets/{sec}", get(secrets::get).delete(secrets::delete))
            .route("/secrets/{sec}/value", put(secrets::set_value))
            .route("/secrets/{sec}/grants", post(secrets::grant))
            .route_layer(from_fn(render))
            .layer(Extension(self.store.clone()))
    }

    fn public_routes(&self) -> Router<AppState> {
        Router::new()
            .route(
                "/transfer/{token}",
                get(transfer::download).put(transfer::upload),
            )
            .route_layer(from_fn(render))
            .layer(Extension(self.store.clone()))
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        let mut doc = Doc::openapi();
        doc.merge(crate::modules::uploads::Doc::openapi());
        doc.merge(crate::modules::lineage::Doc::openapi());
        doc.merge(crate::modules::secrets::Doc::openapi());
        doc
    }

    fn background_tasks(&self) -> Vec<Task> {
        let store = self.store.clone();
        vec![Task::new(
            "artifacts.cleanup",
            Role::Background,
            move |cancel| async move {
                let election = LeaderElection::new(
                    store.db.clone(),
                    "artifacts-cleanup",
                    Duration::from_secs(30),
                );
                election
                    .run(cancel, |lead| {
                        let store = store.clone();
                        async move {
                            loop {
                                match crate::modules::uploads::sweep(&store).await {
                                    Ok(swept) => tracing::info!(?swept, "upload cleanup done"),
                                    Err(e) => tracing::warn!(code = e.code(), error = %e, "upload cleanup failed; trying again later"),
                                }
                                tokio::select! {
                                    () = lead.cancelled() => return Ok(()),
                                    () = tokio::time::sleep(SWEEP_EVERY) => {}
                                }
                            }
                        }
                    })
                    .await;
                Ok(())
            },
        )]
    }
}

/// `[object_store]` as a storage backend. Errors are kept for the first use.
fn backend_config(config: &Config) -> Result<BackendConfig, String> {
    let os = &config.object_store;
    match os.url.as_deref() {
        Some(url) if url.starts_with("file://") => Ok(BackendConfig::Local {
            root: url.trim_start_matches("file://").into(),
        }),
        Some(url) => {
            let rest = url.trim_start_matches("s3://");
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if !prefix.trim_matches('/').is_empty() {
                // ponytail: keys always start at `orgs/` in the bucket; a path prefix needs
                // mrl-storage support (object_store PrefixStore).
                tracing::warn!(url, "object_store.url: the path after the bucket is not used; objects go under orgs/ at the bucket root");
            }
            Ok(BackendConfig::S3(S3Config {
                bucket: bucket.to_owned(),
                region: "us-east-1".to_owned(),
                endpoint: os.endpoint.clone(),
                access_key_id: None,
                secret_access_key: None,
                allow_http: os
                    .endpoint
                    .as_deref()
                    .is_some_and(|e| e.starts_with("http://")),
                checksum: true,
                object_lock: false,
            }))
        }
        None => crate::pgbundle::data_dir(&config.pgbundle)
            .map(|d| BackendConfig::Local {
                root: d.join("objects"),
            })
            .map_err(|e| e.to_string()),
    }
}

/// Object storage plus the artifact registry, shared by every P19 endpoint.
pub struct ArtifactStore {
    pub(crate) db: Db,
    config: Result<BackendConfig, String>,
    backend: OnceCell<(Backend, TicketKey)>,
}

impl ArtifactStore {
    fn new(db: Db, config: Result<BackendConfig, String>) -> Self {
        Self {
            db,
            config,
            backend: OnceCell::new(),
        }
    }

    /// The database.
    pub fn db(&self) -> &Db {
        &self.db
    }

    fn connect(&self) -> Result<Backend, Error> {
        let config = self.config.as_ref().map_err(|e| {
            Error::coded(
                "MRL-STORE-006",
                StatusCode::SERVICE_UNAVAILABLE,
                format!("The storage settings can't be used: {e}"),
            )
        })?;
        Ok(Backend::connect(config)?)
    }

    /// The backend with its ticket key. The key comes from the `SecretStore` (made once);
    /// without one, each process makes its own.
    pub async fn backend(&self, secrets: Option<&Arc<dyn SecretStore>>) -> Result<&Backend, Error> {
        Ok(&self.keyed(secrets).await?.0)
    }

    async fn keyed(
        &self,
        secrets: Option<&Arc<dyn SecretStore>>,
    ) -> Result<&(Backend, TicketKey), Error> {
        self.backend
            .get_or_try_init(|| async {
                let key = ticket_key(secrets).await?;
                Ok::<_, Error>((self.connect()?.with_ticket_key(key.clone()), key))
            })
            .await
    }

    /// Checks a ticket token for `method` and returns what it allows.
    pub async fn verify(
        &self,
        state: &AppState,
        token: &str,
        method: TicketMethod,
    ) -> Result<TicketClaims, Error> {
        let (_, key) = self.keyed(state.seams.secrets.as_ref()).await?;
        Ok(verify_ticket(
            key,
            token,
            method,
            OffsetDateTime::now_utc(),
        )?)
    }

    /// A store for `org`.
    pub async fn org_store(&self, state: &AppState, org: &str) -> Result<Store, Error> {
        Ok(self.backend(state.seams.secrets.as_ref()).await?.org(org)?)
    }

    /// A backend without a ticket key, for the cleanup job.
    pub(crate) fn plain_backend(&self) -> Result<Backend, Error> {
        self.connect()
    }

    /// A transfer ticket for `key` in the caller's org. GET tickets (downloads) are audited.
    pub async fn ticket(
        &self,
        state: &AppState,
        ctx: &RequestContext,
        key: &str,
        method: TicketMethod,
        max_size: u64,
    ) -> Result<TransferTicket, Error> {
        let org = org_of(ctx)?;
        let ticket = self
            .org_store(state, &org)
            .await?
            .transfer_ticket(key, method, max_size, TICKET_TTL)
            .await?;
        if method == TicketMethod::Get {
            state
                .seams
                .audit
                .record(AuditEvent {
                    at: state.seams.clock.now(),
                    actor: ctx.principal.id.clone(),
                    org: Some(org),
                    project: ctx.project.clone(),
                    action: "transfer.ticket_issued".to_owned(),
                    target: key.to_owned(),
                    outcome: AuditOutcome::Success,
                    trace_id: ctx.trace_id.clone(),
                })
                .await?;
        }
        Ok(ticket)
    }

    /// Registers an artifact in two phases: a PENDING row reserves the name, every object
    /// is checked against the manifest (size and sha256), then the row is COMMITTED with its
    /// lineage links in one transaction. A mismatch marks it FAILED (MRL-STORE-011).
    pub async fn register(
        &self,
        state: &AppState,
        ctx: &RequestContext,
        new: NewArtifact,
    ) -> Result<Artifact, Error> {
        let org = org_of(ctx)?;
        let manifest_json = serde_json::to_vec(&new.manifest).map_err(|e| {
            Error::coded(
                "MRL-STORE-012",
                StatusCode::UNPROCESSABLE_ENTITY,
                e.to_string(),
            )
        })?;
        let manifest = ArtifactManifest::from_json(&manifest_json)?;
        let name = check_name(&new.name, "/name")?;
        let links = new
            .links
            .iter()
            .enumerate()
            .map(|(i, l)| l.parse(i))
            .collect::<Result<Vec<_>, _>>()?;
        let id = state.seams.ids.new_uuid();
        let store = self.org_store(state, &org).await?;
        let prefix = match &new.prefix {
            Some(p) => {
                let full = format!("{}{}", store.keys().prefix(), p.trim_start_matches('/'));
                store.keys().check_prefix(&full)?;
                if !full.ends_with('/') || full == store.keys().prefix() {
                    return Err(field(
                        "/prefix",
                        "The prefix must be a folder inside your storage, ending in `/`.",
                    ));
                }
                full
            }
            None => format!(
                "{}artifacts/{}/",
                store.keys().prefix(),
                ArtifactId::from_uuid(id)
            ),
        };
        let size: u64 = manifest.files.iter().map(|f| f.size).sum();
        let inserted = sqlx::query(
            "INSERT INTO artifacts (id, org_id, project_id, name, kind, prefix, manifest, size_bytes, state, created_by)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'PENDING', $9)",
        )
        .bind(id)
        .bind(&org)
        .bind(&new.project_id)
        .bind(name)
        .bind(kind_str(&manifest))
        .bind(&prefix)
        .bind(&new.manifest)
        .bind(i64::try_from(size).unwrap_or(i64::MAX))
        .bind(&ctx.principal.id)
        .execute(self.db.pool())
        .await;
        match inserted {
            Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23505") => {
                return Err(Error::coded(
                    codes::NAME_TAKEN,
                    StatusCode::CONFLICT,
                    format!("An artifact named {name:?} already exists here."),
                ))
            }
            other => other?,
        };
        let verified = manifest
            .verify(&store, true, |f| store.keys().join(&prefix, &f.path))
            .await;
        if let Err(e) = verified {
            sqlx::query(
                "UPDATE artifacts SET state = 'FAILED' WHERE id = $1 AND state = 'PENDING'",
            )
            .bind(id)
            .execute(self.db.pool())
            .await?;
            return Err(match e {
                StorageError::ManifestMismatch(problems) => {
                    let list: Vec<String> = problems
                        .iter()
                        .take(20)
                        .map(|p| serde_json::to_string(p).unwrap_or_default())
                        .collect();
                    Error::coded(
                        "MRL-STORE-011",
                        StatusCode::UNPROCESSABLE_ENTITY,
                        format!(
                            "{} file(s) do not match the manifest: {}",
                            problems.len(),
                            list.join(", ")
                        ),
                    )
                }
                other => other.into(),
            });
        }
        let me = NodeRef::new("art", id);
        let (org_tx, links_tx) = (org.clone(), links.clone());
        let committed = self
            .db
            .transaction(|tx| {
                let (org, links) = (org_tx.clone(), links_tx.clone());
                Box::pin(async move {
                    let n = sqlx::query(
                        "UPDATE artifacts SET state = 'COMMITTED', committed_at = now()
                         WHERE id = $1 AND state = 'PENDING'",
                    )
                    .bind(id)
                    .execute(&mut **tx)
                    .await?
                    .rows_affected();
                    for (from, rel, to) in &links {
                        let (from, to) = (from.unwrap_or(me), to.unwrap_or(me));
                        LineageWriter::link(tx, &org, from, *rel, to).await?;
                    }
                    Ok(n == 1)
                })
            })
            .await?;
        if !committed {
            return Err(Error::coded(
                codes::NAME_TAKEN,
                StatusCode::CONFLICT,
                "The registration took too long and was cancelled. Register the artifact again.",
            ));
        }
        self.get(&org, id).await?.ok_or_else(|| {
            ApiError::new(ErrorCode::NotFound, "No artifact with this id exists.").into()
        })
    }

    /// The artifact `id` in `org`, in any state.
    pub async fn get(&self, org: &str, id: Uuid) -> Result<Option<Artifact>, Error> {
        let row: Option<ArtifactRow> = sqlx::query_as(
            "SELECT id, project_id, name, kind, prefix, manifest, size_bytes, state, created_by, created_at, committed_at
             FROM artifacts WHERE org_id = $1 AND id = $2",
        )
        .bind(org)
        .bind(id)
        .fetch_optional(self.db.pool())
        .await?;
        Ok(row.map(Artifact::from))
    }
}

/// The ticket signing key: from the `SecretStore` (created the first time), else per process.
async fn ticket_key(secrets: Option<&Arc<dyn SecretStore>>) -> Result<TicketKey, Error> {
    let fresh = || format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let value = match secrets {
        Some(s) => match s.get(TICKET_KEY_NAME).await? {
            Some(v) => v.expose().to_owned(),
            None => {
                // ponytail: two first-time servers sharing one store can race here and one
                // set of tickets stops verifying; a create-if-absent seam call fixes it.
                let v = fresh();
                s.set(TICKET_KEY_NAME, crate::seams::Secret::new(v.clone()))
                    .await?;
                v
            }
        },
        // ponytail: per-process key; tickets end with the process, and team replicas need a
        // shared SecretStore (P16) to verify each other's tickets.
        None => fresh(),
    };
    Ok(TicketKey::new(value.into_bytes())?)
}

/// The caller's org: the principal's, or [`LOCAL_ORG`] for the solo owner.
pub fn org_of(ctx: &RequestContext) -> Result<String, Error> {
    match (&ctx.org, ctx.principal.kind) {
        (Some(o), _) => Ok(o.clone()),
        (None, PrincipalKind::LocalOwner) => Ok(LOCAL_ORG.to_owned()),
        _ => Err(ApiError::new(
            ErrorCode::Forbidden,
            "Your account is not in an org, so it has no storage.",
        )
        .into()),
    }
}

/// A resource of `kind` in the caller's org.
pub(crate) fn resource(ctx: &RequestContext, kind: &str, id: Option<String>) -> Resource {
    Resource {
        kind: kind.to_owned(),
        id,
        org: org_of(ctx).ok(),
        project: ctx.project.clone(),
    }
}

/// A 422 for one field.
pub(crate) fn field(path: &str, message: &str) -> Error {
    ApiError::validation(vec![FieldError::new(
        path,
        ErrorCode::FieldValue,
        message,
        "Change this field and send the request again.",
    )])
    .into()
}

/// `id` or `id:action` → (id, action).
pub(crate) fn split_action(segment: &str) -> (&str, Option<&str>) {
    match segment.split_once(':') {
        Some((id, action)) => (id, Some(action)),
        None => (segment, None),
    }
}

/// A 404 for an id that does not parse or is not visible.
pub(crate) fn not_found(kind: &str) -> Error {
    ApiError::new(
        ErrorCode::NotFound,
        format!("No {kind} with this id exists."),
    )
    .into()
}

/// Names: 1–128 characters of letters, digits and `. _ -`.
pub(crate) fn check_name<'a>(name: &'a str, path: &str) -> Result<&'a str, Error> {
    let ok = (1..=128).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(name)
    } else {
        Err(field(
            path,
            "Names have 1 to 128 letters, digits, dots, dashes or underscores.",
        ))
    }
}

fn kind_str(m: &ArtifactManifest) -> String {
    serde_json::to_value(m.kind)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_else(|| "other".to_owned())
}

/// A lineage link given when registering: this artifact `relation` `to`, or `from` `relation`
/// this artifact. Exactly one of `from` and `to` is set.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NewLink {
    /// How the two are related.
    pub relation: Relation,
    /// The other entity, when it points at this artifact (like a run that `produced` it).
    #[serde(default)]
    pub from: Option<String>,
    /// The other entity, when this artifact points at it (like `derived_from` a checkpoint).
    #[serde(default)]
    pub to: Option<String>,
}

type ParsedLink = (Option<NodeRef>, Relation, Option<NodeRef>);

impl NewLink {
    fn parse(&self, i: usize) -> Result<ParsedLink, Error> {
        let parse = |s: &str, f: &str| {
            NodeRef::parse(s).map_err(|_| {
                field(
                    &format!("/links/{i}/{f}"),
                    "This is not an id the app knows.",
                )
            })
        };
        match (&self.from, &self.to) {
            (Some(f), None) => Ok((Some(parse(f, "from")?), self.relation, None)),
            (None, Some(t)) => Ok((None, self.relation, Some(parse(t, "to")?))),
            _ => Err(field(
                &format!("/links/{i}"),
                "Set exactly one of `from` and `to`.",
            )),
        }
    }
}

/// `POST /artifacts` body.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NewArtifact {
    /// Unique name in the org (or project).
    pub name: String,
    /// Folder of the files, relative to the org, ending in `/` (like `exports/exp1/`).
    /// Default: `artifacts/{id}/`.
    #[serde(default)]
    pub prefix: Option<String>,
    /// The `artifact-manifest/v1` document.
    #[schema(value_type = Object)]
    pub manifest: serde_json::Value,
    /// Project, if the artifact belongs to one.
    #[serde(default)]
    pub project_id: Option<String>,
    /// Lineage links written when the artifact commits.
    #[serde(default)]
    pub links: Vec<NewLink>,
}

#[derive(sqlx::FromRow)]
struct ArtifactRow {
    id: Uuid,
    project_id: Option<String>,
    name: String,
    kind: String,
    prefix: String,
    manifest: serde_json::Value,
    size_bytes: i64,
    state: String,
    created_by: String,
    created_at: OffsetDateTime,
    committed_at: Option<OffsetDateTime>,
}

/// A registered artifact.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct Artifact {
    /// `art_…`.
    pub id: String,
    /// Name.
    pub name: String,
    /// `model`, `dataset`, `checkpoint`, `eval`, `export` or `other`.
    pub kind: String,
    /// Full key prefix of the files.
    pub prefix: String,
    /// `PENDING`, `COMMITTED` or `FAILED`.
    pub state: String,
    /// Total bytes.
    pub size_bytes: i64,
    /// Project.
    pub project_id: Option<String>,
    /// The manifest.
    #[schema(value_type = Object)]
    pub manifest: serde_json::Value,
    /// Who registered it.
    pub created_by: String,
    /// When it was registered.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
    /// When it committed.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = String, format = DateTime)]
    pub committed_at: Option<OffsetDateTime>,
    #[serde(skip)]
    uuid: Uuid,
}

impl Artifact {
    /// The id as a UUID.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}

impl From<ArtifactRow> for Artifact {
    fn from(r: ArtifactRow) -> Self {
        Self {
            id: ArtifactId::from_uuid(r.id).to_string(),
            uuid: r.id,
            name: r.name,
            kind: r.kind,
            prefix: r.prefix,
            state: r.state,
            size_bytes: r.size_bytes,
            project_id: r.project_id,
            manifest: r.manifest,
            created_by: r.created_by,
            created_at: r.created_at,
            committed_at: r.committed_at,
        }
    }
}

/// `POST /artifacts/{art}:download` body.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DownloadRequest {
    /// A file path from the manifest.
    pub path: String,
}

/// `GET /artifacts` filter.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListQuery {
    /// Only this kind.
    pub kind: Option<String>,
    /// Page size, 1–200 (default 50).
    pub limit: Option<String>,
    /// `next_cursor` from the previous page.
    pub cursor: Option<String>,
}

fn parse_artifact_id(s: &str) -> Result<Uuid, Error> {
    s.parse::<ArtifactId>()
        .map(|a| a.as_uuid())
        .map_err(|_| not_found("artifact"))
}

/// Registers an artifact.
#[utoipa::path(post, path = "/artifacts", tag = "artifacts", request_body = NewArtifact,
    responses((status = 201, body = Artifact), (status = 409, body = crate::error::Problem), (status = 422, body = crate::error::Problem)))]
async fn register(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Json(body): Json<NewArtifact>,
) -> Result<(StatusCode, Json<Artifact>), Error> {
    s.authorize(&ctx, "uploads.write", &resource(&ctx, "artifact", None))
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(store.register(&s, &ctx, body).await?),
    ))
}

/// Reads one artifact.
#[utoipa::path(get, path = "/artifacts/{art}", tag = "artifacts",
    params(("art" = String, Path, description = "`art_…`")),
    responses((status = 200, body = Artifact), (status = 404, body = crate::error::Problem)))]
async fn get_one(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Path(art): Path<String>,
) -> Result<Json<Artifact>, Error> {
    let id = parse_artifact_id(&art)?;
    s.authorize(
        &ctx,
        "lineage.read",
        &resource(&ctx, "artifact", Some(art.clone())),
    )
    .await?;
    let org = org_of(&ctx)?;
    store
        .get(&org, id)
        .await?
        .map(Json)
        .ok_or_else(|| not_found("artifact"))
}

/// Lists committed artifacts, newest last.
#[utoipa::path(get, path = "/artifacts", tag = "artifacts", params(ListQuery),
    responses((status = 200, body = Object, description = "`{items: [Artifact], next_cursor}`")))]
async fn list(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Query(q): Query<ListQuery>,
) -> Result<Json<Page<Artifact>>, Error> {
    s.authorize(&ctx, "lineage.read", &resource(&ctx, "artifact", None))
        .await?;
    let page = PageQuery {
        limit: q.limit.clone(),
        cursor: q.cursor.clone(),
    }
    .resolve()?;
    let after = match page.after.as_deref() {
        Some(c) => Some(Uuid::parse_str(c).map_err(|_| {
            ApiError::new(
                ErrorCode::Pagination,
                "This cursor does not belong to this list.",
            )
        })?),
        None => None,
    };
    let rows: Vec<ArtifactRow> = sqlx::query_as(
        "SELECT id, project_id, name, kind, prefix, manifest, size_bytes, state, created_by, created_at, committed_at
         FROM artifacts
         WHERE org_id = $1 AND state = 'COMMITTED' AND ($2::text IS NULL OR kind = $2)
           AND ($3::uuid IS NULL OR id > $3)
         ORDER BY id LIMIT $4",
    )
    .bind(org_of(&ctx)?)
    .bind(&q.kind)
    .bind(after)
    .bind(i64::try_from(page.fetch()).unwrap_or(201))
    .fetch_all(store.db.pool())
    .await?;
    let items = rows.into_iter().map(Artifact::from).collect();
    Ok(Json(Page::from_overfetch(items, &page, |a| {
        a.uuid.to_string()
    })))
}

/// `POST /artifacts/{art}:download` → a GET ticket for one file of a committed artifact.
#[utoipa::path(post, path = "/artifacts/{art}:download", tag = "artifacts", request_body = DownloadRequest,
    params(("art" = String, Path, description = "`art_…`")),
    responses((status = 200, body = Object, description = "A transfer ticket: `{method, key, max_size, expires_at, url, presigned, headers}`."),
        (status = 404, body = crate::error::Problem)))]
async fn action(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Path(segment): Path<String>,
    Json(body): Json<DownloadRequest>,
) -> Result<Json<TransferTicket>, Error> {
    let (art, act) = split_action(&segment);
    if act != Some("download") {
        return Err(ApiError::new(ErrorCode::RouteNotFound, "No endpoint has this path.").into());
    }
    let id = parse_artifact_id(art)?;
    s.authorize(
        &ctx,
        "lineage.read",
        &resource(&ctx, "artifact", Some(art.to_owned())),
    )
    .await?;
    let org = org_of(&ctx)?;
    let found = store
        .get(&org, id)
        .await?
        .filter(|a| a.state == "COMMITTED")
        .ok_or_else(|| not_found("artifact"))?;
    let manifest =
        ArtifactManifest::from_json(&serde_json::to_vec(&found.manifest).unwrap_or_default())?;
    let file = manifest
        .files
        .iter()
        .find(|f| f.path == body.path)
        .ok_or_else(|| field("/path", "This artifact has no file at this path."))?;
    let keys = store.org_store(&s, &org).await?;
    let key = keys.keys().join(&found.prefix, &file.path)?;
    Ok(Json(
        store
            .ticket(&s, &ctx, &key, TicketMethod::Get, file.size)
            .await?,
    ))
}

#[derive(OpenApi)]
#[openapi(
    paths(register, get_one, list, action, transfer::upload, transfer::download),
    components(schemas(NewArtifact, NewLink, Artifact, DownloadRequest, Relation)),
    tags((name = "artifacts", description = "Generic artifacts and the transfer endpoint."))
)]
struct Doc;

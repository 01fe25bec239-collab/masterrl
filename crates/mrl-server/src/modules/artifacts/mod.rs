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
//! let t = store.ticket(&seams, &ctx, &key, TicketMethod::Get, max).await?;  // audited
//! let art = store.register(&seams, &ctx, new_artifact).await?;   // checks every file
//! ```

mod error;
pub(crate) mod scratch;
mod transfer;

pub use error::{codes, render, Error};
pub use transfer::SHA256_HEADER;

use crate::app::{AppState, ServerModule, Task};
use crate::config::{Config, Mode, Role};
use crate::db::{Db, LeaderElection};
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::http::pagination::{Page, PageQuery};
use crate::modules::lineage::{LineageWriter, NodeRef, Relation};
use crate::seams::{
    AuditEvent, AuditOutcome, Decision, PrincipalKind, RequestContext, Resource, Seams, SecretStore,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::middleware::from_fn;
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use mrl_domain::ids::ArtifactId;
use mrl_storage::{
    sign_ticket, verify_ticket, ArtifactManifest, Backend, BackendConfig, S3Config, StorageError,
    Store, TicketClaims, TicketKey, TicketMethod, TransferOptions, TransferTicket,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
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
            store: Arc::new(
                ArtifactStore::new(
                    db,
                    backend_config(config),
                    config.server.mode,
                    scratch::default_root(),
                )
                .with_module_lookups()
                .with_download_guards(),
            ),
        }
    }

    /// The module with an explicit backend, for a server in `mode`, keeping request scratch
    /// folders under `scratch_root` (tests and embedders).
    pub fn with_backend(
        db: Db,
        backend: BackendConfig,
        mode: Mode,
        scratch_root: std::path::PathBuf,
    ) -> Self {
        Self {
            store: Arc::new(
                ArtifactStore::new(db, Ok(backend), mode, scratch_root)
                    .with_module_lookups()
                    .with_download_guards(),
            ),
        }
    }

    /// The shared store, for other modules.
    pub fn store(&self) -> Arc<ArtifactStore> {
        self.store.clone()
    }

    /// The secret resolver, for other modules (P20 delivers granted secrets with it).
    pub fn secrets(&self) -> crate::modules::secrets::SecretResolver {
        crate::modules::secrets::SecretResolver::new(self.store.db.clone(), self.store.mode)
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
            .route_layer(from_fn(render))
            .layer(Extension(self.store.clone()))
    }

    fn public_routes(&self) -> Router<AppState> {
        Router::new()
            .route(
                "/transfer/{ticket}",
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

/// `[object_store]` as a storage backend, reading the region from `AWS_REGION` or
/// `AWS_DEFAULT_REGION` (default `us-east-1`). Errors are kept for the first use.
fn backend_config(config: &Config) -> Result<BackendConfig, String> {
    backend_config_with(config, &|k| std::env::var(k).ok())
}

/// [`backend_config`] with the environment given.
fn backend_config_with(
    config: &Config,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<BackendConfig, String> {
    let os = &config.object_store;
    match os.url.as_deref() {
        // A file URL as a native path: `file:///C:/mrl` is `C:\mrl` on Windows, and
        // escapes like `%20` are decoded.
        Some(url) if url.starts_with("file://") => url::Url::parse(url)
            .ok()
            .and_then(|u| u.to_file_path().ok())
            .map(|root| BackendConfig::Local { root })
            .ok_or_else(|| format!("object_store.url {url:?} is not a local file path")),
        Some(url) => {
            // Config validation refuses a path after the bucket: objects live under `orgs/`
            // at the bucket root.
            let rest = url.trim_start_matches("s3://");
            let bucket = rest.split('/').next().unwrap_or(rest);
            Ok(BackendConfig::S3(S3Config {
                bucket: bucket.to_owned(),
                region: env("AWS_REGION")
                    .or_else(|| env("AWS_DEFAULT_REGION"))
                    .filter(|r| !r.is_empty())
                    .unwrap_or_else(|| "us-east-1".to_owned()),
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
    mode: Mode,
    scratch: std::path::PathBuf,
    plain: OnceCell<Backend>,
    signing: OnceCell<(Backend, TicketKey)>,
    lookups: std::sync::RwLock<HashMap<String, Arc<dyn NodeLookup>>>,
    guards: std::sync::RwLock<Vec<Arc<dyn DownloadGuard>>>,
}

impl ArtifactStore {
    fn new(
        db: Db,
        config: Result<BackendConfig, String>,
        mode: Mode,
        scratch: std::path::PathBuf,
    ) -> Self {
        Self {
            db,
            config,
            mode,
            scratch,
            plain: OnceCell::new(),
            signing: OnceCell::new(),
            lookups: std::sync::RwLock::new(HashMap::new()),
            guards: std::sync::RwLock::new(Vec::new()),
        }
    }

    /// Adds the lineage lookups other modules own, so every store (the one P19 serves and
    /// the ones other modules build) resolves their kinds. Each part adds one
    /// `self.add_lineage_lookup(kind, Arc::new(…))` line below its marker.
    fn with_module_lookups(self) -> Self {
        // <mrl:lineage-lookups P26> P26 lookups go right below this line.
        // <mrl:lineage-lookups P27> P27 lookups go right below this line.
        self.add_lineage_lookup(
            "dsv",
            Arc::new(crate::modules::datasets::records::DatasetRecords::new(
                self.db.clone(),
            )),
        );
        // <mrl:lineage-lookups P29> P29 lookups go right below this line.
        self
    }

    /// A new private scratch folder (see [`scratch`]).
    pub(crate) fn scratch_dir(&self) -> Result<tempfile::TempDir, Error> {
        scratch::dir(&self.scratch)
    }

    /// Where scratch folders live.
    pub(crate) fn scratch_root(&self) -> &std::path::Path {
        &self.scratch
    }

    /// The database.
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Lets lineage show items of `kind` (an id prefix like `run`): the module that owns
    /// them says which exist in an org and how reading one is authorized. Items of a kind
    /// with no lookup are redacted in graphs and are never a root.
    pub fn add_lineage_lookup(&self, kind: &str, lookup: Arc<dyn NodeLookup>) {
        if let Ok(mut l) = self.lookups.write() {
            l.insert(kind.to_owned(), lookup);
        }
    }

    pub(crate) fn lookup(&self, kind: &str) -> Option<Arc<dyn NodeLookup>> {
        self.lookups.read().ok().and_then(|l| l.get(kind).cloned())
    }

    /// Adds the download guards other modules own, on every store. Each part adds one
    /// `self.add_download_guard(Arc::new(…));` line below its marker.
    fn with_download_guards(self) -> Self {
        // <mrl:download-guards P27> P27 guards go right below this line.
        self.add_download_guard(Arc::new(
            crate::modules::datasets::downloads::DatasetDownloadGuard::new(self.db.clone()),
        ));
        self
    }

    /// Adds a check that runs before any artifact download, after the read check passed.
    pub fn add_download_guard(&self, guard: Arc<dyn DownloadGuard>) {
        if let Ok(mut g) = self.guards.write() {
            g.push(guard);
        }
    }

    fn download_guards(&self) -> Vec<Arc<dyn DownloadGuard>> {
        self.guards.read().map(|g| g.clone()).unwrap_or_default()
    }

    /// The storage backend, without a signing key. A failure is not kept: the next call
    /// tries again.
    pub(crate) async fn backend(&self) -> Result<&Backend, Error> {
        self.plain
            .get_or_try_init(|| async {
                let config = self.config.as_ref().map_err(|e| {
                    Error::coded(
                        "MRL-STORE-006",
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("The storage settings can't be used: {e}"),
                    )
                })?;
                Ok::<_, Error>(Backend::connect(config)?)
            })
            .await
    }

    /// The backend with the ticket signing key (see [`ticket_key`]). Only tickets need it.
    async fn signing(
        &self,
        secrets: Option<&Arc<dyn SecretStore>>,
    ) -> Result<&(Backend, TicketKey), Error> {
        self.signing
            .get_or_try_init(|| async {
                let key = ticket_key(&self.db, self.mode, secrets).await?;
                let backend = self.backend().await?.clone().with_ticket_key(key.clone());
                Ok::<_, Error>((backend, key))
            })
            .await
    }

    /// Checks a ticket token for `method` and returns what it allows. `secrets` is the
    /// server's `SecretStore` (`seams.secrets`), which holds the shared signing key.
    pub async fn verify(
        &self,
        secrets: Option<&Arc<dyn SecretStore>>,
        token: &str,
        method: TicketMethod,
    ) -> Result<TicketClaims, Error> {
        let (_, key) = self.signing(secrets).await?;
        Ok(verify_ticket(
            key,
            token,
            method,
            OffsetDateTime::now_utc(),
        )?)
    }

    /// Storage of `org` for reading and writing. It signs no tickets, so it works without a
    /// signing key; use [`ArtifactStore::ticket`] for those.
    pub async fn org_store(&self, org: &str) -> Result<Store, Error> {
        Ok(self.backend().await?.org(org)?)
    }

    /// A transfer ticket for `key` in the caller's org, valid for [`TICKET_TTL`]. GET
    /// tickets (downloads) are audited. PUT tickets never point into the reserved `blobs/`
    /// area, however it is spelled.
    pub async fn ticket(
        &self,
        seams: &Seams,
        ctx: &RequestContext,
        key: &str,
        method: TicketMethod,
        max_size: u64,
    ) -> Result<TransferTicket, Error> {
        let org = org_of(ctx)?;
        let store = self.signing(seams.secrets.as_ref()).await?.0.org(&org)?;
        if method == TicketMethod::Put && is_immutable(&store, key) {
            return Err(immutable());
        }
        let ticket = store
            .transfer_ticket(key, method, max_size, TICKET_TTL)
            .await?;
        if method == TicketMethod::Get {
            // A successful action is recorded inside a transaction (the audit sink's rule).
            let mut tx = self.db().begin().await?;
            seams
                .audit
                .record_in(
                    &mut tx,
                    AuditEvent {
                        at: seams.clock.now(),
                        actor: ctx.principal.id.clone(),
                        org: Some(org),
                        project: ctx.project.clone(),
                        action: "transfer.ticket_issued".to_owned(),
                        target: key.to_owned(),
                        outcome: AuditOutcome::Success,
                        trace_id: ctx.trace_id.clone(),
                    },
                )
                .await?;
            tx.commit().await?;
        }
        Ok(ticket)
    }

    /// A proxied part PUT ticket whose signed expiry cannot exceed the upload deadline.
    pub(crate) async fn part_ticket(
        &self,
        seams: &Seams,
        ctx: &RequestContext,
        key: String,
        max_size: u64,
        deadline: OffsetDateTime,
    ) -> Result<TransferTicket, Error> {
        let (backend, signer) = self.signing(seams.secrets.as_ref()).await?;
        backend.org(&org_of(ctx)?)?.keys().check(&key)?;
        let now = OffsetDateTime::now_utc();
        let expires_at = deadline.min(now + TICKET_TTL);
        let claims = TicketClaims {
            key: key.clone(),
            method: TicketMethod::Put,
            max_size,
            issued_at: now.unix_timestamp(),
            expires_at: expires_at.unix_timestamp(),
        };
        if claims.expires_at <= claims.issued_at {
            return Err(StorageError::TicketExpired.into());
        }
        let token = sign_ticket(signer, &claims)?;
        let expires_at = OffsetDateTime::from_unix_timestamp(claims.expires_at)
            .map_err(|_| StorageError::TicketInvalid("the deadline is out of range"))?;
        Ok(TransferTicket {
            method: claims.method,
            key,
            max_size,
            expires_at,
            url: format!("{}{token}", mrl_storage::PROXY_PATH),
            presigned: false,
            headers: BTreeMap::new(),
        })
    }

    /// Registers an artifact in two phases: a PENDING row reserves the name; every file is
    /// made a checked copy in `blobs/sha256/…`; then the row is COMMITTED with its lineage
    /// links in one transaction. Any failure after the reservation marks it FAILED (a
    /// mismatch is MRL-STORE-011) and frees the name. Downloads read the copies, so changing
    /// a source later can't change a committed artifact.
    ///
    /// Files come either from `uploads` (completed uploads the caller may see; what the HTTP
    /// API accepts) or, for the server's own modules only, from a storage `prefix`, whose
    /// files are downloaded, hashed and stored. The caller is checked against the
    /// destination (org and `project_id`) before anything is reserved.
    pub async fn register(
        &self,
        seams: &Seams,
        ctx: &RequestContext,
        new: NewArtifact,
        idem: Option<&Idempotency>,
    ) -> Result<Artifact, Error> {
        let org = org_of(ctx)?;
        let dest = destination("artifact", &org, new.project_id.clone());
        allow(seams, ctx, crate::modules::uploads::ACTION, &dest).await?;
        let manifest_json = serde_json::to_vec(&new.manifest).map_err(|e| {
            Error::coded(
                "MRL-STORE-012",
                StatusCode::UNPROCESSABLE_ENTITY,
                e.to_string(),
            )
        })?;
        let manifest = ArtifactManifest::from_json(&manifest_json)?;
        let name = check_name(&new.name, "/name")?;
        let size = total_size(&manifest)?;
        let links = new
            .links
            .iter()
            .enumerate()
            .map(|(i, l)| l.parse(i))
            .collect::<Result<Vec<_>, _>>()?;
        for (i, (from, _, to)) in links.iter().enumerate() {
            if let Some(n) = from.or(*to) {
                self.check_link(seams, ctx, &org, i, n).await?;
            }
        }
        let id = seams.ids.new_uuid();
        let store = self.org_store(&org).await?;
        let (prefix, sources) = self
            .sources(seams, ctx, &org, &store, id, &new, &manifest)
            .await?;
        // A retry of an earlier registration (checked afresh above) answers with its row.
        if let Some(a) = self.earlier(seams, ctx, idem, &org).await? {
            if a.state == "COMMITTED" {
                return Ok(a);
            }
            // The HTTP idempotency claim serializes this key. Replace a reservation left
            // by a dead owner, using a new ID so its delayed cleanup can't touch this try.
            fail_pending(&self.db, a.uuid).await?;
            if let Some(a) = self
                .get(&org, a.uuid)
                .await?
                .filter(|a| a.state == "COMMITTED")
            {
                return Ok(a);
            }
        }
        let inserted = sqlx::query(
            "INSERT INTO artifacts (id, org_id, project_id, name, kind, prefix, manifest, size_bytes, state, created_by, idempotency_key, request_sha256)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'PENDING', $9, $10, $11)",
        )
        .bind(id)
        .bind(&org)
        .bind(&new.project_id)
        .bind(name)
        .bind(kind_str(&manifest))
        .bind(&prefix)
        .bind(&new.manifest)
        .bind(size)
        .bind(&ctx.principal.id)
        .bind(idem.map(|i| &i.key))
        .bind(idem.map(|i| &i.request_sha256))
        .execute(self.db.pool())
        .await;
        match inserted {
            Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23505") => {
                if let Some(a) = self.earlier(seams, ctx, idem, &org).await? {
                    if a.state == "COMMITTED" {
                        return Ok(a);
                    }
                    return Err(ApiError::new(
                        ErrorCode::IdempotencyInProgress,
                        "A request with this key is still running.",
                    )
                    .into());
                }
                return Err(Error::coded(
                    codes::NAME_TAKEN,
                    StatusCode::CONFLICT,
                    format!("An artifact named {name:?} already exists here."),
                ));
            }
            other => other?,
        };
        let mut reservation = Reservation {
            db: self.db.clone(),
            id,
            armed: true,
        };
        let done = match self.snapshot(&store, &manifest, sources).await {
            Ok(()) => self.commit(&org, id, &links).await,
            Err(e) => Err(e),
        };
        if let Err(e) = done {
            fail_pending(&self.db, id).await?;
            reservation.settled();
            return Err(e);
        }
        reservation.settled();
        self.get(&org, id)
            .await?
            .ok_or_else(|| not_found("artifact"))
    }

    /// Stores the checked local file `tmp` (`size` bytes hashing to `sha256`) at `blob`, its
    /// content address. An object already there is reused only when its bytes hash to
    /// `sha256`; one that is missing, another size or corrupted is rewritten from `tmp`.
    /// An intent is recorded first, also when the blob is reused: the cleanup job keeps a
    /// blob with a fresh intent, so it can't delete one a commit is about to reference, and
    /// deletes it after 24 hours when that commit never happens (the server stopped).
    pub(crate) async fn put_blob(
        &self,
        st: &Store,
        tmp: &std::path::Path,
        blob: &str,
        size: u64,
        sha256: &str,
    ) -> Result<(), Error> {
        self.intend(st.keys().org_id(), blob, Some(sha256)).await?;
        if crate::modules::uploads::blob_intact(st, blob, size, sha256).await? {
            return Ok(());
        }
        crate::modules::uploads::store_file(st, tmp, blob, size).await
    }

    /// Records that P19 is about to write or rely on `key` of `org` (a blob when `sha256` is
    /// given). Recorded under the key's lock ([`lock_key`]), which the cleanup job holds
    /// while it checks and deletes that key: the intent is either seen by a pass, or lands
    /// after the pass deleted the object, so the caller's own check finds it missing.
    pub(crate) async fn intend(
        &self,
        org: &str,
        key: &str,
        sha256: Option<&str>,
    ) -> Result<(), Error> {
        let mut tx = self.db.pool().begin().await?;
        lock_key(&mut tx, org, key).await?;
        sqlx::query(
            "INSERT INTO storage_intents (id, org_id, key, sha256) VALUES ($1, $2, $3, $4)",
        )
        .bind(Uuid::now_v7())
        .bind(org)
        .bind(key)
        .bind(sha256)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// The artifact an earlier try with this Idempotency-Key made, if any.
    async fn earlier(
        &self,
        seams: &Seams,
        ctx: &RequestContext,
        idem: Option<&Idempotency>,
        org: &str,
    ) -> Result<Option<Artifact>, Error> {
        let Some(i) = idem else {
            return Ok(None);
        };
        let Some(id) = i
            .earlier(&self.db, Created::Artifact, org, &ctx.principal.id)
            .await?
        else {
            return Ok(None);
        };
        let found = self.get(org, id).await?;
        if let Some(a) = &found {
            allow(seams, ctx, READ, &a.resource(org)).await?;
        }
        Ok(found)
    }

    /// Where each manifest file comes from, checked before anything is reserved, and the
    /// prefix recorded on the row.
    #[allow(clippy::too_many_arguments)]
    async fn sources(
        &self,
        seams: &Seams,
        ctx: &RequestContext,
        org: &str,
        store: &Store,
        id: Uuid,
        new: &NewArtifact,
        manifest: &ArtifactManifest,
    ) -> Result<(String, Vec<Source>), Error> {
        let keys = store.keys();
        if let Some(p) = &new.prefix {
            if !new.uploads.is_empty() {
                return Err(field(
                    "/uploads",
                    "Give either `prefix` or `uploads`, not both.",
                ));
            }
            let full = format!("{}{}", keys.prefix(), p.trim_start_matches('/'));
            keys.check_prefix(&full)?;
            if !full.ends_with('/') || full == keys.prefix() {
                return Err(field(
                    "/prefix",
                    "The prefix must be a folder inside your storage, ending in `/`.",
                ));
            }
            let mut sources = Vec::with_capacity(manifest.files.len());
            for f in &manifest.files {
                let key = keys.join(&full, &f.path)?;
                // Staging areas are cleaned up or rewritten (upload parts, checkpoint
                // attempts): register the finished upload or checkpoint instead.
                if is_staging(&key[keys.prefix().len()..]) {
                    return Err(field(
                        "/prefix",
                        "Artifact files can't live in a staging folder (`uploads/` or `_staging/`); finish the upload first and use its stored file.",
                    ));
                }
                sources.push(Source::Key(key));
            }
            return Ok((full, sources));
        }
        if let Some(extra) = new
            .uploads
            .keys()
            .find(|p| !manifest.files.iter().any(|f| &f.path == *p))
        {
            return Err(field(
                &format!("/uploads/{extra}"),
                "The manifest has no file at this path.",
            ));
        }
        let mut sources = Vec::with_capacity(manifest.files.len());
        for f in &manifest.files {
            let up = new.uploads.get(&f.path).ok_or_else(|| {
                field(
                    &format!("/uploads/{}", f.path),
                    "Every file needs the id of a finished upload with its bytes.",
                )
            })?;
            let blob = crate::modules::uploads::finished(self, seams, ctx, org, up).await?;
            sources.push(Source::Upload(up.clone(), blob));
        }
        Ok((
            format!("{}artifacts/{}/", keys.prefix(), ArtifactId::from_uuid(id)),
            sources,
        ))
    }

    /// Makes every file a checked copy in `blobs/sha256/…` (see [`snapshot_file`]). A
    /// finished upload's stored file already is one; it is re-read and hashed
    /// ([`check_blob`]) so a lost or changed object is caught before commit.
    async fn snapshot(
        &self,
        store: &Store,
        manifest: &ArtifactManifest,
        sources: Vec<Source>,
    ) -> Result<(), Error> {
        let mut problems = Vec::new();
        for (f, src) in manifest.files.iter().zip(sources) {
            let problem = match src {
                Source::Key(key) => self.snapshot_file(store, &key, f).await?,
                Source::Upload(up, b) => check_blob(store, &up, &b, f).await?,
            };
            problems.extend(problem);
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(mismatch(&problems))
        }
    }

    /// Copies `src` to `blobs/sha256/…` when its bytes match `f`. The copy is checked, not
    /// the source: the bytes are downloaded into a private folder, hashed there, and those
    /// same bytes are stored, so a source rewritten meanwhile can't slip in. Content already
    /// stored is reused only when it hashes right (else rewritten from the checked bytes);
    /// nothing under `blobs/` is ever deleted here. Returns
    /// the problem when the source does not match.
    async fn snapshot_file(
        &self,
        store: &Store,
        src: &str,
        f: &mrl_storage::ManifestFile,
    ) -> Result<Option<String>, Error> {
        // ponytail: one file at a time through local disk; copy server-side and hash in
        // parallel when large artifacts make registration slow.
        match store.head(src).await {
            Ok(info) if info.size == f.size => {}
            Ok(info) => {
                return Ok(Some(format!(
                    "{}: {} bytes, the manifest says {}",
                    f.path, info.size, f.size
                )))
            }
            Err(StorageError::NotFound { .. }) => return Ok(Some(format!("{}: missing", f.path))),
            Err(e) => return Err(e.into()),
        }
        let dir = self.scratch_dir()?;
        let local = dir.path().join("file");
        let opts = TransferOptions::default();
        match store
            .download_file(src, &local, Some(&f.sha256), opts)
            .await
        {
            Ok(got) if got.size == f.size => {}
            Ok(got) => {
                return Ok(Some(format!(
                    "{}: {} bytes, the manifest says {}",
                    f.path, got.size, f.size
                )))
            }
            Err(StorageError::ChecksumMismatch { .. }) => {
                return Ok(Some(format!(
                    "{}: its sha256 differs from the manifest",
                    f.path
                )))
            }
            Err(StorageError::NotFound { .. }) => return Ok(Some(format!("{}: missing", f.path))),
            Err(e) => return Err(e.into()),
        }
        let blob = store.keys().blob(&f.sha256)?;
        self.put_blob(store, &local, &blob, f.size, &f.sha256)
            .await?;
        scratch::close(dir)?;
        Ok(None)
    }

    /// PENDING → COMMITTED plus the lineage links, in one transaction. Nothing is linked
    /// unless this call made the transition; a failed link rolls the transition back.
    async fn commit(&self, org: &str, id: Uuid, links: &[ParsedLink]) -> Result<(), Error> {
        let me = NodeRef::new("art", id);
        let (org, links) = (org.to_owned(), links.to_vec());
        let committed = self
            .db
            .transaction(|tx| {
                let (org, links) = (org.clone(), links.clone());
                Box::pin(async move {
                    let n = sqlx::query(
                        "UPDATE artifacts SET state = 'COMMITTED', committed_at = now(), files_checked = true
                         WHERE id = $1 AND state = 'PENDING'",
                    )
                    .bind(id)
                    .execute(&mut **tx)
                    .await?
                    .rows_affected();
                    if n != 1 {
                        return Ok(false);
                    }
                    for (from, rel, to) in &links {
                        let (from, to) = (from.unwrap_or(me), to.unwrap_or(me));
                        LineageWriter::link(tx, &org, from, *rel, to).await?;
                    }
                    Ok(true)
                })
            })
            .await?;
        if committed {
            Ok(())
        } else {
            Err(Error::coded(
                codes::NAME_TAKEN,
                StatusCode::CONFLICT,
                "The registration took too long and was cancelled. Register the artifact again.",
            ))
        }
    }

    /// A link given by a caller may point only at an artifact or upload of the caller's org
    /// that the caller may see; anything else is not found (404), like a GET of it. Other
    /// kinds are linked by the modules that own them (in the transaction that creates
    /// them), since only they can tell whether an id is real.
    async fn check_link(
        &self,
        seams: &Seams,
        ctx: &RequestContext,
        org: &str,
        i: usize,
        node: NodeRef,
    ) -> Result<(), Error> {
        if !crate::modules::lineage::has_table(node.kind) {
            return Err(field(
                &format!("/links/{i}"),
                &format!(
                    "Links to {}_… ids can't be added here; the part of the app that makes them records their links.",
                    node.kind
                ),
            ));
        }
        let visible = match crate::modules::lineage::access(self, org, node).await? {
            Some((action, res)) => {
                seams.authorizer.authorize(ctx, action, &res).await? == Decision::Allow
            }
            None => false,
        };
        if visible {
            Ok(())
        } else {
            Err(not_found("item"))
        }
    }

    /// Finds a committed artifact file and ensures its checked blob exists.
    /// The caller must authorize access to the artifact first.
    pub(crate) async fn download_source(
        &self,
        org: &str,
        found: &Artifact,
        path: &str,
    ) -> Result<(String, mrl_storage::ManifestFile), Error> {
        let manifest =
            ArtifactManifest::from_json(&serde_json::to_vec(&found.manifest).unwrap_or_default())?;
        let file = manifest
            .files
            .iter()
            .find(|f| f.path == path)
            .ok_or_else(|| field("/path", "This artifact has no file at this path."))?;
        // Committed files are read from their checked copies, never from the source prefix.
        // Artifacts committed before copies existed (`files_checked` false) have the copy
        // checked now: reused only when it hashes right, else made again from the prefix.
        let st = self.org_store(org).await?;
        let key = st.keys().blob(&file.sha256)?;
        if !found.files_checked
            && !crate::modules::uploads::blob_intact(&st, &key, file.size, &file.sha256).await?
        {
            let src = st.keys().join(&found.prefix, &file.path)?;
            if let Some(problem) = self.snapshot_file(&st, &src, file).await? {
                return Err(mismatch(&[problem]));
            }
        }
        Ok((key, file.clone()))
    }

    /// The artifact `id` in `org`, in any state.
    pub async fn get(&self, org: &str, id: Uuid) -> Result<Option<Artifact>, Error> {
        let row: Option<ArtifactRow> = sqlx::query_as(
            "SELECT id, project_id, name, kind, prefix, manifest, size_bytes, state, created_by, created_at, committed_at, files_checked
             FROM artifacts WHERE org_id = $1 AND id = $2",
        )
        .bind(org)
        .bind(id)
        .fetch_optional(self.db.pool())
        .await?;
        Ok(row.map(Artifact::from))
    }
}

/// Marks the artifact `id` FAILED if it is still PENDING, which frees its name.
async fn fail_pending(db: &Db, id: Uuid) -> Result<(), Error> {
    sqlx::query("UPDATE artifacts SET state = 'FAILED' WHERE id = $1 AND state = 'PENDING'")
        .bind(id)
        .execute(db.pool())
        .await?;
    Ok(())
}

/// A PENDING artifact row this registration reserved. Dropped before [`Reservation::settled`]
/// (the request was cancelled, like a client that went away), it marks the row FAILED in a
/// task of its own, so the name is free at once instead of after the cleanup job's 24 hours.
struct Reservation {
    db: Db,
    id: Uuid,
    armed: bool,
}

impl Reservation {
    /// The row reached COMMITTED or FAILED; nothing is left to do.
    fn settled(&mut self) {
        self.armed = false;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (db, id) = (self.db.clone(), self.id);
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                if let Err(e) = fail_pending(&db, id).await {
                    tracing::warn!(
                        code = e.code(),
                        "could not free a cancelled registration; the cleanup job will"
                    );
                }
            });
        }
    }
}

/// Finds items of one kind for lineage graphs. The module that owns the kind implements it
/// and registers it with [`ArtifactStore::add_lineage_lookup`].
#[async_trait::async_trait]
pub trait NodeLookup: Send + Sync {
    /// When `id` is an item of `org`: the action and resource that reading it is checked
    /// against, exactly as the owning module's own GET checks it. `None`: not in `org`.
    async fn find(&self, org: &str, id: Uuid) -> Result<Option<(&'static str, Resource)>, Error>;
}

/// A module's extra say on downloading artifacts it owns (like a dataset export's
/// permission and PII policy). Registered with [`ArtifactStore::add_download_guard`].
#[async_trait::async_trait]
pub trait DownloadGuard: Send + Sync {
    /// Runs after the caller passed the artifact read check. `Err` refuses the download;
    /// return `Ok` for artifacts the module does not own.
    async fn check(
        &self,
        state: &AppState,
        ctx: &RequestContext,
        org: &str,
        artifact: &Artifact,
    ) -> Result<(), Error>;
}

/// The stored file of a finished upload, as its row records it.
pub(crate) struct UploadBlob {
    /// Its key, under `blobs/sha256/`.
    pub(crate) key: String,
    pub(crate) size: u64,
    pub(crate) sha256: String,
}

/// Compares a finished upload's stored file with manifest entry `f`: first the row, then
/// the object itself (it must exist with that size, and its bytes must hash to that
/// sha256), since storage can lose or change what the row describes. Returns the problem.
async fn check_blob(
    store: &Store,
    up: &str,
    b: &UploadBlob,
    f: &mrl_storage::ManifestFile,
) -> Result<Option<String>, Error> {
    if b.size != f.size || b.sha256 != f.sha256 {
        return Ok(Some(format!("{}: upload {up} has other content", f.path)));
    }
    match store.head(&b.key).await {
        Ok(info) if info.size == f.size => {}
        Ok(info) => {
            return Ok(Some(format!(
                "{}: the stored file of upload {up} has {} bytes, the manifest says {}",
                f.path, info.size, f.size
            )))
        }
        Err(StorageError::NotFound { .. }) => {
            return Ok(Some(format!(
                "{}: the stored file of upload {up} is missing",
                f.path
            )))
        }
        Err(e) => return Err(e.into()),
    }
    match store.sha256(&b.key).await {
        Ok(sha) if sha == f.sha256 => Ok(None),
        Ok(_) => Ok(Some(format!(
            "{}: the stored file of upload {up} has changed",
            f.path
        ))),
        Err(StorageError::NotFound { .. }) => Ok(Some(format!(
            "{}: the stored file of upload {up} is missing",
            f.path
        ))),
        Err(e) => Err(e.into()),
    }
}

/// Where a manifest file comes from.
enum Source {
    /// A key the server reads, checks and copies (trusted callers only).
    Key(String),
    /// A finished upload (id, its blob).
    Upload(String, UploadBlob),
}

fn mismatch(problems: &[String]) -> Error {
    let shown: Vec<&str> = problems.iter().take(20).map(String::as_str).collect();
    Error::coded(
        "MRL-STORE-011",
        StatusCode::UNPROCESSABLE_ENTITY,
        format!(
            "{} file(s) do not match the manifest: {}",
            problems.len(),
            shown.join("; ")
        ),
    )
}

/// A caller's `Idempotency-Key` (already checked by the middleware) with the hash of its
/// request. P19 answers are never replayed by the middleware, so each creation endpoint
/// finds the row an earlier try with this key made, after a fresh authorization check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Idempotency {
    /// The key.
    pub key: String,
    /// sha256 of the parsed request, re-serialized.
    pub request_sha256: String,
}

/// Tables whose creations are deduped by [`Idempotency`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum Created {
    Upload,
    Artifact,
    Secret,
}

impl Idempotency {
    /// From the request headers and the parsed body; `None` without the header.
    pub fn of(headers: &axum::http::HeaderMap, body: &impl Serialize) -> Option<Self> {
        let key = headers
            .get(crate::http::idempotency::IDEMPOTENCY_KEY)?
            .to_str()
            .ok()?
            .to_owned();
        let bytes = serde_json::to_vec(body).ok()?;
        Some(Self {
            key,
            request_sha256: mrl_storage::sha256_hex(&bytes),
        })
    }

    /// The id of the row `created_by` already made in `org` with this key (artifacts: not
    /// FAILED). The same key with another request is 422 MRL-IDEM-002.
    pub(crate) async fn earlier(
        &self,
        db: &Db,
        table: Created,
        org: &str,
        created_by: &str,
    ) -> Result<Option<Uuid>, Error> {
        let sql = match table {
            Created::Upload => {
                "SELECT id, request_sha256 FROM uploads
                 WHERE org_id = $1 AND created_by = $2 AND idempotency_key = $3"
            }
            Created::Artifact => {
                "SELECT id, request_sha256 FROM artifacts
                 WHERE org_id = $1 AND created_by = $2 AND idempotency_key = $3 AND state <> 'FAILED'"
            }
            Created::Secret => {
                "SELECT id, request_sha256 FROM secrets_metadata
                 WHERE org_id = $1 AND created_by = $2 AND idempotency_key = $3"
            }
        };
        let found: Option<(Uuid, Option<String>)> = sqlx::query_as(sql)
            .bind(org)
            .bind(created_by)
            .bind(&self.key)
            .fetch_optional(db.pool())
            .await?;
        match found {
            Some((_, sha)) if sha.as_deref() != Some(self.request_sha256.as_str()) => {
                Err(ApiError::new(
                    ErrorCode::IdempotencyMismatch,
                    "This key was already used with a different request body.",
                )
                .into())
            }
            other => Ok(other.map(|(id, _)| id)),
        }
    }
}

/// True for a unique violation of a `*_idempotency` index: a concurrent try with the same
/// key won.
pub(crate) fn is_idempotency_conflict(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(d) if d.code().as_deref() == Some("23505")
        && d.constraint().is_some_and(|c| c.ends_with("_idempotency")))
}

/// A new item of `kind` about to be created in `org` and `project`: creation is
/// authorized against exactly where it will live.
pub(crate) fn destination(kind: &str, org: &str, project: Option<String>) -> Resource {
    Resource {
        kind: kind.to_owned(),
        id: None,
        org: Some(org.to_owned()),
        project,
    }
}

/// `seams.authorize`, as the HTTP layer maps it: Deny → 403, Hide → 404.
pub(crate) async fn allow(
    seams: &Seams,
    ctx: &RequestContext,
    action: &str,
    res: &Resource,
) -> Result<(), Error> {
    match seams.authorizer.authorize(ctx, action, res).await? {
        Decision::Allow => Ok(()),
        Decision::Deny => Err(ApiError::new(
            ErrorCode::Forbidden,
            "You don't have permission to do this.",
        )
        .into()),
        Decision::Hide => Err(not_found(&res.kind)),
    }
}

/// The sum of the file sizes, which must fit PostgreSQL's `bigint`.
fn total_size(m: &ArtifactManifest) -> Result<i64, Error> {
    m.files
        .iter()
        .try_fold(0u64, |sum, f| sum.checked_add(f.size))
        .and_then(|t| i64::try_from(t).ok())
        .ok_or_else(|| {
            field(
                "/manifest/files",
                "The files add up to more than 9223372036854775807 bytes, the most one artifact can hold.",
            )
        })
}

/// One path segment as a case-insensitive file system sees it: lowercase, without the
/// trailing dots and spaces Windows ignores.
fn canonical(segment: &str) -> String {
    segment.trim_end_matches(['.', ' ']).to_lowercase()
}

/// True for keys (relative to the org) in an area that is cleaned up or rewritten.
fn is_staging(relative: &str) -> bool {
    let mut segs = relative.split('/').filter(|s| !s.is_empty()).map(canonical);
    let first = segs.next();
    first.as_deref() == Some("uploads")
        || first.as_deref() == Some("_staging")
        || segs.any(|s| s == "_staging")
}

/// True for keys in the org's reserved `blobs/` area, however it is spelled (`BLOBS`,
/// `Blobs.`): content-addressed, written only after the server checked the bytes, and
/// referenced by committed artifacts and finished uploads.
fn is_immutable(store: &Store, key: &str) -> bool {
    let prefix = store.keys().prefix();
    let rest = match key.get(..prefix.len()) {
        Some(p) if p.eq_ignore_ascii_case(prefix) => &key[prefix.len()..],
        _ => key,
    };
    rest.split('/')
        .find(|s| !s.is_empty())
        .is_some_and(|s| canonical(s) == "blobs")
}

fn immutable() -> Error {
    ApiError::new(
        ErrorCode::Forbidden,
        "Stored files can't be overwritten; upload to a new place instead.",
    )
    .into()
}

/// Takes the transaction-scoped lock on object `key` of `org`. Writers recording an intent
/// ([`ArtifactStore::intend`]) and the cleanup job deleting a blob take it, so a deletion
/// never falls between a writer's intent and its check of the object.
pub(crate) async fn lock_key(
    conn: &mut sqlx::PgConnection,
    org: &str,
    key: &str,
) -> Result<(), Error> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("mrl.object:{org}:{key}"))
        .execute(conn)
        .await?;
    Ok(())
}

/// The ticket signing key. With a `SecretStore` it is shared there: created once, under a
/// PostgreSQL advisory lock, so servers starting together on one database agree on it. A
/// team server without a store signs nothing (MRL-ENV-190), since its replicas could not
/// verify each other's tickets; a solo server without one uses a key per process.
async fn ticket_key(
    db: &Db,
    mode: Mode,
    secrets: Option<&Arc<dyn SecretStore>>,
) -> Result<TicketKey, Error> {
    let fresh = || format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let value = match (secrets, mode) {
        (Some(s), _) => {
            let mut tx = db.pool().begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(TICKET_KEY_NAME)
                .execute(&mut *tx)
                .await?;
            let value = match s.get(TICKET_KEY_NAME).await? {
                Some(v) => v.expose().to_owned(),
                None => {
                    let v = fresh();
                    s.set(TICKET_KEY_NAME, crate::seams::Secret::new(v.clone()))
                        .await?;
                    v
                }
            };
            tx.commit().await?;
            value
        }
        (None, Mode::Solo) => fresh(),
        (None, _) => {
            return Err(Error::coded(
                codes::NO_SECRET_STORE,
                StatusCode::SERVICE_UNAVAILABLE,
                "This team server has no secret store, so it can't sign upload and download links that every server copy accepts.",
            ))
        }
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

/// An existing item of `kind` in `org`, with the project recorded on it. Reading it, listing
/// it and linking to it are all checked against this same resource.
pub(crate) fn owned(kind: &str, org: &str, id: String, project: Option<String>) -> Resource {
    Resource {
        kind: kind.to_owned(),
        id: Some(id),
        org: Some(org.to_owned()),
        project,
    }
}

/// The action that reads artifacts (and the lineage graph).
pub(crate) const READ: &str = "lineage.read";

impl Artifact {
    /// What reads of this artifact are authorized against.
    pub(crate) fn resource(&self, org: &str) -> Resource {
        owned("artifact", org, self.id.clone(), self.project_id.clone())
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
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NewArtifact {
    /// Unique name in the org (or project).
    pub name: String,
    /// Folder of the files, relative to the org, ending in `/` (like `exports/exp1/`). Only
    /// the server's own modules may use it ([`ArtifactStore::register`]); the HTTP API
    /// refuses it and takes `uploads` instead, since a folder says nothing about who may
    /// read its files.
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
    /// Where each file's bytes are: manifest path → id of a finished upload the caller may
    /// see, with exactly that size and sha256. Every file needs one.
    #[serde(default)]
    pub uploads: BTreeMap<String, String>,
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
    files_checked: bool,
}

/// A stored file to stage for the attempt.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InputFile {
    /// Name the worker sees, like `model`, `dataset.train` or `resume_from`.
    pub name: String,
    /// Object key in the job org's storage area, like `orgs/{org}/blobs/sha256/…`.
    pub key: String,
    /// Lowercase hex sha256 of the file.
    pub sha256: String,
    /// Size in bytes.
    pub size: u64,
    /// Path relative to the attempt's staging folder.
    pub local_path: String,
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
    /// Full key prefix the files were registered from. Downloads read the checked copies in
    /// `blobs/sha256/…`, so later changes there do not matter.
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
    #[serde(skip)]
    files_checked: bool,
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
            files_checked: r.files_checked,
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
    headers: axum::http::HeaderMap,
    Json(body): Json<NewArtifact>,
) -> Result<(StatusCode, Json<Artifact>), Error> {
    if body.prefix.is_some() {
        return Err(field(
            "/prefix",
            "Send each file as a finished upload in `uploads`; storage folders are only for the server's own modules.",
        ));
    }
    Ok((
        StatusCode::CREATED,
        Json(
            store
                .register(
                    &s.seams,
                    &ctx,
                    body.clone(),
                    Idempotency::of(&headers, &body).as_ref(),
                )
                .await?,
        ),
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
    let org = org_of(&ctx)?;
    let found = store
        .get(&org, id)
        .await?
        .ok_or_else(|| not_found("artifact"))?;
    s.authorize(&ctx, READ, &found.resource(&org)).await?;
    Ok(Json(found))
}

/// Lists committed artifacts, newest last.
#[utoipa::path(get, path = "/artifacts", tag = "artifacts", operation_id = "list_artifacts", params(ListQuery),
    responses((status = 200, body = Object, description = "`{items: [Artifact], next_cursor}`")))]
async fn list(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Query(q): Query<ListQuery>,
) -> Result<Json<Page<Artifact>>, Error> {
    s.authorize(&ctx, READ, &resource(&ctx, "artifact", None))
        .await?;
    let org = org_of(&ctx)?;
    let page = PageQuery {
        limit: q.limit.clone(),
        cursor: q.cursor.clone(),
    }
    .resolve()?;
    let mut after = match page.after.as_deref() {
        Some(c) => Some(Uuid::parse_str(c).map_err(|_| {
            ApiError::new(
                ErrorCode::Pagination,
                "This cursor does not belong to this list.",
            )
        })?),
        None => None,
    };
    // Each row is checked like a GET of it; hidden rows are skipped and the scan goes on
    // until the page is full, so cursors only ever hold ids the caller may see.
    let want = page.fetch();
    let batch = i64::try_from(want).unwrap_or(201);
    let mut items = Vec::with_capacity(want);
    'scan: loop {
        let rows: Vec<ArtifactRow> = sqlx::query_as(
            "SELECT id, project_id, name, kind, prefix, manifest, size_bytes, state, created_by, created_at, committed_at, files_checked
             FROM artifacts
             WHERE org_id = $1 AND state = 'COMMITTED' AND ($2::text IS NULL OR kind = $2)
               AND ($3::uuid IS NULL OR id > $3)
             ORDER BY id LIMIT $4",
        )
        .bind(&org)
        .bind(&q.kind)
        .bind(after)
        .bind(batch)
        .fetch_all(store.db.pool())
        .await?;
        let last_batch = rows.len() < want;
        for row in rows {
            after = Some(row.id);
            let a = Artifact::from(row);
            if s.seams
                .authorizer
                .authorize(&ctx, READ, &a.resource(&org))
                .await?
                == Decision::Allow
            {
                items.push(a);
                if items.len() == want {
                    break 'scan;
                }
            }
        }
        if last_batch {
            break;
        }
    }
    Ok(Json(Page::from_overfetch(items, &page, |a| {
        a.uuid.to_string()
    })))
}

/// `POST /artifacts/{art}:download` → a GET ticket for one file of a committed artifact.
#[utoipa::path(post, path = "/artifacts/{art}:download", tag = "artifacts", operation_id = "download_artifact", request_body = DownloadRequest,
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
    let org = org_of(&ctx)?;
    let found = store
        .get(&org, id)
        .await?
        .filter(|a| a.state == "COMMITTED")
        .ok_or_else(|| not_found("artifact"))?;
    s.authorize(&ctx, READ, &found.resource(&org)).await?;
    for guard in store.download_guards() {
        guard.check(&s, &ctx, &org, &found).await?;
    }
    let (key, file) = store.download_source(&org, &found, &body.path).await?;
    Ok(Json(
        store
            .ticket(&s.seams, &ctx, &key, TicketMethod::Get, file.size)
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

#[cfg(test)]
mod tests {
    use super::{is_staging, total_size, ArtifactManifest};

    fn manifest(sizes: &[u64]) -> ArtifactManifest {
        let files: Vec<serde_json::Value> = sizes
            .iter()
            .enumerate()
            .map(|(i, s)| serde_json::json!({"path": format!("f{i}"), "size": s, "sha256": "0".repeat(64), "role": "other"}))
            .collect();
        ArtifactManifest::from_json(
            serde_json::json!({"schema_version": "artifact-manifest/v1", "kind": "other",
                "created_at": "2026-11-16T00:00:00Z", "files": files})
            .to_string()
            .as_bytes(),
        )
        .unwrap()
    }

    #[test]
    fn storage_settings_follow_the_platform_and_region() {
        use super::{backend_config_with, BackendConfig};
        let config = |url: &str| {
            let mut c = crate::config::Config::default();
            c.object_store.url = Some(url.to_owned());
            c
        };
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| (*v).to_owned())
            }
        };
        let s3 = |pairs| match backend_config_with(&config("s3://b"), &env(pairs)) {
            Ok(BackendConfig::S3(c)) => c,
            other => panic!("unexpected: {:?}", other.err()),
        };
        assert_eq!(s3(&[]).region, "us-east-1");
        assert_eq!(
            s3(&[("AWS_DEFAULT_REGION", "eu-west-1")]).region,
            "eu-west-1"
        );
        assert_eq!(
            s3(&[
                ("AWS_REGION", "ap-south-1"),
                ("AWS_DEFAULT_REGION", "eu-west-1")
            ])
            .region,
            "ap-south-1"
        );
        assert_eq!(s3(&[]).bucket, "b");
        let local = |url| match backend_config_with(&config(url), &env(&[])) {
            Ok(BackendConfig::Local { root }) => root,
            other => panic!("unexpected: {:?}", other.err()),
        };
        #[cfg(windows)]
        assert_eq!(
            local("file:///C:/mrl/objects"),
            std::path::PathBuf::from(r"C:\mrl\objects")
        );
        #[cfg(unix)]
        {
            assert_eq!(
                local("file:///var/mrl"),
                std::path::PathBuf::from("/var/mrl")
            );
            assert_eq!(
                local("file:///var/my%20data"),
                std::path::PathBuf::from("/var/my data")
            );
        }
    }

    #[test]
    fn sizes_must_fit_bigint() {
        let max = i64::MAX as u64;
        assert_eq!(total_size(&manifest(&[])).unwrap(), 0);
        assert_eq!(total_size(&manifest(&[0])).unwrap(), 0);
        assert_eq!(total_size(&manifest(&[10, 32])).unwrap(), 42);
        assert_eq!(total_size(&manifest(&[max])).unwrap(), i64::MAX);
        assert_eq!(total_size(&manifest(&[max - 1, 1])).unwrap(), i64::MAX);
        assert!(total_size(&manifest(&[max + 1])).is_err());
        assert!(total_size(&manifest(&[max, 1])).is_err());
        assert!(
            total_size(&manifest(&[u64::MAX, 1])).is_err(),
            "u64 overflow"
        );
    }

    #[test]
    fn staging_areas_are_refused() {
        assert!(is_staging("uploads/up_1/parts/000000"));
        assert!(is_staging("runs/r/checkpoints/c/_staging/attempt-1/w.bin"));
        assert!(!is_staging("evals/e1/results.json"));
        assert!(!is_staging("exports/uploads/x"));
        assert!(!is_staging("blobs/sha256/aa/bb/x"));
    }
}

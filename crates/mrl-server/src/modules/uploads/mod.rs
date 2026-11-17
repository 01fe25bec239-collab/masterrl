//! Resumable uploads (P19).
//!
//! 1. `POST /uploads {filename, size, sha256, part_size?}` opens an upload (24 h to finish).
//! 2. Send parts `0..part_count`: `PUT /uploads/{up}/parts/{n}` (signed in), or ask
//!    `POST /uploads/{up}:presign {parts}` for PUT tickets to `/api/v1/transfer/{ticket}`.
//!    Every part but the last has exactly `part_size` bytes.
//! 3. `GET /uploads/{up}` lists the parts that arrived, so a client resumes after a crash.
//! 4. `POST /uploads/{up}:complete` joins the parts, checks the sha256 of the whole file and
//!    stores it as the blob `orgs/{org}/blobs/sha256/…` (once per content). `:abort` cancels.
//!
//! Parts are objects under `orgs/{org}/uploads/{id}/parts/`. The cleanup job expires open
//! uploads after 24 hours and deletes anything under `uploads/` older than that which no
//! open upload owns.

use crate::app::AppState;
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::http::BODY_LIMIT;
use crate::modules::artifacts::{
    codes, field, not_found, org_of, resource, split_action, ArtifactStore, Error,
};
use crate::seams::RequestContext;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use mrl_domain::ids::UploadId;
use mrl_storage::{sha256_hex, StorageError, Store, TicketMethod, TransferOptions, TransferTicket};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

/// Smallest part size.
pub const MIN_PART_SIZE: u64 = 1 << 20;
/// Largest (and default) part size: one request body (P07's 2 MiB limit).
pub const MAX_PART_SIZE: u64 = BODY_LIMIT as u64;
/// Most parts per upload.
pub const MAX_PARTS: u64 = 10_000;
/// Most tickets per `:presign` call.
const MAX_PRESIGN: usize = 1000;

/// `POST /uploads` body.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NewUpload {
    /// File name, shown to people (1–255 characters, no `/` or `\`).
    pub filename: String,
    /// Total size in bytes.
    pub size: u64,
    /// sha256 of the whole file, lowercase hex.
    pub sha256: String,
    /// Bytes per part, 1–2 MiB (default 2 MiB).
    #[serde(default)]
    pub part_size: Option<u64>,
    /// Project, if the upload belongs to one.
    #[serde(default)]
    pub project_id: Option<String>,
}

/// An upload and its progress.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct Upload {
    /// `up_…`.
    pub id: String,
    /// File name.
    pub filename: String,
    /// Total size in bytes.
    pub size: u64,
    /// Expected sha256.
    pub sha256: String,
    /// Bytes per part.
    pub part_size: u64,
    /// Number of parts.
    pub part_count: u64,
    /// `OPEN`, `COMPLETED`, `ABORTED` or `EXPIRED`.
    pub state: String,
    /// Parts already stored (open uploads only).
    pub parts_received: Vec<u64>,
    /// The finished file's key (completed uploads only).
    pub blob_key: Option<String>,
    /// Project.
    pub project_id: Option<String>,
    /// When it was opened.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
    /// An open upload expires at this time.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub expires_at: OffsetDateTime,
}

/// `POST /uploads/{up}:presign` body.
#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PresignRequest {
    /// Part numbers; default: every part not received yet (at most 1000).
    #[serde(default)]
    pub parts: Option<Vec<u64>>,
}

/// One PUT ticket per part.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct PartTicket {
    /// Part number.
    pub part: u64,
    /// The ticket: `PUT` its `url` with the part's bytes.
    #[schema(value_type = Object)]
    pub ticket: TransferTicket,
}

#[derive(sqlx::FromRow)]
struct Row {
    id: Uuid,
    project_id: Option<String>,
    filename: String,
    size: i64,
    sha256: String,
    part_size: i64,
    state: String,
    blob_key: Option<String>,
    created_at: OffsetDateTime,
    expires_at: OffsetDateTime,
}

macro_rules! columns {
    () => {
        "id, project_id, filename, size, sha256, part_size, state, blob_key, created_at, expires_at"
    };
}

impl Row {
    fn size(&self) -> u64 {
        u64::try_from(self.size).unwrap_or(0)
    }
    fn part_size(&self) -> u64 {
        u64::try_from(self.part_size).unwrap_or(1).max(1)
    }
    fn part_count(&self) -> u64 {
        self.size().div_ceil(self.part_size())
    }
    /// Bytes part `n` must have.
    fn part_len(&self, n: u64) -> u64 {
        self.size()
            .saturating_sub(n.saturating_mul(self.part_size()))
            .min(self.part_size())
    }
    fn is_open(&self) -> bool {
        self.state == "OPEN" && self.expires_at > OffsetDateTime::now_utc()
    }
    fn key(&self) -> String {
        UploadId::from_uuid(self.id).to_string()
    }
    fn into_upload(self, parts_received: Vec<u64>) -> Upload {
        Upload {
            id: self.key(),
            part_count: self.part_count(),
            size: self.size(),
            part_size: self.part_size(),
            filename: self.filename,
            sha256: self.sha256,
            state: self.state,
            parts_received,
            blob_key: self.blob_key,
            project_id: self.project_id,
            created_at: self.created_at,
            expires_at: self.expires_at,
        }
    }
}

fn closed(row: &Row) -> Error {
    let state = if row.state == "OPEN" {
        "EXPIRED"
    } else {
        &row.state
    };
    Error::coded(
        codes::UPLOAD_CLOSED,
        StatusCode::CONFLICT,
        format!("This upload is {state} and takes no more changes."),
    )
}

async fn load(store: &ArtifactStore, org: &str, up: &str) -> Result<Row, Error> {
    let id = up
        .parse::<UploadId>()
        .map_err(|_| not_found("upload"))?
        .as_uuid();
    sqlx::query_as(concat!(
        "SELECT ",
        columns!(),
        " FROM uploads WHERE org_id = $1 AND id = $2"
    ))
    .bind(org)
    .bind(id)
    .fetch_optional(store.db().pool())
    .await?
    .ok_or_else(|| not_found("upload"))
}

/// Stored parts: part number → size. Ignores objects that are not parts.
async fn received(store: &Store, row: &Row) -> Result<BTreeMap<u64, u64>, Error> {
    let prefix = store.keys().upload_parts(&row.key())?;
    Ok(store
        .list(&prefix)
        .await?
        .into_iter()
        .filter_map(|o| {
            let n = o.key.strip_prefix(&prefix)?.parse::<u64>().ok()?;
            Some((n, o.size))
        })
        .collect())
}

async fn delete_parts(store: &Store, upload_key: &str) -> Result<(), Error> {
    for o in store.list(&store.keys().upload_parts(upload_key)?).await? {
        store.delete(&o.key).await?;
    }
    Ok(())
}

fn part_key(store: &Store, row: &Row, n: u64) -> Result<String, Error> {
    Ok(store
        .keys()
        .upload_part(&row.key(), usize::try_from(n).unwrap_or(usize::MAX))?)
}

fn check_new(new: &NewUpload) -> Result<u64, Error> {
    let mut errors = Vec::new();
    let bad = |path: &str, msg: String| {
        FieldError::new(
            path,
            ErrorCode::FieldValue,
            msg,
            "Change this field and send the request again.",
        )
    };
    let name_ok = (1..=255).contains(&new.filename.chars().count())
        && !new.filename.contains(['/', '\\', '\0']);
    if !name_ok {
        errors.push(bad(
            "/filename",
            "The file name needs 1 to 255 characters and no `/` or `\\`.".into(),
        ));
    }
    if sha256_hex_ok(&new.sha256).is_none() {
        errors.push(bad(
            "/sha256",
            "The sha256 must be 64 lowercase hex characters.".into(),
        ));
    }
    let part_size = new.part_size.unwrap_or(MAX_PART_SIZE);
    if !(MIN_PART_SIZE..=MAX_PART_SIZE).contains(&part_size) {
        errors.push(bad(
            "/part_size",
            format!("Parts can be {MIN_PART_SIZE} to {MAX_PART_SIZE} bytes, not {part_size}."),
        ));
    } else if new.size.div_ceil(part_size) > MAX_PARTS {
        errors.push(bad(
            "/size",
            format!(
                "With {part_size}-byte parts an upload can be at most {} bytes.",
                part_size * MAX_PARTS
            ),
        ));
    }
    if errors.is_empty() {
        Ok(part_size)
    } else {
        Err(ApiError::validation(errors).into())
    }
}

fn sha256_hex_ok(s: &str) -> Option<&str> {
    (s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))).then_some(s)
}

/// Opens an upload.
#[utoipa::path(post, path = "/uploads", tag = "uploads", request_body = NewUpload,
    responses((status = 201, body = Upload), (status = 422, body = crate::error::Problem)))]
pub(crate) async fn create(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Json(new): Json<NewUpload>,
) -> Result<(StatusCode, Json<Upload>), Error> {
    s.authorize(&ctx, "uploads.write", &resource(&ctx, "upload", None))
        .await?;
    let part_size = check_new(&new)?;
    let org = org_of(&ctx)?;
    let id = s.seams.ids.new_uuid();
    let row: Row = sqlx::query_as(concat!(
        "INSERT INTO uploads (id, org_id, project_id, created_by, filename, size, sha256, part_size, state, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'OPEN', now() + interval '24 hours')
         RETURNING ",
        columns!()
    ))
    .bind(id)
    .bind(&org)
    .bind(&new.project_id)
    .bind(&ctx.principal.id)
    .bind(&new.filename)
    .bind(i64::try_from(new.size).unwrap_or(i64::MAX))
    .bind(&new.sha256)
    .bind(i64::try_from(part_size).unwrap_or(i64::MAX))
    .fetch_one(store.db().pool())
    .await?;
    Ok((StatusCode::CREATED, Json(row.into_upload(Vec::new()))))
}

/// Reads an upload and the parts that arrived.
#[utoipa::path(get, path = "/uploads/{up}", tag = "uploads",
    params(("up" = String, Path, description = "`up_…`")),
    responses((status = 200, body = Upload), (status = 404, body = crate::error::Problem)))]
pub(crate) async fn get(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Path(up): Path<String>,
) -> Result<Json<Upload>, Error> {
    s.authorize(
        &ctx,
        "uploads.write",
        &resource(&ctx, "upload", Some(up.clone())),
    )
    .await?;
    let org = org_of(&ctx)?;
    let row = load(&store, &org, &up).await?;
    let parts = if row.state == "OPEN" {
        let st = store.org_store(&s, &org).await?;
        received(&st, &row)
            .await?
            .into_iter()
            .filter(|(n, size)| *n < row.part_count() && *size == row.part_len(*n))
            .map(|(n, _)| n)
            .collect()
    } else {
        Vec::new()
    };
    Ok(Json(row.into_upload(parts)))
}

/// Stores one part. With `x-mrl-content-sha256`, the part must match it.
#[utoipa::path(put, path = "/uploads/{up}/parts/{n}", tag = "uploads",
    params(("up" = String, Path, description = "`up_…`"), ("n" = u64, Path, description = "Part number from 0.")),
    request_body(content = Vec<u8>, content_type = "application/octet-stream"),
    responses((status = 200, body = Object, description = "`{part, size, sha256}`"), (status = 409, body = crate::error::Problem)))]
pub(crate) async fn put_part(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Path((up, n)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<serde_json::Value>, Error> {
    s.authorize(
        &ctx,
        "uploads.write",
        &resource(&ctx, "upload", Some(up.clone())),
    )
    .await?;
    let org = org_of(&ctx)?;
    let row = load(&store, &org, &up).await?;
    if !row.is_open() {
        return Err(closed(&row));
    }
    let n = n
        .parse::<u64>()
        .ok()
        .filter(|n| *n < row.part_count())
        .ok_or_else(|| field("/n", "This upload has no part with this number."))?;
    let want = row.part_len(n);
    if body.len() as u64 != want {
        return Err(Error::coded(
            codes::PARTS_INCOMPLETE,
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("Part {n} has {} bytes; it must have {want}.", body.len()),
        ));
    }
    let sha = sha256_hex(&body);
    if let Some(h) = headers.get(crate::modules::artifacts::SHA256_HEADER) {
        let expected = h.to_str().unwrap_or_default();
        if expected != sha {
            return Err(StorageError::ChecksumMismatch {
                key: format!("part {n}"),
                expected: expected.chars().take(64).collect(),
                actual: sha,
            }
            .into());
        }
    }
    let st = store.org_store(&s, &org).await?;
    st.put(&part_key(&st, &row, n)?, body).await?;
    Ok(Json(
        serde_json::json!({ "part": n, "size": want, "sha256": sha }),
    ))
}

/// `POST /uploads/{up}:complete`, `:abort` or `:presign`.
#[utoipa::path(post, path = "/uploads/{up}:complete", tag = "uploads",
    params(("up" = String, Path, description = "`up_…`")),
    responses((status = 200, body = Upload), (status = 409, body = crate::error::Problem), (status = 422, body = crate::error::Problem)))]
pub(crate) async fn action(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Path(segment): Path<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, Error> {
    let (up, act) = split_action(&segment);
    s.authorize(
        &ctx,
        "uploads.write",
        &resource(&ctx, "upload", Some(up.to_owned())),
    )
    .await?;
    let org = org_of(&ctx)?;
    let row = load(&store, &org, up).await?;
    match act {
        Some("complete") => json(complete(&s, &store, &org, row).await?),
        Some("abort") => json(abort(&s, &store, &org, row).await?),
        Some("presign") => {
            let req: PresignRequest = if body.is_empty() {
                PresignRequest::default()
            } else {
                serde_json::from_slice(&body).map_err(|e| {
                    ApiError::new(
                        ErrorCode::BadBody,
                        format!("The body is not a presign request: {e}"),
                    )
                })?
            };
            let tickets = presign(&s, &store, &ctx, &org, row, req).await?;
            Ok(Json(serde_json::json!({ "tickets": tickets })))
        }
        _ => Err(ApiError::new(ErrorCode::RouteNotFound, "No endpoint has this path.").into()),
    }
}

fn json(v: impl Serialize) -> Result<Json<serde_json::Value>, Error> {
    Ok(Json(serde_json::to_value(v).unwrap_or_default()))
}

async fn presign(
    s: &AppState,
    store: &ArtifactStore,
    ctx: &RequestContext,
    org: &str,
    row: Row,
    req: PresignRequest,
) -> Result<Vec<PartTicket>, Error> {
    if !row.is_open() {
        return Err(closed(&row));
    }
    let st = store.org_store(s, org).await?;
    let parts = match req.parts {
        Some(p) => p,
        None => {
            let have = received(&st, &row).await?;
            (0..row.part_count())
                .filter(|n| have.get(n) != Some(&row.part_len(*n)))
                .take(MAX_PRESIGN)
                .collect()
        }
    };
    if parts.len() > MAX_PRESIGN {
        return Err(field("/parts", "Ask for at most 1000 parts at a time."));
    }
    let mut out = Vec::with_capacity(parts.len());
    for n in parts {
        if n >= row.part_count() {
            return Err(field(
                "/parts",
                "This upload has no part with one of these numbers.",
            ));
        }
        let key = part_key(&st, &row, n)?;
        let ticket = store
            .ticket(s, ctx, &key, TicketMethod::Put, row.part_len(n))
            .await?;
        out.push(PartTicket { part: n, ticket });
    }
    Ok(out)
}

async fn abort(s: &AppState, store: &ArtifactStore, org: &str, row: Row) -> Result<Upload, Error> {
    let done = sqlx::query(
        "UPDATE uploads SET state = 'ABORTED', finished_at = now() WHERE id = $1 AND state = 'OPEN'",
    )
    .bind(row.id)
    .execute(store.db().pool())
    .await?
    .rows_affected();
    if done == 0 {
        return Err(closed(&row));
    }
    delete_parts(&store.org_store(s, org).await?, &row.key()).await?;
    let row = load(store, org, &row.key()).await?;
    Ok(row.into_upload(Vec::new()))
}

/// Joins the parts into a temporary file while hashing, checks the sha256, stores the blob.
async fn complete(
    s: &AppState,
    store: &ArtifactStore,
    org: &str,
    row: Row,
) -> Result<Upload, Error> {
    if !row.is_open() {
        return Err(closed(&row));
    }
    let st = store.org_store(s, org).await?;
    let have = received(&st, &row).await?;
    let wrong: Vec<String> = (0..row.part_count())
        .filter_map(|n| match have.get(&n) {
            None => Some(format!("{n} (missing)")),
            Some(size) if *size != row.part_len(n) => Some(format!("{n} ({size} bytes)")),
            Some(_) => None,
        })
        .collect();
    if !wrong.is_empty() {
        let shown: Vec<&str> = wrong.iter().take(20).map(String::as_str).collect();
        return Err(Error::coded(
            codes::PARTS_INCOMPLETE,
            StatusCode::CONFLICT,
            format!(
                "{} part(s) need sending again: {}.",
                wrong.len(),
                shown.join(", ")
            ),
        ));
    }
    let tmp = std::env::temp_dir().join(format!(
        "mrl-upload-{}-{}",
        row.key(),
        Uuid::new_v4().simple()
    ));
    let result = join_and_store(&st, &row, &tmp).await;
    for p in [tmp.clone(), ledger_of(&tmp)] {
        let _ = tokio::fs::remove_file(p).await;
    }
    let blob = result?;
    let done = sqlx::query(
        "UPDATE uploads SET state = 'COMPLETED', blob_key = $2, finished_at = now()
         WHERE id = $1 AND state = 'OPEN'",
    )
    .bind(row.id)
    .bind(&blob)
    .execute(store.db().pool())
    .await?
    .rows_affected();
    if done == 0 {
        return Err(closed(&row));
    }
    delete_parts(&st, &row.key()).await?;
    Ok(load(store, org, &row.key()).await?.into_upload(Vec::new()))
}

/// `upload_file` keeps its resume ledger next to the file.
pub(crate) fn ledger_of(path: &std::path::Path) -> std::path::PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_owned();
    name.push(".mrl-upload.json");
    path.with_file_name(name)
}

async fn join_and_store(st: &Store, row: &Row, tmp: &std::path::Path) -> Result<String, Error> {
    let io = |e: std::io::Error| {
        Error::from(StorageError::Io {
            path: tmp.to_owned(),
            source: e,
        })
    };
    let mut file = tokio::fs::File::create(tmp).await.map_err(io)?;
    let mut hash = Sha256::new();
    for n in 0..row.part_count() {
        let bytes = st.get(&part_key(st, row, n)?).await?;
        hash.update(&bytes);
        file.write_all(&bytes).await.map_err(io)?;
    }
    file.flush().await.map_err(io)?;
    drop(file);
    let actual: String = hash.finalize().iter().map(|b| format!("{b:02x}")).collect();
    if actual != row.sha256 {
        return Err(StorageError::ChecksumMismatch {
            key: row.filename.clone(),
            expected: row.sha256.clone(),
            actual,
        }
        .into());
    }
    let blob = st.keys().blob(&actual)?;
    let exists = match st.head(&blob).await {
        Ok(info) => info.size == row.size(),
        Err(StorageError::NotFound { .. }) => false,
        Err(e) => return Err(e.into()),
    };
    if !exists {
        store_file(st, tmp, &blob, row.size()).await?;
    }
    Ok(blob)
}

/// Stores the local file `tmp` of `size` bytes at `key`: one put up to 16 MiB, else a
/// parallel multipart transfer. The caller deletes `tmp` and its [`ledger_of`] file.
pub(crate) async fn store_file(
    st: &Store,
    tmp: &std::path::Path,
    key: &str,
    size: u64,
) -> Result<(), Error> {
    let opts = TransferOptions::default();
    if size <= opts.part_size {
        let data = tokio::fs::read(tmp).await.map_err(|e| StorageError::Io {
            path: tmp.to_owned(),
            source: e,
        })?;
        st.put(key, Bytes::from(data)).await?;
    } else {
        st.upload_file(tmp, key, opts).await?;
    }
    Ok(())
}

/// What one cleanup pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swept {
    /// Open uploads past their 24 hours.
    pub expired: u64,
    /// Objects deleted under `uploads/`.
    pub objects: u64,
    /// Artifacts left PENDING for 24 hours, now FAILED.
    pub stale_artifacts: u64,
}

/// One cleanup pass: expires open uploads after 24 hours, deletes objects under each org's
/// `uploads/` older than 24 hours that no open upload owns, and fails artifacts left
/// PENDING for 24 hours.
pub async fn sweep(store: &ArtifactStore) -> Result<Swept, Error> {
    let pool = store.db().pool();
    let expired = sqlx::query(
        "UPDATE uploads SET state = 'EXPIRED', finished_at = now()
         WHERE state = 'OPEN' AND expires_at < now()",
    )
    .execute(pool)
    .await?
    .rows_affected();
    let stale_artifacts = sqlx::query(
        "UPDATE artifacts SET state = 'FAILED'
         WHERE state = 'PENDING' AND created_at < now() - interval '24 hours'",
    )
    .execute(pool)
    .await?
    .rows_affected();
    // ponytail: one listing per org that ever uploaded; page through orgs when there are many.
    let orgs: Vec<String> = sqlx::query_scalar("SELECT DISTINCT org_id FROM uploads")
        .fetch_all(pool)
        .await?;
    let open: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM uploads WHERE state = 'OPEN'")
        .fetch_all(pool)
        .await?;
    let open: std::collections::HashSet<String> = open
        .into_iter()
        .map(|id| UploadId::from_uuid(id).to_string())
        .collect();
    let backend = store.plain_backend()?;
    let cutoff = OffsetDateTime::now_utc() - time::Duration::hours(24);
    let mut objects = 0;
    for org in orgs {
        let st = backend.org(&org)?;
        let prefix = format!("{}uploads/", st.keys().prefix());
        for o in st.list(&prefix).await? {
            let owner = o.key[prefix.len()..].split('/').next().unwrap_or_default();
            if o.last_modified < cutoff && !open.contains(owner) {
                st.delete(&o.key).await?;
                objects += 1;
            }
        }
    }
    Ok(Swept {
        expired,
        objects,
        stale_artifacts,
    })
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(create, get, put_part, action),
    components(schemas(NewUpload, Upload, PresignRequest, PartTicket)),
    tags((name = "uploads", description = "Resumable uploads. Also `POST /uploads/{up}:abort` and `POST /uploads/{up}:presign` (body `PresignRequest`, answer `{tickets: [PartTicket]}`)."))
)]
pub(crate) struct Doc;

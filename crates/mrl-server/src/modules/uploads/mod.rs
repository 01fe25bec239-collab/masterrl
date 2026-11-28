//! Resumable uploads (P19).
//!
//! 1. `POST /uploads {filename, size, sha256, part_size?}` opens an upload (24 h to finish).
//! 2. Send parts `0..part_count`: `PUT /uploads/{up}/parts/{n}` (signed in), or ask
//!    `POST /uploads/{up}:presign {parts}` for PUT tickets to `/api/v1/transfer/{ticket}`.
//!    Every part but the last has exactly `part_size` bytes. On every backend (S3 too) part
//!    tickets go through the server's transfer endpoint, never straight to the bucket: a
//!    presigned S3 PUT can't limit its size, and the endpoint enforces key, expiry and size.
//! 3. `GET /uploads/{up}` lists the parts that arrived, so a client resumes after a crash.
//! 4. `POST /uploads/{up}:complete` joins the parts, checks the sha256 of the whole file and
//!    stores it as the blob `orgs/{org}/blobs/sha256/…` (once per content). `:abort` cancels.
//!
//! Every part write (signed in or through a ticket) must have the part's exact length and
//! is published only while the upload is OPEN and before its deadline. Bytes staged by an
//! interrupted write stay invisible and are collected with the upload. Part tickets never outlive the
//! upload. Parts are objects under `orgs/{org}/uploads/{id}/parts/`. The cleanup job expires open
//! uploads after 24 hours and deletes anything under `uploads/` older than that which no
//! open upload owns, plus scratch folders a killed process left behind.

use crate::app::AppState;
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::http::BODY_LIMIT;
use crate::modules::artifacts::{
    codes, destination, field, is_idempotency_conflict, not_found, org_of, owned, scratch,
    split_action, ArtifactStore, Created, Error, Idempotency, UploadBlob,
};
use crate::seams::{Decision, RequestContext};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use mrl_domain::ids::UploadId;
use mrl_storage::{sha256_hex, StorageError, Store, TransferOptions, TransferTicket};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

/// The action every upload endpoint checks.
pub(crate) const ACTION: &str = "uploads.write";

/// Smallest part size.
pub const MIN_PART_SIZE: u64 = 1 << 20;
/// Largest (and default) part size: one request body (P07's 2 MiB limit).
pub const MAX_PART_SIZE: u64 = BODY_LIMIT as u64;
/// Most parts per upload.
pub const MAX_PARTS: u64 = 10_000;
/// Most tickets per `:presign` call.
const MAX_PRESIGN: usize = 1000;

/// `POST /uploads` body.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
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

/// `POST /uploads/{up}:presign` answer.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct PresignResponse {
    /// One ticket per part asked for.
    pub tickets: Vec<PartTicket>,
}

/// One PUT ticket per part.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct PartTicket {
    /// Part number.
    pub part: u64,
    /// The ticket: `PUT` its `url` (the server's `/api/v1/transfer/{ticket}`, so
    /// `presigned` is false on every backend) with exactly the part's bytes. It expires after
    /// 10 minutes and allows this one part's key and size.
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

/// The upload `up` of the caller's org, if the caller may see it: the same check as
/// lineage links to it (`ACTION` on the upload with its recorded project).
async fn load_visible(
    s: &AppState,
    store: &ArtifactStore,
    ctx: &RequestContext,
    org: &str,
    up: &str,
) -> Result<Row, Error> {
    let row = load(store, org, up).await?;
    s.authorize(
        ctx,
        ACTION,
        &owned("upload", org, row.key(), row.project_id.clone()),
    )
    .await?;
    Ok(row)
}

/// The blob of the finished upload `up` of `org`, for registering an artifact from it. Not
/// found (404) unless it exists in `org` and the caller may see it; it must be COMPLETED.
pub(crate) async fn finished(
    store: &ArtifactStore,
    seams: &crate::seams::Seams,
    ctx: &RequestContext,
    org: &str,
    up: &str,
) -> Result<UploadBlob, Error> {
    let row = load(store, org, up).await?;
    // Denied and hidden both look like a missing upload; an authorizer that fails is
    // reported as such (503), not as "not found".
    let res = owned("upload", org, row.key(), row.project_id.clone());
    if seams.authorizer.authorize(ctx, ACTION, &res).await? != Decision::Allow {
        return Err(not_found("upload"));
    }
    let key = match (&row.blob_key, row.state.as_str()) {
        (Some(k), "COMPLETED") => k.clone(),
        _ => {
            return Err(field(
                &format!("/uploads/{up}"),
                "This upload is not finished; complete it first.",
            ))
        }
    };
    Ok(UploadBlob {
        key,
        size: row.size(),
        sha256: row.sha256,
    })
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
async fn received(
    db: &crate::db::Db,
    store: &Store,
    row: &Row,
) -> Result<BTreeMap<u64, (String, u64)>, Error> {
    let keys: BTreeMap<u64, String> = sqlx::query_as::<_, (i64, String)>(
        "SELECT part, object_key FROM upload_parts WHERE upload_id = $1",
    )
    .bind(row.id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .filter_map(|(n, key)| u64::try_from(n).ok().map(|n| (n, key)))
    .collect();
    let prefix = store.keys().upload_parts(&row.key())?;
    let objects = store.list(&prefix).await?;
    let mut parts = BTreeMap::new();
    for object in objects {
        let Some(name) = object.key.strip_prefix(&prefix) else {
            continue;
        };
        let number = name.split_once('-').map_or(name, |(n, _)| n);
        let Ok(n) = number.parse() else { continue };
        // Legacy canonical keys remain readable until their upload closes. A staged key
        // is visible only when the database published that exact object.
        let visible = keys
            .get(&n)
            .map_or(name == number, |key| *key == object.key);
        if visible {
            parts.insert(n, (object.key, object.size));
        }
    }
    Ok(parts)
}

async fn lock_upload(tx: &mut crate::db::Tx, id: Uuid) -> Result<Row, Error> {
    sqlx::query_as(concat!(
        "SELECT ",
        columns!(),
        " FROM uploads WHERE id = $1 FOR UPDATE"
    ))
    .bind(id)
    .fetch_one(&mut **tx)
    .await
    .map_err(Error::from)
}

async fn clear_part_refs(tx: &mut crate::db::Tx, id: Uuid) -> Result<(), Error> {
    sqlx::query("DELETE FROM upload_parts WHERE upload_id = $1")
        .bind(id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Deletes a closed upload's parts. The upload's new state is already committed, so a
/// failure is only logged: the cleanup job deletes leftover parts after 24 hours.
async fn tidy(store: &Store, upload_key: &str) {
    if let Err(e) = delete_parts(store, upload_key).await {
        tracing::warn!(
            code = e.code(),
            upload = upload_key,
            "could not delete the parts of a closed upload; the cleanup job will"
        );
    }
}

/// A ticket PUT to an upload part.
pub(crate) struct PartTarget {
    row: Row,
    n: u64,
}

pub(crate) async fn part_target(
    store: &ArtifactStore,
    st: &Store,
    key: &str,
) -> Result<Option<PartTarget>, Error> {
    let Some(rest) = key.strip_prefix(st.keys().prefix()) else {
        return Ok(None);
    };
    let segs: Vec<&str> = rest.split('/').collect();
    let (up, n) = match segs.as_slice() {
        ["uploads", up, "parts", n] if up.parse::<UploadId>().is_ok() => (*up, *n),
        _ => return Ok(None),
    };
    let row = load(store, st.keys().org_id(), up).await?;
    let Some(n) = n.parse::<u64>().ok().filter(|n| *n < row.part_count()) else {
        return Err(field("/n", "This upload has no part with this number."));
    };
    if !row.is_open() {
        return Err(closed(&row));
    }
    Ok(Some(PartTarget { row, n }))
}

impl PartTarget {
    pub(crate) fn check_len(&self, got: u64) -> Result<(), Error> {
        let want = self.row.part_len(self.n);
        if got == want {
            Ok(())
        } else {
            Err(Error::coded(
                codes::PARTS_INCOMPLETE,
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("This part has {got} bytes; it must have {want}."),
            ))
        }
    }

    pub(crate) async fn publish(
        &self,
        store: &ArtifactStore,
        st: &Store,
        body: Bytes,
        digest: &str,
    ) -> Result<(), Error> {
        self.check_len(body.len() as u64)?;
        let key = format!("{}-{digest}", part_key(st, &self.row, self.n)?);
        st.put(&key, body).await?;

        let mut tx = store.db().pool().begin().await?;
        let row = lock_upload(&mut tx, self.row.id).await?;
        let updated = sqlx::query(
            "INSERT INTO upload_parts (upload_id, part, object_key)
             SELECT id, $2, $3 FROM uploads
             WHERE id = $1 AND state = 'OPEN' AND expires_at > clock_timestamp()
             ON CONFLICT (upload_id, part) DO UPDATE SET object_key = EXCLUDED.object_key",
        )
        .bind(row.id)
        .bind(i64::try_from(self.n).map_err(|_| field("/n", "The part number is out of range."))?)
        .bind(&key)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        if updated == 0 {
            if let Err(e) = st.delete(&key).await {
                tracing::warn!(code = e.code(), "could not remove an unpublished part");
            }
            return Err(closed(&row));
        }
        Ok(())
    }
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
#[utoipa::path(post, path = "/uploads", tag = "uploads", operation_id = "create_upload", request_body = NewUpload,
    responses((status = 201, body = Upload), (status = 422, body = crate::error::Problem)))]
pub(crate) async fn create(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    headers: HeaderMap,
    Json(new): Json<NewUpload>,
) -> Result<(StatusCode, Json<Upload>), Error> {
    let org = org_of(&ctx)?;
    s.authorize(
        &ctx,
        ACTION,
        &destination("upload", &org, new.project_id.clone()),
    )
    .await?;
    let part_size = check_new(&new)?;
    // A retry with the same Idempotency-Key (authorized afresh above) gets the same upload.
    let idem = Idempotency::of(&headers, &new);
    let earlier = || async {
        match &idem {
            Some(i) => match i
                .earlier(store.db(), Created::Upload, &org, &ctx.principal.id)
                .await?
            {
                Some(id) => Ok(Some(
                    load_visible(&s, &store, &ctx, &org, &UploadId::from_uuid(id).to_string())
                        .await?,
                )),
                None => Ok(None),
            },
            None => Ok::<_, Error>(None),
        }
    };
    if let Some(row) = earlier().await? {
        return Ok((StatusCode::CREATED, Json(row.into_upload(Vec::new()))));
    }
    let id = s.seams.ids.new_uuid();
    let row: Result<Row, sqlx::Error> = sqlx::query_as(concat!(
        "INSERT INTO uploads (id, org_id, project_id, created_by, filename, size, sha256, part_size, state, expires_at, idempotency_key, request_sha256)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'OPEN', now() + interval '24 hours', $9, $10)
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
    .bind(idem.as_ref().map(|i| &i.key))
    .bind(idem.as_ref().map(|i| &i.request_sha256))
    .fetch_one(store.db().pool())
    .await;
    let row = match row {
        Err(e) if is_idempotency_conflict(&e) => {
            earlier().await?.ok_or_else(|| not_found("upload"))?
        }
        other => other?,
    };
    Ok((StatusCode::CREATED, Json(row.into_upload(Vec::new()))))
}

/// Reads an upload and the parts that arrived.
#[utoipa::path(get, path = "/uploads/{up}", tag = "uploads", operation_id = "get_upload",
    params(("up" = String, Path, description = "`up_…`")),
    responses((status = 200, body = Upload), (status = 404, body = crate::error::Problem)))]
pub(crate) async fn get(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Path(up): Path<String>,
) -> Result<Json<Upload>, Error> {
    let org = org_of(&ctx)?;
    let row = load_visible(&s, &store, &ctx, &org, &up).await?;
    let parts = if row.state == "OPEN" {
        let st = store.org_store(&org).await?;
        received(store.db(), &st, &row)
            .await?
            .into_iter()
            .filter(|(n, (_, size))| *n < row.part_count() && *size == row.part_len(*n))
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
    let org = org_of(&ctx)?;
    let row = load_visible(&s, &store, &ctx, &org, &up).await?;
    if !row.is_open() {
        return Err(closed(&row));
    }
    let n = n
        .parse::<u64>()
        .ok()
        .filter(|n| *n < row.part_count())
        .ok_or_else(|| field("/n", "This upload has no part with this number."))?;
    let want = row.part_len(n);
    let target = PartTarget { row, n };
    target.check_len(body.len() as u64)?;
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
    let st = store.org_store(&org).await?;
    target.publish(&store, &st, body, &sha).await?;
    Ok(Json(
        serde_json::json!({ "part": n, "size": want, "sha256": sha }),
    ))
}

/// `POST /uploads/{up}:complete`, `:abort` or `:presign` (one route; each is documented as
/// its own operation below).
pub(crate) async fn action(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    ctx: RequestContext,
    Path(segment): Path<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, Error> {
    let (up, act) = split_action(&segment);
    match act {
        Some("complete") => json(complete_upload(&s, &store, &ctx, up).await?),
        Some("abort") => json(abort_upload(&s, &store, &ctx, up).await?),
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
            json(presign_upload(&s, &store, &ctx, up, req).await?)
        }
        _ => Err(ApiError::new(ErrorCode::RouteNotFound, "No endpoint has this path.").into()),
    }
}

fn json(v: impl Serialize) -> Result<Json<serde_json::Value>, Error> {
    Ok(Json(serde_json::to_value(v).unwrap_or_default()))
}

/// Asks for PUT tickets for parts (default: every part not received yet, at most 1000).
/// The tickets point at the server's transfer endpoint on every backend.
#[utoipa::path(post, path = "/uploads/{up}:presign", tag = "uploads", request_body(content = PresignRequest, description = "Optional; an empty body asks for every missing part."),
    params(("up" = String, Path, description = "`up_…`")),
    responses((status = 200, body = PresignResponse), (status = 409, body = crate::error::Problem), (status = 422, body = crate::error::Problem)))]
async fn presign_upload(
    s: &AppState,
    store: &ArtifactStore,
    ctx: &RequestContext,
    up: &str,
    req: PresignRequest,
) -> Result<PresignResponse, Error> {
    let org = org_of(ctx)?;
    let row = load_visible(s, store, ctx, &org, up).await?;
    if !row.is_open() {
        return Err(closed(&row));
    }
    let st = store.org_store(&org).await?;
    let parts = match req.parts {
        Some(p) => p,
        None => {
            let have = received(store.db(), &st, &row).await?;
            (0..row.part_count())
                .filter(|n| have.get(n).map(|(_, size)| *size) != Some(row.part_len(*n)))
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
            .part_ticket(&s.seams, ctx, key, row.part_len(n), row.expires_at)
            .await?;
        out.push(PartTicket { part: n, ticket });
    }
    Ok(PresignResponse { tickets: out })
}

/// Cancels an open upload and deletes its parts.
#[utoipa::path(post, path = "/uploads/{up}:abort", tag = "uploads",
    params(("up" = String, Path, description = "`up_…`")),
    responses((status = 200, body = Upload), (status = 404, body = crate::error::Problem), (status = 409, body = crate::error::Problem)))]
async fn abort_upload(
    s: &AppState,
    store: &ArtifactStore,
    ctx: &RequestContext,
    up: &str,
) -> Result<Upload, Error> {
    let org = &org_of(ctx)?;
    let row = load_visible(s, store, ctx, org, up).await?;
    let mut tx = store.db().pool().begin().await?;
    let row = lock_upload(&mut tx, row.id).await?;
    let stopped = sqlx::query_as::<_, Row>(concat!(
        "UPDATE uploads SET state = 'ABORTED', finished_at = clock_timestamp()
         WHERE id = $1 AND state = 'OPEN' AND expires_at > clock_timestamp() RETURNING ",
        columns!()
    ))
    .bind(row.id)
    .fetch_optional(&mut *tx)
    .await?;
    let stopped = match stopped {
        Some(row) => row,
        None if row.state == "ABORTED" => row,
        None => return Err(closed(&row)),
    };
    clear_part_refs(&mut tx, stopped.id).await?;
    tx.commit().await?;
    match store.org_store(org).await {
        Ok(st) => tidy(&st, &stopped.key()).await,
        Err(e) => tracing::warn!(code = e.code(), "could not clean up an aborted upload"),
    }
    Ok(stopped.into_upload(Vec::new()))
}

/// Finishes an upload: joins the parts while hashing, checks the sha256 of the whole file
/// and stores it as `blobs/sha256/…` (once per content).
#[utoipa::path(post, path = "/uploads/{up}:complete", tag = "uploads",
    params(("up" = String, Path, description = "`up_…`")),
    responses((status = 200, body = Upload), (status = 409, body = crate::error::Problem), (status = 422, body = crate::error::Problem)))]
async fn complete_upload(
    s: &AppState,
    store: &ArtifactStore,
    ctx: &RequestContext,
    up: &str,
) -> Result<Upload, Error> {
    let org = &org_of(ctx)?;
    let row = load_visible(s, store, ctx, org, up).await?;
    if row.state == "COMPLETED" {
        return Ok(row.into_upload(Vec::new()));
    }
    if !row.is_open() {
        return Err(closed(&row));
    }
    let st = store.org_store(org).await?;
    let have = received(store.db(), &st, &row).await?;
    let wrong: Vec<String> = (0..row.part_count())
        .filter_map(|n| match have.get(&n) {
            None => Some(format!("{n} (missing)")),
            Some((_, size)) if *size != row.part_len(n) => Some(format!("{n} ({size} bytes)")),
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
    let dir = store.scratch_dir()?;
    let file = dir.path().join("file");
    let sha = join(&st, &row, &have, &file).await?;
    let blob = st.keys().blob(&sha)?;
    store.put_blob(&st, &file, &blob, row.size(), &sha).await?;
    scratch::close(dir)?;
    let mut tx = store.db().pool().begin().await?;
    let row = lock_upload(&mut tx, row.id).await?;
    let completed = sqlx::query_as::<_, Row>(concat!(
        "UPDATE uploads SET state = 'COMPLETED', blob_key = $2, finished_at = clock_timestamp()
         WHERE id = $1 AND state = 'OPEN' AND expires_at > clock_timestamp() RETURNING ",
        columns!()
    ))
    .bind(row.id)
    .bind(&blob)
    .fetch_optional(&mut *tx)
    .await?;
    let completed = match completed {
        Some(row) => row,
        None if row.state == "COMPLETED" => row,
        None => return Err(closed(&row)),
    };
    clear_part_refs(&mut tx, completed.id).await?;
    tx.commit().await?;
    tidy(&st, &completed.key()).await;
    Ok(completed.into_upload(Vec::new()))
}

/// Joins the parts into `tmp`, checking each part's length as read, the total and the
/// sha256; returns the sha256.
async fn join(
    st: &Store,
    row: &Row,
    parts: &BTreeMap<u64, (String, u64)>,
    tmp: &std::path::Path,
) -> Result<String, Error> {
    let io = |e: std::io::Error| {
        Error::from(StorageError::Io {
            path: tmp.to_owned(),
            source: e,
        })
    };
    let mut file = tokio::fs::File::create(tmp).await.map_err(io)?;
    let mut hash = Sha256::new();
    let mut total = 0u64;
    // Each part is checked as read, not as listed: a ticket PUT may have replaced it since.
    for n in 0..row.part_count() {
        let (key, _) = parts.get(&n).ok_or_else(|| {
            Error::coded(
                codes::PARTS_INCOMPLETE,
                StatusCode::CONFLICT,
                format!("Part {n} is missing. Send it again."),
            )
        })?;
        let bytes = st.get(key).await?;
        let (got, want) = (bytes.len() as u64, row.part_len(n));
        if got != want {
            return Err(Error::coded(
                codes::PARTS_INCOMPLETE,
                StatusCode::CONFLICT,
                format!("Part {n} changed while finishing: it has {got} bytes, not {want}. Send it again."),
            ));
        }
        total += got;
        hash.update(&bytes);
        file.write_all(&bytes).await.map_err(io)?;
    }
    file.flush().await.map_err(io)?;
    drop(file);
    if total != row.size() {
        return Err(Error::coded(
            codes::PARTS_INCOMPLETE,
            StatusCode::CONFLICT,
            format!(
                "The parts add up to {total} bytes, not {}. Send them again.",
                row.size()
            ),
        ));
    }
    let actual: String = hash.finalize().iter().map(|b| format!("{b:02x}")).collect();
    if actual != row.sha256 {
        return Err(StorageError::ChecksumMismatch {
            key: row.filename.clone(),
            expected: row.sha256.clone(),
            actual,
        }
        .into());
    }
    Ok(actual)
}

/// True when `blob` holds `size` bytes hashing to `sha256`.
pub(crate) async fn blob_intact(
    st: &Store,
    blob: &str,
    size: u64,
    sha256: &str,
) -> Result<bool, Error> {
    match st.head(blob).await {
        Ok(info) if info.size == size => match st.sha256(blob).await {
            Ok(have) => Ok(have == sha256),
            Err(StorageError::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        },
        Ok(_) | Err(StorageError::NotFound { .. }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Stores the local file `tmp` of `size` bytes at `key`: one put up to 16 MiB, else a
/// parallel multipart transfer, whose ledger sits next to `tmp` (keep both in a scratch
/// folder).
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
    /// Blobs whose commit never happened, deleted.
    pub blobs: u64,
    /// Scratch folders older than 24 hours (left by a killed process), deleted.
    pub scratch: u64,
}

/// One cleanup pass at the current time; see [`sweep_at`].
pub async fn sweep(store: &ArtifactStore) -> Result<Swept, Error> {
    sweep_at(store, OffsetDateTime::now_utc()).await
}

/// One cleanup pass as if it were `now` (tests move it forward): expires open uploads past
/// their deadline; fails artifacts left PENDING for 24 hours; deletes objects under the
/// `uploads/` area of every org P19 knows (uploads, artifacts, write intents) that are older
/// than 24 hours and no open upload owns, which covers multipart staging too; deletes blobs
/// whose write intent is older than 24 hours when nothing references them (a commit that
/// never happened); and deletes scratch folders older than 24 hours. Native S3 multipart
/// sessions are not visible here (P08 / bucket lifecycle).
pub async fn sweep_at(store: &ArtifactStore, now: OffsetDateTime) -> Result<Swept, Error> {
    let pool = store.db().pool();
    let cutoff = now - time::Duration::hours(24);
    let expired: i64 = sqlx::query_scalar(
        "WITH expired AS (
             UPDATE uploads SET state = 'EXPIRED', finished_at = now()
             WHERE state = 'OPEN' AND expires_at < $1 RETURNING id
         ), removed AS (
             DELETE FROM upload_parts USING expired WHERE upload_parts.upload_id = expired.id
         )
         SELECT count(*) FROM expired",
    )
    .bind(now)
    .fetch_one(pool)
    .await?;
    let stale_artifacts = sqlx::query(
        "UPDATE artifacts SET state = 'FAILED' WHERE state = 'PENDING' AND created_at < $1",
    )
    .bind(cutoff)
    .execute(pool)
    .await?
    .rows_affected();
    // ponytail: one listing per known org; page through orgs when there are many.
    let orgs: Vec<String> = sqlx::query_scalar(
        "SELECT org_id FROM uploads UNION SELECT org_id FROM artifacts
         UNION SELECT org_id FROM storage_intents",
    )
    .fetch_all(pool)
    .await?;
    let open: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM uploads WHERE state = 'OPEN'")
        .fetch_all(pool)
        .await?;
    let open: std::collections::HashSet<String> = open
        .into_iter()
        .map(|id| UploadId::from_uuid(id).to_string())
        .collect();
    let backend = store.backend().await?;
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
    // Blobs written for a commit that never happened. Still referenced (a finished upload,
    // an artifact not FAILED) or written again since: kept.
    let intents: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT DISTINCT org_id, key, sha256 FROM storage_intents
         WHERE sha256 IS NOT NULL AND created_at < $1",
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await?;
    let mut blobs = 0;
    for (org, key, sha) in intents {
        // Checked and deleted under the key's lock: a writer's fresh intent is either seen
        // here, or recorded after the delete (and its own check then finds no object).
        let mut tx = pool.begin().await?;
        crate::modules::artifacts::lock_key(&mut tx, &org, &key).await?;
        let used: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM uploads WHERE org_id = $1 AND blob_key = $2 AND state = 'COMPLETED')
                 OR EXISTS (SELECT 1 FROM artifacts a, jsonb_array_elements(a.manifest -> 'files') f
                            WHERE a.org_id = $1 AND a.state <> 'FAILED' AND f ->> 'sha256' = $3)
                 OR EXISTS (SELECT 1 FROM model_files WHERE object_key = $2)
                 OR EXISTS (SELECT 1 FROM storage_intents
                            WHERE org_id = $1 AND key = $2 AND created_at >= $4)",
        )
        .bind(&org)
        .bind(&key)
        .bind(&sha)
        .bind(cutoff)
        .fetch_one(&mut *tx)
        .await?;
        if !used {
            backend.org(&org)?.delete(&key).await?;
            blobs += 1;
        }
        tx.commit().await?;
    }
    // Kept a second day so the org stays visible until its staging is old enough to go.
    sqlx::query("DELETE FROM storage_intents WHERE created_at < $1")
        .bind(cutoff - time::Duration::hours(24))
        .execute(pool)
        .await?;
    let root = store.scratch_root().to_owned();
    let scratch_cutoff = std::time::SystemTime::from(now) - scratch::MAX_AGE;
    let scratch = tokio::task::spawn_blocking(move || scratch::sweep(&root, scratch_cutoff))
        .await
        .unwrap_or(0);
    Ok(Swept {
        expired: u64::try_from(expired).unwrap_or_default(),
        objects,
        stale_artifacts,
        blobs,
        scratch,
    })
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(create, get, put_part, complete_upload, abort_upload, presign_upload),
    components(schemas(NewUpload, Upload, PresignRequest, PresignResponse, PartTicket)),
    tags((name = "uploads", description = "Resumable uploads."))
)]
pub(crate) struct Doc;

#[cfg(test)]
mod tests {
    use utoipa::OpenApi;

    /// A part replaced by a shorter body after it was listed: the bytes read are checked,
    /// even when they hash to the declared sha256.
    #[tokio::test]
    async fn parts_are_checked_as_read() {
        let dir = tempfile::tempdir().unwrap();
        let st = mrl_storage::Backend::connect(&mrl_storage::BackendConfig::Local {
            root: dir.path().join("objects"),
        })
        .unwrap()
        .org("org-a")
        .unwrap();
        let short = b"short".to_vec();
        let row = super::Row {
            id: uuid::Uuid::now_v7(),
            project_id: None,
            filename: "f".into(),
            // Declared: 10 bytes whose hash is the 5-byte body's (what an attacker chose).
            size: 10,
            sha256: mrl_storage::sha256_hex(&short),
            part_size: 1 << 20,
            state: "OPEN".into(),
            blob_key: None,
            created_at: time::OffsetDateTime::now_utc(),
            expires_at: time::OffsetDateTime::now_utc(),
        };
        let key = super::part_key(&st, &row, 0).unwrap();
        st.put(&key, short.into()).await.unwrap();
        let parts = std::collections::BTreeMap::from([(0, (key, 10))]);
        let err = super::join(&st, &row, &parts, &dir.path().join("file"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), super::codes::PARTS_INCOMPLETE);
        assert!(
            st.list(&format!("{}blobs/", st.keys().prefix()))
                .await
                .unwrap_or_default()
                .is_empty(),
            "nothing was stored"
        );
    }

    #[test]
    fn each_upload_action_is_its_own_operation() {
        let doc = super::Doc::openapi();
        for (path, id) in [
            ("/uploads/{up}:complete", "complete_upload"),
            ("/uploads/{up}:abort", "abort_upload"),
            ("/uploads/{up}:presign", "presign_upload"),
        ] {
            let post = doc
                .paths
                .paths
                .get(path)
                .and_then(|p| p.post.as_ref())
                .unwrap_or_else(|| panic!("POST {path} is documented"));
            assert_eq!(post.operation_id.as_deref(), Some(id), "{path}");
        }
        let presign = serde_json::to_string(&doc.paths.paths["/uploads/{up}:presign"]).unwrap();
        assert!(presign.contains("PresignRequest") && presign.contains("PresignResponse"));
        let tickets =
            serde_json::to_string(&doc.components.unwrap().schemas["PartTicket"]).unwrap();
        assert!(tickets.contains("/api/v1/transfer/{ticket}"), "{tickets}");
    }
}

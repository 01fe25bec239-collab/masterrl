//! `PUT|GET /api/v1/transfer/{ticket}`: the server-proxied transfer endpoint. The ticket is
//! the only credential (no sign-in); it allows one method on one key up to its size limit.
//!
//! - PUT: the body streams into a temporary file, counted against the ticket's size limit
//!   (the API's 2 MiB limit does not apply here) and, when the client sends
//!   `x-mrl-content-sha256`, checked against that hash; then it is stored at the ticket's key
//!   (one put up to 16 MiB, multipart above).
//! - GET: the object must fit the ticket's size limit. `Range: bytes=N-` and `bytes=N-M`
//!   resume a download (206); the body streams in 8 MiB pieces.

use super::{ArtifactStore, Error};
use crate::app::AppState;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use mrl_storage::{StorageError, TicketMethod};
use std::sync::Arc;

/// Header with the sha256 of a PUT body (lowercase hex), as the agent sends it.
pub const SHA256_HEADER: &str = "x-mrl-content-sha256";
const CHUNK: u64 = 8 << 20;

fn too_large(detail: String) -> Error {
    Error::coded("MRL-STORE-009", StatusCode::PAYLOAD_TOO_LARGE, detail)
}

/// Uploads one object with a PUT ticket.
#[utoipa::path(put, path = "/transfer/{ticket}", tag = "artifacts",
    params(("ticket" = String, Path, description = "The ticket token.")),
    request_body(content = Vec<u8>, content_type = "application/octet-stream"),
    responses((status = 200, body = Object, description = "`{key, size, sha256}`"),
        (status = 403, body = crate::error::Problem), (status = 413, body = crate::error::Problem)))]
pub(super) async fn upload(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    Path(token): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<serde_json::Value>, Error> {
    let claims = store.verify(&s, &token, TicketMethod::Put).await?;
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if let Some(n) = declared {
        claims.check_size(n).map_err(|e| too_large(e.to_string()))?;
    }
    // Streamed into a temporary file while counting and hashing, so the ticket's size limit
    // holds even without a content-length and nothing large sits in memory.
    let tmp = std::env::temp_dir().join(format!("mrl-transfer-{}", uuid::Uuid::new_v4().simple()));
    let result = receive(&s, &store, &claims, &headers, body, &tmp).await;
    for p in [tmp.clone(), crate::modules::uploads::ledger_of(&tmp)] {
        let _ = tokio::fs::remove_file(p).await;
    }
    let (size, sha) = result?;
    Ok(Json(
        serde_json::json!({ "key": claims.key, "size": size, "sha256": sha }),
    ))
}

async fn receive(
    s: &AppState,
    store: &ArtifactStore,
    claims: &mrl_storage::TicketClaims,
    headers: &HeaderMap,
    body: Body,
    tmp: &std::path::Path,
) -> Result<(u64, String), Error> {
    use futures_util::StreamExt;
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncWriteExt;
    let io = |e: std::io::Error| {
        Error::from(StorageError::Io {
            path: tmp.to_owned(),
            source: e,
        })
    };
    let mut file = tokio::fs::File::create(tmp).await.map_err(io)?;
    let (mut size, mut hash) = (0u64, Sha256::new());
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            crate::error::ApiError::new(
                crate::error::ErrorCode::BadBody,
                format!("The upload stopped before it finished: {e}"),
            )
        })?;
        size += chunk.len() as u64;
        claims
            .check_size(size)
            .map_err(|e| too_large(e.to_string()))?;
        hash.update(&chunk);
        file.write_all(&chunk).await.map_err(io)?;
    }
    file.flush().await.map_err(io)?;
    drop(file);
    let sha: String = hash.finalize().iter().map(|b| format!("{b:02x}")).collect();
    if let Some(want) = headers.get(SHA256_HEADER) {
        let want = want.to_str().unwrap_or_default();
        if want != sha {
            return Err(StorageError::ChecksumMismatch {
                key: claims.key.clone(),
                expected: want.chars().take(64).collect(),
                actual: sha,
            }
            .into());
        }
    }
    let st = store.org_store(s, claims.org_id()).await?;
    crate::modules::uploads::store_file(&st, tmp, &claims.key, size).await?;
    Ok((size, sha))
}

/// Downloads one object with a GET ticket. Supports `Range`.
#[utoipa::path(get, path = "/transfer/{ticket}", tag = "artifacts",
    params(("ticket" = String, Path, description = "The ticket token.")),
    responses((status = 200, description = "The object bytes."), (status = 206, description = "Part of the object."),
        (status = 403, body = crate::error::Problem), (status = 416, description = "The range starts past the end.")))]
pub(super) async fn download(
    State(s): State<AppState>,
    Extension(store): Extension<Arc<ArtifactStore>>,
    Path(token): Path<String>,
    headers: HeaderMap,
) -> Result<Response, Error> {
    let claims = store.verify(&s, &token, TicketMethod::Get).await?;
    let org = store.org_store(&s, claims.org_id()).await?;
    let info = org.head(&claims.key).await?;
    claims.check_size(info.size)?;
    let size = info.size;
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(|v| parse_range(v, size));
    let (start, end, status) = match range {
        None => (0, size, StatusCode::OK),
        Some(Some((a, b))) => (a, b, StatusCode::PARTIAL_CONTENT),
        Some(None) => {
            let mut res = StatusCode::RANGE_NOT_SATISFIABLE.into_response();
            if let Ok(v) = HeaderValue::from_str(&format!("bytes */{size}")) {
                res.headers_mut().insert(header::CONTENT_RANGE, v);
            }
            return Ok(res);
        }
    };
    let key = claims.key.clone();
    let stream = futures_util::stream::try_unfold(start, move |at| {
        let (org, key) = (org.clone(), key.clone());
        async move {
            if at >= end {
                return Ok::<_, StorageError>(None);
            }
            let next = (at + CHUNK).min(end);
            Ok(Some((org.get_range(&key, at..next).await?, next)))
        }
    });
    let mut res = Response::new(Body::from_stream(stream));
    *res.status_mut() = status;
    let h = res.headers_mut();
    h.insert(header::CONTENT_LENGTH, HeaderValue::from(end - start));
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if status == StatusCode::PARTIAL_CONTENT {
        if let Ok(v) = HeaderValue::from_str(&format!("bytes {start}-{}/{size}", end - 1)) {
            h.insert(header::CONTENT_RANGE, v);
        }
    }
    if let Some(etag) = info.etag.and_then(|e| HeaderValue::from_str(&e).ok()) {
        h.insert(header::ETAG, etag);
    }
    Ok(res)
}

/// `bytes=N-` or `bytes=N-M` → `[start, end)`. `None` when it starts at or past the end or
/// is not a single range this endpoint reads.
fn parse_range(v: &str, size: u64) -> Option<(u64, u64)> {
    let (a, b) = v.trim().strip_prefix("bytes=")?.split_once('-')?;
    let start: u64 = a.trim().parse().ok()?;
    let end = match b.trim() {
        "" => size,
        b => b.parse::<u64>().ok()?.checked_add(1)?.min(size),
    };
    (start < end).then_some((start, end))
}

#[cfg(test)]
mod tests {
    use super::parse_range;

    #[test]
    fn ranges() {
        assert_eq!(parse_range("bytes=0-", 10), Some((0, 10)));
        assert_eq!(parse_range("bytes=4-", 10), Some((4, 10)));
        assert_eq!(parse_range("bytes=2-5", 10), Some((2, 6)));
        assert_eq!(parse_range("bytes=2-99", 10), Some((2, 10)));
        assert_eq!(parse_range("bytes=10-", 10), None);
        assert_eq!(parse_range("bytes=5-2", 10), None);
        assert_eq!(parse_range("bytes=-5", 10), None);
        assert_eq!(parse_range("items=0-", 10), None);
    }
}

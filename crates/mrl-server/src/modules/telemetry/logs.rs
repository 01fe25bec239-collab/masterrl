//! Logs: the hot tail in `log_chunks`, sealed segments in object storage (`log_index`), reading
//! and searching both, and the full download.
//!
//! Sealing writes up to 512 chunks of one attempt, rank and stream as JSON lines to
//! `orgs/<org>/runs/<run>/logs/<attempt>/<rank>/<stream>-<id_lo>-<id_hi>-<uuid>.jsonl`, then, in one
//! transaction, records the segment and deletes exactly those chunks (all of them or none:
//! otherwise the segment is discarded). A reader takes segments and hot chunks from one
//! snapshot, so every chunk is seen exactly once, in id order.

use super::ingest::sha256_hex;
use super::{field_error, parse_num, read_run, storage_org, stored_file_error, Telemetry};
use crate::app::AppState;
use crate::http::pagination::{decode_cursor, encode_cursor};
use crate::modules::jobs::JobsError;
use crate::seams::RequestContext;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use mrl_domain::ids::{AttemptId, RunId};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::collections::BTreeMap;
use std::sync::Arc;
use time::OffsetDateTime;
use uuid::Uuid;

/// Chunks per sealed segment.
const SEGMENT_CHUNKS: i64 = 512;
/// Segments and hot chunks one page looks at.
const PAGE_SEGMENTS: usize = 8;
const PAGE_HOT: i64 = 2000;
/// Default and largest page size.
const DEFAULT_LIMIT: usize = 200;
const MAX_LIMIT: usize = 1000;
/// Longest search text.
const MAX_QUERY: usize = 200;

/// One log chunk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct LogEntryView {
    /// Position in the run's log (increasing).
    pub id: i64,
    /// `att_…`.
    pub attempt_id: String,
    /// Process rank.
    pub rank: i32,
    /// `stdout`, `stderr`, `harness` or `agent` (the server's own notes).
    pub stream: String,
    /// Byte offset in the stream.
    pub offset: i64,
    /// When it arrived.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub received_at: OffsetDateTime,
    /// The text (invalid UTF-8 shown as �).
    pub text: String,
}

/// A page of log chunks.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct LogPage {
    /// Chunks in log order.
    pub items: Vec<LogEntryView>,
    /// Pass as `cursor` to continue; null when nothing more is stored yet.
    pub next_cursor: Option<String>,
}

/// Query parameters of the log routes.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct LogParams {
    /// Only this attempt, `att_…`.
    pub attempt: Option<String>,
    /// Only this rank.
    pub rank: Option<String>,
    /// Only this stream: `stdout`, `stderr`, `harness` or `agent`.
    pub stream: Option<String>,
    /// Only chunks containing this text (ignores case; at most 200 characters).
    pub q: Option<String>,
    /// Page size, 1–1000 (default 200).
    pub limit: Option<String>,
    /// `next_cursor` from the previous page.
    pub cursor: Option<String>,
}

/// A stored chunk, hot or sealed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Chunk {
    id: i64,
    #[serde(with = "uuid_text")]
    attempt: Uuid,
    rank: i32,
    stream: String,
    offset: i64,
    #[serde(with = "time::serde::rfc3339")]
    received_at: OffsetDateTime,
    #[serde(with = "b64")]
    data: Vec<u8>,
}

mod b64 {
    use super::{Engine, STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(d)?;
        STANDARD.decode(text).map_err(serde::de::Error::custom)
    }
}

mod uuid_text {
    use serde::{Deserialize, Deserializer, Serializer};
    use uuid::Uuid;

    pub fn serialize<S: Serializer>(v: &Uuid, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(v)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Uuid, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl Chunk {
    fn view(&self) -> LogEntryView {
        LogEntryView {
            id: self.id,
            attempt_id: AttemptId::from_uuid(self.attempt).to_string(),
            rank: self.rank,
            stream: self.stream.clone(),
            offset: self.offset,
            received_at: self.received_at,
            text: String::from_utf8_lossy(&self.data).into_owned(),
        }
    }
}

fn chunk_of(r: &sqlx::postgres::PgRow) -> Result<Chunk, sqlx::Error> {
    Ok(Chunk {
        id: r.try_get("id")?,
        attempt: r.try_get("attempt_id")?,
        rank: r.try_get("rank")?,
        stream: r.try_get("stream")?,
        offset: r.try_get("offset")?,
        received_at: r.try_get("received_at")?,
        data: r.try_get("data")?,
    })
}

/// Seals chunks older than `after` into segments; returns how many segments it wrote.
pub async fn seal(t: &Telemetry, after: std::time::Duration) -> Result<usize, JobsError> {
    let cutoff =
        OffsetDateTime::now_utc() - time::Duration::try_from(after).unwrap_or(time::Duration::MAX);
    let groups = sqlx::query(
        "SELECT org_id, run_id, attempt_id, rank, stream FROM log_chunks
          WHERE received_at < $1 GROUP BY 1, 2, 3, 4, 5 LIMIT 100",
    )
    .bind(cutoff)
    .fetch_all(t.db.pool())
    .await?;
    let mut sealed = 0;
    for g in &groups {
        let (org, run, attempt): (Uuid, Uuid, Uuid) = (
            g.try_get("org_id")?,
            g.try_get("run_id")?,
            g.try_get("attempt_id")?,
        );
        let (rank, stream): (i32, String) = (g.try_get("rank")?, g.try_get("stream")?);
        loop {
            let chunks = sqlx::query(
                "SELECT id, attempt_id, rank, stream, \"offset\", received_at, data FROM log_chunks
                  WHERE attempt_id = $1 AND rank = $2 AND stream = $3 AND received_at < $4
                  ORDER BY id LIMIT $5",
            )
            .bind(attempt)
            .bind(rank)
            .bind(&stream)
            .bind(cutoff)
            .bind(SEGMENT_CHUNKS)
            .fetch_all(t.db.pool())
            .await?
            .iter()
            .map(chunk_of)
            .collect::<Result<Vec<_>, _>>()?;
            if chunks.is_empty() {
                break;
            }
            if seal_segment(t, org, run, &chunks).await? {
                sealed += 1;
            }
            if chunks.len() < usize::try_from(SEGMENT_CHUNKS).unwrap_or(0) {
                break;
            }
        }
    }
    Ok(sealed)
}

async fn seal_segment(
    t: &Telemetry,
    org: Uuid,
    run: Uuid,
    chunks: &[Chunk],
) -> Result<bool, JobsError> {
    let (Some(first), Some(last)) = (chunks.first(), chunks.last()) else {
        return Ok(false);
    };
    let mut body = Vec::new();
    for c in chunks {
        let line = serde_json::to_vec(c).map_err(|e| stored_file_error("log", &e))?;
        body.extend_from_slice(&line);
        body.push(b'\n');
    }
    let sha = sha256_hex(&body);
    let store = t
        .store
        .org_store(&storage_org(org))
        .await
        .map_err(|e| stored_file_error("log", &e))?;
    let key = format!(
        "{}runs/{}/logs/{}/{}/{}-{}-{}-{}.jsonl",
        store.keys().prefix(),
        RunId::from_uuid(run),
        AttemptId::from_uuid(first.attempt),
        first.rank,
        first.stream,
        first.id,
        last.id,
        sha
    );
    super::remember_object(t.db.pool(), org, &key).await?;
    let mut tx = t.db.begin().await?;
    super::remember_object(&mut *tx, org, &key).await?;
    let ids: Vec<i64> = chunks.iter().map(|c| c.id).collect();
    let offsets = chunks.iter().map(|c| c.offset);
    let deleted = sqlx::query("DELETE FROM log_chunks WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if deleted != u64::try_from(ids.len()).unwrap_or(0) {
        return Ok(false);
    }
    store
        .put(&key, body.into())
        .await
        .map_err(|e| stored_file_error("log", &e))?;
    sqlx::query(
        "INSERT INTO log_index (org_id, run_id, attempt_id, rank, stream, offset_lo, offset_hi,
                id_lo, id_hi, chunk_count, object_key, sha256)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
    )
    .bind(org)
    .bind(run)
    .bind(first.attempt)
    .bind(first.rank)
    .bind(&first.stream)
    .bind(offsets.clone().min().unwrap_or(0))
    .bind(offsets.max().unwrap_or(0))
    .bind(first.id)
    .bind(last.id)
    .bind(i32::try_from(chunks.len()).unwrap_or(i32::MAX))
    .bind(&key)
    .bind(&sha)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// Which chunks a reader wants.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LogFilter {
    /// One attempt.
    pub attempt: Option<Uuid>,
    /// One rank.
    pub rank: Option<i32>,
    /// One stream.
    pub stream: Option<String>,
    /// Lower-case search text.
    pub q: Option<String>,
}

impl LogFilter {
    fn from_params(p: &LogParams) -> Result<Self, JobsError> {
        let attempt = p
            .attempt
            .as_deref()
            .map(|a| {
                a.parse::<AttemptId>().map(|a| a.as_uuid()).map_err(|_| {
                    field_error(
                        "attempt",
                        "attempt must be an att_… id.",
                        "Use an id from GET /runs/{run}/attempts.",
                    )
                })
            })
            .transpose()?;
        let stream = match p.stream.as_deref() {
            None => None,
            Some(s @ ("stdout" | "stderr" | "harness" | "agent")) => Some(s.to_owned()),
            Some(other) => {
                return Err(field_error(
                    "stream",
                    &format!("stream {other:?} is not stdout, stderr, harness or agent."),
                    "Pick one of the four streams, or leave it out for all.",
                ))
            }
        };
        let q = match p.q.as_deref() {
            Some(q) if q.chars().count() > MAX_QUERY => {
                return Err(field_error(
                    "q",
                    "The search text is longer than 200 characters.",
                    "Search for a shorter piece of text.",
                ))
            }
            Some(q) if !q.is_empty() => Some(q.to_lowercase()),
            _ => None,
        };
        Ok(Self {
            attempt,
            rank: parse_num(p.rank.as_deref(), "rank")?,
            stream,
            q,
        })
    }

    fn matches(&self, c: &Chunk) -> bool {
        self.attempt.is_none_or(|a| a == c.attempt)
            && self.rank.is_none_or(|r| r == c.rank)
            && self.stream.as_deref().is_none_or(|s| s == c.stream)
            && self
                .q
                .as_deref()
                .is_none_or(|q| String::from_utf8_lossy(&c.data).to_lowercase().contains(q))
    }
}

/// The chunks of `run` after id `after` that pass `f`: at most `limit`, and the cursor to go on
/// from (`None` when everything stored so far was read).
pub(crate) async fn read_page(
    t: &Telemetry,
    run: Uuid,
    f: &LogFilter,
    after: i64,
    limit: usize,
) -> Result<(Vec<Chunk>, Option<i64>), JobsError> {
    let mut tx = t.db.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let segs = sqlx::query(
        "SELECT org_id, id_lo, object_key, sha256 FROM log_index
          WHERE run_id = $1 AND id_hi > $2
            AND ($3::uuid IS NULL OR attempt_id = $3) AND ($4::int IS NULL OR rank = $4)
            AND ($5::text IS NULL OR stream = $5)
          ORDER BY id_lo LIMIT $6",
    )
    .bind(run)
    .bind(after)
    .bind(f.attempt)
    .bind(f.rank)
    .bind(f.stream.as_deref())
    .bind(i64::try_from(PAGE_SEGMENTS + 1).unwrap_or(9))
    .fetch_all(&mut *tx)
    .await?;
    // Chunks at or past `bound` wait for a later page: an unread segment may hold lower ids.
    let mut bound = match segs.get(PAGE_SEGMENTS) {
        Some(s) => s.try_get::<i64, _>("id_lo")?,
        None => i64::MAX,
    };
    let hot = sqlx::query(
        "SELECT id, attempt_id, rank, stream, \"offset\", received_at, data FROM log_chunks
          WHERE run_id = $1 AND id > $2 AND id < $3
            AND ($4::uuid IS NULL OR attempt_id = $4) AND ($5::int IS NULL OR rank = $5)
            AND ($6::text IS NULL OR stream = $6)
          ORDER BY id LIMIT $7",
    )
    .bind(run)
    .bind(after)
    .bind(bound)
    .bind(f.attempt)
    .bind(f.rank)
    .bind(f.stream.as_deref())
    .bind(PAGE_HOT)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    let mut all: Vec<Chunk> = hot.iter().map(chunk_of).collect::<Result<_, _>>()?;
    if all.len() >= usize::try_from(PAGE_HOT).unwrap_or(usize::MAX) {
        bound = bound.min(all.last().map_or(i64::MAX, |c| c.id.saturating_add(1)));
    }
    for s in segs.iter().take(PAGE_SEGMENTS) {
        let (org, key, sha): (Uuid, String, String) = (
            s.try_get("org_id")?,
            s.try_get("object_key")?,
            s.try_get("sha256")?,
        );
        all.extend(load_segment(t, org, &key, &sha).await?);
    }
    all.retain(|c| c.id > after && c.id < bound);
    all.sort_by_key(|c| c.id);
    let mut out = Vec::new();
    for c in all.iter() {
        if f.matches(c)
            || match &f.q {
                Some(q) => crosses_boundary(t, run, c, q).await?,
                None => false,
            }
        {
            out.push(c.clone());
            if out.len() == limit {
                let more = all.last().is_some_and(|l| l.id > c.id) || bound != i64::MAX;
                return Ok((out, more.then_some(c.id)));
            }
        }
    }
    // Every id below `bound` was looked at.
    Ok((out, (bound != i64::MAX).then(|| bound - 1)))
}

// Start matches in this chunk, then look ahead by byte offset across the hot/sealed
// boundary and across pages. Never join across a gap or a different stream.
async fn crosses_boundary(t: &Telemetry, run: Uuid, c: &Chunk, q: &str) -> Result<bool, JobsError> {
    if c.stream == "agent" || c.data.is_empty() {
        return Ok(false);
    }
    let budget = q.len() * 4 + 4;
    let tail = &c.data[c.data.len().saturating_sub(budget)..];
    let prefix = String::from_utf8_lossy(tail).to_lowercase();
    let incomplete = tail
        .iter()
        .rposition(|b| b & 0xc0 != 0x80)
        .filter(|&i| std::str::from_utf8(&tail[i..]).is_err_and(|e| e.error_len().is_none()));
    if incomplete.is_none()
        && !prefix
            .char_indices()
            .any(|(i, _)| q.starts_with(&prefix[i..]))
    {
        return Ok(false);
    }
    let start_limit = incomplete.map_or(prefix.len(), |e| {
        String::from_utf8_lossy(&tail[..e]).to_lowercase().len() + 1
    });
    let Some(start) = c
        .offset
        .checked_add(i64::try_from(c.data.len()).unwrap_or(i64::MAX))
    else {
        return Ok(false);
    };
    let end = start.saturating_add(i64::try_from(budget).unwrap_or(0));
    let mut tx = t.db.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let rows = sqlx::query(
        r#"SELECT "offset", substring(data FOR $7) AS data FROM log_chunks
          WHERE run_id = $1 AND attempt_id = $2 AND rank = $3 AND stream = $4
            AND "offset" >= $5 AND "offset" < $6"#,
    )
    .bind(run)
    .bind(c.attempt)
    .bind(c.rank)
    .bind(&c.stream)
    .bind(start)
    .bind(end)
    .bind(i32::try_from(budget).unwrap_or(i32::MAX))
    .fetch_all(&mut *tx)
    .await?;
    let mut following = BTreeMap::<i64, Vec<u8>>::new();
    for row in rows {
        following.insert(row.try_get("offset")?, row.try_get("data")?);
    }
    let segments = sqlx::query(
        "SELECT org_id, object_key, sha256 FROM log_index
          WHERE run_id = $1 AND attempt_id = $2 AND rank = $3 AND stream = $4
            AND offset_hi >= $5 AND offset_lo < $6",
    )
    .bind(run)
    .bind(c.attempt)
    .bind(c.rank)
    .bind(&c.stream)
    .bind(start)
    .bind(end)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    for row in segments {
        let (org, key, sha): (_, String, String) = (
            row.try_get("org_id")?,
            row.try_get("object_key")?,
            row.try_get("sha256")?,
        );
        for mut chunk in load_segment(t, org, &key, &sha).await? {
            if (start..end).contains(&chunk.offset) {
                chunk.data.truncate(budget);
                following.entry(chunk.offset).or_insert(chunk.data);
            }
        }
    }
    let mut offset = start;
    let mut combined = tail.to_vec();
    while offset < end {
        let Some(bytes) = following.remove(&offset).filter(|bytes| !bytes.is_empty()) else {
            break;
        };
        let room = usize::try_from(end - offset).unwrap_or(0);
        combined.extend(bytes.iter().take(room));
        offset += i64::try_from(bytes.len().min(room)).unwrap_or(0);
    }
    Ok(String::from_utf8_lossy(&combined)
        .to_lowercase()
        .match_indices(q)
        .any(|(i, _)| i < start_limit))
}

async fn load_segment(
    t: &Telemetry,
    org: Uuid,
    key: &str,
    sha: &str,
) -> Result<Vec<Chunk>, JobsError> {
    let store = t
        .store
        .org_store(&storage_org(org))
        .await
        .map_err(|e| stored_file_error("log", &e))?;
    let bytes = store
        .get(key)
        .await
        .map_err(|e| stored_file_error("log", &e))?;
    if sha256_hex(&bytes) != sha {
        return Err(stored_file_error("log", &"checksum mismatch"));
    }
    bytes
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_slice::<Chunk>(l).map_err(|e| stored_file_error("log", &e)))
        .collect()
}

fn page_limit(v: Option<&str>) -> Result<usize, JobsError> {
    let n = parse_num::<usize>(v, "limit")?.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&n) {
        return Err(field_error(
            "limit",
            &format!("limit {n} is outside 1–{MAX_LIMIT}."),
            "Ask for between 1 and 1000 chunks.",
        ));
    }
    Ok(n)
}

fn after_cursor(c: Option<&str>) -> Result<i64, JobsError> {
    match c {
        None => Ok(0),
        Some(c) => decode_cursor(c)?.parse().map_err(|_| {
            JobsError::Api(crate::error::ApiError::new(
                crate::error::ErrorCode::Pagination,
                "cursor must be the next_cursor value from an earlier page, unchanged.",
            ))
        }),
    }
}

/// `GET /runs/{run}/logs`: the run's log in order, hot and sealed, with filters and search.
#[utoipa::path(get, path = "/runs/{run}/logs", tag = "telemetry", operation_id = "list_run_logs",
    params(("run" = String, Path, description = "`run_…`"), LogParams),
    responses((status = 200, body = LogPage), (status = 404, description = "No such run"),
        (status = 503, description = "A sealed log file could not be read (MRL-STORE-220)")))]
pub async fn list(
    State(state): State<AppState>,
    Extension(t): Extension<Arc<Telemetry>>,
    ctx: RequestContext,
    Path(run): Path<String>,
    Query(p): Query<LogParams>,
) -> Result<Json<LogPage>, JobsError> {
    let row = read_run(&state, &t.db, &ctx, &run, "metrics_logs.read").await?;
    let filter = LogFilter::from_params(&p)?;
    let limit = page_limit(p.limit.as_deref())?;
    let after = after_cursor(p.cursor.as_deref())?;
    let (items, next) = read_page(&t, row.id, &filter, after, limit).await?;
    Ok(Json(LogPage {
        items: items.iter().map(Chunk::view).collect(),
        next_cursor: next.map(|n| encode_cursor(&n.to_string())),
    }))
}

/// `GET /runs/{run}/logs:download`: the whole log (filters as for the list) as one text file.
#[utoipa::path(get, path = "/runs/{run}/logs:download", tag = "telemetry", operation_id = "download_run_logs",
    params(("run" = String, Path, description = "`run_…`"), LogParams),
    responses((status = 200, description = "The log text", content_type = "text/plain"),
        (status = 404, description = "No such run")))]
pub async fn download(
    State(state): State<AppState>,
    Extension(t): Extension<Arc<Telemetry>>,
    ctx: RequestContext,
    Path(run): Path<String>,
    Query(p): Query<LogParams>,
) -> Result<Response, JobsError> {
    let row = read_run(&state, &t.db, &ctx, &run, "metrics_logs.download").await?;
    let filter = LogFilter::from_params(&p)?;
    let run_id = row.id;
    let body = futures_util::stream::unfold(Some(0i64), move |after| {
        let (t, filter) = (t.clone(), filter.clone());
        async move {
            let after = after?;
            match read_page(&t, run_id, &filter, after, MAX_LIMIT).await {
                Ok((items, next)) => {
                    let bytes: Vec<u8> =
                        items.iter().flat_map(|c| c.data.iter().copied()).collect();
                    Some((Ok::<_, std::io::Error>(bytes), next))
                }
                Err(e) => Some((Err(std::io::Error::other(e.to_string())), None)),
            }
        }
    });
    let mut res = Body::from_stream(body).into_response();
    let h = res.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    if let Ok(v) = HeaderValue::from_str(&format!(
        "attachment; filename=\"{}.log\"",
        RunId::from_uuid(run_id)
    )) {
        h.insert(header::CONTENT_DISPOSITION, v);
    }
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_lines_round_trip_any_bytes() {
        let c = Chunk {
            id: 7,
            attempt: Uuid::now_v7(),
            rank: 1,
            stream: "stderr".into(),
            offset: 64,
            received_at: OffsetDateTime::UNIX_EPOCH,
            data: vec![0, 159, 146, 150, b'\n'],
        };
        let line = serde_json::to_vec(&c).unwrap();
        assert!(!line.contains(&b'\n'));
        assert_eq!(serde_json::from_slice::<Chunk>(&line).unwrap(), c);
        assert_eq!(c.view().text, "\u{0}\u{FFFD}\u{FFFD}\u{FFFD}\n");
    }

    #[test]
    fn filters_check_every_field() {
        let bad = LogParams {
            stream: Some("nope".into()),
            ..LogParams::default()
        };
        assert_eq!(
            LogFilter::from_params(&bad).unwrap_err().code(),
            "MRL-VAL-004"
        );
        let f = LogFilter::from_params(&LogParams {
            q: Some("ERROR".into()),
            rank: Some("0".into()),
            ..LogParams::default()
        })
        .unwrap();
        let mut c = Chunk {
            id: 1,
            attempt: Uuid::nil(),
            rank: 0,
            stream: "stdout".into(),
            offset: 0,
            received_at: OffsetDateTime::UNIX_EPOCH,
            data: b"an Error here".to_vec(),
        };
        assert!(f.matches(&c));
        c.rank = 1;
        assert!(!f.matches(&c));
    }
}

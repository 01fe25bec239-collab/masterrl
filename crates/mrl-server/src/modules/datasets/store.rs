//! Rows, reads of stored job outputs, and the one place a version becomes READY or INVALID.
//!
//! A job's outputs are checked in two steps: [`read_outputs`] fetches and checks the files
//! the sandbox wrote (no transaction open), then [`record_version`] writes the decision in
//! the caller's transaction. Both are idempotent: a version that is already decided stays as
//! it is.

use super::codes;
use crate::db::Tx;
use crate::modules::events::{NewEvent, OutboxWriter};
use crate::modules::jobs::JobsError;
use crate::modules::lineage::{LineageWriter, NodeRef, Relation};
use crate::seams::SeamError;
use axum::http::StatusCode;
use mrl_domain::ids::DatasetVersionId;
use mrl_storage::{ArtifactManifest, StorageError, Store};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use time::OffsetDateTime;
use uuid::Uuid;

/// Largest JSON output (summary, report, preview) the server reads.
pub const MAX_JSON_BYTES: u64 = 8 * 1024 * 1024;
/// The version summary DATA_PREP writes last (see `mrl_runtime.data.adapter`).
pub const SUMMARY: &str = "dataset.json";
/// Sorted record hashes (version diffs).
pub const RECORD_HASHES: &str = "record_hashes.txt";
/// Sorted prompt hashes (leakage checks against this version).
pub const CONTENT_HASHES: &str = "content_hashes.txt";

/// Lowercase hex.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// 64 lowercase hex characters as 32 bytes.
pub fn sha_bytes(s: &str) -> Option<Vec<u8>> {
    if s.len() != 64 || !s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    (0..32)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok())
        .collect()
}

/// The content hash of a version: sha256 over its shards' sha256s (lowercase hex), sorted,
/// each followed by a newline. The same records give the same shards, so the same hash.
pub fn content_hash<'a>(shards: impl IntoIterator<Item = &'a str>) -> [u8; 32] {
    let mut sorted: Vec<&str> = shards.into_iter().collect();
    sorted.sort_unstable();
    let mut h = Sha256::new();
    for s in sorted {
        h.update(s.as_bytes());
        h.update(b"\n");
    }
    h.finalize().into()
}

/// PostgreSQL can't store NUL in text or jsonb: removes it from every string and key.
pub fn scrub_nul(v: Value) -> Value {
    match v {
        Value::String(s) if s.contains('\0') => Value::String(s.replace('\0', "")),
        Value::Array(a) => Value::Array(a.into_iter().map(scrub_nul).collect()),
        Value::Object(o) => Value::Object(
            o.into_iter()
                .map(|(k, v)| (k.replace('\0', ""), scrub_nul(v)))
                .collect::<Map<_, _>>(),
        ),
        other => other,
    }
}

/// A dataset.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DatasetRow {
    /// Id.
    pub id: Uuid,
    /// Owning org (the project's).
    pub org_id: Uuid,
    /// Project.
    pub project_id: Uuid,
    /// Unique name in the project.
    pub name: String,
    /// Description.
    pub description: String,
    /// SPDX id, `custom` or `unknown`.
    pub license_spdx: String,
    /// `public`, `internal`, `confidential` or `restricted-pii`.
    pub sensitivity: String,
    /// `research`, `commercial`, `internal`.
    pub permitted_use: Vec<String>,
    /// Why the data may be used.
    pub consent_basis: Option<String>,
    /// Where it came from.
    pub source_urls: Vec<String>,
    /// Dataset card (Markdown).
    pub card_md: String,
    /// Registered as an evaluation set (leakage is checked against it).
    pub eval_set: bool,
    /// Bumped by every change (ETag).
    pub version: i64,
    /// Who made it.
    pub created_by: String,
    /// Created.
    pub created_at: OffsetDateTime,
    /// Last changed.
    pub updated_at: OffsetDateTime,
}

/// Columns of [`DatasetRow`] (table alias `d`).
pub const DATASET_COLS: &str = "d.id, d.org_id, d.project_id, d.name, d.description, \
    d.license_spdx, d.sensitivity, d.permitted_use, d.consent_basis, d.source_urls, d.card_md, \
    d.eval_set, d.version, d.created_by, d.created_at, d.updated_at";

/// A dataset version, with its dataset's project and sensitivity.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct VersionRow {
    /// Id.
    pub id: Uuid,
    /// Owning org.
    pub org_id: Uuid,
    /// Dataset.
    pub dataset_id: Uuid,
    /// The dataset's project.
    pub project_id: Uuid,
    /// The dataset's sensitivity.
    pub sensitivity: String,
    /// 1, 2, … within the dataset.
    pub seq: i32,
    /// Canonical schema id.
    pub schema_id: String,
    /// `INGESTING`, `VALIDATING`, `READY`, `INVALID`, `ARCHIVED`.
    pub state: String,
    /// `{code, message}` for INVALID.
    pub state_reason: Option<Value>,
    /// `upload`, `hub`, `remote_ref`, `transform`, `generated`.
    pub source_kind: String,
    /// Where it came from (upload ids, repo and SHA, URL, parent).
    pub source_ref: Value,
    /// The version it was made from.
    pub parent_version_id: Option<Uuid>,
    /// The transform that made it.
    pub transform_spec: Option<Value>,
    /// Rows in the shards.
    pub row_count: Option<i64>,
    /// Bytes in the shards.
    pub byte_size: Option<i64>,
    /// See [`content_hash`].
    pub content_sha256: Option<Vec<u8>>,
    /// Rows per split.
    pub splits: Option<Value>,
    /// `dataset.json` as DATA_PREP wrote it.
    pub summary: Option<Value>,
    /// The job working on it.
    pub job_id: Option<Uuid>,
    /// Who made it.
    pub created_by: String,
    /// Created.
    pub created_at: OffsetDateTime,
    /// Became READY.
    pub ready_at: Option<OffsetDateTime>,
}

/// Columns of [`VersionRow`] (aliases `v` and `d`).
pub const VERSION_COLS: &str = "v.id, v.org_id, v.dataset_id, d.project_id, d.sensitivity, \
    v.seq, v.schema_id, v.state, v.state_reason, v.source_kind, v.source_ref, \
    v.parent_version_id, v.transform_spec, v.row_count, v.byte_size, v.content_sha256, \
    v.splits, v.summary, v.job_id, v.created_by, v.created_at, v.ready_at";

/// A stored file of a version.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct FileRow {
    /// Path inside the version.
    pub path: String,
    /// Split (shards only).
    pub split: Option<String>,
    /// `shard`, `original` or `derived`.
    pub role: String,
    /// Object key (a checked blob).
    pub object_key: String,
    /// sha256.
    pub sha256: Vec<u8>,
    /// Size.
    pub size_bytes: i64,
    /// Rows (shards only).
    pub row_count: Option<i64>,
}

/// The version `id` (any state) of a dataset that is not deleted.
pub async fn version(db: &crate::db::Db, id: Uuid) -> Result<Option<VersionRow>, JobsError> {
    Ok(sqlx::query_as::<_, VersionRow>(sqlx::AssertSqlSafe(format!(
        "SELECT {VERSION_COLS} FROM dataset_versions v JOIN datasets d ON d.id = v.dataset_id \
         WHERE v.id = $1 AND d.deleted_at IS NULL"
    )))
    .bind(id)
    .fetch_optional(db.pool())
    .await?)
}

/// The dataset `id`; a deleted one only with `deleted_too`.
pub async fn dataset(
    db: &crate::db::Db,
    id: Uuid,
    deleted_too: bool,
) -> Result<Option<DatasetRow>, JobsError> {
    Ok(sqlx::query_as::<_, DatasetRow>(sqlx::AssertSqlSafe(format!(
        "SELECT {DATASET_COLS} FROM datasets d WHERE d.id = $1 AND ($2 OR d.deleted_at IS NULL)"
    )))
    .bind(id)
    .bind(deleted_too)
    .fetch_optional(db.pool())
    .await?)
}

/// The files of a version.
pub async fn files(db: &crate::db::Db, version: Uuid) -> Result<Vec<FileRow>, JobsError> {
    Ok(sqlx::query_as::<_, FileRow>(
        "SELECT path, split, role, object_key, sha256, size_bytes, row_count \
         FROM dataset_files WHERE dataset_version_id = $1 ORDER BY path",
    )
    .bind(version)
    .fetch_all(db.pool())
    .await?)
}

/// One file of a version.
pub async fn file(
    db: &crate::db::Db,
    version: Uuid,
    path: &str,
) -> Result<Option<FileRow>, JobsError> {
    Ok(sqlx::query_as::<_, FileRow>(
        "SELECT path, split, role, object_key, sha256, size_bytes, row_count \
         FROM dataset_files WHERE dataset_version_id = $1 AND path = $2",
    )
    .bind(version)
    .bind(path)
    .fetch_optional(db.pool())
    .await?)
}

// ---- reading job outputs ----

/// Why stored outputs could not be used.
#[derive(Debug, thiserror::Error)]
pub enum OutputsError {
    /// Storage did not answer; try again later.
    #[error("storage did not answer: {0}")]
    Transient(String),
    /// The outputs are missing a file or are malformed; the result can't be used.
    #[error("{0}")]
    Broken(String),
}

impl From<StorageError> for OutputsError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::NotFound { .. } | StorageError::ChecksumMismatch { .. } => {
                Self::Broken(format!("a stored output is missing or damaged: {e}"))
            }
            other => Self::Transient(other.to_string()),
        }
    }
}

/// A job's output artifact: its id and checked file list.
#[derive(Debug, Clone)]
pub struct Outputs {
    /// The P19 artifact.
    pub artifact_id: Uuid,
    /// Its files.
    pub files: Vec<mrl_storage::ManifestFile>,
}

impl Outputs {
    /// Reads an `artifact-manifest/v1` document.
    pub fn new(artifact_id: Uuid, manifest: &Value) -> Result<Self, OutputsError> {
        let bytes =
            serde_json::to_vec(manifest).map_err(|e| OutputsError::Broken(e.to_string()))?;
        let m = ArtifactManifest::from_json(&bytes)
            .map_err(|e| OutputsError::Broken(format!("the output list is malformed: {e}")))?;
        Ok(Self {
            artifact_id,
            files: m.files,
        })
    }

    /// The listed file at `path`.
    pub fn get(&self, path: &str) -> Option<&mrl_storage::ManifestFile> {
        self.files.iter().find(|f| f.path == path)
    }

    /// The object key of the listed file at `path` (its checked blob).
    pub fn key(&self, store: &Store, path: &str) -> Result<String, OutputsError> {
        let f = self
            .get(path)
            .ok_or_else(|| OutputsError::Broken(format!("the job wrote no {path}")))?;
        store
            .keys()
            .blob(&f.sha256)
            .map_err(|e| OutputsError::Broken(e.to_string()))
    }

    /// The JSON file at `path`, size-capped and hash-checked.
    pub async fn json(&self, store: &Store, path: &str) -> Result<Value, OutputsError> {
        let f = self
            .get(path)
            .ok_or_else(|| OutputsError::Broken(format!("the job wrote no {path}")))?;
        if f.size > MAX_JSON_BYTES {
            return Err(OutputsError::Broken(format!(
                "{path} is larger than {MAX_JSON_BYTES} bytes"
            )));
        }
        let bytes = store.get(&self.key(store, path)?).await?;
        if mrl_storage::sha256_hex(&bytes) != f.sha256 {
            return Err(OutputsError::Broken(format!(
                "{path} does not match its hash"
            )));
        }
        serde_json::from_slice(&bytes)
            .map(scrub_nul)
            .map_err(|e| OutputsError::Broken(format!("{path} is not JSON: {e}")))
    }
}

#[derive(Debug, Deserialize)]
struct ShardEntry {
    path: String,
    split: String,
    rows: u64,
}

#[derive(Debug, Deserialize)]
struct BrowseEntry {
    path: String,
}

#[derive(Debug, Deserialize)]
struct LeakEntry {
    records: u64,
    percent: f64,
}

#[derive(Debug, Deserialize)]
struct Summary {
    schema_id: String,
    rows: u64,
    #[serde(default)]
    shards: Vec<ShardEntry>,
    passed: bool,
    #[serde(default)]
    error_code: Option<String>,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    browse: Vec<BrowseEntry>,
    #[serde(default)]
    leakage: BTreeMap<String, LeakEntry>,
}

/// A file to record for a version.
#[derive(Debug, Clone, PartialEq)]
pub struct NewFile {
    /// Path inside the version.
    pub path: String,
    /// Split (shards).
    pub split: Option<String>,
    /// `shard`, `original` or `derived`.
    pub role: &'static str,
    /// Object key.
    pub key: String,
    /// sha256 (hex).
    pub sha256: String,
    /// Size.
    pub size: u64,
    /// Rows (shards).
    pub rows: Option<u64>,
}

/// What a DATA_PREP (or transform) job decided about a version.
#[derive(Debug, Clone)]
pub enum Checked {
    /// The data passed: its shards and derived files.
    Ready {
        /// `dataset.json`.
        summary: Value,
        /// `validation_report.json`.
        validation: Value,
        /// `stats.json`.
        stats: Value,
        /// Shards and derived files.
        files: Vec<NewFile>,
        /// Rows in the shards.
        rows: u64,
        /// Bytes in the shards.
        bytes: u64,
        /// See [`content_hash`].
        content: [u8; 32],
        /// Eval version → (records, ratio 0–1).
        leakage: Vec<(Uuid, u64, f64)>,
        /// The output artifact.
        artifact_id: Uuid,
    },
    /// The data did not pass (or the outputs were unusable): why.
    Invalid {
        /// The code, like MRL-DATA-001.
        code: String,
        /// Plain reason.
        reason: String,
        /// `dataset.json`, when there is one.
        summary: Option<Value>,
        /// `validation_report.json`, when there is one.
        validation: Option<Value>,
        /// The output artifact, when there is one.
        artifact_id: Option<Uuid>,
    },
}

impl Checked {
    /// INVALID because the outputs can't be used.
    pub fn broken(why: impl Into<String>, artifact_id: Option<Uuid>) -> Self {
        Self::Invalid {
            code: codes::OUTPUTS_BROKEN.to_string(),
            reason: why.into(),
            summary: None,
            validation: None,
            artifact_id,
        }
    }
}

/// Reads and checks a DATA_PREP job's outputs for a version of `schema_id`. `Err` only for
/// passing storage problems; unusable outputs are [`Checked::Invalid`].
pub async fn read_outputs(
    store: &Store,
    outputs: &Outputs,
    schema_id: &str,
) -> Result<Checked, OutputsError> {
    match check(store, outputs, schema_id).await {
        Err(OutputsError::Broken(why)) => Ok(Checked::broken(why, Some(outputs.artifact_id))),
        other => other,
    }
}

async fn check(store: &Store, outputs: &Outputs, schema_id: &str) -> Result<Checked, OutputsError> {
    let raw = outputs.json(store, SUMMARY).await?;
    let summary: Summary = serde_json::from_value(raw.clone())
        .map_err(|e| OutputsError::Broken(format!("{SUMMARY} is malformed: {e}")))?;
    if summary.schema_id != schema_id {
        return Err(OutputsError::Broken(format!(
            "the job checked {} instead of {schema_id}",
            summary.schema_id
        )));
    }
    let validation = match outputs.get("validation_report.json") {
        Some(_) => Some(outputs.json(store, "validation_report.json").await?),
        None => None,
    };
    if !summary.passed {
        return Ok(Checked::Invalid {
            code: summary
                .error_code
                .filter(|c| c.starts_with("MRL-"))
                .unwrap_or_else(|| "MRL-DATA-001".to_owned()),
            reason: summary.reason.chars().take(2000).collect(),
            summary: Some(raw),
            validation,
            artifact_id: Some(outputs.artifact_id),
        });
    }
    let validation =
        validation.ok_or_else(|| OutputsError::Broken("the job wrote no report".into()))?;
    let stats = outputs.json(store, "stats.json").await?;
    let mut files = Vec::new();
    let (mut rows, mut bytes) = (0u64, 0u64);
    let mut shard_hashes = Vec::new();
    for s in &summary.shards {
        let f = outputs
            .get(&s.path)
            .ok_or_else(|| OutputsError::Broken(format!("the shard {} is missing", s.path)))?;
        rows += s.rows;
        bytes = bytes.saturating_add(f.size);
        shard_hashes.push(f.sha256.clone());
        files.push(NewFile {
            path: s.path.clone(),
            split: Some(s.split.clone()),
            role: "shard",
            key: outputs.key(store, &s.path)?,
            sha256: f.sha256.clone(),
            size: f.size,
            rows: Some(s.rows),
        });
    }
    if rows != summary.rows || summary.shards.is_empty() {
        return Err(OutputsError::Broken(format!(
            "{SUMMARY} lists {} rows but its shards hold {rows}",
            summary.rows
        )));
    }
    let derived = [
        SUMMARY,
        "validation_report.json",
        "stats.json",
        RECORD_HASHES,
        CONTENT_HASHES,
    ]
    .into_iter()
    .map(str::to_owned)
    .chain(summary.browse.iter().map(|b| b.path.clone()));
    for path in derived {
        let f = outputs
            .get(&path)
            .ok_or_else(|| OutputsError::Broken(format!("the job wrote no {path}")))?;
        files.push(NewFile {
            key: outputs.key(store, &path)?,
            sha256: f.sha256.clone(),
            size: f.size,
            path,
            split: None,
            role: "derived",
            rows: None,
        });
    }
    let leakage = summary
        .leakage
        .iter()
        .filter_map(|(label, l)| {
            let id: DatasetVersionId = label.parse().ok()?;
            Some((id.as_uuid(), l.records, (l.percent / 100.0).clamp(0.0, 1.0)))
        })
        .collect();
    Ok(Checked::Ready {
        content: content_hash(shard_hashes.iter().map(String::as_str)),
        summary: raw,
        validation,
        stats,
        files,
        rows,
        bytes,
        leakage,
        artifact_id: outputs.artifact_id,
    })
}

async fn insert_files(
    tx: &mut Tx,
    org: Uuid,
    version: Uuid,
    files: &[NewFile],
) -> Result<(), JobsError> {
    for f in files {
        let sha = sha_bytes(&f.sha256).ok_or_else(|| {
            JobsError::Seam(SeamError::new("datasets", "a file hash is malformed"))
        })?;
        sqlx::query(
            "INSERT INTO dataset_files (dataset_version_id, org_id, path, split, role, object_key, \
               sha256, size_bytes, row_count) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (dataset_version_id, path) DO NOTHING",
        )
        .bind(version)
        .bind(org)
        .bind(&f.path)
        .bind(&f.split)
        .bind(f.role)
        .bind(&f.key)
        .bind(sha)
        .bind(i64::try_from(f.size).unwrap_or(i64::MAX))
        .bind(f.rows.map(|r| i64::try_from(r).unwrap_or(i64::MAX)))
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

async fn event(
    tx: &mut Tx,
    org: Uuid,
    version: Uuid,
    kind: &str,
    body: Value,
) -> Result<(), JobsError> {
    let e = NewEvent::new(kind, "dataset_version", version, 1, body)
        .map_err(|e| JobsError::Seam(SeamError::new("datasets", e.to_string())))?
        .org(org);
    OutboxWriter::write(tx, &e).await?;
    Ok(())
}

/// Writes the decision for version `id` (which must still be INGESTING or VALIDATING).
/// Returns false, changing nothing, when it was already decided.
pub async fn record_version(tx: &mut Tx, id: Uuid, checked: &Checked) -> Result<bool, JobsError> {
    let row: Option<(Uuid, String, Option<Uuid>)> = sqlx::query_as(
        "SELECT org_id, state, parent_version_id FROM dataset_versions WHERE id = $1 FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((org, state, parent)) = row else {
        return Ok(false);
    };
    if !matches!(state.as_str(), "INGESTING" | "VALIDATING") {
        return Ok(false);
    }
    let dsv = DatasetVersionId::from_uuid(id).to_string();
    match checked {
        Checked::Ready {
            summary,
            validation,
            stats,
            files,
            rows,
            bytes,
            content,
            leakage,
            artifact_id,
        } => {
            insert_files(tx, org, id, files).await?;
            let key = |p: &str| files.iter().find(|f| f.path == p).map(|f| f.key.clone());
            sqlx::query(
                "UPDATE dataset_versions SET state = 'READY', state_reason = NULL, row_count = $2, \
                   byte_size = $3, content_sha256 = $4, summary = $5, validation = $6, stats = $7, \
                   splits = $8, validation_report_key = $9, stats_key = $10, artifact_id = $11, \
                   ready_at = now() WHERE id = $1",
            )
            .bind(id)
            .bind(i64::try_from(*rows).unwrap_or(i64::MAX))
            .bind(i64::try_from(*bytes).unwrap_or(i64::MAX))
            .bind(content.as_slice())
            .bind(summary)
            .bind(validation)
            .bind(stats)
            .bind(summary.get("splits").cloned().unwrap_or(json!({})))
            .bind(key("validation_report.json"))
            .bind(key("stats.json"))
            .bind(artifact_id)
            .execute(&mut **tx)
            .await?;
            for (eval, records, ratio) in leakage {
                sqlx::query(
                    "INSERT INTO eval_contamination (dataset_version_id, eval_dataset_version_id, \
                       org_id, overlap_ratio, overlapping_rows, method) \
                     SELECT $1, e.id, $2, $3::float8::numeric, $4, 'exact' FROM dataset_versions e \
                      WHERE e.id = $5 AND e.org_id = $2 \
                     ON CONFLICT DO NOTHING",
                )
                .bind(id)
                .bind(org)
                .bind(*ratio)
                .bind(i64::try_from(*records).unwrap_or(i64::MAX))
                .bind(eval)
                .execute(&mut **tx)
                .await?;
            }
            if let Some(p) = parent {
                LineageWriter::link(
                    tx,
                    &crate::modules::checkpoints::service::storage_org(org),
                    NodeRef::new("dsv", id),
                    Relation::DerivedFrom,
                    NodeRef::new("dsv", p),
                )
                .await?;
            }
            event(
                tx,
                org,
                id,
                "dataset_version.ready",
                json!({"dataset_version_id": dsv, "rows": rows}),
            )
            .await?;
        }
        Checked::Invalid {
            code,
            reason,
            summary,
            validation,
            artifact_id,
        } => {
            sqlx::query(
                "UPDATE dataset_versions SET state = 'INVALID', state_reason = $2, summary = $3, \
                   validation = $4, artifact_id = COALESCE($5, artifact_id) WHERE id = $1",
            )
            .bind(id)
            .bind(json!({"code": code, "message": reason}))
            .bind(summary)
            .bind(validation)
            .bind(artifact_id)
            .execute(&mut **tx)
            .await?;
            event(
                tx,
                org,
                id,
                "dataset_version.invalid",
                json!({"dataset_version_id": dsv, "code": code}),
            )
            .await?;
        }
    }
    Ok(true)
}

/// 409 MRL-DATA-002: the version is not READY.
pub fn not_ready(state: &str) -> JobsError {
    JobsError::domain(
        codes::NOT_READY,
        StatusCode::CONFLICT,
        format!("This dataset version is {state}, not ready, so it can't be used yet."),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_hash_ignores_order_and_hex_round_trips() {
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        assert_eq!(
            content_hash([a.as_str(), b.as_str()]),
            content_hash([b.as_str(), a.as_str()])
        );
        assert_ne!(content_hash([a.as_str()]), content_hash([b.as_str()]));
        let expected: [u8; 32] = Sha256::digest(format!("{a}\n{b}\n")).into();
        assert_eq!(content_hash([b.as_str(), a.as_str()]), expected);
        let bytes = sha_bytes(&hex(&expected)).unwrap();
        assert_eq!(bytes, expected.to_vec());
        assert!(sha_bytes("zz").is_none());
    }

    #[test]
    fn nul_is_scrubbed_everywhere() {
        let v = scrub_nul(json!({"a\u{0}b": ["x\u{0}", {"k": "\u{0}"}], "n": 1}));
        assert_eq!(v, json!({"ab": ["x", {"k": ""}], "n": 1}));
    }
}

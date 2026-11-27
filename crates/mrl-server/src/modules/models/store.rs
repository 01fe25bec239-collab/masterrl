//! Rows of the model tables and the queries every route shares.

use crate::db::{Db, DbError, Tx};
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

/// A `models` row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ModelRow {
    /// Id.
    pub id: Uuid,
    /// Owning org.
    pub org_id: Uuid,
    /// Owning project.
    pub project_id: Uuid,
    /// Unique name in the project.
    pub name: String,
    /// Family of the newest READY version.
    pub family: Option<String>,
    /// Who created it.
    pub created_by: String,
    /// When.
    pub created_at: OffsetDateTime,
}

/// A `model_versions` row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct VersionRow {
    /// Id.
    pub id: Uuid,
    /// Owning org.
    pub org_id: Uuid,
    /// Owning project.
    pub project_id: Uuid,
    /// Its model.
    pub model_id: Uuid,
    /// 1, 2, … within the model.
    pub seq: i32,
    /// `full_weights`, `adapter`, `inference_only`; `None` until inspected.
    pub kind: Option<String>,
    /// `hub`, `upload`, `node_path`, `checkpoint`, `merge`.
    pub source_kind: String,
    /// Where it came from.
    pub source_ref: Value,
    /// The base of an adapter (or of a merge).
    pub base_model_version_id: Option<Uuid>,
    /// Model family for the capability check.
    pub family: Option<String>,
    /// `architectures[0]`.
    pub architecture: Option<String>,
    /// `model_type`.
    pub model_type: Option<String>,
    /// Parameters.
    pub param_count: Option<i64>,
    /// Weight dtypes.
    pub dtypes: Vec<String>,
    /// Context length.
    pub context_length: Option<i32>,
    /// Mixture of experts.
    pub is_moe: bool,
    /// Quantization, like `bnb-4bit`.
    pub quantization: Option<String>,
    /// Hash of the tokenizer files.
    pub tokenizer_sha256: Option<Vec<u8>>,
    /// Hash of the default chat template.
    pub chat_template_sha256: Option<Vec<u8>>,
    /// Ships its own Python code.
    pub requires_remote_code: bool,
    /// Who approved the code.
    pub remote_code_approved_by: Option<String>,
    /// When.
    pub remote_code_approved_at: Option<OffsetDateTime>,
    /// The revision approved.
    pub remote_code_approved_for: Option<String>,
    /// SPDX id.
    pub license_spdx: Option<String>,
    /// `spdx`, `custom`, `unknown`.
    pub license_kind: Option<String>,
    /// Hash of the license text the project acknowledges.
    pub license_sha256: Option<Vec<u8>>,
    /// The hash runs record as `base_model_sha256`.
    pub weights_sha256: Option<Vec<u8>>,
    /// Hash of the file list.
    pub manifest_sha256: Option<Vec<u8>>,
    /// `IMPORTING`, `READY`, `INVALID`, `ARCHIVED`.
    pub state: String,
    /// `EXPERIMENTAL`, `CANDIDATE`, `APPROVED`, `ARCHIVED`.
    pub stage: String,
    /// Per engine version.
    pub compatibility: Value,
    /// The inspection report.
    pub inspection: Option<Value>,
    /// Why it is INVALID.
    pub invalid_reason: Option<Value>,
    /// The job that inspects or builds it.
    pub job_id: Option<Uuid>,
    /// Who created it.
    pub created_by: String,
    /// When.
    pub created_at: OffsetDateTime,
    /// When it became READY.
    pub ready_at: Option<OffsetDateTime>,
}

/// Columns of [`VersionRow`], for `SELECT` (a macro, so queries stay literal strings).
macro_rules! version_cols {
    () => {
        "id, org_id, project_id, model_id, seq, kind, source_kind, \
    source_ref, base_model_version_id, family, architecture, model_type, param_count, dtypes, \
    context_length, is_moe, quantization, tokenizer_sha256, chat_template_sha256, \
    requires_remote_code, remote_code_approved_by, remote_code_approved_at, \
    remote_code_approved_for, license_spdx, license_kind, license_sha256, weights_sha256, \
    manifest_sha256, state, stage, compatibility, inspection, invalid_reason, job_id, \
    created_by, created_at, ready_at"
    };
}
pub(crate) use version_cols;

/// Columns of [`ModelRow`].
macro_rules! model_cols {
    () => {
        "id, org_id, project_id, name, family, created_by, created_at"
    };
}
pub(crate) use model_cols;

impl VersionRow {
    /// The revision approvals name: the Hub commit, else the hash of the file list.
    pub fn revision(&self) -> Option<String> {
        self.source_ref
            .get("commit")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| self.manifest_sha256.as_deref().map(hex))
    }

    /// The custom code may run: not needed, or approved for this exact revision.
    pub fn code_approved(&self) -> bool {
        !self.requires_remote_code
            || (self.remote_code_approved_for.is_some()
                && self.remote_code_approved_for == self.revision())
    }
}

/// A `model_files` row.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct FileRow {
    /// Path inside the model folder, `/`-separated.
    pub path: String,
    /// `weights`, `config`, `tokenizer`, `adapter`, `code`, `other`.
    pub role: String,
    /// The content-addressed blob.
    pub object_key: String,
    /// sha256.
    pub sha256: Vec<u8>,
    /// Bytes.
    pub size_bytes: i64,
    /// `safetensors`, `gguf`, `json`, `txt`, `other`.
    pub format: String,
}

/// Lowercase hex.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// 32 bytes from 64 hex digits.
pub fn unhex(text: &str) -> Option<Vec<u8>> {
    if text.len() != 64 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (0..32)
        .map(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16).ok())
        .collect()
}

/// sha256 of the sorted `name\0hash\n` lines of `items` (hashes in hex).
pub fn list_hash(items: impl Iterator<Item = (String, String)>) -> Vec<u8> {
    let mut lines: Vec<String> = items.map(|(p, s)| format!("{p}\0{s}\n")).collect();
    lines.sort();
    Sha256::digest(lines.concat().as_bytes()).to_vec()
}

/// [`list_hash`] of files.
pub fn files_hash<'a>(files: impl Iterator<Item = &'a FileRow>) -> Vec<u8> {
    list_hash(files.map(|f| (f.path.clone(), hex(&f.sha256))))
}

/// The weights hash: the single weights file's sha256, or [`list_hash`] of all of them.
pub fn weights_hash(files: &[FileRow]) -> Option<Vec<u8>> {
    let weights: Vec<&FileRow> = files
        .iter()
        .filter(|f| matches!(f.role.as_str(), "weights" | "adapter"))
        .filter(|f| matches!(f.format.as_str(), "safetensors" | "gguf" | "onnx"))
        .collect();
    match weights.as_slice() {
        [] => None,
        [one] => Some(one.sha256.clone()),
        many => Some(files_hash(many.iter().copied())),
    }
}

/// The version `id`, any state.
pub async fn version(db: &Db, id: Uuid) -> Result<Option<VersionRow>, DbError> {
    Ok(sqlx::query_as(concat!(
        "SELECT ",
        version_cols!(),
        " FROM model_versions WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(db.pool())
    .await?)
}

/// The version `id`, locked until `tx` ends.
pub async fn lock_version(tx: &mut Tx, id: Uuid) -> Result<Option<VersionRow>, DbError> {
    Ok(sqlx::query_as(concat!(
        "SELECT ",
        version_cols!(),
        " FROM model_versions WHERE id = $1 FOR UPDATE"
    ))
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?)
}

/// The version a built-in job inspects or builds.
pub async fn version_of_job(db: &Db, job: Uuid) -> Result<Option<VersionRow>, DbError> {
    Ok(sqlx::query_as(concat!(
        "SELECT ",
        version_cols!(),
        " FROM model_versions WHERE job_id = $1"
    ))
    .bind(job)
    .fetch_optional(db.pool())
    .await?)
}

/// The model `id`.
pub async fn model(db: &Db, id: Uuid) -> Result<Option<ModelRow>, DbError> {
    Ok(sqlx::query_as(concat!(
        "SELECT ",
        model_cols!(),
        " FROM models WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(db.pool())
    .await?)
}

/// The version's files, by path.
pub async fn files(db: &Db, version: Uuid) -> Result<Vec<FileRow>, DbError> {
    Ok(sqlx::query_as(
        "SELECT path, role, object_key, sha256, size_bytes, format FROM model_files
          WHERE model_version_id = $1 ORDER BY path",
    )
    .bind(version)
    .fetch_all(db.pool())
    .await?)
}

/// The model `name` of the project, created when missing; locked until `tx` ends (so two
/// versions of one model get different numbers).
pub async fn get_or_create_model(
    tx: &mut Tx,
    org: Uuid,
    project: Uuid,
    name: &str,
    created_by: &str,
) -> Result<ModelRow, DbError> {
    Ok(sqlx::query_as(concat!(
        "INSERT INTO models (id, org_id, project_id, name, created_by) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (org_id, project_id, name) DO UPDATE SET name = EXCLUDED.name
         RETURNING ",
        model_cols!()
    ))
    .bind(Uuid::now_v7())
    .bind(org)
    .bind(project)
    .bind(name)
    .bind(created_by)
    .fetch_one(&mut **tx)
    .await?)
}

/// What a new version starts with.
#[derive(Debug, Clone)]
pub struct NewVersion {
    /// `hub`, `upload`, `node_path`, `checkpoint`, `merge`.
    pub source_kind: &'static str,
    /// Where it comes from.
    pub source_ref: Value,
    /// The base (adapters, merges).
    pub base_model_version_id: Option<Uuid>,
    /// Files known up front (uploads, promoted checkpoints); the job's outputs add the rest.
    pub files: Vec<FileRow>,
    /// Who asks.
    pub created_by: String,
}

/// A new IMPORTING version of `model` (locked by [`get_or_create_model`]), with its known files.
pub async fn insert_version(
    tx: &mut Tx,
    model: &ModelRow,
    new: &NewVersion,
) -> Result<VersionRow, DbError> {
    let row: VersionRow = sqlx::query_as(concat!(
        "INSERT INTO model_versions (id, org_id, project_id, model_id, seq, source_kind,
                source_ref, base_model_version_id, state, created_by)
         VALUES ($1, $2, $3, $4,
                 (SELECT COALESCE(MAX(seq), 0) + 1 FROM model_versions WHERE model_id = $4),
                 $5, $6, $7, 'IMPORTING', $8)
         RETURNING ",
        version_cols!()
    ))
    .bind(Uuid::now_v7())
    .bind(model.org_id)
    .bind(model.project_id)
    .bind(model.id)
    .bind(new.source_kind)
    .bind(&new.source_ref)
    .bind(new.base_model_version_id)
    .bind(&new.created_by)
    .fetch_one(&mut **tx)
    .await?;
    insert_files(tx, &row, &new.files).await?;
    Ok(row)
}

/// Records `files` for `version`.
pub async fn insert_files(
    tx: &mut Tx,
    version: &VersionRow,
    files: &[FileRow],
) -> Result<(), DbError> {
    for f in files {
        sqlx::query(
            "INSERT INTO model_files (model_version_id, org_id, path, role, object_key, sha256,
                    size_bytes, format)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(version.id)
        .bind(version.org_id)
        .bind(&f.path)
        .bind(&f.role)
        .bind(&f.object_key)
        .bind(&f.sha256)
        .bind(f.size_bytes)
        .bind(&f.format)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// Links the version to the job that inspects or builds it.
pub async fn set_job(tx: &mut Tx, version: Uuid, job: Uuid) -> Result<(), DbError> {
    sqlx::query("UPDATE model_versions SET job_id = $2 WHERE id = $1")
        .bind(version)
        .bind(job)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// License hashes the project has acknowledged for this model.
pub async fn acknowledged(db: &Db, project: Uuid, model: Uuid) -> Result<Vec<Vec<u8>>, DbError> {
    Ok(sqlx::query_scalar(
        "SELECT license_text_sha256 FROM license_acknowledgements
          WHERE project_id = $1 AND model_id = $2",
    )
    .bind(project)
    .bind(model)
    .fetch_all(db.pool())
    .await?)
}

/// The role and format the registry records for a file at `path` (P15's roles).
pub fn classify(path: &str, role: Option<&str>) -> (String, String) {
    let lower = path.to_ascii_lowercase();
    let format = if lower.ends_with(".safetensors") {
        "safetensors"
    } else if lower.ends_with(".gguf") {
        "gguf"
    } else if lower.ends_with(".onnx") {
        "onnx"
    } else if lower.ends_with(".json") {
        "json"
    } else if lower.ends_with(".txt") || lower.ends_with(".md") || lower.ends_with(".jinja") {
        "txt"
    } else {
        "other"
    };
    let role = role
        .filter(|r| ["weights", "config", "tokenizer", "adapter", "code"].contains(r))
        .unwrap_or("other");
    (role.to_owned(), format.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, role: &str, sha: u8) -> FileRow {
        let (_, format) = classify(path, Some(role));
        FileRow {
            path: path.into(),
            role: role.into(),
            object_key: String::new(),
            sha256: vec![sha; 32],
            size_bytes: 1,
            format,
        }
    }

    #[test]
    fn weights_hash_is_the_single_file_or_the_sorted_list() {
        let one = [
            file("model.safetensors", "weights", 7),
            file("config.json", "config", 1),
        ];
        assert_eq!(weights_hash(&one), Some(vec![7; 32]));
        let a = [
            file("b.safetensors", "weights", 2),
            file("a.safetensors", "weights", 1),
        ];
        let b = [
            file("a.safetensors", "weights", 1),
            file("b.safetensors", "weights", 2),
        ];
        assert_eq!(weights_hash(&a), weights_hash(&b));
        assert_ne!(weights_hash(&a), Some(vec![1; 32]));
        assert_eq!(weights_hash(&[file("config.json", "config", 1)]), None);
    }

    #[test]
    fn hex_round_trips_and_refuses_bad_text() {
        let h = hex(&[0xab; 32]);
        assert_eq!(unhex(&h), Some(vec![0xab; 32]));
        assert_eq!(unhex("zz"), None);
        assert_eq!(unhex(&"g".repeat(64)), None);
    }
}

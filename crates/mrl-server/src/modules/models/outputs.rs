//! Recording what a model job produced: the version becomes READY with its inspection, files,
//! weights hash and compatibility, or INVALID with the reason. [`ModelOutputs`] (completion
//! chain) calls [`record`] once INT-6 registered the job's outputs as an artifact;
//! [`ModelJobEnds`] (event consumer) marks the version INVALID when its job fails, is
//! cancelled or rejected. Export jobs get their `exported_as` lineage edge.
//!
//! The job's `outputs/` hold `inspection.json` (P15's `ModelInspection` without the tensor
//! list, plus `license` and `source`) and the model files under `model/`. Every model job
//! writes the files (imports copy even staged ones), so a version's files are always objects
//! of its job's output artifact: they outlive uploads and checkpoint retention.

use super::codes;
use super::gates;
use super::store::{self, unhex, FileRow, VersionRow};
use super::Models;
use crate::db::{Db, DbError, Tx};
use crate::modules::events::{ConsumerError, Event, EventConsumer};
use crate::modules::jobs::{scope, CompletionVerifier, JobRow, Rejection, Verification};
use crate::modules::lineage::{LineageWriter, NodeRef, Relation};
use crate::seams::SeamError;
use mrl_domain::errors::ErrorCode;
use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;

/// The inspection report file in a job's outputs.
pub const INSPECTION_FILE: &str = "inspection.json";
/// Where a job's outputs hold the model files.
pub const MODEL_DIR: &str = "model/";
/// Largest inspection report read.
const MAX_REPORT: u64 = 16 * 1024 * 1024;
/// File names that hold pickled Python objects (never stored as a version's files).
const PICKLE_SUFFIXES: [&str; 6] = [".bin", ".pt", ".pth", ".ckpt", ".pkl", ".pickle"];

/// The artifact a job's outputs were registered as.
#[derive(Debug, Clone)]
pub struct Produced {
    /// The artifact id.
    pub artifact_id: Uuid,
    /// Its `artifact-manifest/v1` document.
    pub manifest: Value,
    /// Its full object-key prefix (ends with `/`); a file's key is the prefix plus its path.
    pub prefix: String,
}

fn seam(e: impl std::fmt::Display) -> SeamError {
    SeamError::new("models", e.to_string())
}

/// The org text objects of `org` are stored under.
pub fn storage_org(org: Uuid) -> String {
    scope::org_text(org).unwrap_or_else(|| crate::modules::artifacts::LOCAL_ORG.to_owned())
}

/// Why a version is INVALID.
#[derive(Debug, Clone)]
struct Invalid {
    code: ErrorCode,
    message: String,
}

fn invalid(code: ErrorCode, message: impl Into<String>) -> Invalid {
    Invalid {
        code,
        message: message.into(),
    }
}

fn rejection(reason: &Value) -> Rejection {
    let code = reason["code"]
        .as_str()
        .and_then(|c| c.parse::<ErrorCode>().ok())
        .unwrap_or(codes::UNREADABLE);
    Rejection::new(
        code,
        None,
        reason["message"]
            .as_str()
            .unwrap_or("The model was refused."),
    )
}

/// What `version`'s settled state means for its job.
fn settled(version: &VersionRow) -> Option<Verification> {
    match version.state.as_str() {
        "READY" => Some(Verification::Verified),
        "INVALID" => Some(Verification::Failed(rejection(
            version.invalid_reason.as_ref().unwrap_or(&Value::Null),
        ))),
        _ => None,
    }
}

/// Records the outputs of `job` for the version it inspects or builds (see the module docs).
/// Abstains for jobs that build no version. Repeating it answers the same.
pub async fn record(
    models: &Models,
    job: Uuid,
    produced: Option<&Produced>,
) -> Result<Verification, SeamError> {
    let Some(version) = store::version_of_job(&models.db, job).await.map_err(seam)? else {
        return Ok(Verification::Abstain);
    };
    if let Some(done) = settled(&version) {
        return Ok(done);
    }
    let Some(produced) = produced else {
        return settle(
            models,
            &version,
            Err(invalid(
                codes::UNREADABLE,
                "The job finished without writing its results.",
            )),
        )
        .await;
    };
    let decided = match read_report(models, &version, produced).await? {
        Ok(report) => decide(models, &version, produced, report).await?,
        Err(why) => Err(why),
    };
    settle(models, &version, decided).await
}

/// Marks the version `job` builds INVALID (in `tx`) because the job ended without results;
/// a settled version keeps its outcome.
pub async fn job_ended(tx: &mut Tx, job: Uuid, why: &str) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE model_versions SET state = 'INVALID', invalid_reason = $2
          WHERE job_id = $1 AND state = 'IMPORTING'",
    )
    .bind(job)
    .bind(json!({"code": codes::UNREADABLE.to_string(), "message": why}))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The inspection report from the outputs.
async fn read_report(
    models: &Models,
    version: &VersionRow,
    produced: &Produced,
) -> Result<Result<Value, Invalid>, SeamError> {
    let entry = produced.manifest["files"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|f| f["path"] == INSPECTION_FILE);
    let Some((sha, size)) = entry.and_then(|f| Some((f["sha256"].as_str()?, f["size"].as_u64()?)))
    else {
        return Ok(Err(invalid(
            codes::UNREADABLE,
            "The job wrote no inspection report.",
        )));
    };
    if size > MAX_REPORT {
        return Ok(Err(invalid(
            codes::UNREADABLE,
            "The inspection report is too large.",
        )));
    }
    let st = models
        .store
        .org_store(&storage_org(version.org_id))
        .await
        .map_err(seam)?;
    let Ok(key) = st.keys().join(&produced.prefix, INSPECTION_FILE) else {
        return Ok(Err(invalid(
            codes::UNREADABLE,
            "The job's outputs are not where they were registered.",
        )));
    };
    let bytes = st.get(&key).await.map_err(seam)?;
    if mrl_storage::sha256_hex(&bytes) != sha {
        return Ok(Err(invalid(
            codes::UNREADABLE,
            "The stored inspection report does not match its hash.",
        )));
    }
    Ok(serde_json::from_slice(&bytes).map_err(|_| {
        invalid(
            codes::UNREADABLE,
            "The inspection report is not valid JSON.",
        )
    }))
}

/// What the version is, given its report: the READY update, or why it is INVALID.
struct Ready {
    files: Vec<FileRow>,
    report: Value,
    family: String,
    compatibility: Value,
    license: (Option<String>, Option<String>, Option<Vec<u8>>),
}

async fn decide(
    models: &Models,
    version: &VersionRow,
    produced: &Produced,
    report: Value,
) -> Result<Result<Ready, Invalid>, SeamError> {
    if let Some(e) = report.get("error").filter(|e| e.is_object()) {
        let code = e["code"]
            .as_str()
            .and_then(|c| c.parse().ok())
            .unwrap_or(codes::UNREADABLE);
        return Ok(Err(invalid(
            code,
            e["message"].as_str().unwrap_or("The model was refused."),
        )));
    }
    if report["refusal"].as_str() == Some(&codes::PICKLE_REFUSED.to_string()) {
        return Ok(Err(invalid(
            codes::PICKLE_REFUSED,
            "The model's weights are pickle files, which can run code when loaded. Only safetensors weights are accepted.",
        )));
    }
    let kind = report["kind"].as_str().unwrap_or_default().to_owned();
    if !matches!(kind.as_str(), "full_weights" | "adapter" | "inference_only") {
        return Ok(Err(invalid(
            codes::UNREADABLE,
            "No model weights were found in these files.",
        )));
    }
    let base = match version.base_model_version_id {
        Some(b) => gates::ready_in(&models.db, version.org_id, version.project_id, b)
            .await
            .map_err(seam)?,
        None => None,
    };
    if kind == "adapter" && base.is_none() {
        return Ok(Err(invalid(
            codes::ADAPTER_BASE,
            "This is a LoRA adapter: import it with the READY base model version it was trained on.",
        )));
    }
    let files = match files_of(models, version, produced, &report).await? {
        Ok(f) => f,
        Err(why) => return Ok(Err(why)),
    };
    if store::weights_hash(&files).is_none() {
        return Ok(Err(invalid(
            codes::UNREADABLE,
            "No model weights were found in these files.",
        )));
    }
    let family = match (&kind[..], &base) {
        ("adapter", Some(b)) => b.family.clone().unwrap_or_else(|| "unknown".into()),
        _ => gates::family(&report),
    };
    let license = match (&report["license"], &base) {
        (Value::Object(l), _) => (
            l.get("spdx").and_then(Value::as_str).map(str::to_owned),
            l.get("kind").and_then(Value::as_str).map(str::to_owned),
            l.get("sha256").and_then(Value::as_str).and_then(unhex),
        ),
        // Derived versions carry their base's license.
        (_, Some(b)) => (
            b.license_spdx.clone(),
            b.license_kind.clone(),
            b.license_sha256.clone(),
        ),
        _ => (None, Some("unknown".into()), None),
    };
    let mut probe = version.clone();
    apply_facts(&mut probe, &report, &kind, &family);
    // Compatibility is about the model; the code approval is a separate gate.
    probe.remote_code_approved_for = probe.revision();
    let compatibility = gates::compatibility(&models.db, &gates::facts(&probe)).await?;
    Ok(Ok(Ready {
        files,
        report,
        family,
        compatibility,
        license,
    }))
}

/// The version's files: the outputs under `model/`.
async fn files_of(
    models: &Models,
    version: &VersionRow,
    produced: &Produced,
    report: &Value,
) -> Result<Result<Vec<FileRow>, Invalid>, SeamError> {
    let roles: HashMap<&str, &str> = report["files"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|f| Some((f["path"].as_str()?, f["role"].as_str()?)))
        .collect();
    let st = models
        .store
        .org_store(&storage_org(version.org_id))
        .await
        .map_err(seam)?;
    let damaged = || {
        Ok(Err(invalid(
            codes::UNREADABLE,
            "The job's file list is damaged.",
        )))
    };
    let mut files = Vec::new();
    for f in produced.manifest["files"].as_array().into_iter().flatten() {
        let Some(path) = f["path"].as_str().and_then(|p| p.strip_prefix(MODEL_DIR)) else {
            continue;
        };
        let (Some(sha), Some(size), Ok(key)) = (
            f["sha256"].as_str().and_then(unhex),
            f["size"].as_i64(),
            st.keys()
                .join(&produced.prefix, &format!("{MODEL_DIR}{path}")),
        ) else {
            return damaged();
        };
        let lower = path.to_ascii_lowercase();
        if PICKLE_SUFFIXES.iter().any(|s| lower.ends_with(s)) {
            return Ok(Err(invalid(
                codes::PICKLE_REFUSED,
                format!("{path} is a pickle file, which can run code when loaded."),
            )));
        }
        let (role, format) = store::classify(path, roles.get(path).copied());
        files.push(FileRow {
            path: path.to_owned(),
            role,
            object_key: key,
            sha256: sha,
            size_bytes: size,
            format,
        });
    }
    Ok(Ok(files))
}

fn apply_facts(v: &mut VersionRow, report: &Value, kind: &str, family: &str) {
    v.kind = Some(kind.to_owned());
    v.family = Some(family.to_owned());
    v.architecture = report["architecture"].as_str().map(str::to_owned);
    v.model_type = report["model_type"].as_str().map(str::to_owned);
    v.param_count = report["param_count"].as_i64();
    v.dtypes = report["params_by_dtype"]
        .as_object()
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    v.context_length = report["context_length"]
        .as_i64()
        .and_then(|c| i32::try_from(c).ok());
    v.is_moe = !report["moe"].is_null();
    v.quantization = report["quantization"].as_str().map(str::to_owned);
    v.requires_remote_code = report["requires_remote_code"].as_bool().unwrap_or(false);
    v.chat_template_sha256 = report["chat_template_sha256"].as_str().and_then(unhex);
    v.tokenizer_sha256 = report["tokenizer_sha256"]
        .as_object()
        .filter(|m| !m.is_empty())
        .map(|m| {
            store::list_hash(
                m.iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned()))),
            )
        });
}

/// Writes the outcome under the version's lock; a version another call settled first keeps
/// that outcome.
async fn settle(
    models: &Models,
    version: &VersionRow,
    decided: Result<Ready, Invalid>,
) -> Result<Verification, SeamError> {
    let mut tx = models.db.begin().await.map_err(seam)?;
    let Some(current) = store::lock_version(&mut tx, version.id)
        .await
        .map_err(seam)?
    else {
        return Ok(Verification::Abstain);
    };
    if let Some(done) = settled(&current) {
        return Ok(done);
    }
    let outcome = match decided {
        Ok(ready) => {
            write_ready(&mut tx, &current, ready).await.map_err(seam)?;
            Verification::Verified
        }
        Err(why) => {
            let reason = json!({"code": why.code.to_string(), "message": why.message});
            sqlx::query(
                "UPDATE model_versions SET state = 'INVALID', invalid_reason = $2 WHERE id = $1",
            )
            .bind(current.id)
            .bind(&reason)
            .execute(&mut *tx)
            .await
            .map_err(seam)?;
            Verification::Failed(rejection(&reason))
        }
    };
    tx.commit().await.map_err(seam)?;
    Ok(outcome)
}

async fn write_ready(tx: &mut Tx, v: &VersionRow, ready: Ready) -> Result<(), DbError> {
    let Ready {
        files,
        mut report,
        family,
        compatibility,
        license,
    } = ready;
    store::insert_files(tx, v, &files).await?;
    let mut row = v.clone();
    let kind = report["kind"].as_str().unwrap_or_default().to_owned();
    apply_facts(&mut row, &report, &kind, &family);
    let mut source_ref = v.source_ref.clone();
    if let (Some(s), Value::Object(src)) = (source_ref.as_object_mut(), &report["source"]) {
        for (k, val) in src {
            s.entry(k.clone()).or_insert_with(|| val.clone());
        }
    }
    if let Some(r) = report.as_object_mut() {
        r.remove("tensors");
        r.remove("path");
    }
    let manifest = store::files_hash(files.iter());
    sqlx::query(
        "UPDATE model_versions SET state = 'READY', ready_at = now(), kind = $2, family = $3,
                architecture = $4, model_type = $5, param_count = $6, dtypes = $7,
                context_length = $8, is_moe = $9, quantization = $10, tokenizer_sha256 = $11,
                chat_template_sha256 = $12, requires_remote_code = $13, license_spdx = $14,
                license_kind = $15, license_sha256 = $16, weights_sha256 = $17,
                manifest_sha256 = $18, compatibility = $19, inspection = $20, source_ref = $21
          WHERE id = $1",
    )
    .bind(v.id)
    .bind(&row.kind)
    .bind(&family)
    .bind(&row.architecture)
    .bind(&row.model_type)
    .bind(row.param_count)
    .bind(&row.dtypes)
    .bind(row.context_length)
    .bind(row.is_moe)
    .bind(&row.quantization)
    .bind(&row.tokenizer_sha256)
    .bind(&row.chat_template_sha256)
    .bind(row.requires_remote_code)
    .bind(&license.0)
    .bind(&license.1)
    .bind(&license.2)
    .bind(store::weights_hash(&files))
    .bind(&manifest)
    .bind(&compatibility)
    .bind(&report)
    .bind(&source_ref)
    .execute(&mut **tx)
    .await?;
    sqlx::query("UPDATE models SET family = $2 WHERE id = $1")
        .bind(v.model_id)
        .bind(&family)
        .execute(&mut **tx)
        .await?;
    link(tx, v).await
}

/// Lineage of a new version: what it was promoted from or made from.
async fn link(tx: &mut Tx, v: &VersionRow) -> Result<(), DbError> {
    let org = storage_org(v.org_id);
    let mv = NodeRef::new("mv", v.id);
    let id = |key: &str, kind: &str| {
        v.source_ref[key]
            .as_str()
            .and_then(|s| NodeRef::parse(s).ok())
            .filter(|n| n.kind == kind)
    };
    if let Some(ckpt) = id("checkpoint_id", "ckpt") {
        LineageWriter::link(tx, &org, ckpt, Relation::PromotedTo, mv).await?;
    }
    if let Some(adapter) = id("adapter", "mv") {
        LineageWriter::link(tx, &org, mv, Relation::DerivedFrom, adapter).await?;
    }
    if let Some(base) = v.base_model_version_id {
        LineageWriter::link(
            tx,
            &org,
            mv,
            Relation::DerivedFrom,
            NodeRef::new("mv", base),
        )
        .await?;
    }
    Ok(())
}

// ponytail: chains are built from the database alone, so the verifier reaches storage through
// the models module of this process (set once at startup by `ModelsModule::new`).
static MODELS: std::sync::OnceLock<std::sync::Arc<Models>> = std::sync::OnceLock::new();

/// Makes `models` the one [`ModelOutputs::new`] uses (first call wins).
pub(crate) fn install(models: std::sync::Arc<Models>) {
    let _ = MODELS.set(models);
}

/// Completion chain: vouches for model jobs (`import_model`, `merge_adapter`,
/// `export_model`): waits for their registered outputs, records the version (see the module
/// docs) or the export's lineage. Abstains for every other job.
pub struct ModelOutputs {
    db: Db,
    models: Option<std::sync::Arc<Models>>,
}

impl ModelOutputs {
    /// The verifier of this process's models module.
    pub fn new(db: Db) -> Self {
        Self { db, models: None }
    }

    /// The verifier for these models (tests).
    pub fn with(models: std::sync::Arc<Models>) -> Self {
        Self {
            db: models.db.clone(),
            models: Some(models),
        }
    }
}

/// The built-in tasks whose jobs P26 vouches for.
const TASKS: [&str; 3] = ["import_model", "merge_adapter", "export_model"];

#[async_trait::async_trait]
impl CompletionVerifier for ModelOutputs {
    fn name(&self) -> &'static str {
        "model_outputs"
    }

    async fn verify(&self, job: &JobRow) -> Result<Verification, SeamError> {
        let task: Option<String> =
            sqlx::query_scalar("SELECT task FROM builtin_jobs WHERE job_id = $1")
                .bind(job.id)
                .fetch_optional(self.db.pool())
                .await
                .map_err(seam)?;
        let Some(task) = task.filter(|t| TASKS.contains(&t.as_str())) else {
            return Ok(Verification::Abstain);
        };
        let Some(models) = self.models.as_ref().or_else(|| MODELS.get()) else {
            return Ok(Verification::RetryLater(
                "the models module has not started".into(),
            ));
        };
        let Some(out) = crate::modules::builtin::outputs(&self.db, job.org_id, job.id)
            .await
            .map_err(seam)?
        else {
            // INT-6's own verifier fails jobs whose outputs can't be saved.
            return Ok(Verification::RetryLater(
                "the job's results are still being saved".into(),
            ));
        };
        let org = storage_org(job.org_id);
        let Some(artifact) = models
            .store
            .get(&org, out.artifact_id)
            .await
            .map_err(seam)?
        else {
            return Ok(Verification::RetryLater(
                "the job's results are not readable yet".into(),
            ));
        };
        if task == "export_model" {
            return exported(&self.db, job, out.artifact_id).await;
        }
        let produced = Produced {
            artifact_id: out.artifact_id,
            manifest: out.manifest,
            prefix: artifact.prefix,
        };
        record(models, job.id, Some(&produced)).await
    }
}

/// An export job's outputs: the version it exported gets its `exported_as` edge.
async fn exported(db: &Db, job: &JobRow, artifact: Uuid) -> Result<Verification, SeamError> {
    let mv = job
        .subject_ref
        .as_ref()
        .and_then(|s| s["model_version_id"].as_str())
        .and_then(|s| NodeRef::parse(s).ok());
    let Some(mv) = mv.filter(|n| n.kind == "mv") else {
        return Ok(Verification::Abstain);
    };
    let mut tx = db.begin().await.map_err(seam)?;
    LineageWriter::link(
        &mut tx,
        &storage_org(job.org_id),
        mv,
        Relation::ExportedAs,
        NodeRef::new("art", artifact),
    )
    .await
    .map_err(seam)?;
    tx.commit().await.map_err(seam)?;
    Ok(Verification::Verified)
}

/// Event consumer: a model job that failed, was cancelled or rejected leaves its version
/// INVALID (see [`job_ended`]).
pub struct ModelJobEnds;

#[async_trait::async_trait]
impl EventConsumer for ModelJobEnds {
    fn name(&self) -> &'static str {
        "models-job-ends"
    }

    fn accepts(&self, t: &str) -> bool {
        matches!(t, "job.failed" | "job.cancelled" | "job.rejected")
    }

    async fn handle(&self, tx: &mut Tx, event: &Event) -> Result<(), ConsumerError> {
        let why = match event.event_type.as_str() {
            "job.cancelled" => "The job making this version was cancelled.",
            "job.rejected" => "The job making this version was refused before it ran.",
            _ => "The job making this version failed.",
        };
        job_ended(tx, event.aggregate_id, why).await?;
        Ok(())
    }
}

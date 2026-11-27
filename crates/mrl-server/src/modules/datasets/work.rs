//! Durable handoff between INT-6 output registration and dataset publication.

use super::store::{self, Checked, Outputs, OutputsError};
use super::Datasets;
use crate::db::{Db, Tx};
use crate::modules::artifacts::InputFile;
use crate::modules::audit::PgAuditSink;
use crate::modules::builtin::{self, BuiltinGrant, BuiltinJob};
use crate::modules::checkpoints::service::storage_org;
use crate::modules::jobs::{
    CompletionVerifier, JobRow, JobTransitions, JobsError, Rejection, Verification,
};
use crate::seams::{AuditEvent, AuditOutcome, AuditSink, SeamError};
use mrl_domain::failure::{FailureClass, FailureCode};
use mrl_domain::ids::DatasetVersionId;
use serde_json::{json, Value};
use uuid::Uuid;

pub(super) fn seam(error: impl std::fmt::Display) -> JobsError {
    SeamError::new("datasets", error.to_string()).into()
}

pub(super) async fn queue(
    tx: &mut Tx,
    version: Uuid,
    purpose: &str,
    job: BuiltinJob,
) -> Result<Uuid, JobsError> {
    let row = builtin::submit(tx, job).await?;
    sqlx::query("INSERT INTO dataset_work (job_id, version_id, purpose) VALUES ($1,$2,$3)")
        .bind(row.id)
        .bind(version)
        .bind(purpose)
        .execute(&mut **tx)
        .await?;
    Ok(row.id)
}

pub(super) fn job(
    owner: (Uuid, Uuid),
    version: Uuid,
    task: (&str, &str),
    created_by: &str,
    inputs: Value,
    files: Vec<InputFile>,
    grants: Vec<BuiltinGrant>,
) -> BuiltinJob {
    BuiltinJob {
        org_id: owner.0,
        project_id: owner.1,
        kind: task.0.into(),
        task: task.1.into(),
        inputs,
        files,
        subject: json!({"dataset_version_id": DatasetVersionId::from_uuid(version)}),
        created_by: created_by.into(),
        grants,
        node_id: None,
    }
}

pub(super) fn input(role: &str, path: &str, key: String, sha256: String, size: u64) -> InputFile {
    InputFile {
        name: role.into(),
        local_path: format!("{role}/{path}"),
        key,
        sha256,
        size,
    }
}

pub(super) async fn dataset_inputs(
    db: &Db,
    id: Uuid,
    role: &str,
) -> Result<Vec<InputFile>, JobsError> {
    Ok(store::files(db, id)
        .await?
        .into_iter()
        .filter(|f| f.role == "shard" || f.path == store::SUMMARY)
        .map(|f| {
            input(
                role,
                &f.path,
                f.object_key,
                store::hex(&f.sha256),
                f.size_bytes as u64,
            )
        })
        .collect())
}

pub(super) async fn eval_inputs(
    db: &Db,
    (org, project): (Uuid, Uuid),
    inputs: &mut Value,
    files: &mut Vec<InputFile>,
) -> Result<(), JobsError> {
    let sets: Vec<(Uuid, String, Vec<u8>, i64)> = sqlx::query_as(
        "SELECT v.id, f.object_key, f.sha256, f.size_bytes FROM datasets d
         JOIN LATERAL (SELECT id FROM dataset_versions
           WHERE dataset_id=d.id AND org_id=d.org_id AND state='READY' ORDER BY seq DESC LIMIT 1) v ON true
         JOIN dataset_files f ON f.dataset_version_id=v.id AND f.org_id=d.org_id
         WHERE d.org_id=$1 AND d.project_id=$2 AND d.deleted_at IS NULL AND d.eval_set
           AND f.path='content_hashes.txt' ORDER BY v.id",
    ).bind(org).bind(project).fetch_all(db.pool()).await?;
    for (i, (id, key, sha, size)) in sets.into_iter().enumerate() {
        let role = format!("against_{i}");
        inputs["against"][&role] = json!(DatasetVersionId::from_uuid(id));
        files.push(input(
            &role,
            "content_hashes.txt",
            key,
            store::hex(&sha),
            size as u64,
        ));
    }
    Ok(())
}

/// The dataset lane's completion check. The background task has the configured object
/// store. Its decision is durable, so the scheduler can retry or run on another replica.
pub struct DatasetVerifier(Db);

impl DatasetVerifier {
    /// Checks jobs registered in this database.
    pub fn new(db: Db) -> Self {
        Self(db)
    }
}

#[async_trait::async_trait]
impl CompletionVerifier for DatasetVerifier {
    fn name(&self) -> &'static str {
        "dataset_outputs"
    }

    async fn verify(&self, job: &JobRow) -> Result<Verification, SeamError> {
        if !matches!(job.kind.as_str(), "IMPORT" | "DATA_PREP") {
            return Ok(Verification::Abstain);
        }
        let decision: Option<(bool, Option<String>)> =
            sqlx::query_as("SELECT result IS NOT NULL, failure FROM dataset_work WHERE job_id=$1")
                .bind(job.id)
                .fetch_optional(self.0.pool())
                .await
                .map_err(verifier_error)?;
        let Some((done, failure)) = decision else {
            return Ok(Verification::Abstain);
        };
        if builtin::outputs(&self.0, job.org_id, job.id)
            .await
            .map_err(verifier_error)?
            .is_none()
        {
            return Ok(Verification::RetryLater(
                "The dataset job's outputs are still being saved.".into(),
            ));
        }
        if !done && failure.is_none() {
            // A RetryLater verdict would requeue the finished attempt in P17.
            return Err(SeamError::new(
                "datasets",
                "The dataset job's registered outputs are still being checked.",
            ));
        }
        Ok(if let Some(reason) = failure {
            Verification::Failed(Rejection::failure(
                FailureCode::class_only(FailureClass::VerificationFailed),
                reason,
            ))
        } else {
            Verification::Verified
        })
    }
}

fn verifier_error(error: sqlx::Error) -> SeamError {
    SeamError::new("datasets", error.to_string())
}

#[derive(sqlx::FromRow)]
struct Pending {
    job_id: Uuid,
    org_id: Uuid,
    project_id: Uuid,
    version_id: Uuid,
    schema_id: String,
    purpose: String,
    state: String,
    state_version: i64,
    state_reason: Option<Value>,
    created_by: String,
}

enum Decision {
    Version(Checked),
    FollowUp {
        source: Value,
        inputs: Value,
        files: Vec<InputFile>,
    },
    Report(Value),
    Fail(String),
}

async fn inspect(svc: &Datasets, p: &Pending) -> Result<Option<Decision>, OutputsError> {
    if p.state != "FINALIZING" {
        let reason = p
            .state_reason
            .as_ref()
            .and_then(|v| v.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("The dataset job stopped before its outputs were saved.");
        return Ok(Some(Decision::Fail(reason.chars().take(2000).collect())));
    }
    let Some(raw) = builtin::outputs(&svc.db, p.org_id, p.job_id)
        .await
        .map_err(|e| OutputsError::Transient(e.to_string()))?
    else {
        return Ok(None);
    };
    let outputs = Outputs::new(raw.artifact_id, &raw.manifest)?;
    let st = svc
        .store
        .org_store(&storage_org(p.org_id))
        .await
        .map_err(|e| OutputsError::Transient(e.to_string()))?;
    let decision = match p.purpose.as_str() {
        "prepare" => Decision::Version(store::read_outputs(&st, &outputs, &p.schema_id).await?),
        "fetch" => {
            let source = outputs.json(&st, "import.json").await?;
            let files = artifact_inputs(&st, &raw.manifest, "source", "files/")?;
            if files.is_empty()
                || source["source"]
                    .as_str()
                    .is_none_or(|s| !matches!(s, "hub" | "remote"))
                || (source["source"] == "hub"
                    && source["sha"]
                        .as_str()
                        .is_none_or(|s| s.len() != 40 || !s.bytes().all(|b| b.is_ascii_hexdigit())))
            {
                return Err(OutputsError::Broken(
                    "The import did not report a dataset snapshot.".into(),
                ));
            }
            let mut inputs = json!({"schema_id": p.schema_id});
            let mut files = files;
            eval_inputs(&svc.db, (p.org_id, p.project_id), &mut inputs, &mut files)
                .await
                .map_err(|e| OutputsError::Transient(e.to_string()))?;
            // Keep the pinned revision and checked inventory. The remote URL can carry credentials.
            let mut source = source;
            if let Some(doc) = source.as_object_mut() {
                doc.remove("url");
            }
            Decision::FollowUp {
                source,
                inputs,
                files,
            }
        }
        "hub_push" => Decision::Report(outputs.json(&st, "hub_push.json").await?),
        "tokenize" => Decision::Report(outputs.json(&st, "token_preview.json").await?),
        "transform_preview" => Decision::Report(outputs.json(&st, "preview.json").await?),
        "jsonl" => {
            let files = artifact_inputs(&st, &raw.manifest, "export", "export/")?;
            if files.is_empty() || files.iter().any(|f| !f.local_path.ends_with(".jsonl")) {
                return Err(OutputsError::Broken(
                    "The job did not write JSONL export files.".into(),
                ));
            }
            Decision::Report(json!(files))
        }
        _ => return Err(OutputsError::Broken("Unknown dataset job purpose.".into())),
    };
    Ok(Some(decision))
}

pub(super) fn artifact_inputs(
    st: &mrl_storage::Store,
    manifest: &Value,
    role: &str,
    prefix: &str,
) -> Result<Vec<InputFile>, OutputsError> {
    let manifest = mrl_storage::ArtifactManifest::from_json(
        &serde_json::to_vec(manifest).map_err(|e| OutputsError::Broken(e.to_string()))?,
    )
    .map_err(|e| OutputsError::Broken(e.to_string()))?;
    manifest
        .files
        .into_iter()
        .filter(|f| f.path.starts_with(prefix))
        .map(|f| {
            let path = &f.path[prefix.len()..];
            let key = st
                .keys()
                .blob(&f.sha256)
                .map_err(|e| OutputsError::Broken(e.to_string()))?;
            Ok(input(role, path, key, f.sha256, f.size))
        })
        .collect()
}

async fn save(svc: &Datasets, p: &Pending, decision: Decision) -> Result<(), JobsError> {
    let mut tx = svc.db.begin().await?;
    let Some(job_row) = JobTransitions::lock(&mut tx, p.org_id, p.job_id).await? else {
        return Ok(());
    };
    if job_row.state != p.state || job_row.state_version != p.state_version {
        return Ok(());
    }
    let pending = sqlx::query_scalar::<_, bool>(
        "SELECT result IS NULL AND failure IS NULL FROM dataset_work WHERE job_id=$1 FOR UPDATE",
    )
    .bind(p.job_id)
    .fetch_one(&mut *tx)
    .await?;
    if !pending {
        return Ok(());
    }
    let audit = PgAuditSink::new(svc.db.clone());
    let (value, failure) = match decision {
        Decision::FollowUp {
            source,
            inputs,
            files,
        } => {
            let next = queue(
                &mut tx,
                p.version_id,
                "prepare",
                job(
                    (p.org_id, p.project_id),
                    p.version_id,
                    ("DATA_PREP", "data_prep"),
                    &p.created_by,
                    inputs,
                    files.clone(),
                    vec![],
                ),
            )
            .await?;
            originals(&mut tx, p.version_id, p.org_id, &files).await?;
            sqlx::query(
                "UPDATE dataset_versions SET source_ref=source_ref || $1, state='VALIDATING', job_id=$2
                 WHERE id=$3 AND job_id=$4",
            ).bind(source).bind(next).bind(p.version_id).bind(p.job_id).execute(&mut *tx).await?;
            audit
                .record_in(
                    &mut tx,
                    AuditEvent {
                        at: time::OffsetDateTime::now_utc(),
                        actor: "system:datasets".into(),
                        org: crate::modules::jobs::scope::org_text(p.org_id),
                        project: Some(
                            mrl_domain::ids::ProjectId::from_uuid(p.project_id).to_string(),
                        ),
                        action: "dataset_version.imported".into(),
                        target: DatasetVersionId::from_uuid(p.version_id).to_string(),
                        outcome: AuditOutcome::Success,
                        trace_id: p.job_id.simple().to_string(),
                    },
                )
                .await?;
            (Some(json!({})), None)
        }
        Decision::Version(checked) => {
            store::record_version(&mut tx, &audit, p.version_id, &checked).await?;
            match checked {
                Checked::Invalid { code, reason, .. }
                    if code == super::codes::OUTPUTS_BROKEN.to_string() =>
                {
                    (None, Some(reason))
                }
                _ => (Some(json!({})), None),
            }
        }
        Decision::Report(value) => (Some(value), None),
        Decision::Fail(reason) => {
            if matches!(p.purpose.as_str(), "prepare" | "fetch") {
                store::record_version(
                    &mut tx,
                    &audit,
                    p.version_id,
                    &Checked::broken(&reason, None),
                )
                .await?;
            }
            (None, Some(reason))
        }
    };
    sqlx::query("UPDATE dataset_work SET result=$2, failure=$3 WHERE job_id=$1")
        .bind(p.job_id)
        .bind(value)
        .bind(failure)
        .execute(&mut *tx)
        .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(())
}

pub(super) async fn originals(
    tx: &mut Tx,
    id: Uuid,
    org: Uuid,
    files: &[InputFile],
) -> Result<(), JobsError> {
    for file in files.iter().filter(|f| f.name == "source") {
        let path = file
            .local_path
            .strip_prefix("source/")
            .ok_or_else(|| seam("Invalid original path."))?;
        sqlx::query(
            "INSERT INTO dataset_files
            (dataset_version_id, org_id, path, role, object_key, sha256, size_bytes)
            VALUES ($1,$2,$3,'original',$4,decode($5,'hex'),$6)",
        )
        .bind(id)
        .bind(org)
        .bind(format!("originals/{path}"))
        .bind(&file.key)
        .bind(&file.sha256)
        .bind(file.size as i64)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(super) async fn check_pending(svc: &Datasets) -> Result<(), JobsError> {
    let pending: Vec<Pending> = sqlx::query_as(
        "SELECT w.job_id, j.org_id, j.project_id, w.version_id, v.schema_id, w.purpose,
                j.state, j.state_version, j.state_reason, j.created_by
         FROM dataset_work w JOIN jobs j ON j.id=w.job_id
         JOIN dataset_versions v ON v.id=w.version_id AND v.org_id=j.org_id
         WHERE w.result IS NULL AND w.failure IS NULL
           AND (j.state IN ('FAILED','CANCELLED','REJECTED') OR j.state='FINALIZING'
                AND EXISTS(SELECT 1 FROM builtin_jobs b WHERE b.job_id=j.id AND b.artifact_id IS NOT NULL AND b.upload_attempt_id=j.current_attempt_id))
         ORDER BY j.id",
    ).fetch_all(svc.db.pool()).await?;
    for p in pending {
        let decision = match inspect(svc, &p).await {
            Ok(Some(decision)) => decision,
            Ok(None) => continue,
            Err(OutputsError::Broken(reason)) => Decision::Fail(reason),
            Err(OutputsError::Transient(reason)) => {
                tracing::warn!(job = %p.job_id, %reason, "dataset outputs will be checked again");
                continue;
            }
        };
        if let Err(error) = save(svc, &p, decision).await {
            tracing::warn!(job = %p.job_id, %error, "dataset decision will be retried");
        }
    }
    Ok(())
}

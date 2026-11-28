//! Dataset version and derived job submissions.

use super::{api, store, work, Datasets};
use crate::app::AppState;
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::modules::artifacts::{self, Idempotency, InputFile};
use crate::modules::builtin::BuiltinGrant;
use crate::modules::checkpoints::service::storage_org;
use crate::modules::jobs::{scope, JobsError};
use crate::seams::RequestContext;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use mrl_domain::ids::{ArtifactId, DatasetVersionId, JobId, ProjectId, SecretId, UploadId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

fn bad(path: &str, reason: &str) -> JobsError {
    ApiError::validation(vec![FieldError::new(
        path,
        ErrorCode::FieldValue,
        reason,
        "Choose a supported value and try again.",
    )])
    .into()
}

fn schema_ok(id: &str) -> bool {
    matches!(
        id,
        "pretrain.text.v1"
            | "sft.messages.v1"
            | "sft.prompt_completion.v1"
            | "pref.pairwise.v1"
            | "pref.unpaired.v1"
            | "rm.pairwise.v1"
            | "rm.pointwise.v1"
            | "rm.process.v1"
            | "rl.prompt.v1"
            | "rl.rollout.v1"
            | "agent.multiturn.v1"
            | "distill.v1"
            | "eval.v1"
            | "mm.messages.v1"
    )
}

/// The source for a new dataset version.
#[derive(Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum DatasetVersionSource {
    /// One or more completed uploads in this project.
    Upload {
        /// Canonical dataset schema.
        schema_id: String,
        /// Completed upload IDs.
        uploads: Vec<String>,
    },
    /// A pinned snapshot of a Hugging Face dataset repository.
    Hub {
        /// Canonical dataset schema.
        schema_id: String,
        /// Hub repository, owner/name.
        repo: String,
        /// Branch, tag or commit. Defaults to main.
        revision: Option<String>,
        /// Optional repository config.
        config: Option<String>,
        /// Optional split.
        split: Option<String>,
        /// Project or org secret with hub_import use.
        secret_id: Option<String>,
    },
    /// A snapshot of an HTTPS file.
    Remote {
        /// Canonical dataset schema.
        schema_id: String,
        /// HTTPS file URL.
        url: String,
    },
    /// A declarative transformation of a version in this project.
    Transform {
        /// Parent dataset version ID.
        parent_version_id: String,
        /// The transform/v1 specification.
        #[schema(value_type = Object)]
        transform_spec: Value,
    },
}

/// Request to preview a transform against a READY version.
#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetTransformPreviewRequest {
    /// The transform/v1 specification.
    #[schema(value_type = Object)]
    pub transform_spec: Value,
}

/// Request to tokenize a dataset using a registered tokenizer artifact.
#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetTokenizeRequest {
    /// Committed tokenizer artifact in this project.
    pub tokenizer_artifact_id: String,
    /// Context window size.
    pub context_length: Option<u32>,
    /// Maximum records to inspect, capped at 2000.
    pub max_records: Option<u32>,
}

/// A queued dataset operation.
#[derive(Serialize, utoipa::ToSchema)]
pub struct DatasetWorkAccepted {
    /// ID to poll on this version.
    job_id: String,
}

/// What a queued preview or export job produced.
#[derive(Serialize, utoipa::ToSchema)]
pub struct DatasetWorkResult {
    /// Current job state.
    state: String,
    /// Terminal failure, if any.
    failure: Option<String>,
    /// Tokenization or transform preview report.
    #[schema(value_type = Option<Object>)]
    report: Option<Value>,
    /// Download links for a completed JSONL export.
    export: Option<api::DatasetExport>,
}

async fn uploaded(
    state: &AppState,
    svc: &Datasets,
    ctx: &RequestContext,
    owner: (Uuid, Uuid),
    ids: &[String],
) -> Result<Vec<InputFile>, JobsError> {
    if ids.is_empty() || ids.len() > 1000 {
        return Err(bad(
            "/uploads",
            "Give between 1 and 1000 completed uploads.",
        ));
    }
    let org = storage_org(owner.0);
    let caller_org = artifacts::org_of(ctx).map_err(work::seam)?;
    let local = ctx.principal.kind == crate::seams::PrincipalKind::LocalOwner;
    let mut files = Vec::new();
    let mut names = HashSet::new();
    let mut seen = HashSet::new();
    for raw in ids {
        let id: UploadId = scope::parse_id(raw, "upload")?;
        if !seen.insert(id.as_uuid()) {
            return Err(bad("/uploads", "Each upload may appear only once."));
        }
        let project = ProjectId::from_uuid(owner.1).to_string();
        let row: Option<(String, String, i64, String, String, Option<String>)> = sqlx::query_as(
            "SELECT org_id, filename, size, sha256, state, blob_key FROM uploads
             WHERE id=$1 AND project_id=$2 AND (org_id=$3 OR ($4 AND org_id=$5))",
        )
        .bind(id.as_uuid())
        .bind(&project)
        .bind(&org)
        .bind(local)
        .bind(&caller_org)
        .fetch_optional(svc.db.pool())
        .await?;
        let (from, name, size, sha, status, key) =
            row.ok_or_else(|| JobsError::not_found("upload"))?;
        scope::authorize(
            state,
            ctx,
            "uploads.write",
            &scope::resource("upload", Some(raw.clone()), owner.0, owner.1),
        )
        .await?;
        let key = key
            .filter(|_| status == "COMPLETED")
            .ok_or_else(|| bad("/uploads", "Finish each upload before creating a version."))?;
        let st = svc.store.org_store(&org).await.map_err(work::seam)?;
        let target = st.keys().blob(&sha).map_err(work::seam)?;
        // Check the name using the storage path parser used by P19.
        st.keys()
            .join(st.keys().prefix(), &name)
            .map_err(|_| bad("/uploads", "An upload has an unsafe filename."))?;
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(bad("/uploads", "Use distinct upload filenames."));
        }
        let size = u64::try_from(size).map_err(work::seam)?;
        if from != org {
            copy_local(svc, &from, &st, &key, &target, &sha, size).await?;
        } else if key != target {
            return Err(work::seam("The upload does not point to its checked blob."));
        }
        files.push(work::input("source", &name, target, sha, size));
    }
    Ok(files)
}

async fn copy_local(
    svc: &Datasets,
    from: &str,
    target: &mrl_storage::Store,
    key: &str,
    destination: &str,
    sha: &str,
    size: u64,
) -> Result<(), JobsError> {
    svc.store
        .intend(target.keys().org_id(), destination, Some(sha))
        .await
        .map_err(work::seam)?;
    if crate::modules::uploads::blob_intact(target, destination, size, sha)
        .await
        .map_err(work::seam)?
    {
        return Ok(());
    }
    let source = svc.store.org_store(from).await.map_err(work::seam)?;
    let scratch = svc.store.scratch_dir().map_err(work::seam)?;
    let path = scratch.path().join("upload");
    let mut file = tokio::fs::File::create(&path).await.map_err(work::seam)?;
    let mut start = 0;
    while start < size {
        let end = (start + 4 * 1024 * 1024).min(size);
        let bytes = source
            .get_range(key, start..end)
            .await
            .map_err(work::seam)?;
        file.write_all(&bytes).await.map_err(work::seam)?;
        start = end;
    }
    file.flush().await.map_err(work::seam)?;
    drop(file);
    if mrl_storage::sha256_file(&path).await.map_err(work::seam)? != (size, sha.to_owned()) {
        return Err(work::seam(
            "The completed upload no longer matches its digest.",
        ));
    }
    svc.store
        .put_blob(target, &path, destination, size, sha)
        .await
        .map_err(work::seam)
}

async fn hub_grant(
    state: &AppState,
    svc: &Datasets,
    ctx: &RequestContext,
    owner: (Uuid, Uuid),
    secret: &str,
    allowed_use: &str,
) -> Result<BuiltinGrant, JobsError> {
    let id: SecretId = scope::parse_id(secret, "secret")?;
    let row: Option<(Option<String>, Vec<String>)> = sqlx::query_as(
        "SELECT project_id, allowed_uses FROM secrets_metadata WHERE org_id=$1 AND id=$2
         AND NOT deleting AND (project_id IS NULL OR project_id=$3)",
    )
    .bind(storage_org(owner.0))
    .bind(id.as_uuid())
    .bind(ProjectId::from_uuid(owner.1).to_string())
    .fetch_optional(svc.db.pool())
    .await?;
    let (project, uses) = row.ok_or_else(|| JobsError::not_found("secret"))?;
    scope::authorize(
        state,
        ctx,
        "secrets.manage",
        &crate::seams::Resource {
            kind: "secret".into(),
            id: Some(secret.into()),
            org: scope::org_text(owner.0),
            project,
        },
    )
    .await?;
    if !uses.iter().any(|u| u == allowed_use) {
        return Err(bad(
            "/secret_id",
            &format!("The secret must allow {allowed_use}."),
        ));
    }
    Ok(BuiltinGrant {
        secret: id,
        allowed_use: allowed_use.into(),
        name: "HF_TOKEN".into(),
    })
}

pub(super) struct Prepared {
    schema: String,
    source: &'static str,
    provenance: Value,
    parent: Option<Uuid>,
    spec: Option<Value>,
    inputs: Value,
    files: Vec<InputFile>,
    grants: Vec<BuiltinGrant>,
}

fn transform_spec(spec: &Value) -> Result<&str, JobsError> {
    scope::no_nul(spec)?;
    spec.get("to")
        .and_then(Value::as_str)
        .filter(|id| schema_ok(id))
        .filter(|_| {
            spec["schema_version"] == "transform/v1"
                && spec["steps"].as_array().is_some_and(|s| !s.is_empty())
        })
        .ok_or_else(|| {
            bad(
                "/transform_spec",
                "Give a transform/v1 spec with a dataset schema and steps.",
            )
        })
}

async fn prepare(
    state: &AppState,
    svc: &Datasets,
    ctx: &RequestContext,
    owner: (Uuid, Uuid),
    source: DatasetVersionSource,
) -> Result<Prepared, JobsError> {
    let mut plan = match source {
        DatasetVersionSource::Upload { schema_id, uploads } => Prepared {
            schema: schema_id.clone(),
            source: "upload",
            provenance: json!({"uploads": uploads}),
            parent: None,
            spec: None,
            inputs: json!({"schema_id": schema_id}),
            files: uploaded(state, svc, ctx, owner, &uploads).await?,
            grants: vec![],
        },
        DatasetVersionSource::Hub {
            schema_id,
            repo,
            revision,
            config,
            split,
            secret_id,
        } => {
            let revision = revision.unwrap_or_else(|| "main".into());
            let mut inputs = json!({"source":"hub","repo":repo,"revision":revision,"config":config,"split":split});
            let mut grants = vec![];
            if let Some(secret) = secret_id {
                grants.push(hub_grant(state, svc, ctx, owner, &secret, "hub_import").await?);
                inputs["token_env"] = json!("MRL_CREDENTIAL_HF_TOKEN");
            }
            Prepared {
                schema: schema_id,
                source: "hub",
                provenance: json!({"repo":repo,"revision":revision}),
                parent: None,
                spec: None,
                inputs,
                files: vec![],
                grants,
            }
        }
        DatasetVersionSource::Remote { schema_id, url } => {
            let mut safe = reqwest::Url::parse(&url)
                .ok()
                .filter(|u| {
                    u.scheme() == "https"
                        && u.host_str().is_some()
                        && u.username().is_empty()
                        && u.password().is_none()
                })
                .ok_or_else(|| {
                    bad(
                        "/url",
                        "Give an HTTPS file URL without a user name or password.",
                    )
                })?;
            safe.set_query(None);
            safe.set_fragment(None);
            Prepared {
                schema: schema_id,
                source: "remote_ref",
                provenance: json!({"url":safe.as_str()}),
                parent: None,
                spec: None,
                inputs: json!({"source":"remote","url":url}),
                files: vec![],
                grants: vec![],
            }
        }
        DatasetVersionSource::Transform {
            parent_version_id,
            transform_spec: spec,
        } => {
            let parent =
                api::authorized_version(state, &svc.db, ctx, &parent_version_id, "datasets.read")
                    .await?;
            if (parent.org_id, parent.project_id) != owner {
                return Err(JobsError::not_found("dataset version"));
            }
            api::ready(&parent)?;
            Prepared {
                schema: transform_spec(&spec)?.into(),
                source: "transform",
                provenance: json!({"parent_version_id":parent_version_id}),
                parent: Some(parent.id),
                spec: Some(spec.clone()),
                inputs: json!({"spec":spec}),
                files: work::dataset_inputs(&svc.db, parent.id, "parent").await?,
                grants: vec![],
            }
        }
    };
    if !schema_ok(&plan.schema) {
        return Err(bad("/schema_id", "Choose a canonical dataset schema."));
    }
    if plan.source == "upload" {
        plan.provenance["upload_sha256"] = json!(plan
            .files
            .iter()
            .map(|file| &file.sha256)
            .collect::<Vec<_>>());
    }
    if matches!(plan.source, "upload" | "transform") {
        work::eval_inputs(&svc.db, owner, &mut plan.inputs, &mut plan.files).await?;
    }
    Ok(plan)
}

/// Creates a version and queues its first INT-6 job.
#[utoipa::path(post, path="/datasets/{ds}/versions", operation_id="create_dataset_version", tag="datasets",
    params(("ds" = String, Path, description="Dataset ID")), request_body=DatasetVersionSource,
    responses((status=202, body=api::DatasetVersionInfo), (status=422, body=crate::error::Problem)))]
pub async fn create_version(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path(ds): Path<String>,
    headers: HeaderMap,
    Json(source): Json<DatasetVersionSource>,
) -> Result<impl IntoResponse, JobsError> {
    let dataset = api::authorized_dataset(&state, &svc.db, &ctx, &ds, "datasets.import").await?;
    let owner = (dataset.org_id, dataset.project_id);
    let idem = Idempotency::of(&headers, &source);
    // Check a replay before reading external inputs again, then recheck under the dataset lock.
    if let Some(key) = &idem {
        let existing = {
            let mut conn = svc.db.pool().acquire().await?;
            earlier(&mut conn, dataset.id, &ctx.principal.id, key).await?
        };
        if let Some(id) = existing {
            return accepted(&svc, id).await;
        }
    }
    scope::no_nul(&serde_json::to_value(&source).map_err(work::seam)?)?;
    let p = prepare(&state, &svc, &ctx, owner, source).await?;
    let mut tx = svc.db.begin().await?;
    let (seq,): (i32,) = sqlx::query_as(
        "SELECT next_seq FROM datasets WHERE id=$1 AND deleted_at IS NULL FOR UPDATE",
    )
    .bind(dataset.id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| JobsError::not_found("dataset"))?;
    if let Some(key) = &idem {
        if let Some(id) = earlier(&mut tx, dataset.id, &ctx.principal.id, key).await? {
            tx.rollback().await.map_err(crate::db::DbError::from)?;
            return accepted(&svc, id).await;
        }
    }
    let id = state.seams.ids.new_uuid();
    let (purpose, task, status) = match p.source {
        "hub" | "remote_ref" => ("fetch", ("IMPORT", "import_dataset"), "INGESTING"),
        "transform" => ("prepare", ("DATA_PREP", "transform"), "VALIDATING"),
        _ => ("prepare", ("DATA_PREP", "data_prep"), "VALIDATING"),
    };
    sqlx::query(
        "INSERT INTO dataset_versions (id,org_id,dataset_id,seq,schema_id,state,source_kind,
            source_ref,parent_version_id,transform_spec,created_by,request_key,request_hash)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
    )
    .bind(id)
    .bind(owner.0)
    .bind(dataset.id)
    .bind(seq)
    .bind(p.schema)
    .bind(status)
    .bind(p.source)
    .bind(p.provenance)
    .bind(p.parent)
    .bind(p.spec)
    .bind(&ctx.principal.id)
    .bind(idem.as_ref().map(|k| &k.key))
    .bind(idem.as_ref().map(|k| &k.request_sha256))
    .execute(&mut *tx)
    .await?;
    work::originals(&mut tx, id, owner.0, &p.files).await?;
    let job = work::queue(
        &mut tx,
        id,
        purpose,
        work::job(
            owner,
            id,
            task,
            &ctx.principal.id,
            p.inputs,
            p.files,
            p.grants,
        ),
    )
    .await?;
    sqlx::query("UPDATE dataset_versions SET job_id=$1 WHERE id=$2")
        .bind(job)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE datasets SET next_seq=next_seq+1 WHERE id=$1")
        .bind(dataset.id)
        .execute(&mut *tx)
        .await?;
    scope::audit(
        &mut tx,
        &state,
        &ctx,
        owner,
        "datasets.version.create",
        DatasetVersionId::from_uuid(id).to_string(),
    )
    .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    accepted(&svc, id).await
}

async fn earlier(
    conn: &mut sqlx::PgConnection,
    dataset: Uuid,
    actor: &str,
    key: &Idempotency,
) -> Result<Option<Uuid>, JobsError> {
    let row: Option<(Uuid, String)> = sqlx::query_as(
        "SELECT id,request_hash FROM dataset_versions WHERE dataset_id=$1 AND created_by=$2 AND request_key=$3",
    ).bind(dataset).bind(actor).bind(&key.key).fetch_optional(conn).await?;
    match row {
        Some((_, hash)) if hash != key.request_sha256 => Err(ApiError::new(
            ErrorCode::IdempotencyMismatch,
            "This key was already used for a different dataset version request.",
        )
        .into()),
        other => Ok(other.map(|(id, _)| id)),
    }
}

async fn accepted(
    svc: &Datasets,
    id: Uuid,
) -> Result<(StatusCode, Json<api::DatasetVersionInfo>), JobsError> {
    let row = store::version(&svc.db, id)
        .await?
        .ok_or_else(|| JobsError::not_found("dataset version"))?;
    Ok((StatusCode::ACCEPTED, Json(api::version_view(&row, None))))
}

async fn derive(
    state: &AppState,
    svc: &Datasets,
    ctx: &RequestContext,
    v: &store::VersionRow,
    operation: (&str, &str, &str),
    inputs: Value,
    files: Vec<InputFile>,
) -> Result<Response, JobsError> {
    let owner = (v.org_id, v.project_id);
    let job = work::job(
        owner,
        v.id,
        ("DATA_PREP", operation.1),
        &ctx.principal.id,
        inputs,
        files,
        vec![],
    );
    derive_job(state, svc, ctx, v, operation.0, operation.2, job).await
}

async fn derive_job(
    state: &AppState,
    svc: &Datasets,
    ctx: &RequestContext,
    v: &store::VersionRow,
    purpose: &str,
    action: &str,
    job: crate::modules::builtin::BuiltinJob,
) -> Result<Response, JobsError> {
    let owner = (v.org_id, v.project_id);
    let mut tx = svc.db.begin().await?;
    let id = work::queue(&mut tx, v.id, purpose, job).await?;
    scope::audit(
        &mut tx,
        state,
        ctx,
        owner,
        action,
        JobId::from_uuid(id).to_string(),
    )
    .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(DatasetWorkAccepted {
            job_id: JobId::from_uuid(id).to_string(),
        }),
    )
        .into_response())
}

/// Queues a preview on the first 20 rows.
#[utoipa::path(post, path="/dataset-versions/{dsv}:transform-preview", operation_id="preview_dataset_transform", tag="datasets",
    params(("dsv"=String,Path,description="Dataset version ID")), request_body=DatasetTransformPreviewRequest,
    responses((status=202, body=DatasetWorkAccepted)))]
pub async fn transform_preview(
    state: &AppState,
    svc: &Datasets,
    ctx: &RequestContext,
    dsv: &str,
    req: DatasetTransformPreviewRequest,
) -> Result<Response, JobsError> {
    let v = api::authorized_version(state, &svc.db, ctx, dsv, "datasets.import").await?;
    api::ready(&v)?;
    transform_spec(&req.transform_spec)?;
    let files = work::dataset_inputs(&svc.db, v.id, "parent").await?;
    derive(
        state,
        svc,
        ctx,
        &v,
        (
            "transform_preview",
            "transform",
            "datasets.transform_preview",
        ),
        json!({"spec":req.transform_spec,"preview":true}),
        files,
    )
    .await
}

/// Queues a tokenization histogram.
#[utoipa::path(post, path="/dataset-versions/{dsv}:tokenize-preview", operation_id="tokenize_dataset_preview", tag="datasets",
    params(("dsv"=String,Path,description="Dataset version ID")), request_body=DatasetTokenizeRequest,
    responses((status=202, body=DatasetWorkAccepted)))]
pub async fn tokenize(
    state: &AppState,
    svc: &Datasets,
    ctx: &RequestContext,
    dsv: &str,
    req: DatasetTokenizeRequest,
) -> Result<Response, JobsError> {
    let v = api::authorized_version(state, &svc.db, ctx, dsv, "datasets.read").await?;
    api::ready(&v)?;
    let id: ArtifactId = scope::parse_id(&req.tokenizer_artifact_id, "artifact")?;
    let org = storage_org(v.org_id);
    let project = ProjectId::from_uuid(v.project_id).to_string();
    let artifact = svc
        .store
        .get(&org, id.as_uuid())
        .await
        .map_err(work::seam)?
        .filter(|a| a.state == "COMMITTED" && a.project_id.as_deref() == Some(&project))
        .ok_or_else(|| JobsError::not_found("artifact"))?;
    scope::authorize(state, ctx, "lineage.read", &artifact.resource(&org)).await?;
    let st = svc.store.org_store(&org).await.map_err(work::seam)?;
    let mut tokenizer = work::artifact_inputs(&st, &artifact.manifest, "tokenizer", "")
        .map_err(work::seam)?
        .into_iter()
        .filter(|f| {
            f.local_path.strip_prefix("tokenizer/").is_some_and(|path| {
                matches!(
                    path,
                    "tokenizer.json"
                        | "tokenizer_config.json"
                        | "chat_template.jinja"
                        | "tokenizer/tokenizer.json"
                        | "tokenizer/tokenizer_config.json"
                        | "tokenizer/chat_template.jinja"
                )
            })
        })
        .collect::<Vec<_>>();
    for f in &mut tokenizer {
        if let Some(path) = f.local_path.strip_prefix("tokenizer/tokenizer/") {
            f.local_path = format!("tokenizer/{path}");
        }
    }
    if !tokenizer
        .iter()
        .any(|f| f.local_path == "tokenizer/tokenizer.json")
    {
        return Err(bad(
            "/tokenizer_artifact_id",
            "The artifact needs a tokenizer.json file.",
        ));
    }
    let mut files = work::dataset_inputs(&svc.db, v.id, "dataset").await?;
    files.extend(tokenizer);
    if req.context_length == Some(0) || req.max_records == Some(0) {
        return Err(bad("/max_records", "Choose a positive limit."));
    }
    derive(
        state,
        svc,
        ctx,
        &v,
        ("tokenize", "data_prep", "datasets.tokenize_preview"),
        json!({"mode":"tokenize","context_length":req.context_length,
            "max_records":req.max_records.unwrap_or(2000).min(2000)}),
        files,
    )
    .await
}

pub(super) async fn jsonl(
    state: &AppState,
    svc: &Datasets,
    ctx: &RequestContext,
    dsv: &str,
) -> Result<Response, JobsError> {
    let v = api::authorized_version(state, &svc.db, ctx, dsv, "datasets.export").await?;
    api::export_allowed(&v)?;
    api::ready(&v)?;
    let files = work::dataset_inputs(&svc.db, v.id, "dataset").await?;
    derive(
        state,
        svc,
        ctx,
        &v,
        ("jsonl", "materialize_dataset", "datasets.jsonl"),
        json!({"format":"jsonl"}),
        files,
    )
    .await
}

pub(super) async fn push_to_hub(
    state: &AppState,
    svc: &Datasets,
    ctx: &RequestContext,
    dsv: &str,
    req: api::DatasetExportRequest,
) -> Result<Response, JobsError> {
    let v = api::authorized_version(state, &svc.db, ctx, dsv, "datasets.export").await?;
    api::export_allowed(&v)?;
    api::ready(&v)?;
    let repo = req
        .repo
        .as_deref()
        .filter(|repo| {
            repo.split_once('/').is_some_and(|(a, b)| {
                [a, b].iter().all(|s| {
                    s.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
                        && s.bytes()
                            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
                })
            })
        })
        .ok_or_else(|| bad("/repo", "Give a Hub repository as owner/name."))?;
    let secret = req
        .secret_id
        .as_deref()
        .ok_or_else(|| bad("/secret_id", "Choose a hub_push credential."))?;
    let owner = (v.org_id, v.project_id);
    let grant = hub_grant(state, svc, ctx, owner, secret, "hub_push").await?;
    let files = work::dataset_inputs(&svc.db, v.id, "dataset").await?;
    let job = work::job(
        owner,
        v.id,
        ("IMPORT", "push_dataset_to_hub"),
        &ctx.principal.id,
        json!({"repo":repo, "version_id":dsv}),
        files,
        vec![grant],
    );
    derive_job(state, svc, ctx, &v, "hub_push", "datasets.hub_push", job).await
}

/// Reads a version-scoped preview or export job result.
#[utoipa::path(get, path="/dataset-versions/{dsv}/work/{job}", operation_id="get_dataset_work", tag="datasets",
    params(("dsv"=String,Path,description="Dataset version ID"),("job"=String,Path,description="Dataset job ID")),
    responses((status=200, body=DatasetWorkResult), (status=404, body=crate::error::Problem)))]
pub async fn result(
    State(state): State<AppState>,
    Extension(svc): Extension<Arc<Datasets>>,
    ctx: RequestContext,
    Path((dsv, job)): Path<(String, String)>,
) -> Result<Json<DatasetWorkResult>, JobsError> {
    let v = api::authorized_version(&state, &svc.db, &ctx, &dsv, "datasets.read").await?;
    let id: JobId = scope::parse_id(&job, "dataset operation")?;
    let row: Option<(String, Option<Value>, Option<String>, String)> = sqlx::query_as(
        "SELECT w.purpose,w.result,w.failure,j.state FROM dataset_work w JOIN jobs j ON j.id=w.job_id
         WHERE w.job_id=$1 AND w.version_id=$2 AND j.org_id=$3 AND j.project_id=$4
           AND w.purpose IN ('tokenize','transform_preview','jsonl','hub_push')",
    ).bind(id.as_uuid()).bind(v.id).bind(v.org_id).bind(v.project_id).fetch_optional(svc.db.pool()).await?;
    let (purpose, mut report, failure, status) =
        row.ok_or_else(|| JobsError::not_found("dataset operation"))?;
    let mut export = None;
    if matches!(purpose.as_str(), "jsonl" | "hub_push") {
        let current =
            api::authorized_version(&state, &svc.db, &ctx, &dsv, "datasets.export").await?;
        api::export_allowed(&current)?;
        if purpose == "jsonl" {
            if let Some(data) = report.take().filter(|_| status == "COMPLETED") {
                let files = serde_json::from_value::<Vec<InputFile>>(data).map_err(work::seam)?;
                export =
                    Some(api::export_links(&state, &svc, &ctx, &current, "jsonl", files).await?);
            }
        }
    }
    Ok(Json(DatasetWorkResult {
        state: status,
        failure,
        report,
        export,
    }))
}

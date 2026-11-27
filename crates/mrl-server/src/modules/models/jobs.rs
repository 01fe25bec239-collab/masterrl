//! The model jobs: `POST /projects/{prj}/models:import`, `POST /model-versions/{mv}:merge-adapter`,
//! `POST /model-versions/{mv}:export` and `POST /checkpoints/{ckpt}:promote` (P23's route calls
//! [`promote`]). Each makes a built-in job (INT-6) in one transaction with its audit row;
//! imports, merges and promotions also make the IMPORTING version the job fills in (see
//! [`super::outputs`]).
//!
//! Sources of an import: `hub` (a repository revision, resolved to its commit by the job; a
//! token secret is handed over as a credential, never as an input), `upload` (finished
//! uploads, one per file) and `node_path` (a folder on a machine: needs `compute.manage`
//! too, since it reads that machine's disk). Secrets for the Hub and export targets must
//! allow the use `model_download` (imports) or `model_export` (exports).

use super::api::{authorized_version, version_view, ModelVersionInfo};
use super::store::{self, hex, NewVersion, VersionRow};
use super::submission::Submission;
use super::{codes, Models};
use crate::app::AppState;
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::modules::artifacts::InputFile;
use crate::modules::builtin::{self, BuiltinGrant, BuiltinJob};
use crate::modules::checkpoints::service::CheckpointRow;
use crate::modules::checkpoints::CheckpointService;
use crate::modules::jobs::{scope, JobsError};
use crate::modules::runs::experiments::project_scope;
use crate::seams::RequestContext;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use mrl_domain::ids::{JobId, ModelVersionId, NodeId, ProjectId, SecretId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

/// Most files one import may name.
const MAX_FILES: usize = 512;
/// File names never staged into a model (pickled Python objects).
const PICKLE: [&str; 6] = [".bin", ".pt", ".pth", ".ckpt", ".pkl", ".pickle"];
/// The only export format of this release.
const EXPORT_FORMAT: &str = "hf-safetensors";

/// Where an import's files come from.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ImportSource {
    /// A Hugging Face Hub repository at a branch, tag or commit.
    Hub {
        /// `org/name`.
        repo: String,
        /// Branch, tag or commit (default `main`); the version records the commit.
        #[serde(default)]
        revision: Option<String>,
        /// A Hub token secret (`sec_…`) for private or gated repositories.
        #[serde(default)]
        secret_id: Option<String>,
    },
    /// Finished uploads: path in the model folder → `up_…`.
    Upload {
        /// The files.
        files: BTreeMap<String, String>,
    },
    /// A folder on a machine.
    NodePath {
        /// Absolute folder path on the machine.
        path: String,
        /// The machine (`node_…`); any machine when left out.
        #[serde(default)]
        node_id: Option<String>,
    },
}

/// `POST /projects/{prj}/models:import` body.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ImportModel {
    /// The model (created when missing).
    pub name: String,
    /// Where the files come from.
    pub source: ImportSource,
    /// For LoRA adapters: the READY base version (`mv_…`) they were trained on.
    #[serde(default)]
    pub base_model_version_id: Option<String>,
    /// Convert pickle-only weights (refused in this release: MRL-MODEL-260).
    #[serde(default)]
    pub convert_pickle: bool,
}

/// `POST /model-versions/{mv}:merge-adapter` body.
#[derive(Debug, Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct MergeAdapter {
    /// The model to add the merged version to (default: the adapter's model).
    #[serde(default)]
    pub name: Option<String>,
    /// `auto` (keep each tensor's type), `float32`, `float16` or `bfloat16`.
    #[serde(default)]
    pub dtype: Option<String>,
}

/// Where an export goes.
#[derive(Debug, Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ExportTarget {
    /// `artifact` (default: download it from the app), `s3` or `hf_hub`.
    #[serde(default)]
    pub kind: Option<String>,
    /// `s3://bucket/prefix` (s3).
    #[serde(default)]
    pub url: Option<String>,
    /// S3-compatible endpoint (s3; default AWS).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// S3 region (s3).
    #[serde(default)]
    pub region: Option<String>,
    /// `org/name` (hf_hub).
    #[serde(default)]
    pub repo: Option<String>,
    /// Make the Hub repository private (hf_hub; default true).
    #[serde(default)]
    pub private: Option<bool>,
    /// The credentials (`sec_…`): S3 keys as JSON, or a Hub write token.
    #[serde(default)]
    pub secret_id: Option<String>,
}

/// `POST /model-versions/{mv}:export` body.
#[derive(Debug, Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ExportModel {
    /// `hf-safetensors` (the default; others answer MRL-MODEL-261 in this release).
    #[serde(default)]
    pub format: Option<String>,
    /// Where it goes.
    #[serde(default)]
    pub target: ExportTarget,
}

/// A started export.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ModelExportJob {
    /// `job_…`; its outputs are the exported folder and `export-result.json`.
    pub job_id: String,
    /// The exported version.
    pub model_version_id: String,
}

fn invalid(path: &str, message: &str) -> JobsError {
    ApiError::validation(vec![FieldError::new(
        path,
        ErrorCode::FieldValue,
        message,
        "Change it and send the request again.",
    )])
    .into()
}

fn conflict(code: mrl_domain::errors::ErrorCode, message: impl Into<String>) -> JobsError {
    JobsError::domain(code, StatusCode::CONFLICT, message)
}

/// A storage error (P19) as an API answer.
fn p19(e: crate::modules::artifacts::Error) -> JobsError {
    match e {
        crate::modules::artifacts::Error::Api(a) => JobsError::Api(a),
        crate::modules::artifacts::Error::Coded {
            code,
            status,
            detail,
        } => match code.parse() {
            Ok(code) => JobsError::domain(code, status, detail),
            Err(_) => JobsError::Seam(crate::seams::SeamError::new("models", detail)),
        },
    }
}

/// A relative `/` path that stays in its folder.
fn safe_path(p: &str) -> bool {
    !p.is_empty()
        && p.len() <= 1024
        && !p.starts_with('/')
        && !p.contains(['\\', '\0'])
        && p.split('/').all(|s| !matches!(s, "" | "." | ".."))
}

fn pickle(p: &str) -> bool {
    let p = p.to_ascii_lowercase();
    PICKLE.iter().any(|s| p.ends_with(s))
}

fn repo_ok(repo: &str) -> bool {
    let parts: Vec<&str> = repo.split('/').collect();
    (1..=2).contains(&parts.len())
        && parts.iter().all(|p| {
            (1..=96).contains(&p.len())
                && p.bytes().next().is_some_and(|b| b.is_ascii_alphanumeric())
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        })
}

async fn secret(
    state: &AppState,
    models: &Models,
    ctx: &RequestContext,
    (org, project): (Uuid, Uuid),
    id: &str,
    path: &str,
    (allowed_use, name): (&str, &str),
) -> Result<BuiltinGrant, JobsError> {
    let secret = id
        .parse::<SecretId>()
        .map_err(|_| invalid(path, "This is not a secret id (`sec_…`)."))?;
    crate::modules::secrets::SecretResolver::new(models.db.clone(), state.config.server.mode)
        .prepare_grant(
            &state.seams,
            ctx,
            &super::outputs::storage_org(org),
            secret,
            "env",
            Some((ProjectId::from_uuid(project), allowed_use)),
        )
        .await
        .map_err(p19)?;
    Ok(BuiltinGrant {
        secret,
        allowed_use: allowed_use.into(),
        name: name.into(),
    })
}

/// The files of `v` staged as the folder input `name`.
async fn staged(models: &Models, v: &VersionRow, name: &str) -> Result<Vec<InputFile>, JobsError> {
    Ok(store::files(&models.db, v.id)
        .await?
        .into_iter()
        .map(|f| InputFile {
            name: name.into(),
            key: f.object_key,
            sha256: hex(&f.sha256),
            size: u64::try_from(f.size_bytes).unwrap_or_default(),
            local_path: format!("{name}/{}", f.path),
        })
        .collect())
}

/// What a version-making job needs besides its version.
struct VersionJob {
    model: String,
    source_kind: &'static str,
    source_ref: Value,
    base: Option<Uuid>,
    kind: &'static str,
    task: &'static str,
    inputs: Value,
    files: Vec<InputFile>,
    grants: Vec<BuiltinGrant>,
    node_id: Option<Uuid>,
    audit: &'static str,
    checkpoint: Option<(Uuid, Uuid)>,
}

/// Creates the model (when missing), its IMPORTING version and the job, audited, in one
/// transaction; answers 202 with the version.
async fn start(
    state: &AppState,
    db: &crate::db::Db,
    ctx: &RequestContext,
    (org, project): (Uuid, Uuid),
    job: VersionJob,
    submission: &Submission<'_>,
) -> Result<Response, JobsError> {
    super::api::check_name(&job.model)?;
    let mut tx = db.begin().await?;
    if let Some(replay) = submission.claim(&mut tx).await? {
        return Ok(replay);
    }
    if let Some((run, checkpoint)) = job.checkpoint {
        use crate::modules::checkpoints::service::{lock_row, lock_run};
        lock_run(&mut tx, run).await?;
        let locked = lock_row(&mut tx, checkpoint).await?;
        if !locked.is_some_and(|row| row.state == "COMMITTED") {
            return Err(conflict(
                codes::NOT_PROMOTABLE,
                "The checkpoint is no longer committed.",
            ));
        }
    }
    let model =
        store::get_or_create_model(&mut tx, org, project, &job.model, &ctx.principal.id).await?;
    let mut v = store::insert_version(
        &mut tx,
        &model,
        &NewVersion {
            source_kind: job.source_kind,
            source_ref: job.source_ref,
            base_model_version_id: job.base,
            files: vec![],
            created_by: ctx.principal.id.clone(),
        },
    )
    .await?;
    let mv = ModelVersionId::from_uuid(v.id).to_string();
    let row = builtin::submit(
        &mut tx,
        BuiltinJob {
            org_id: org,
            project_id: project,
            kind: job.kind.into(),
            task: job.task.into(),
            inputs: job.inputs,
            files: job.files,
            subject: json!({ "model_version_id": mv }),
            created_by: ctx.principal.id.clone(),
            grants: job.grants,
            node_id: job.node_id,
        },
    )
    .await?;
    store::set_job(&mut tx, v.id, row.id).await?;
    scope::audit(&mut tx, state, ctx, (org, project), job.audit, mv).await?;
    v.job_id = Some(row.id);
    let response = version_view(&v, &[], None);
    submission.save(&mut tx, &response).await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

/// `POST /projects/{prj}/models:import`: a new version of the model, IMPORTING until its job
/// has fetched and inspected the files.
#[utoipa::path(post, path = "/projects/{prj}/models:import", operation_id = "import_model", tag = "models",
    params(("prj" = String, Path, description = "Project id, `prj_…`.")),
    request_body = ImportModel,
    responses((status = 202, description = "The new IMPORTING version; `job_id` fills it in.", body = ModelVersionInfo),
              (status = 404, description = "No such project, upload or base version.", body = crate::error::Problem),
              (status = 422, description = "The request is not valid.", body = crate::error::Problem)))]
pub async fn import(
    State(state): State<AppState>,
    Extension(models): Extension<Arc<Models>>,
    ctx: RequestContext,
    Path(prj): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ImportModel>,
) -> Result<Response, JobsError> {
    super::api::check_name(&body.name)?;
    let (org, project) =
        project_scope(&state, &ctx, &models.projects, &prj, "models.import").await?;
    let submission = Submission::new((org, project), &ctx, &headers, "import", &prj, &body)?;
    if let Some(replay) = submission.replay(&models.db).await? {
        return Ok(replay);
    }
    let base = match &body.base_model_version_id {
        Some(b) => {
            let id: ModelVersionId = scope::parse_id(b, "model version")?;
            let v = super::gates::ready_in(&models.db, org, project, id.as_uuid())
                .await?
                .ok_or_else(|| JobsError::not_found("base model version"))?;
            Some(v.id)
        }
        None => None,
    };
    let mut grants = vec![];
    let mut files = vec![];
    let mut node_id = None;
    let (source_kind, source_ref, source) = match &body.source {
        ImportSource::Hub {
            repo,
            revision,
            secret_id,
        } => {
            let revision = revision.clone().unwrap_or_else(|| "main".into());
            if !repo_ok(repo) {
                return Err(invalid(
                    "/source/repo",
                    "Name a Hub repository like `org/name`.",
                ));
            }
            if !(1..=255).contains(&revision.len()) || !safe_path(&revision) {
                return Err(invalid("/source/revision", "Name a branch, tag or commit."));
            }
            let mut src = json!({"kind": "hub", "repo": repo, "revision": revision});
            if let Some(s) = secret_id {
                grants.push(
                    secret(
                        &state,
                        &models,
                        &ctx,
                        (org, project),
                        s,
                        "/source/secret_id",
                        ("model_download", "hf"),
                    )
                    .await?,
                );
                src["token_env"] = Value::from(mrl_proto::credential_env("hf"));
            }
            ("hub", json!({"repo": repo, "revision": revision}), src)
        }
        ImportSource::Upload { files: named } => {
            if named.is_empty() || named.len() > MAX_FILES {
                return Err(invalid(
                    "/source/files",
                    "Name between 1 and 512 uploaded files.",
                ));
            }
            let org_text = super::outputs::storage_org(org);
            for (path, up) in named {
                if !safe_path(path) || pickle(path) {
                    return Err(invalid(
                        &format!("/source/files/{path}"),
                        "Give a relative path inside the model folder; pickle files are never accepted.",
                    ));
                }
                let blob = crate::modules::uploads::finished(
                    &models.store,
                    &state.seams,
                    &ctx,
                    &org_text,
                    up,
                )
                .await
                .map_err(p19)?;
                files.push(InputFile {
                    name: "model".into(),
                    key: blob.key,
                    sha256: blob.sha256,
                    size: blob.size,
                    local_path: format!("model/{path}"),
                });
            }
            let uploads: Value = named
                .iter()
                .map(|(p, u)| (p.clone(), Value::from(u.as_str())))
                .collect::<serde_json::Map<_, _>>()
                .into();
            (
                "upload",
                json!({ "uploads": uploads }),
                json!({"kind": "staged"}),
            )
        }
        ImportSource::NodePath { path, node_id: n } => {
            let res = scope::resource(
                "project",
                Some(mrl_domain::ids::ProjectId::from_uuid(project).to_string()),
                org,
                project,
            );
            scope::authorize(&state, &ctx, "compute.manage", &res).await?;
            if !path.starts_with('/') || path.contains('\0') || path.len() > 4096 {
                return Err(invalid(
                    "/source/path",
                    "Give the folder as an absolute path.",
                ));
            }
            node_id = match n {
                Some(n) => Some(
                    n.parse::<NodeId>()
                        .map_err(|_| invalid("/source/node_id", "This is not a machine id."))?
                        .as_uuid(),
                ),
                None => None,
            };
            (
                "node_path",
                json!({"path": path, "node_id": n}),
                json!({"kind": "node_path", "path": path}),
            )
        }
    };
    start(
        &state,
        &models.db,
        &ctx,
        (org, project),
        VersionJob {
            model: body.name,
            source_kind,
            source_ref,
            base,
            kind: "IMPORT",
            task: "import_model",
            inputs: json!({"source": source, "convert_pickle": body.convert_pickle}),
            files,
            grants,
            node_id,
            audit: "models.import",
            checkpoint: None,
        },
        &submission,
    )
    .await
}

/// `POST /model-versions/{mv}:merge-adapter`: a READY LoRA adapter merged into its
/// full-precision base, as a new full-weights version.
#[utoipa::path(post, path = "/model-versions/{mv}:merge-adapter", operation_id = "merge_model_adapter", tag = "models",
    params(("mv" = String, Path, description = "The adapter's version id, `mv_…`.")),
    request_body = MergeAdapter,
    responses((status = 202, description = "The new IMPORTING version; `job_id` builds it.", body = ModelVersionInfo),
              (status = 409, description = "Not a READY adapter with a full-precision base (MRL-MODEL-264, MRL-MODEL-263).", body = crate::error::Problem)))]
pub async fn merge(
    state: &AppState,
    models: &Models,
    ctx: &RequestContext,
    mv: &str,
    headers: &HeaderMap,
    body: MergeAdapter,
) -> Result<Response, JobsError> {
    let adapter = authorized_version(state, models, ctx, mv, &["models.import"]).await?;
    let submission = Submission::new(
        (adapter.org_id, adapter.project_id),
        ctx,
        headers,
        "merge",
        mv,
        &body,
    )?;
    if let Some(replay) = submission.replay(&models.db).await? {
        return Ok(replay);
    }
    let base = match (
        adapter.state.as_str(),
        adapter.kind.as_deref(),
        adapter.base_model_version_id,
    ) {
        ("READY", Some("adapter"), Some(b)) => {
            super::gates::ready_in(&models.db, adapter.org_id, adapter.project_id, b).await?
        }
        _ => None,
    }
    .ok_or_else(|| {
        conflict(
            codes::ADAPTER_BASE,
            "Only a READY LoRA adapter whose base version is READY can be merged.",
        )
    })?;
    if base.kind.as_deref() != Some("full_weights") || base.quantization.is_some() {
        return Err(conflict(
            codes::ADAPTER_BASE,
            "The base model must be full-precision weights to merge into; this one is quantized or not full weights.",
        ));
    }
    let dtype = body.dtype.unwrap_or_else(|| "auto".into());
    if !matches!(dtype.as_str(), "auto" | "float32" | "float16" | "bfloat16") {
        return Err(invalid("/dtype", "Use auto, float32, float16 or bfloat16."));
    }
    let model = match body.name {
        Some(n) => n,
        None => {
            store::model(&models.db, adapter.model_id)
                .await?
                .ok_or_else(|| JobsError::not_found("model"))?
                .name
        }
    };
    let mut files = staged(models, &base, "base").await?;
    files.extend(staged(models, &adapter, "adapter").await?);
    let adapter_id = ModelVersionId::from_uuid(adapter.id).to_string();
    start(
        state,
        &models.db,
        ctx,
        (adapter.org_id, adapter.project_id),
        VersionJob {
            model,
            source_kind: "merge",
            source_ref: json!({ "adapter": adapter_id }),
            base: Some(base.id),
            kind: "MERGE_ADAPTER",
            task: "merge_adapter",
            inputs: json!({ "dtype": dtype }),
            files,
            grants: vec![],
            node_id: None,
            audit: "models.merge_adapter",
            checkpoint: None,
        },
        &submission,
    )
    .await
}

/// `POST /model-versions/{mv}:export`: the version as a standard Hugging Face folder, kept as
/// the job's artifact and sent to S3 or the Hub when asked.
#[utoipa::path(post, path = "/model-versions/{mv}:export", operation_id = "export_model_version", tag = "models",
    params(("mv" = String, Path, description = "Model version id, `mv_…`.")),
    request_body = ExportModel,
    responses((status = 202, description = "The export job.", body = ModelExportJob),
              (status = 409, description = "Not READY (MRL-MODEL-263), or code not approved (MRL-MODEL-002).", body = crate::error::Problem),
              (status = 422, description = "A format or target this release can't export to (MRL-MODEL-261).", body = crate::error::Problem)))]
pub async fn export(
    state: &AppState,
    models: &Models,
    ctx: &RequestContext,
    mv: &str,
    headers: &HeaderMap,
    body: ExportModel,
) -> Result<Response, JobsError> {
    let v = authorized_version(state, models, ctx, mv, &["models.export"]).await?;
    let submission = Submission::new((v.org_id, v.project_id), ctx, headers, "export", mv, &body)?;
    if let Some(replay) = submission.replay(&models.db).await? {
        return Ok(replay);
    }
    let format = body.format.unwrap_or_else(|| EXPORT_FORMAT.into());
    if format != EXPORT_FORMAT
        || v.kind.as_deref() == Some("inference_only")
        || v.quantization.is_some()
    {
        return Err(JobsError::domain(
            codes::EXPORT_FORMAT_UNAVAILABLE,
            StatusCode::UNPROCESSABLE_ENTITY,
            "Only full-precision safetensors weights export in this release (as hf-safetensors); GGUF and quantized formats come later.",
        ));
    }
    if v.state != "READY" {
        return Err(conflict(
            codes::NOT_READY,
            format!("This model version is {}, not READY.", v.state),
        ));
    }
    if !v.code_approved() {
        return Err(conflict(
            codes::REMOTE_CODE,
            "This version ships its own code; a maintainer must approve it before it leaves the app.",
        ));
    }
    let t = body.target;
    let kind = t.kind.clone().unwrap_or_else(|| "artifact".into());
    let mut grants = vec![];
    let mut target = json!({ "kind": kind });
    match kind.as_str() {
        "artifact" => {}
        "s3" | "hf_hub" => {
            let s = t.secret_id.as_deref().ok_or_else(|| {
                invalid("/target/secret_id", "Name the secret with the credentials.")
            })?;
            grants.push(
                secret(
                    state,
                    models,
                    ctx,
                    (v.org_id, v.project_id),
                    s,
                    "/target/secret_id",
                    ("model_export", "export"),
                )
                .await?,
            );
            target["credential_env"] = Value::from(mrl_proto::credential_env("export"));
            if kind == "s3" {
                let url = t.url.unwrap_or_default();
                if !url.starts_with("s3://") || url.len() > 1024 {
                    return Err(invalid("/target/url", "Give an s3://bucket/prefix URL."));
                }
                target["url"] = url.into();
                target["endpoint"] = t.endpoint.into();
                target["region"] = t.region.into();
            } else {
                let repo = t.repo.unwrap_or_default();
                if !repo_ok(&repo) || !repo.contains('/') {
                    return Err(invalid(
                        "/target/repo",
                        "Name a Hub repository like `org/name`.",
                    ));
                }
                target["repo"] = repo.into();
                target["private"] = t.private.unwrap_or(true).into();
            }
        }
        _ => return Err(invalid("/target/kind", "Export to artifact, s3 or hf_hub.")),
    }
    let id = ModelVersionId::from_uuid(v.id).to_string();
    let model = store::model(&models.db, v.model_id)
        .await?
        .ok_or_else(|| JobsError::not_found("model"))?;
    let base = v
        .base_model_version_id
        .map(|b| ModelVersionId::from_uuid(b).to_string());
    let card = json!({
        "name": model.name, "version": id, "revision": v.revision(), "base": base,
        "source": v.source_kind, "license": v.license_spdx,
    });
    let lineage =
        json!({"model_version_id": id, "source_ref": v.source_ref, "base_model_version_id": base});
    let files = if v.kind.as_deref() == Some("adapter") {
        let base = super::gates::adapter_base(&models.db, &v)
            .await?
            .ok_or_else(|| {
                conflict(
                    codes::ADAPTER_BASE,
                    "The adapter's original base model is unavailable.",
                )
            })?;
        if !base.code_approved() {
            return Err(conflict(
                codes::REMOTE_CODE,
                "A maintainer must approve the base model's code before export.",
            ));
        }
        let mut files = staged(models, &base, "model").await?;
        files.extend(staged(models, &v, "model_adapter").await?);
        files
    } else {
        staged(models, &v, "model").await?
    };
    let mut tx = models.db.begin().await?;
    if let Some(replay) = submission.claim(&mut tx).await? {
        return Ok(replay);
    }
    let row = builtin::submit(
        &mut tx,
        BuiltinJob {
            org_id: v.org_id,
            project_id: v.project_id,
            kind: "EXPORT".into(),
            task: "export_model".into(),
            inputs: json!({"format": format, "card": card, "lineage": lineage, "target": target}),
            files,
            subject: json!({ "model_version_id": id }),
            created_by: ctx.principal.id.clone(),
            grants,
            node_id: None,
        },
    )
    .await?;
    scope::audit(
        &mut tx,
        state,
        ctx,
        (v.org_id, v.project_id),
        "models.export",
        format!("{id}->{kind}"),
    )
    .await?;
    let response = ModelExportJob {
        job_id: JobId::from_uuid(row.id).to_string(),
        model_version_id: id,
    };
    submission.save(&mut tx, &response).await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

#[utoipa::path(post, path = "/checkpoints/{ckpt}:promote", operation_id = "promote_checkpoint", tag = "models",
    params(("ckpt" = String, Path, description = "Checkpoint id, `ckpt_…`.")),
    responses((status = 202, description = "The new IMPORTING version, in the model named after the run.", body = ModelVersionInfo),
              (status = 409, description = "Not COMMITTED, its bytes changed, or no safetensors weights (MRL-MODEL-265).", body = crate::error::Problem)))]
/// `POST /checkpoints/{ckpt}:promote` (P23's route; `row` is authorized for
/// `checkpoints.read` in `scope`): after a full re-hash, the checkpoint's safetensors model
/// files become a new EXPERIMENTAL version of the model named after the run, inspected by an
/// import job (which copies the files, so checkpoint retention can't take them away).
pub async fn promote(
    state: &AppState,
    ctx: &RequestContext,
    service: &CheckpointService,
    row: CheckpointRow,
    (org, project): (Uuid, Uuid),
    headers: &HeaderMap,
) -> Result<Response, JobsError> {
    let res = scope::resource(
        "project",
        Some(mrl_domain::ids::ProjectId::from_uuid(project).to_string()),
        org,
        project,
    );
    scope::authorize(state, ctx, "models.import", &res).await?;
    let ckpt = mrl_domain::ids::CheckpointId::from_uuid(row.id).to_string();
    let submission = Submission::new((org, project), ctx, headers, "promote", &ckpt, &())?;
    if let Some(replay) = submission.replay(service.db()).await? {
        return Ok(replay);
    }
    let row = service.require_full_verification(row.id).await?;
    let files: Vec<InputFile> = service
        .files(row.id)
        .await?
        .into_iter()
        .filter(|f| safe_path(&f.path) && !pickle(&f.path))
        .map(|f| InputFile {
            name: "model".into(),
            sha256: hex(&f.sha256),
            size: u64::try_from(f.size_bytes).unwrap_or_default(),
            local_path: format!("model/{}", f.path),
            key: f.object_key,
        })
        .collect();
    if !files.iter().any(|f| f.local_path.ends_with(".safetensors")) {
        return Err(conflict(
            codes::NOT_PROMOTABLE,
            format!(
                "This checkpoint ({} layout) has no safetensors model weights to promote.",
                row.layout
            ),
        ));
    }
    let (run_name, base): (String, Option<Uuid>) =
        sqlx::query_as("SELECT name, base_model_version_id FROM runs WHERE id = $1")
            .bind(row.run_id)
            .fetch_one(service.db().pool())
            .await?;
    let model: String = run_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .take(128)
        .collect();
    let model = if model.is_empty() {
        "promoted".into()
    } else {
        model
    };
    // The run's starting version: the base of an adapter, the parent of full weights.
    let base = match base {
        Some(b) => super::gates::ready_in(service.db(), org, project, b)
            .await?
            .map(|v| v.id),
        None => None,
    };
    start(
        state,
        service.db(),
        ctx,
        (org, project),
        VersionJob {
            model,
            source_kind: "checkpoint",
            source_ref: json!({
                "checkpoint_id": ckpt,
                "run_id": mrl_domain::ids::RunId::from_uuid(row.run_id).to_string(),
                "global_step": row.global_step,
                "layout": row.layout,
            }),
            base,
            kind: "IMPORT",
            task: "import_model",
            inputs: json!({"source": {"kind": "staged"}}),
            files,
            grants: vec![],
            node_id: None,
            audit: "models.promote",
            checkpoint: Some((row.run_id, row.id)),
        },
        &submission,
    )
    .await
}

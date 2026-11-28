//! The check endpoints: each starts one REWARD_PREVIEW job ([`super::checks`]) and answers
//! 202 with its report, which `GET /reward-versions/{rwdv}/reports` shows once the job ends.
//!
//! | Route | Action | Needs |
//! |---|---|---|
//! | `POST /reward-versions/{rwdv}:test` | `rewards.write` | DRAFT or FAILED_TESTS, test cases |
//! | `POST /reward-versions/{rwdv}:preview` | `rewards.write` | a dataset artifact |
//! | `POST /reward-versions/{rwdv}:regression` | `rewards.write` | not published; a published earlier version with test cases |
//! | `POST /rewards:compare` | `rewards.write` on `a`, `rewards.read` on `b` | two versions of one org, a dataset artifact |
//! | `GET /reward-versions/{rwdv}/reports` | `rewards.read` | |
//!
//! Every start is audited in its transaction (`rewards.test`, `rewards.preview`,
//! `rewards.regression`, `rewards.compare`).

use super::api::{body, read_version, vid, Ctx};
use super::checks::{self, Mode, NewCheck, CODE, DATASET, MODEL};
use super::launch::{code_inputs, folder_inputs, StageError};
use super::store::{self, ReportRow, VersionRow};
use super::{codes, RewardsError};
use crate::app::AppState;
use crate::error::{ErrorCode, FieldError};
use crate::http::pagination::{Page, PageQuery};
use crate::modules::artifacts::{InputFile, LOCAL_ORG};
use crate::modules::jobs::{scope, JobsError};
use crate::seams::{Decision, RequestContext, SeamError};
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use mrl_domain::ids::{ArtifactId, JobId, ProjectId};
use mrl_domain::state::RewardVersionEvent;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use time::OffsetDateTime;
use utoipa::ToSchema;
use uuid::Uuid;

/// Most answers written per prompt.
pub const MAX_GENERATIONS: u32 = 16;
/// Most new tokens per written answer.
pub const MAX_NEW_TOKENS: u32 = 1024;

fn five() -> u32 {
    5
}
fn one() -> u32 {
    1
}
fn sixty_four() -> u32 {
    64
}
fn stored() -> String {
    "stored".into()
}
fn prompt() -> String {
    "prompt".into()
}
fn completion() -> String {
    "completion".into()
}
fn reference() -> String {
    "reference".into()
}

/// Which samples a preview or comparison scores.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = RewardPreviewRequest)]
pub struct PreviewBody {
    /// A committed artifact of this project (or of the whole org) with the samples: JSON Lines
    /// (`.jsonl`) or Parquet files, one sample per row.
    pub dataset_artifact_id: String,
    /// Rows drawn at random (with `seed`); 1 to `rewards.max_preview_samples` samples in all.
    #[serde(default = "five")]
    pub n: u32,
    /// Seed for drawing rows and writing answers.
    #[serde(default)]
    pub seed: u64,
    /// `stored`: score the rows' own completions. `generate`: write `num_generations` answers
    /// per prompt with the model (on the CPU).
    #[serde(default = "stored")]
    pub completions: String,
    /// Answers per prompt when generating (1–16).
    #[serde(default = "one")]
    pub num_generations: u32,
    /// Most new tokens per written answer (1–1024).
    #[serde(default = "sixty_four")]
    pub max_new_tokens: u32,
    /// The model's files, to write answers (an artifact, like a model version's files).
    pub model_artifact_id: Option<String>,
    /// The rows' prompt field (text, or chat messages).
    #[serde(default = "prompt")]
    pub prompt_field: String,
    /// The rows' completion field (for `stored`).
    #[serde(default = "completion")]
    pub completion_field: String,
    /// The rows' reference field (the answer components compare with).
    #[serde(default = "reference")]
    pub reference_field: String,
}

/// `POST /rewards:compare`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = RewardCompareRequest)]
pub struct CompareBody {
    /// The first version, `rwdv_…` (the report belongs to it).
    pub a: String,
    /// The second version, `rwdv_…`, of any reward in the same org.
    pub b: String,
    /// The samples both score.
    pub samples: PreviewBody,
}

/// A check and, once its job ended, its results.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[schema(as = RewardReport)]
pub struct ReportView {
    /// The REWARD_PREVIEW job, `job_…` (also the report's id).
    pub job_id: Option<String>,
    /// `test`, `preview`, `regression` or `compare`.
    pub mode: String,
    /// `PENDING` until the job ends, then `READY` or `INVALID`.
    pub state: String,
    /// The version checked, `rwdv_…`.
    pub version_id: String,
    /// The baseline (regression) or the second version (compare).
    pub other_version_id: Option<String>,
    /// What was asked.
    #[schema(value_type = Object)]
    pub request: Value,
    /// The server's results once READY: a test summary, the regression comparison, or
    /// statistics (histograms, rates, latency, length correlation, learnability, agreement).
    #[schema(value_type = Option<Object>)]
    pub results: Option<Value>,
    /// Why it is INVALID.
    pub reason: Option<String>,
    /// The job's outputs artifact (`report.json`, `rows.jsonl`).
    pub artifact_id: Option<String>,
    /// Who asked.
    pub created_by: String,
    /// When.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub created_at: OffsetDateTime,
    /// When it settled.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub finished_at: Option<OffsetDateTime>,
}

fn report_view(r: &ReportRow) -> ReportView {
    ReportView {
        job_id: r.job_id.map(|j| JobId::from_uuid(j).to_string()),
        mode: r.mode.clone(),
        state: r.state.clone(),
        version_id: vid(r.version_id),
        other_version_id: r.other_version_id.map(vid),
        request: r.request.clone(),
        results: r.results.clone(),
        reason: r.reason.clone(),
        artifact_id: r.artifact_id.map(|a| ArtifactId::from_uuid(a).to_string()),
        created_by: r.created_by.clone(),
        created_at: r.created_at,
        finished_at: r.finished_at,
    }
}

fn started(r: &ReportRow) -> Response {
    (StatusCode::ACCEPTED, Json(report_view(r))).into_response()
}

fn field(path: &str, message: impl Into<String>, hint: &str) -> FieldError {
    FieldError::new(path, ErrorCode::FieldValue, message, hint)
}

fn org_text(org: Uuid) -> String {
    scope::org_text(org).unwrap_or_else(|| LOCAL_ORG.to_owned())
}

fn staged(e: StageError) -> RewardsError {
    match e {
        StageError::Gone(why) => {
            RewardsError::problem(codes::BUNDLE_INVALID, StatusCode::CONFLICT, why)
        }
        StageError::Storage(_) => SeamError::new("artifacts", e.to_string()).into(),
    }
}

fn nothing(detail: &str) -> RewardsError {
    RewardsError::problem(codes::NOTHING_TO_CHECK, StatusCode::CONFLICT, detail)
}

fn illegal(detail: &str) -> RewardsError {
    RewardsError::problem(codes::ILLEGAL_STEP, StatusCode::CONFLICT, detail)
}

/// A committed artifact the caller may read, of `project` or org-wide (else a field error at
/// `path`).
async fn readable_artifact(
    state: &AppState,
    ctx: &RequestContext,
    c: &Ctx,
    (org, project): (Uuid, Uuid),
    raw: &str,
    path: &str,
) -> Result<Uuid, RewardsError> {
    let bad = |m: String| {
        RewardsError::fields(vec![field(
            path,
            m,
            "Upload the files as an artifact of this project, then name it here.",
        )])
    };
    let id: ArtifactId = raw
        .parse()
        .map_err(|_| bad("This is not an artifact id (art_…).".into()))?;
    let org_text = org_text(org);
    let found = c
        .store
        .get(&org_text, id.as_uuid())
        .await
        .map_err(|e| RewardsError::from(SeamError::new("artifacts", e.to_string())))?;
    let visible = match &found {
        Some(a) => {
            state
                .seams
                .authorizer
                .authorize(ctx, "lineage.read", &a.resource(&org_text))
                .await?
                == Decision::Allow
        }
        None => false,
    };
    let Some(a) = found.filter(|_| visible) else {
        return Err(bad("No artifact with this id exists.".into()));
    };
    if a.state != "COMMITTED" {
        return Err(bad(format!(
            "The artifact is {}; use a committed one.",
            a.state
        )));
    }
    let own = ProjectId::from_uuid(project).to_string();
    if a.project_id.as_deref().is_some_and(|p| p != own) {
        return Err(bad("The artifact belongs to another project.".into()));
    }
    Ok(id.as_uuid())
}

/// Checks a preview body; returns the task's `preview` settings and the dataset and model
/// files to stage.
async fn samples(
    state: &AppState,
    ctx: &RequestContext,
    c: &Ctx,
    scope_ids: (Uuid, Uuid),
    p: &PreviewBody,
    at: &str,
) -> Result<(Value, Vec<InputFile>), RewardsError> {
    let mut problems = Vec::new();
    let max = c.config.max_preview_samples;
    if !(1..=MAX_GENERATIONS).contains(&p.num_generations) {
        problems.push(field(
            &format!("{at}/num_generations"),
            format!("Use 1 to {MAX_GENERATIONS} answers per prompt."),
            "Lower num_generations.",
        ));
    } else if p.n == 0 || p.n.saturating_mul(p.num_generations) > max {
        problems.push(field(
            &format!("{at}/n"),
            format!("A preview scores 1 to {max} samples (rows times answers per prompt)."),
            "Draw fewer rows, or write fewer answers per prompt.",
        ));
    }
    if !(1..=MAX_NEW_TOKENS).contains(&p.max_new_tokens) {
        problems.push(field(
            &format!("{at}/max_new_tokens"),
            format!("Use 1 to {MAX_NEW_TOKENS} new tokens."),
            "Lower max_new_tokens.",
        ));
    }
    let generate = match p.completions.as_str() {
        "stored" => false,
        "generate" => true,
        _ => {
            problems.push(field(
                &format!("{at}/completions"),
                "Use `stored` or `generate`.",
                "`stored` scores the dataset's own completions.",
            ));
            false
        }
    };
    if generate && p.model_artifact_id.is_none() {
        problems.push(field(
            &format!("{at}/model_artifact_id"),
            "Writing answers needs the model's files.",
            "Name the artifact with the model's files, or use completions: stored.",
        ));
    }
    for (name, value) in [
        ("prompt_field", &p.prompt_field),
        ("completion_field", &p.completion_field),
        ("reference_field", &p.reference_field),
    ] {
        if value.is_empty() || value.len() > 100 {
            problems.push(field(
                &format!("{at}/{name}"),
                "Use a field name of 1 to 100 characters.",
                "Name a field the dataset's rows have.",
            ));
        }
    }
    if !problems.is_empty() {
        return Err(RewardsError::fields(problems));
    }
    let org = org_text(scope_ids.0);
    let dataset = readable_artifact(
        state,
        ctx,
        c,
        scope_ids,
        &p.dataset_artifact_id,
        &format!("{at}/dataset_artifact_id"),
    )
    .await?;
    let mut files = folder_inputs(&c.store, &org, dataset, DATASET)
        .await
        .map_err(staged)?;
    let mut preview = json!({
        "n": p.n, "seed": p.seed, "completions": p.completions,
        "num_generations": p.num_generations, "max_new_tokens": p.max_new_tokens,
        "dataset": DATASET, "prompt_field": p.prompt_field,
        "completion_field": p.completion_field, "reference_field": p.reference_field,
    });
    if let (true, Some(raw)) = (generate, &p.model_artifact_id) {
        let model = readable_artifact(
            state,
            ctx,
            c,
            scope_ids,
            raw,
            &format!("{at}/model_artifact_id"),
        )
        .await?;
        files.extend(
            folder_inputs(&c.store, &org, model, MODEL)
                .await
                .map_err(staged)?,
        );
        preview["model"] = json!(MODEL);
    }
    Ok((preview, files))
}

async fn code_of(c: &Ctx, v: &VersionRow, which: usize) -> Result<Vec<InputFile>, RewardsError> {
    let (name, dir) = CODE[which];
    code_inputs(&c.store, &org_text(v.org_id), v, name, dir)
        .await
        .map_err(staged)
}

async fn audit_start(
    tx: &mut crate::db::Tx,
    state: &AppState,
    ctx: &RequestContext,
    v: &VersionRow,
    action: &str,
) -> Result<(), RewardsError> {
    scope::audit(tx, state, ctx, (v.org_id, v.project_id), action, vid(v.id)).await?;
    Ok(())
}

#[utoipa::path(post, path = "/reward-versions/{rwdv}:test", tag = "rewards", operation_id = "test_reward_version",
    params(("rwdv" = String, Path, description = "Reward version id, `rwdv_…`.")),
    responses((status = 202, description = "The test run started: the version is TESTING until its report is in.", body = ReportView),
              (status = 404, description = "No such reward version.", body = crate::error::Problem),
              (status = 409, description = "Its tests are running or it is published (MRL-STATE-290), or it has no test cases (MRL-STATE-292).", body = crate::error::Problem)))]
pub(super) async fn test_version(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(rwdv): Path<String>,
) -> Result<Response, RewardsError> {
    let found = read_version(&state, &ctx, &c, &rwdv, "rewards.write").await?;
    let cases = store::cases(&c.db, found.id).await?;
    if cases.is_empty() {
        return Err(nothing(
            "This version has no test cases. Create a version with test cases to test it.",
        ));
    }
    let files = code_of(&c, &found, 0).await?;
    let mut tx = c.db.begin().await?;
    let v = store::lock_version(&mut tx, found.id)
        .await?
        .ok_or_else(|| JobsError::not_found("reward version"))?;
    match v.state.as_str() {
        "DRAFT" | "FAILED_TESTS" => {}
        "TESTING" => return Err(illegal("This version's tests are already running.")),
        _ => return Err(illegal("A published version has already passed its tests.")),
    }
    let report = checks::start(
        &mut tx,
        NewCheck {
            mode: Mode::Test,
            version: &v,
            other: None,
            cases: &cases,
            baseline_cases: &[],
            preview: None,
            files,
            request: json!({}),
            created_by: ctx.principal.id.clone(),
        },
    )
    .await?;
    let job = report.job_id;
    let v = store::step(
        &mut tx,
        &v,
        RewardVersionEvent::StartTests,
        json!({"job_id": job.map(|j| JobId::from_uuid(j).to_string())}),
    )
    .await?;
    sqlx::query("UPDATE reward_versions SET test_job_id = $2 WHERE id = $1")
        .bind(v.id)
        .bind(job)
        .execute(&mut *tx)
        .await?;
    audit_start(&mut tx, &state, &ctx, &v, "rewards.test").await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(started(&report))
}

#[utoipa::path(post, path = "/reward-versions/{rwdv}:preview", tag = "rewards", operation_id = "preview_reward_version",
    params(("rwdv" = String, Path, description = "Reward version id, `rwdv_…`.")),
    request_body = PreviewBody,
    responses((status = 202, description = "The preview started.", body = ReportView),
              (status = 404, description = "No such reward version.", body = crate::error::Problem),
              (status = 422, description = "The samples need a change (MRL-VAL-*).", body = crate::error::Problem)))]
pub(super) async fn preview_version(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(rwdv): Path<String>,
    raw: Bytes,
) -> Result<Response, RewardsError> {
    let v = read_version(&state, &ctx, &c, &rwdv, "rewards.write").await?;
    let b: PreviewBody = body(&raw)?;
    let (preview, mut files) = samples(&state, &ctx, &c, (v.org_id, v.project_id), &b, "").await?;
    files.extend(code_of(&c, &v, 0).await?);
    let request: Value = serde_json::from_slice(&raw).unwrap_or_default();
    let mut tx = c.db.begin().await?;
    let report = checks::start(
        &mut tx,
        NewCheck {
            mode: Mode::Preview,
            version: &v,
            other: None,
            cases: &[],
            baseline_cases: &[],
            preview: Some(preview),
            files,
            request,
            created_by: ctx.principal.id.clone(),
        },
    )
    .await?;
    audit_start(&mut tx, &state, &ctx, &v, "rewards.preview").await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(started(&report))
}

#[utoipa::path(post, path = "/reward-versions/{rwdv}:regression", tag = "rewards", operation_id = "regression_reward_version",
    params(("rwdv" = String, Path, description = "Reward version id, `rwdv_…`.")),
    responses((status = 202, description = "The comparison with the published version started; publishing needs it.", body = ReportView),
              (status = 404, description = "No such reward version.", body = crate::error::Problem),
              (status = 409, description = "It is published (MRL-STATE-290), or no earlier version with test cases is published (MRL-STATE-292).", body = crate::error::Problem)))]
pub(super) async fn regression_version(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(rwdv): Path<String>,
) -> Result<Response, RewardsError> {
    let found = read_version(&state, &ctx, &c, &rwdv, "rewards.write").await?;
    let unpublished = |v: &VersionRow| {
        if matches!(v.state.as_str(), "DRAFT" | "TESTING" | "FAILED_TESTS") {
            Ok(())
        } else {
            Err(illegal(
                "Only a version that isn't published yet is compared with the published one.",
            ))
        }
    };
    unpublished(&found)?;
    let mut tx = c.db.begin().await?;
    let baseline = store::latest_published(&mut tx, found.reward_id, found.id).await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    let no_baseline = "No earlier version of this reward is published, so there is nothing to compare with. A first version publishes without a comparison.";
    let baseline = baseline.ok_or_else(|| nothing(no_baseline))?;
    let baseline_cases = store::cases(&c.db, baseline.id).await?;
    if baseline_cases.is_empty() {
        return Err(nothing(
            "The published version has no test cases to compare on.",
        ));
    }
    let mut files = code_of(&c, &found, 0).await?;
    files.extend(code_of(&c, &baseline, 1).await?);
    let mut tx = c.db.begin().await?;
    store::lock_definition(&mut tx, found.reward_id).await?;
    let v = store::lock_version(&mut tx, found.id)
        .await?
        .ok_or_else(|| JobsError::not_found("reward version"))?;
    unpublished(&v)?;
    let now = store::latest_published(&mut tx, v.reward_id, v.id).await?;
    if now.as_ref().map(|b| b.id) != Some(baseline.id) {
        return Err(JobsError::changed("reward").into());
    }
    let report = checks::start(
        &mut tx,
        NewCheck {
            mode: Mode::Regression,
            version: &v,
            other: Some(&baseline),
            cases: &[],
            baseline_cases: &baseline_cases,
            preview: None,
            files,
            request: json!({"against_version_id": vid(baseline.id)}),
            created_by: ctx.principal.id.clone(),
        },
    )
    .await?;
    audit_start(&mut tx, &state, &ctx, &v, "rewards.regression").await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(started(&report))
}

#[utoipa::path(post, path = "/rewards:compare", tag = "rewards", operation_id = "compare_reward_versions",
    request_body = CompareBody,
    responses((status = 202, description = "The comparison started; its report belongs to version a.", body = ReportView),
              (status = 404, description = "No such reward version.", body = crate::error::Problem),
              (status = 422, description = "The request needs a change (MRL-VAL-*).", body = crate::error::Problem)))]
pub(super) async fn compare_versions(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    raw: Bytes,
) -> Result<Response, RewardsError> {
    let b: CompareBody = body(&raw)?;
    let a = read_version(&state, &ctx, &c, &b.a, "rewards.write").await?;
    let other = read_version(&state, &ctx, &c, &b.b, "rewards.read").await?;
    if a.id == other.id || a.org_id != other.org_id {
        let message = if a.id == other.id {
            "Compare two different versions."
        } else {
            "Both versions must belong to the same organization."
        };
        return Err(RewardsError::fields(vec![field(
            "/b",
            message,
            "Name another reward version as b.",
        )]));
    }
    let (preview, mut files) = samples(
        &state,
        &ctx,
        &c,
        (a.org_id, a.project_id),
        &b.samples,
        "/samples",
    )
    .await?;
    files.extend(code_of(&c, &a, 0).await?);
    files.extend(code_of(&c, &other, 1).await?);
    let request: Value = serde_json::from_slice(&raw).unwrap_or_default();
    let mut tx = c.db.begin().await?;
    let report = checks::start(
        &mut tx,
        NewCheck {
            mode: Mode::Compare,
            version: &a,
            other: Some(&other),
            cases: &[],
            baseline_cases: &[],
            preview: Some(preview),
            files,
            request,
            created_by: ctx.principal.id.clone(),
        },
    )
    .await?;
    audit_start(&mut tx, &state, &ctx, &a, "rewards.compare").await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(started(&report))
}

#[utoipa::path(get, path = "/reward-versions/{rwdv}/reports", tag = "rewards", operation_id = "list_reward_reports",
    params(("rwdv" = String, Path, description = "Reward version id, `rwdv_…`."), PageQuery),
    responses((status = 200, description = "Checks of this version (and comparisons naming it), newest first.", body = Page<ReportView>),
              (status = 404, description = "No such reward version.", body = crate::error::Problem)))]
pub(super) async fn list_reports(
    State(state): State<AppState>,
    Extension(c): Extension<Ctx>,
    ctx: RequestContext,
    Path(rwdv): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page<ReportView>>, RewardsError> {
    let v = read_version(&state, &ctx, &c, &rwdv, "rewards.read").await?;
    let page = q.resolve()?;
    let after = scope::after::<JobId>(&page, |j| j.as_uuid())?;
    let rows = sqlx::query_as::<_, ReportRow>(concat!(
        "SELECT ",
        crate::reward_report_cols!(),
        " FROM reward_reports WHERE org_id = $1 AND (version_id = $2 OR other_version_id = $2)
           AND job_id IS NOT NULL AND ($3::uuid IS NULL OR job_id < $3)
         ORDER BY job_id DESC LIMIT $4"
    ))
    .bind(v.org_id)
    .bind(v.id)
    .bind(after)
    .bind(page.fetch() as i64)
    .fetch_all(c.db.pool())
    .await?;
    let (rows, next_cursor) = scope::cut(rows, &page, |r| {
        r.job_id
            .map(|j| JobId::from_uuid(j).to_string())
            .unwrap_or_default()
    });
    Ok(Json(Page {
        items: rows.iter().map(report_view).collect(),
        next_cursor,
    }))
}

//! Generated samples: `GET /runs/{run}/samples` with step, reward and text filters.
//!
//! Each worker `SampleBatch` is one JSON file in object storage and one `samples` row (step,
//! count, reward range). A page holds whole batches, so it may hold a few more samples than
//! `limit`.

use super::{field_error, parse_num, read_run, storage_org, stored_file_error, Telemetry};
use crate::app::AppState;
use crate::http::pagination::{decode_cursor, encode_cursor};
use crate::modules::jobs::JobsError;
use crate::seams::RequestContext;
use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use mrl_domain::ids::AttemptId;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

/// Batches one page reads.
const PAGE_BATCHES: i64 = 50;
const PAGE_BYTES: u64 = 16 << 20;
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 500;

/// One generated sample.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct SampleView {
    /// The batch it came in.
    pub batch_id: i64,
    /// Trainer step.
    pub step: i64,
    /// `att_…`.
    pub attempt_id: String,
    /// The worker's id for the sample.
    pub sample_id: String,
    /// Group (prompts sampled together).
    pub group_id: String,
    /// Prompt text.
    pub prompt: String,
    /// Generated text.
    pub completion: String,
    /// Total reward, if scored.
    pub reward_total: Option<f64>,
    /// Reward per component.
    pub reward_components: BTreeMap<String, f64>,
    /// True when generation hit the length limit.
    pub truncated: bool,
    /// Generated tokens.
    pub completion_tokens: u32,
    /// Policy version that generated it.
    pub policy_version: u64,
}

/// A page of samples.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct SamplePage {
    /// Samples, oldest batch first.
    pub items: Vec<SampleView>,
    /// Pass as `cursor` to continue; null at the end.
    pub next_cursor: Option<String>,
}

/// Query parameters of `GET /runs/{run}/samples`.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct SampleParams {
    /// Only this step.
    pub step: Option<String>,
    /// Steps from here.
    pub min_step: Option<String>,
    /// Steps up to here.
    pub max_step: Option<String>,
    /// Only samples with at least this total reward.
    pub min_reward: Option<String>,
    /// Only samples with at most this total reward.
    pub max_reward: Option<String>,
    /// Only samples whose prompt or completion contains this text (ignores case).
    pub q: Option<String>,
    /// 1–500 (default 50).
    pub limit: Option<String>,
    /// `next_cursor` from the previous page.
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StoredBatch {
    samples: Vec<StoredSample>,
}

#[derive(Debug, Deserialize)]
struct StoredSample {
    sample_id: String,
    group_id: String,
    prompt: String,
    completion: String,
    reward_total: Option<f64>,
    #[serde(default)]
    reward_components: BTreeMap<String, f64>,
    truncated: bool,
    completion_tokens: u32,
    policy_version: u64,
}

/// `GET /runs/{run}/samples`.
#[utoipa::path(get, path = "/runs/{run}/samples", tag = "telemetry", operation_id = "list_run_samples",
    params(("run" = String, Path, description = "`run_…`"), SampleParams),
    responses((status = 200, body = SamplePage), (status = 404, description = "No such run"),
        (status = 503, description = "A sample file could not be read (MRL-STORE-220)")))]
pub async fn list(
    State(state): State<AppState>,
    Extension(t): Extension<Arc<Telemetry>>,
    ctx: RequestContext,
    Path(run): Path<String>,
    Query(p): Query<SampleParams>,
) -> Result<Json<SamplePage>, JobsError> {
    let row = read_run(&state, &t.db, &ctx, &run, "metrics_logs.read").await?;
    let step: Option<i64> = parse_num(p.step.as_deref(), "step")?;
    let min_step = step.or(parse_num(p.min_step.as_deref(), "min_step")?);
    let max_step = step.or(parse_num(p.max_step.as_deref(), "max_step")?);
    let min_reward: Option<f64> = parse_num(p.min_reward.as_deref(), "min_reward")?;
    let max_reward: Option<f64> = parse_num(p.max_reward.as_deref(), "max_reward")?;
    if [min_reward, max_reward]
        .into_iter()
        .flatten()
        .any(|v| !v.is_finite())
    {
        return Err(field_error(
            "reward",
            "Reward bounds must be finite.",
            "Use a finite number.",
        ));
    }
    let limit = parse_num::<usize>(p.limit.as_deref(), "limit")?.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(field_error(
            "limit",
            &format!("limit {limit} is outside 1–{MAX_LIMIT}."),
            "Ask for between 1 and 500 samples.",
        ));
    }
    let q =
        p.q.as_deref()
            .filter(|q| !q.is_empty())
            .map(str::to_lowercase);
    let (after, first_index) = sample_cursor(p.cursor.as_deref())?;
    let batches = sqlx::query(
        "SELECT id, org_id, attempt_id, step, object_key, sha256 FROM samples
          WHERE run_id = $1 AND (id > $2 OR (id = $2 AND $8))
            AND ($3::bigint IS NULL OR step >= $3) AND ($4::bigint IS NULL OR step <= $4)
            AND ($5::float8 IS NULL OR reward_max >= $5) AND ($6::float8 IS NULL OR reward_min <= $6)
          ORDER BY id LIMIT $7",
    )
    .bind(row.id)
    .bind(after)
    .bind(min_step)
    .bind(max_step)
    .bind(min_reward)
    .bind(max_reward)
    .bind(PAGE_BATCHES)
    .bind(first_index.is_some())
    .fetch_all(t.db.pool())
    .await?;
    let mut items = Vec::new();
    let mut next = None;
    let mut remaining = PAGE_BYTES;
    for (batch_index, b) in batches.iter().enumerate() {
        let (id, org, attempt, step, key): (i64, Uuid, Uuid, i64, String) = (
            b.try_get("id")?,
            b.try_get("org_id")?,
            b.try_get("attempt_id")?,
            b.try_get("step")?,
            b.try_get("object_key")?,
        );
        let store = t
            .store
            .org_store(&storage_org(org))
            .await
            .map_err(|e| stored_file_error("samples", &e))?;
        let size = store
            .head(&key)
            .await
            .map_err(|e| stored_file_error("samples", &e))?
            .size;
        let offset = if id == after {
            first_index.unwrap_or(0)
        } else {
            0
        };
        if batch_index > 0 && size > remaining {
            next = Some(format!("{id}:{offset}"));
            break;
        }
        remaining = remaining.saturating_sub(size);
        let bytes = store
            .get(&key)
            .await
            .map_err(|e| stored_file_error("samples", &e))?;
        let sha: Option<String> = b.try_get("sha256")?;
        if sha.is_some_and(|sha| super::ingest::sha256_hex(&bytes) != sha) {
            return Err(stored_file_error("samples", &"checksum mismatch"));
        }
        let doc: StoredBatch =
            serde_json::from_slice(&bytes).map_err(|e| stored_file_error("samples", &e))?;
        let count = doc.samples.len();
        for (index, s) in doc.samples.into_iter().enumerate().skip(offset) {
            let r = s.reward_total;
            let in_range = min_reward.is_none_or(|m| r.is_some_and(|r| r >= m))
                && max_reward.is_none_or(|m| r.is_some_and(|r| r <= m));
            let text = q.as_deref().is_none_or(|q| {
                s.prompt.to_lowercase().contains(q) || s.completion.to_lowercase().contains(q)
            });
            if in_range && text {
                items.push(SampleView {
                    batch_id: id,
                    step,
                    attempt_id: AttemptId::from_uuid(attempt).to_string(),
                    sample_id: s.sample_id,
                    group_id: s.group_id,
                    prompt: s.prompt,
                    completion: s.completion,
                    reward_total: s.reward_total,
                    reward_components: s.reward_components,
                    truncated: s.truncated,
                    completion_tokens: s.completion_tokens,
                    policy_version: s.policy_version,
                });
                if items.len() == limit {
                    if index + 1 < count {
                        next = Some(format!("{id}:{}", index + 1));
                    } else if batch_index + 1 < batches.len()
                        || i64::try_from(batches.len()).unwrap_or(0) == PAGE_BATCHES
                    {
                        next = Some(id.to_string());
                    }
                    return Ok(Json(SamplePage {
                        items,
                        next_cursor: next.map(|c| encode_cursor(&c)),
                    }));
                }
            }
        }
        if i64::try_from(batch_index + 1).unwrap_or(0) == PAGE_BATCHES {
            next = Some(id.to_string());
        }
    }
    Ok(Json(SamplePage {
        items,
        next_cursor: next.map(|c| encode_cursor(&c)),
    }))
}

fn sample_cursor(cursor: Option<&str>) -> Result<(i64, Option<usize>), JobsError> {
    let Some(cursor) = cursor else {
        return Ok((0, None));
    };
    let decoded = decode_cursor(cursor)?;
    let parsed = if let Some((id, index)) = decoded.split_once(':') {
        id.parse::<i64>()
            .ok()
            .zip(index.parse::<usize>().ok())
            .map(|(id, ix)| (id, Some(ix)))
    } else {
        decoded.parse::<i64>().ok().map(|id| (id, None))
    };
    parsed.filter(|(id, _)| *id > 0).ok_or_else(|| {
        JobsError::Api(crate::error::ApiError::new(
            crate::error::ErrorCode::Pagination,
            "cursor must be the next_cursor value from an earlier page, unchanged.",
        ))
    })
}

//! Metric history: the series of a run, downsampled queries and run comparison.
//!
//! A query cuts `[from, to]` into at most `max_points` equal groups and returns, per group,
//! the average with its min/max envelope and the number of points. Points come from the raw
//! table where it still has them (per step, the latest attempt that reached it) and from the
//! rollups for older ranges (step buckets on the step axis, minutes on the time axis).

use super::{field_error, parse_num, read_run, Telemetry};
use crate::app::AppState;
use crate::db::{Db, DbError};
use crate::modules::jobs::JobsError;
use crate::seams::RequestContext;
use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use mrl_domain::ids::{AttemptId, RunId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use time::OffsetDateTime;
use uuid::Uuid;

/// Default and largest `max_points`.
pub const DEFAULT_MAX_POINTS: i64 = 1000;
const MAX_MAX_POINTS: i64 = 10_000;
/// Series per query, and metrics per comparison.
const MAX_SERIES: usize = 20;
/// Runs per comparison.
const MAX_COMPARE: usize = 10;
const STEP_BUCKET: i64 = super::ingest::STEP_BUCKET;

/// One metric series of a run.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct MetricSeriesView {
    /// Series id (use it in `series=`).
    pub series_id: i64,
    /// Name, like `train/loss`.
    pub name: String,
    /// Split, like `train` or `eval` (may be empty).
    pub split: String,
    /// Rank, or null for the aggregate.
    pub rank: Option<i32>,
    /// `gauge`, `counter` or `histogram_summary`.
    pub kind: String,
    /// `live`, `sampled` or `delayed`.
    pub freshness: String,
    /// False when the engine manifest does not declare this metric.
    pub declared: bool,
}

/// The run's latest progress report.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RunProgressView {
    /// The attempt that sent it, `att_…`.
    pub attempt_id: String,
    /// Global step.
    pub step: i64,
    /// Planned steps.
    pub total_steps: i64,
    /// Epoch (fractional).
    pub epoch: f64,
    /// Phase, like `train` or `eval`.
    pub phase: String,
    /// Tokens seen so far.
    pub tokens_seen: i64,
    /// Estimated seconds left.
    pub eta_seconds: f64,
    /// When it arrived.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub updated_at: OffsetDateTime,
}

/// Peak resource use of one attempt, device and phase.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct ResourceActualView {
    /// `att_…`.
    pub attempt_id: String,
    /// Device, like `gpu0`; empty for the whole host.
    pub device: String,
    /// Phase.
    pub phase: String,
    /// What was measured, like `torch_allocated_peak`.
    pub signal: String,
    /// Highest value seen.
    pub peak: f64,
}

/// `GET /runs/{run}/metrics`.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RunMetrics {
    /// `run_…`.
    pub run_id: String,
    /// Every series, oldest first.
    pub series: Vec<MetricSeriesView>,
    /// Latest progress, if any arrived.
    pub progress: Option<RunProgressView>,
    /// Peak resource use from `resource/…` metrics.
    pub resources: Vec<ResourceActualView>,
}

/// One point of a query answer.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct MetricPointView {
    /// Start of the group: a step, or seconds since 1970 on the time axis.
    pub x: i64,
    /// Average of the group.
    pub value: f64,
    /// Smallest value in the group.
    pub min: f64,
    /// Largest value in the group.
    pub max: f64,
    /// Points in the group.
    pub n: i64,
}

/// One series of a query answer.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct MetricSeriesPoints {
    /// Series id.
    pub series_id: i64,
    /// Name.
    pub name: String,
    /// Split.
    pub split: String,
    /// At most `max_points` points.
    pub points: Vec<MetricPointView>,
}

/// `GET /runs/{run}/metrics/query`.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct MetricQueryResult {
    /// `run_…`.
    pub run_id: String,
    /// `step` or `time`.
    pub axis: String,
    /// The limit used.
    pub max_points: i64,
    /// The series asked for.
    pub series: Vec<MetricSeriesPoints>,
}

/// Query parameters of `GET /runs/{run}/metrics/query`.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct MetricQueryParams {
    /// Comma-separated series ids or metric names (a name means each of its splits); default:
    /// the first 20 series.
    pub series: Option<String>,
    /// `step` (default) or `time`.
    pub axis: Option<String>,
    /// First step (step axis).
    pub from_step: Option<String>,
    /// Last step (step axis).
    pub to_step: Option<String>,
    /// First second since 1970 (time axis).
    pub from_time: Option<String>,
    /// Last second since 1970 (time axis).
    pub to_time: Option<String>,
    /// 2–10000, default 1000.
    pub max_points: Option<String>,
}

/// X axis of a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Axis {
    /// Global step.
    Step,
    /// Seconds since 1970.
    Time,
}

/// The points of one series over `[from, to]` (each `None` = the series' own end), cut into
/// at most `max_points` groups.
pub async fn series_points(
    db: &Db,
    series: i64,
    axis: Axis,
    from: Option<i64>,
    to: Option<i64>,
    max_points: i64,
) -> Result<Vec<MetricPointView>, DbError> {
    let (lo, hi, raw_lo) = extent(db, series, axis).await?;
    let (Some(from), Some(to)) = (from.or(lo), to.or(hi)) else {
        return Ok(Vec::new());
    };
    if to < from {
        return Ok(Vec::new());
    }
    let width = group_width(from, to, max_points);
    let sql = match axis {
        Axis::Step => {
            "WITH raw AS (
               SELECT DISTINCT ON (step) step AS x, value FROM metric_points
                WHERE series_id = $1 AND step BETWEEN $2 AND $3
                ORDER BY step, attempt_no DESC, ts DESC),
             parts AS (
               SELECT x, value AS mn, value AS mx, value AS sm, 1::bigint AS n FROM raw
               UNION ALL
               SELECT bucket_start * $6, min, max, avg * n, n::bigint FROM metric_rollups
                WHERE series_id = $1 AND tier = 'step_bucket'
                  AND bucket_start * $6 BETWEEN $2 AND $3 AND (bucket_start + 1) * $6 <= $5)
             SELECT $2 + ((x - $2) / $4) * $4 AS g, min(mn) AS mn, max(mx) AS mx,
                    sum(sm) / sum(n) AS v, sum(n)::bigint AS n
               FROM parts GROUP BY 1 ORDER BY 1"
        }
        Axis::Time => {
            "WITH parts AS (
               SELECT floor(extract(epoch FROM ts))::bigint AS x, value AS mn, value AS mx, value AS sm,
                      1::bigint AS n FROM metric_points
                WHERE series_id = $1 AND ts >= to_timestamp($2) AND ts < to_timestamp($3 + 1)
               UNION ALL
               SELECT bucket_start * $6, min, max, avg * n, n::bigint FROM metric_rollups
                WHERE series_id = $1 AND tier = '1m'
                  AND bucket_start * $6 BETWEEN $2 AND $3 AND (bucket_start + 1) * $6 <= $5)
             SELECT $2 + ((x - $2) / $4) * $4 AS g, min(mn) AS mn, max(mx) AS mx,
                    sum(sm) / sum(n) AS v, sum(n)::bigint AS n
               FROM parts GROUP BY 1 ORDER BY 1"
        }
    };
    let bucket = match axis {
        Axis::Step => STEP_BUCKET,
        Axis::Time => 60,
    };
    let rows = sqlx::query(sql)
        .bind(series)
        .bind(from)
        .bind(to)
        .bind(width)
        .bind(raw_lo.unwrap_or(i64::MAX))
        .bind(bucket)
        .fetch_all(db.pool())
        .await?;
    rows.iter()
        .map(|r| {
            Ok(MetricPointView {
                x: r.try_get("g")?,
                value: r.try_get("v")?,
                min: r.try_get("mn")?,
                max: r.try_get("mx")?,
                n: r.try_get("n")?,
            })
        })
        .collect()
}

/// Group width: the smallest that cuts `[from, to]` into at most `max_points` groups.
pub fn group_width(from: i64, to: i64, max_points: i64) -> i64 {
    let span = i128::from(to) - i128::from(from) + 1;
    let max = i128::from(max_points.max(1));
    i64::try_from((span + max - 1) / max)
        .unwrap_or(i64::MAX)
        .max(1)
}

/// `(first, last, first raw)` x of a series: raw points and rollups together.
async fn extent(
    db: &Db,
    series: i64,
    axis: Axis,
) -> Result<(Option<i64>, Option<i64>, Option<i64>), DbError> {
    let sql = match axis {
        Axis::Step => {
            "SELECT least(r.lo, u.lo) AS lo, greatest(r.hi, u.hi) AS hi, r.lo AS raw_lo FROM
               (SELECT min(step) AS lo, max(step) AS hi FROM metric_points WHERE series_id = $1) r,
               (SELECT min(bucket_start) * 10 AS lo, max(bucket_start) * 10 + 9 AS hi
                  FROM metric_rollups WHERE series_id = $1 AND tier = 'step_bucket') u"
        }
        Axis::Time => {
            "SELECT least(r.lo, u.lo) AS lo, greatest(r.hi, u.hi) AS hi, r.lo AS raw_lo FROM
               (SELECT floor(extract(epoch FROM min(ts)))::bigint AS lo,
                       floor(extract(epoch FROM max(ts)))::bigint AS hi
                  FROM metric_points WHERE series_id = $1) r,
               (SELECT min(bucket_start) * 60 AS lo, max(bucket_start) * 60 + 59 AS hi
                  FROM metric_rollups WHERE series_id = $1 AND tier = '1m') u"
        }
    };
    let r = sqlx::query(sql).bind(series).fetch_one(db.pool()).await?;
    Ok((r.try_get("lo")?, r.try_get("hi")?, r.try_get("raw_lo")?))
}

fn series_view(r: &sqlx::postgres::PgRow) -> Result<MetricSeriesView, sqlx::Error> {
    Ok(MetricSeriesView {
        series_id: r.try_get("id")?,
        name: r.try_get("name")?,
        split: r.try_get("split")?,
        rank: r.try_get("rank")?,
        kind: r.try_get("kind")?,
        freshness: r.try_get("freshness")?,
        declared: r.try_get("declared")?,
    })
}

const SERIES_COLS: &str = "id, name, split, rank, kind, freshness, declared";

/// The latest progress of `run`.
pub async fn progress(db: &Db, run: Uuid) -> Result<Option<RunProgressView>, DbError> {
    let r = sqlx::query(
        "SELECT attempt_id, step, total_steps, epoch, phase, tokens_seen, eta_seconds, updated_at
           FROM run_progress WHERE run_id = $1",
    )
    .bind(run)
    .fetch_optional(db.pool())
    .await?;
    r.map(|r| {
        Ok::<_, sqlx::Error>(RunProgressView {
            attempt_id: AttemptId::from_uuid(r.try_get("attempt_id")?).to_string(),
            step: r.try_get("step")?,
            total_steps: r.try_get("total_steps")?,
            epoch: r.try_get("epoch")?,
            phase: r.try_get("phase")?,
            tokens_seen: r.try_get("tokens_seen")?,
            eta_seconds: r.try_get("eta_seconds")?,
            updated_at: r.try_get("updated_at")?,
        })
    })
    .transpose()
    .map_err(DbError::from)
}

/// `GET /runs/{run}/metrics`: every series, the latest progress and resource peaks.
#[utoipa::path(get, path = "/runs/{run}/metrics", tag = "telemetry", operation_id = "list_run_metrics",
    params(("run" = String, Path, description = "`run_…`")),
    responses((status = 200, body = RunMetrics), (status = 404, description = "No such run")))]
pub async fn list_series(
    State(state): State<AppState>,
    Extension(t): Extension<Arc<Telemetry>>,
    ctx: RequestContext,
    Path(run): Path<String>,
) -> Result<Json<RunMetrics>, JobsError> {
    let row = read_run(&state, &t.db, &ctx, &run, "metrics_logs.read").await?;
    let series = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {SERIES_COLS} FROM metric_series WHERE run_id = $1 ORDER BY id"
    )))
    .bind(row.id)
    .fetch_all(t.db.pool())
    .await?
    .iter()
    .map(series_view)
    .collect::<Result<Vec<_>, _>>()?;
    let resources = sqlx::query(
        "SELECT attempt_id, device, phase, signal, peak FROM resource_actuals
          WHERE run_id = $1 ORDER BY attempt_id, device, phase, signal",
    )
    .bind(row.id)
    .fetch_all(t.db.pool())
    .await?
    .iter()
    .map(|r| {
        Ok(ResourceActualView {
            attempt_id: AttemptId::from_uuid(r.try_get("attempt_id")?).to_string(),
            device: r.try_get("device")?,
            phase: r.try_get("phase")?,
            signal: r.try_get("signal")?,
            peak: r.try_get("peak")?,
        })
    })
    .collect::<Result<Vec<_>, sqlx::Error>>()?;
    Ok(Json(RunMetrics {
        run_id: RunId::from_uuid(row.id).to_string(),
        series,
        progress: progress(&t.db, row.id).await?,
        resources,
    }))
}

/// The series of `run` named by `tokens` (ids or names), or the first 20.
async fn pick_series(
    db: &Db,
    run: Uuid,
    tokens: &[String],
) -> Result<Vec<MetricSeriesView>, JobsError> {
    let (ids, names): (Vec<&String>, Vec<&String>) = tokens
        .iter()
        .partition(|t| t.bytes().all(|b| b.is_ascii_digit()));
    let ids: Vec<i64> = ids.iter().filter_map(|t| t.parse().ok()).collect();
    let names: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {SERIES_COLS} FROM metric_series WHERE run_id = $1
            AND ($4 OR id = ANY($2) OR name = ANY($3)) ORDER BY id LIMIT $5"
    )))
    .bind(run)
    .bind(&ids)
    .bind(&names)
    .bind(tokens.is_empty())
    .bind(i64::try_from(MAX_SERIES).unwrap_or(20))
    .fetch_all(db.pool())
    .await?;
    Ok(rows.iter().map(series_view).collect::<Result<_, _>>()?)
}

fn max_points(v: Option<i64>) -> Result<i64, JobsError> {
    let n = v.unwrap_or(DEFAULT_MAX_POINTS);
    if !(2..=MAX_MAX_POINTS).contains(&n) {
        return Err(field_error(
            "max_points",
            &format!("max_points {n} is outside 2–{MAX_MAX_POINTS}."),
            "Ask for between 2 and 10000 points.",
        ));
    }
    Ok(n)
}

/// `GET /runs/{run}/metrics/query`: at most `max_points` points per series, with a min/max
/// envelope; per step the latest attempt wins.
#[utoipa::path(get, path = "/runs/{run}/metrics/query", tag = "telemetry", operation_id = "query_run_metrics",
    params(("run" = String, Path, description = "`run_…`"), MetricQueryParams),
    responses((status = 200, body = MetricQueryResult), (status = 404, description = "No such run"),
        (status = 422, description = "A parameter is out of range")))]
pub async fn query(
    State(state): State<AppState>,
    Extension(t): Extension<Arc<Telemetry>>,
    ctx: RequestContext,
    Path(run): Path<String>,
    Query(q): Query<MetricQueryParams>,
) -> Result<Json<MetricQueryResult>, JobsError> {
    let row = read_run(&state, &t.db, &ctx, &run, "metrics_logs.read").await?;
    let axis = match q.axis.as_deref() {
        None | Some("step") => Axis::Step,
        Some("time") => Axis::Time,
        Some(other) => {
            return Err(field_error(
                "axis",
                &format!("axis {other:?} is not step or time."),
                "Use axis=step or axis=time.",
            ))
        }
    };
    let (from, to) = match axis {
        Axis::Step => (
            parse_num::<i64>(q.from_step.as_deref(), "from_step")?,
            parse_num::<i64>(q.to_step.as_deref(), "to_step")?,
        ),
        Axis::Time => (
            parse_num::<i64>(q.from_time.as_deref(), "from_time")?,
            parse_num::<i64>(q.to_time.as_deref(), "to_time")?,
        ),
    };
    let limit = max_points(parse_num(q.max_points.as_deref(), "max_points")?)?;
    let tokens: Vec<String> = q
        .series
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if tokens.len() > MAX_SERIES {
        return Err(field_error(
            "series",
            &format!(
                "{} series asked for; at most {MAX_SERIES} fit one query.",
                tokens.len()
            ),
            "Ask for fewer series, or split them over several queries.",
        ));
    }
    let mut out = Vec::new();
    for s in pick_series(&t.db, row.id, &tokens).await? {
        out.push(MetricSeriesPoints {
            points: series_points(&t.db, s.series_id, axis, from, to, limit).await?,
            series_id: s.series_id,
            name: s.name,
            split: s.split,
        });
    }
    Ok(Json(MetricQueryResult {
        run_id: RunId::from_uuid(row.id).to_string(),
        axis: match axis {
            Axis::Step => "step",
            Axis::Time => "time",
        }
        .to_owned(),
        max_points: limit,
        series: out,
    }))
}

/// `POST /runs:compare` body.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RunCompareRequest {
    /// 2–10 different `run_…` ids.
    pub run_ids: Vec<String>,
    /// Metric names to overlay (each split separately); default: the names every run has
    /// (at most 20).
    #[serde(default)]
    pub metrics: Vec<String>,
    /// Points per overlay line, 2–10000 (default 1000).
    #[serde(default)]
    pub max_points: Option<i64>,
}

/// One compared run.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RunCompareRun {
    /// `run_…`.
    pub run_id: String,
    /// Run name.
    pub name: String,
    /// Job state.
    pub state: String,
}

/// One config setting whose value differs between the runs.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RunCompareDiff {
    /// JSON pointer into the resolved config, like `/hyperparameters/lr`.
    pub path: String,
    /// Value per `run_…` (null where a run doesn't have it).
    #[schema(value_type = Object)]
    pub values: BTreeMap<String, Value>,
}

/// One metric overlaid across runs, on one step grid.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RunCompareSeries {
    /// Metric name.
    pub name: String,
    /// Split.
    pub split: String,
    /// Points per `run_…` (runs without this metric are left out); groups start at the same
    /// steps for every run.
    pub runs: BTreeMap<String, Vec<MetricPointView>>,
}

/// `POST /runs:compare` answer.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RunCompareResult {
    /// The runs, in the order asked.
    pub runs: Vec<RunCompareRun>,
    /// Only the settings that differ.
    pub config_diff: Vec<RunCompareDiff>,
    /// Overlays.
    pub metrics: Vec<RunCompareSeries>,
}

/// Every leaf of `v` (arrays count as one value) by JSON pointer.
pub fn flatten(v: &Value, prefix: &str, out: &mut BTreeMap<String, Value>) {
    match v {
        Value::Object(m) if !m.is_empty() => {
            for (k, x) in m {
                let k = k.replace('~', "~0").replace('/', "~1");
                flatten(x, &format!("{prefix}/{k}"), out);
            }
        }
        other => {
            out.insert(prefix.to_owned(), other.clone());
        }
    }
}

/// The settings whose values differ between `configs` (one per run id).
pub fn config_diff(configs: &[(String, Value)]) -> Vec<RunCompareDiff> {
    let flat: Vec<(String, BTreeMap<String, Value>)> = configs
        .iter()
        .map(|(id, c)| {
            let mut m = BTreeMap::new();
            flatten(c, "", &mut m);
            (id.clone(), m)
        })
        .collect();
    let paths: BTreeSet<&String> = flat.iter().flat_map(|(_, m)| m.keys()).collect();
    paths
        .into_iter()
        .filter_map(|p| {
            let values: BTreeMap<String, Value> = flat
                .iter()
                .map(|(id, m)| (id.clone(), m.get(p).cloned().unwrap_or(Value::Null)))
                .collect();
            let present = flat.iter().filter(|(_, m)| m.contains_key(p)).count();
            let first = values.values().next();
            let same = present == flat.len() && values.values().all(|v| Some(v) == first);
            (!same).then(|| RunCompareDiff {
                path: p.clone(),
                values,
            })
        })
        .collect()
}

/// `POST /runs:compare`: the config settings that differ, and metric overlays on one grid.
#[utoipa::path(post, path = "/runs:compare", tag = "telemetry", operation_id = "compare_runs",
    request_body = RunCompareRequest,
    responses((status = 200, body = RunCompareResult), (status = 404, description = "A run doesn't exist or is hidden"),
        (status = 422, description = "Fewer than 2 or more than 10 runs, or a repeated run")))]
pub async fn compare(
    State(state): State<AppState>,
    Extension(t): Extension<Arc<Telemetry>>,
    ctx: RequestContext,
    Json(req): Json<RunCompareRequest>,
) -> Result<Json<RunCompareResult>, JobsError> {
    let distinct: BTreeSet<&String> = req.run_ids.iter().collect();
    if !(2..=MAX_COMPARE).contains(&req.run_ids.len()) || distinct.len() != req.run_ids.len() {
        return Err(field_error(
            "run_ids",
            "Compare 2 to 10 different runs.",
            "List each run once, between 2 and 10 runs.",
        ));
    }
    if req.metrics.len() > MAX_SERIES {
        return Err(field_error(
            "metrics",
            &format!("At most {MAX_SERIES} metrics can be compared at once."),
            "Pick fewer metrics.",
        ));
    }
    let limit = max_points(req.max_points)?;
    let mut rows = Vec::new();
    for id in &req.run_ids {
        rows.push(read_run(&state, &t.db, &ctx, id, "metrics_logs.read").await?);
    }
    let mut configs = Vec::new();
    for r in &rows {
        let resolved: Option<Value> =
            sqlx::query_scalar("SELECT resolved FROM run_configs WHERE run_id = $1")
                .bind(r.id)
                .fetch_optional(t.db.pool())
                .await?;
        configs.push((
            RunId::from_uuid(r.id).to_string(),
            resolved.unwrap_or(Value::Null),
        ));
    }
    let ids: Vec<Uuid> = rows.iter().map(|r| r.id).collect();
    // (name, split) -> series id per run.
    let series = sqlx::query(
        "SELECT run_id, id, name, split FROM metric_series
          WHERE run_id = ANY($1) AND rank IS NULL AND (cardinality($2::text[]) = 0 OR name = ANY($2))
          ORDER BY name, split",
    )
    .bind(&ids)
    .bind(&req.metrics)
    .fetch_all(t.db.pool())
    .await?;
    let mut by_key: BTreeMap<(String, String), BTreeMap<Uuid, i64>> = BTreeMap::new();
    for r in &series {
        by_key
            .entry((r.try_get("name")?, r.try_get("split")?))
            .or_default()
            .insert(r.try_get("run_id")?, r.try_get("id")?);
    }
    if req.metrics.is_empty() {
        by_key.retain(|_, runs| runs.len() == ids.len());
    }
    let mut metrics = Vec::new();
    for ((name, split), runs) in by_key.into_iter().take(MAX_SERIES) {
        // One grid for every run: the union of their ranges.
        let (mut lo, mut hi) = (None::<i64>, None::<i64>);
        for s in runs.values() {
            let (a, b, _) = extent(&t.db, *s, Axis::Step).await?;
            lo = [lo, a].into_iter().flatten().min();
            hi = [hi, b].into_iter().flatten().max();
        }
        let mut lines = BTreeMap::new();
        for (run, s) in &runs {
            let pts = series_points(&t.db, *s, Axis::Step, lo, hi, limit).await?;
            lines.insert(RunId::from_uuid(*run).to_string(), pts);
        }
        metrics.push(RunCompareSeries {
            name,
            split,
            runs: lines,
        });
    }
    Ok(Json(RunCompareResult {
        runs: rows
            .iter()
            .map(|r| RunCompareRun {
                run_id: RunId::from_uuid(r.id).to_string(),
                name: r.name.clone(),
                state: r.state.clone(),
            })
            .collect(),
        config_diff: config_diff(&configs),
        metrics,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn group_width_keeps_groups_within_the_limit() {
        assert_eq!(group_width(0, 999_999, 2000), 500);
        assert_eq!(group_width(0, 9, 2000), 1);
        assert_eq!(group_width(5, 5, 2), 1);
        assert_eq!(group_width(0, 1000, 1000), 2);
        let w = group_width(0, i64::MAX, 2);
        assert!(w > 0);
    }

    #[test]
    fn config_diff_keeps_only_differing_keys() {
        let a =
            json!({"lr": 0.1, "opt": {"name": "adam", "beta": [0.9, 0.99]}, "seed": 1, "a/b": 1});
        let b = json!({"lr": 0.2, "opt": {"name": "adam", "beta": [0.9, 0.98]}, "seed": 1, "extra": true, "a/b": 1});
        let d = config_diff(&[("run_a".into(), a), ("run_b".into(), b)]);
        let paths: Vec<&str> = d.iter().map(|d| d.path.as_str()).collect();
        assert_eq!(paths, vec!["/extra", "/lr", "/opt/beta"]);
        assert_eq!(d[0].values["run_a"], Value::Null);
        assert_eq!(d[1].values["run_b"], json!(0.2));
    }
}

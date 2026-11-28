//! Resource estimates (Doc 07 §6): a range with a confidence label, never a guarantee.
//!
//! - The range comes from the engine's own `estimate()` in a dry-run probe
//!   (`submission::probe`); it is stored as it came in `resource_estimates`.
//! - [`Calibration`]: per configuration class ([`calibration_key`]: engine × method × PEFT ×
//!   GPU bucket) the mean and spread of `ln(actual peak / analytic expected)` over finished
//!   runs. [`Calibrate`] (event consumer, `job.completed`) adds one sample per attempt; the
//!   next estimate of that class is corrected with it (§6.11).
//! - `GET /runs/{run}/resources`: the estimate next to the measured peaks, per phase.

use crate::app::AppState;
use crate::db::Db;
use crate::modules::events::{ConsumerError, Event, EventConsumer};
use crate::modules::jobs::scope;
use crate::modules::jobs::{JobSpec, JobsError};
use crate::seams::RequestContext;
use crate::Tx;
use axum::extract::{Path, State};
use axum::{Extension, Json};
use mrl_domain::ids::{AttemptId, RunId};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

const GIB: f64 = (1u64 << 30) as f64;

/// The four preflight verdicts (Doc 24 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Expected to fit with the machine's headroom kept free.
    Fits,
    /// Close to the limit: the run starts with safety settings.
    Tight,
    /// Expected to run out of memory: the run does not start.
    DoesNotFit,
    /// No estimate or no machine to compare with: the run starts in probe mode.
    Unknown,
}

/// How far an estimate can be trusted (Doc 07 §6.11).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Confidence {
    /// At least 20 finished runs of this kind, close together.
    High,
    /// At least 5 finished runs of this kind.
    Medium,
    /// The engine's formulas only.
    Low,
}

/// A low–expected–high range.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, utoipa::ToSchema)]
pub struct EstimateRange {
    /// Low end.
    pub lo: f64,
    /// Most likely value.
    pub expected: f64,
    /// High end.
    pub hi: f64,
}

impl EstimateRange {
    /// The engine's `lo..hi` with the midpoint as the expected value.
    pub fn analytic(lo: f64, hi: f64) -> Self {
        // ponytail: the Worker Protocol carries only lo/hi; the midpoint stands for P15's
        // `expected`. Add an `expected` field to ResourceEstimate if the two drift apart.
        Self {
            lo,
            expected: (lo + hi) / 2.0,
            hi,
        }
    }
}

/// Calibration statistics of one configuration class.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Calibration {
    /// Samples.
    pub n: u32,
    /// Mean of `ln(actual / expected)`.
    pub mean: f64,
    /// Sample standard deviation of the same (0 with fewer than two samples).
    pub sd: f64,
}

impl Calibration {
    /// From `n`, `Σx` and `Σx²`.
    pub fn from_sums(n: i64, sum: f64, sum_sq: f64) -> Self {
        if n <= 0 {
            return Self::default();
        }
        let nf = n as f64;
        let mean = sum / nf;
        let sd = if n < 2 {
            0.0
        } else {
            ((sum_sq - nf * mean * mean) / (nf - 1.0)).max(0.0).sqrt()
        };
        Self {
            n: u32::try_from(n).unwrap_or(u32::MAX),
            mean,
            sd,
        }
    }

    /// `high` if n ≥ 20 and σ ≤ 0.08; `medium` if n ≥ 5 and σ ≤ 0.15; else `low`.
    pub fn confidence(&self) -> Confidence {
        if self.n >= 20 && self.sd <= 0.08 {
            Confidence::High
        } else if self.n >= 5 && self.sd <= 0.15 {
            Confidence::Medium
        } else {
            Confidence::Low
        }
    }

    /// The corrected range: `expected × e^μ`, `× e^(±1.64σ)`, never narrower than ±5 %.
    /// With `low` confidence the engine's own range is kept.
    pub fn apply(&self, analytic: EstimateRange) -> EstimateRange {
        if self.confidence() == Confidence::Low {
            return analytic;
        }
        let expected = analytic.expected * self.mean.exp();
        let spread = (1.64 * self.sd).exp().max(1.05);
        EstimateRange {
            lo: expected / spread,
            expected,
            hi: expected * spread,
        }
    }
}

/// The configuration class of a run: engine × method × PEFT mode × GPU bucket.
pub fn calibration_key(engine: &str, algorithm: &str, resolved: &Value, gpu: &str) -> String {
    let peft = resolved
        .pointer("/common/update/strategy")
        .and_then(Value::as_str)
        .unwrap_or("full");
    format!("{engine}|{algorithm}|{peft}|{gpu}")
}

/// One stored estimate.
#[derive(Debug, Clone, PartialEq)]
pub struct EstimateRow {
    /// Id.
    pub id: Uuid,
    /// Configuration class.
    pub calibration_key: String,
    /// Calibration captured when the probe answered.
    pub calibration: Calibration,
    /// `<engine>@<version>`.
    pub estimator_version: String,
    /// The engine's range per GPU, GiB (absent when the engine gave none).
    pub gpu_mem: Option<(f64, f64)>,
    /// Planned optimizer steps.
    pub total_steps: Option<i64>,
    /// Wall time range, seconds.
    pub wall: Option<(f64, f64)>,
    /// What the range assumes.
    pub assumptions: Vec<String>,
}

fn row_to_estimate(r: &sqlx::postgres::PgRow) -> Result<EstimateRow, sqlx::Error> {
    let lo: Option<f64> = r.try_get("gpu_mem_lo_gib")?;
    let hi: Option<f64> = r.try_get("gpu_mem_hi_gib")?;
    let wlo: Option<f64> = r.try_get("wall_lo_seconds")?;
    let whi: Option<f64> = r.try_get("wall_hi_seconds")?;
    let assumptions: Value = r.try_get("assumptions")?;
    Ok(EstimateRow {
        id: r.try_get("id")?,
        calibration_key: r.try_get("calibration_key")?,
        calibration: Calibration::from_sums(
            i64::from(
                r.try_get::<Option<i32>, _>("calibration_n")?
                    .unwrap_or_default(),
            ),
            r.try_get::<Option<f64>, _>("calibration_sum_ln")?
                .unwrap_or_default(),
            r.try_get::<Option<f64>, _>("calibration_sum_ln_sq")?
                .unwrap_or_default(),
        ),
        estimator_version: r.try_get("estimator_version")?,
        gpu_mem: lo.zip(hi),
        total_steps: r.try_get("total_steps")?,
        wall: wlo.zip(whi),
        assumptions: assumptions
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|a| a.as_str().map(str::to_owned))
            .collect(),
    })
}

macro_rules! estimate_cols {
    () => {
        "id, calibration_key, estimator_version, gpu_mem_lo_gib, gpu_mem_hi_gib, total_steps, \
         wall_lo_seconds, wall_hi_seconds, assumptions, calibration_n, calibration_sum_ln, calibration_sum_ln_sq"
    };
}

/// Fingerprint of the workload the estimator checked. Keep the version tag when adding fields.
/// A role's `gpu.min_vram_bytes` is left out: submit derives it from the estimate itself
/// ([`crate::modules::submission::policy::require_vram`]).
pub fn inputs_sha256(spec: &JobSpec) -> Vec<u8> {
    let mut resources = spec.resources.clone();
    for role in &mut resources.roles {
        role.gpu.min_vram_bytes = None;
    }
    crate::modules::submission::resolve::sha256(&json!({
        "version": 2,
        "kind": spec.kind,
        "algorithm": spec.algorithm_id,
        "config": spec.resolved,
        "model": spec.base_model_version_id.to_string(),
        "reward": spec.reward_version_id.map(|id| id.to_string()),
        "datasets": spec.dataset_version_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "resources": resources,
    }))
}

/// The estimate selected when the run was submitted. Older adjusted aliases never measured
/// the saved config and are treated as unknown.
pub(crate) async fn estimate_for_run(
    conn: &mut sqlx::PgConnection,
    run: Uuid,
) -> Result<Option<EstimateRow>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT e.id, e.calibration_key, e.estimator_version, e.gpu_mem_lo_gib, e.gpu_mem_hi_gib,
                e.total_steps, e.wall_lo_seconds, e.wall_hi_seconds, e.assumptions,
                e.calibration_n, e.calibration_sum_ln, e.calibration_sum_ln_sq
           FROM run_configs c JOIN runs r ON r.id = c.run_id
           JOIN resource_estimates e ON e.id = c.estimate_id
          WHERE c.run_id = $1 AND e.org_id = r.org_id AND e.project_id = r.project_id
            AND e.state = 'READY' AND e.ok IS TRUE AND e.gpu_mem_hi_gib > 0
            AND e.probe_spec->'resolved' = c.resolved",
    )
    .bind(run)
    .fetch_optional(conn)
    .await?;
    row.as_ref().map(row_to_estimate).transpose()
}

/// The newest successful estimate of these exact inputs in the project.
pub async fn latest(
    db: &Db,
    org: Uuid,
    project: Uuid,
    sha: &[u8],
) -> Result<Option<EstimateRow>, sqlx::Error> {
    let row = sqlx::query(concat!(
        "SELECT ",
        estimate_cols!(),
        " FROM resource_estimates
         WHERE org_id = $1 AND project_id = $2 AND state = 'READY' AND inputs_sha256 = $3
           AND ok IS TRUE AND gpu_mem_hi_gib > 0
         ORDER BY decided_at DESC, id DESC LIMIT 1"
    ))
    .bind(org)
    .bind(project)
    .bind(sha)
    .fetch_optional(db.pool())
    .await?;
    row.as_ref().map(row_to_estimate).transpose()
}

/// Estimate `id`, once answered.
pub async fn ready(db: &Db, id: Uuid) -> Result<Option<EstimateRow>, sqlx::Error> {
    let row = sqlx::query(concat!(
        "SELECT ",
        estimate_cols!(),
        " FROM resource_estimates WHERE id = $1 AND state = 'READY'"
    ))
    .bind(id)
    .fetch_optional(db.pool())
    .await?;
    row.as_ref().map(row_to_estimate).transpose()
}

/// How the estimate was corrected.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct CalibrationView {
    /// Configuration class: engine|method|PEFT|GPU.
    pub key: String,
    /// Finished runs of this class.
    pub n: u32,
    /// Correction applied to the expected value (`e^μ`; 1 = none).
    pub factor: f64,
    /// Spread of the measurements (standard deviation of the log ratio).
    pub spread: f64,
}

/// An estimate as the API shows it (Doc 09 §6.5 `Estimate`).
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RunEstimate {
    /// `fits`, `tight`, `does_not_fit` or `unknown`.
    pub verdict: Verdict,
    /// `high`, `medium` or `low`.
    pub confidence: Confidence,
    /// GPU memory per GPU, GiB, after calibration.
    pub per_gpu_memory_gib: Option<EstimateRange>,
    /// The engine's own range before calibration, GiB.
    pub analytic_gpu_memory_gib: Option<EstimateRange>,
    /// Planned optimizer steps.
    pub total_steps: Option<i64>,
    /// Wall time, seconds.
    pub wall_seconds: Option<EstimateRange>,
    /// The correction from finished runs of the same kind.
    pub calibration: Option<CalibrationView>,
    /// What the range assumes, in plain words.
    pub assumptions: Vec<String>,
    /// Id of the stored estimate (a UUID), when there is one.
    pub estimate_id: Option<String>,
}

impl RunEstimate {
    /// No estimate.
    pub fn unknown(why: impl Into<String>) -> Self {
        Self {
            verdict: Verdict::Unknown,
            confidence: Confidence::Low,
            per_gpu_memory_gib: None,
            analytic_gpu_memory_gib: None,
            total_steps: None,
            wall_seconds: None,
            calibration: None,
            assumptions: vec![why.into()],
            estimate_id: None,
        }
    }

    /// `row` corrected by `cal`; the verdict starts `unknown` (the preflight sets it).
    pub fn of(row: &EstimateRow) -> Self {
        let cal = row.calibration;
        let analytic = row.gpu_mem.map(|(lo, hi)| EstimateRange::analytic(lo, hi));
        Self {
            verdict: Verdict::Unknown,
            confidence: if analytic.is_some() {
                cal.confidence()
            } else {
                Confidence::Low
            },
            per_gpu_memory_gib: analytic.map(|a| cal.apply(a)),
            analytic_gpu_memory_gib: analytic,
            total_steps: row.total_steps,
            wall_seconds: row.wall.map(|(lo, hi)| EstimateRange::analytic(lo, hi)),
            calibration: Some(CalibrationView {
                key: row.calibration_key.clone(),
                n: cal.n,
                factor: if cal.confidence() == Confidence::Low {
                    1.0
                } else {
                    cal.mean.exp()
                },
                spread: cal.sd,
            }),
            assumptions: row.assumptions.clone(),
            estimate_id: Some(row.id.to_string()),
        }
    }
}

/// Tracks completed attempts and reconciles their calibration against arriving measurements.
pub struct Calibrate(Db);

impl Calibrate {
    /// Creates a calibration consumer and reconciler using this installation's database.
    pub fn new(db: Db) -> Self {
        Self(db)
    }

    /// Applies new or changed peaks, with one contribution per attempt.
    pub async fn reconcile(&self) -> Result<(), ConsumerError> {
        let mut tx = self.0.begin().await?;
        let attempts: Vec<Uuid> = sqlx::query_scalar(
            "SELECT i.attempt_id FROM estimator_calibration_inputs i
             JOIN estimator_calibration_measurements m ON m.attempt_id = i.attempt_id
             LEFT JOIN estimator_calibration_samples s ON s.attempt_id = i.attempt_id
             WHERE m.ratio IS DISTINCT FROM s.ratio
             ORDER BY i.attempt_id LIMIT 100 FOR UPDATE OF i SKIP LOCKED",
        )
        .fetch_all(&mut *tx)
        .await?;
        for attempt in attempts {
            self.sample(&mut tx, attempt).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn sample(&self, tx: &mut Tx, attempt: Uuid) -> Result<(), ConsumerError> {
        // Read after locking the input so overlapping reconcilers cannot apply a stale delta.
        let Some(row) = sqlx::query(
            "SELECT m.*, s.ratio AS previous_ratio, s.key AS previous_key
               FROM estimator_calibration_measurements m
               LEFT JOIN estimator_calibration_samples s ON s.attempt_id = m.attempt_id
              WHERE m.attempt_id = $1 AND m.ratio IS DISTINCT FROM s.ratio",
        )
        .bind(attempt)
        .fetch_optional(&mut **tx)
        .await?
        else {
            return Ok(());
        };
        let key = match row.try_get::<Option<String>, _>("previous_key")? {
            Some(key) => key,
            None => calibration_key(
                row.try_get("estimator_version")?,
                row.try_get("algorithm_id")?,
                &row.try_get("resolved")?,
                row.try_get("architecture")?,
            ),
        };
        let ratio: f64 = row.try_get("ratio")?;
        let previous: Option<f64> = row.try_get("previous_ratio")?;
        sqlx::query(
            "INSERT INTO estimator_calibration_samples (attempt_id, key, estimate_id, ratio)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (attempt_id) DO UPDATE SET ratio = EXCLUDED.ratio, at = now()",
        )
        .bind(attempt)
        .bind(&key)
        .bind(row.try_get::<Uuid, _>("estimate_id")?)
        .bind(ratio)
        .execute(&mut **tx)
        .await?;
        let x = ratio.ln();
        let old = previous.map_or(0.0, f64::ln);
        sqlx::query(
            "INSERT INTO estimator_calibration (key, n, sum_ln, sum_ln_sq) VALUES ($1, $2, $3, $4)
             ON CONFLICT (key) DO UPDATE SET n = estimator_calibration.n + EXCLUDED.n,
               sum_ln = estimator_calibration.sum_ln + EXCLUDED.sum_ln,
               sum_ln_sq = estimator_calibration.sum_ln_sq + EXCLUDED.sum_ln_sq,
               updated_at = now()",
        )
        .bind(key)
        .bind(i32::from(previous.is_none()))
        .bind(x - old)
        .bind(x * x - old * old)
        .execute(&mut **tx)
        .await?;
        crate::modules::submission::audit(
            &self.0,
            tx,
            (row.try_get("org_id")?, row.try_get("project_id")?),
            "system:calibration",
            "submission.calibrate",
            AttemptId::from_uuid(attempt).to_string(),
        )
        .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl EventConsumer for Calibrate {
    fn name(&self) -> &'static str {
        "estimates-calibration"
    }

    fn accepts(&self, event_type: &str) -> bool {
        event_type == "job.completed"
    }

    async fn handle(&self, tx: &mut Tx, event: &Event) -> Result<(), ConsumerError> {
        let Some((run, attempt)) = sqlx::query_as::<_, (Uuid, Uuid)>(
            "SELECT run_id, current_attempt_id FROM jobs
              WHERE id = $1 AND run_id IS NOT NULL AND current_attempt_id IS NOT NULL",
        )
        .bind(event.aggregate_id)
        .fetch_optional(&mut **tx)
        .await?
        else {
            return Ok(());
        };
        let Some(estimate) = estimate_for_run(tx, run).await? else {
            return Ok(());
        };
        sqlx::query(
            "INSERT INTO estimator_calibration_inputs (attempt_id, estimate_id)
             VALUES ($1, $2) ON CONFLICT (attempt_id) DO NOTHING",
        )
        .bind(attempt)
        .bind(estimate.id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

/// One measured peak.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct ResourceActual {
    /// `att_…`.
    pub attempt_id: String,
    /// GPU UUID, or empty for the whole host.
    pub device: String,
    /// Phase, like `update`.
    pub phase: String,
    /// `process_peak`, `torch_reserved_peak`, `torch_allocated_peak`, `host_rss_peak` or
    /// `scratch_peak`.
    pub signal: String,
    /// Peak, GiB.
    pub peak_gib: f64,
}

/// Estimated vs measured for one phase.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct PhaseComparison {
    /// Phase.
    pub phase: String,
    /// Estimated GPU memory per GPU, GiB (the run-wide estimate).
    pub estimate_gib: Option<EstimateRange>,
    /// Highest per-GPU peak measured in this phase (process peak, else reserved), GiB.
    pub actual_peak_gib: Option<f64>,
    /// `actual / expected`.
    pub ratio: Option<f64>,
}

/// `GET /runs/{run}/resources`.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RunResources {
    /// `run_…`.
    pub run_id: String,
    /// The estimate of the run's config, if one was made (`runs:validate`).
    pub estimate: Option<RunEstimate>,
    /// Per phase: the estimate next to the measured peak.
    pub phases: Vec<PhaseComparison>,
    /// Every measured peak of every attempt.
    pub actual: Vec<ResourceActual>,
}

/// `GET /runs/{run}/resources`: the estimate next to the measured peaks, per phase.
#[utoipa::path(get, path = "/runs/{run}/resources", operation_id = "get_run_resources",
    tag = "runs", params(("run" = String, Path, description = "Run id, `run_…`.")),
    responses((status = 200, description = "Estimate vs actual.", body = RunResources),
              (status = 404, description = "No such run, or hidden from you.", body = crate::error::Problem)))]
pub async fn resources(
    State(state): State<AppState>,
    Extension(db): Extension<Db>,
    Extension(headroom): Extension<crate::modules::preflight::Headroom>,
    ctx: RequestContext,
    Path(run): Path<String>,
) -> Result<Json<RunResources>, JobsError> {
    let id: RunId = scope::parse_id(&run, "run")?;
    let row = crate::modules::runs::api::find_run(&db, id.as_uuid())
        .await?
        .ok_or_else(|| JobsError::not_found("run"))?;
    let res = scope::resource("run", Some(id.to_string()), row.org_id, row.project_id);
    scope::authorize(&state, &ctx, "runs.read", &res).await?;
    let matched = {
        let mut conn = db.pool().acquire().await?;
        estimate_for_run(&mut conn, row.id).await?
    };
    let estimate = if let Some(e) = matched {
        let mut view = RunEstimate::of(&e);
        view.verdict = crate::modules::preflight::verdict_on_last_machine(
            &db,
            &headroom,
            row.job_id,
            view.per_gpu_memory_gib,
        )
        .await?;
        Some(view)
    } else {
        None
    };
    let rows: Vec<(Uuid, String, String, String, f64)> = sqlx::query_as(
        "SELECT attempt_id, device, phase, signal, peak FROM resource_actuals
          WHERE run_id = $1 AND org_id = $2 ORDER BY attempt_id, phase, device, signal",
    )
    .bind(row.id)
    .bind(row.org_id)
    .fetch_all(db.pool())
    .await?;
    let actual: Vec<ResourceActual> = rows
        .into_iter()
        .map(|(a, device, phase, signal, peak)| ResourceActual {
            attempt_id: AttemptId::from_uuid(a).to_string(),
            device,
            phase,
            signal,
            peak_gib: peak / GIB,
        })
        .collect();
    let range = estimate.as_ref().and_then(|e| e.per_gpu_memory_gib);
    Ok(Json(RunResources {
        run_id: id.to_string(),
        phases: compare_phases(&actual, range),
        estimate,
        actual,
    }))
}

/// Per phase: the highest per-GPU process peak (else reserved) against `range`.
pub fn compare_phases(
    actual: &[ResourceActual],
    range: Option<EstimateRange>,
) -> Vec<PhaseComparison> {
    let mut phases: Vec<&str> = actual.iter().map(|a| a.phase.as_str()).collect();
    phases.sort_unstable();
    phases.dedup();
    phases
        .into_iter()
        .map(|phase| {
            let peak = |signal: &str| {
                actual
                    .iter()
                    .filter(|a| a.phase == phase && !a.device.is_empty() && a.signal == signal)
                    .map(|a| a.peak_gib)
                    .fold(None, |m: Option<f64>, v| Some(m.map_or(v, |m| m.max(v))))
            };
            let actual_peak_gib = peak("process_peak")
                .or_else(|| peak("torch_reserved_peak"))
                .or_else(|| peak("torch_allocated_peak"));
            PhaseComparison {
                phase: phase.to_owned(),
                estimate_gib: range,
                ratio: actual_peak_gib
                    .zip(range)
                    .filter(|(_, r)| r.expected > 0.0)
                    .map(|(a, r)| a / r.expected),
                actual_peak_gib,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sums(xs: &[f64]) -> Calibration {
        let s: f64 = xs.iter().sum();
        let q: f64 = xs.iter().map(|x| x * x).sum();
        Calibration::from_sums(xs.len() as i64, s, q)
    }

    #[test]
    fn calibration_math() {
        let none = Calibration::from_sums(0, 0.0, 0.0);
        assert_eq!((none.n, none.mean, none.sd), (0, 0.0, 0.0));
        let one = sums(&[0.1]);
        assert!((one.mean - 0.1).abs() < 1e-12 && one.sd == 0.0);
        let c = sums(&[0.1, 0.2, 0.3]);
        assert!((c.mean - 0.2).abs() < 1e-12);
        assert!((c.sd - 0.1).abs() < 1e-9, "sample sd: {}", c.sd);
        // Confidence thresholds (Doc 07 §6.11).
        assert_eq!(sums(&[0.1; 4]).confidence(), Confidence::Low);
        assert_eq!(sums(&[0.1; 5]).confidence(), Confidence::Medium);
        assert_eq!(sums(&[0.1; 20]).confidence(), Confidence::High);
        let wide: Vec<f64> = (0..20)
            .map(|i| if i % 2 == 0 { 0.0 } else { 0.3 })
            .collect();
        assert_eq!(sums(&wide).confidence(), Confidence::Low, "σ ≈ 0.154");
        let mid: Vec<f64> = (0..20)
            .map(|i| if i % 2 == 0 { 0.0 } else { 0.2 })
            .collect();
        assert_eq!(sums(&mid).confidence(), Confidence::Medium, "σ ≈ 0.10");
    }

    #[test]
    fn calibration_corrects_and_bounds_the_range() {
        let analytic = EstimateRange::analytic(8.0, 12.0);
        assert_eq!(analytic.expected, 10.0);
        // Low confidence keeps the engine's range.
        assert_eq!(sums(&[0.5; 3]).apply(analytic), analytic);
        // Exact agreement still keeps ±5 %.
        let flat = sums(&[0.0; 20]).apply(analytic);
        assert!((flat.expected - 10.0).abs() < 1e-9);
        assert!((flat.hi - 10.5).abs() < 1e-9 && (flat.lo - 10.0 / 1.05).abs() < 1e-9);
        // Runs that use 20 % more move the expected value up.
        let up = sums(&[1.2f64.ln(); 20]).apply(analytic);
        assert!((up.expected - 12.0).abs() < 1e-9, "{up:?}");
        let c = sums(&[0.0, 0.2, 0.0, 0.2, 0.0, 0.2]);
        let r = c.apply(analytic);
        let k = (1.64 * c.sd).exp();
        assert!((r.hi / r.expected - k).abs() < 1e-9 && (r.expected / r.lo - k).abs() < 1e-9);
    }

    #[test]
    fn keys_name_the_configuration_class() {
        let cfg = json!({"common": {"update": {"strategy": "lora"}}});
        assert_eq!(
            calibration_key("trl", "sft", &cfg, "nvidia"),
            "trl|sft|lora|nvidia"
        );
        assert_eq!(
            calibration_key("trl", "cpt", &json!({}), "cpu"),
            "trl|cpt|full|cpu"
        );
    }

    #[test]
    fn phases_compare_the_process_peak() {
        let a = |phase: &str, device: &str, signal: &str, peak_gib: f64| ResourceActual {
            attempt_id: "att_1".into(),
            device: device.into(),
            phase: phase.into(),
            signal: signal.into(),
            peak_gib,
        };
        let actual = [
            a("update", "GPU-0", "process_peak", 11.0),
            a("update", "GPU-1", "process_peak", 9.0),
            a("update", "", "host_rss_peak", 30.0),
            a("eval", "GPU-0", "torch_reserved_peak", 5.0),
        ];
        let range = EstimateRange::analytic(8.0, 12.0);
        let got = compare_phases(&actual, Some(range));
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].phase, "eval");
        assert_eq!(
            got[0].actual_peak_gib,
            Some(5.0),
            "reserved when no process peak"
        );
        assert_eq!(got[1].actual_peak_gib, Some(11.0), "host rows don't count");
        assert_eq!(got[1].ratio, Some(1.1));
        assert_eq!(compare_phases(&actual, None)[1].ratio, None);
    }
}

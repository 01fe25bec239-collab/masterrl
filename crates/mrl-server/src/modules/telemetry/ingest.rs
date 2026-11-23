//! The lane sinks and what they write.
//!
//! Every relayed worker message names its attempt. A message is taken only when the attempt
//! belongs to a run, its fencing epoch matches and the sending node runs it (a placement or a
//! `LaunchAttempt` command for that node); anything else is dropped and counted
//! (`mrl_telemetry_dropped_total{reason}`). Every delivery claims `(node, stream, seq)`
//! in the transaction that stores its effects.

use super::observer::{IngestedBatch, MetricObservers, ObservedPoint};
use super::storage_org;
use crate::db::{Db, DbError, Tx};
use crate::modules::artifacts::ArtifactStore;
use crate::modules::nodes::lanes::{LaneItem, LaneKind, LaneSink, SinkError};
use mrl_domain::ids::{AttemptId, RunId};
use mrl_proto::agent::v1::{agent_message::Body, WorkerRelay};
use mrl_proto::worker::v1::worker_message::Body as WorkerBody;
use mrl_proto::worker::v1::{
    LogChunk, LogStream, MetricKind, MetricPoint, Progress, RewardBatchStats, RolloutBatchSummary,
    SampleBatch, WorkerMessage,
};
use prometheus::{IntCounterVec, Opts};
use serde_json::json;
use sha2::Digest;
use sqlx::Row;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, OnceLock};
use time::OffsetDateTime;
use uuid::Uuid;

/// Largest stored log chunk; bigger relayed chunks are split.
pub const MAX_CHUNK_BYTES: usize = 64 * 1024;
/// Rows per step rollup bucket (`bucket_start = step / STEP_BUCKET`).
pub const STEP_BUCKET: i64 = 10;
/// Longest metric name and split.
const MAX_NAME: usize = 200;
const MAX_SPLIT: usize = 50;
/// Largest accepted step (bucket arithmetic in SQL must not overflow).
pub const MAX_STEP: i64 = 1 << 50;

/// The run attempt a relayed message belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Owner {
    /// Org of the run.
    pub org: Uuid,
    /// The run.
    pub run: Uuid,
    /// The attempt.
    pub attempt: Uuid,
    /// Its number (later attempts win per step).
    pub attempt_no: i32,
}

/// The gateway stream position of one relayed message.
#[derive(Clone, Copy)]
pub struct Delivery {
    /// The node that sent the envelope.
    pub node: Uuid,
    /// The protobuf stream that carried it.
    pub stream: mrl_proto::agent::v1::Lane,
    /// The gateway sequence number.
    pub seq: u64,
}

impl From<&LaneItem> for Delivery {
    fn from(item: &LaneItem) -> Self {
        Self {
            node: item.node_id,
            stream: item.stream,
            seq: item.seq,
        }
    }
}

impl Delivery {
    async fn claim(self, tx: &mut Tx) -> Result<bool, DbError> {
        let inserted = sqlx::query(
            "INSERT INTO telemetry_receipts (node_id, stream, seq)
             VALUES ($1, $2, $3::text::numeric) ON CONFLICT DO NOTHING",
        )
        .bind(self.node)
        .bind(self.stream as i32)
        .bind(self.seq.to_string())
        .execute(&mut **tx)
        .await?;
        if inserted.rows_affected() == 0 {
            dropped("replay");
            return Ok(false);
        }
        Ok(true)
    }
}

// Take the run lock before assigning any cursor ID so a page can't cross an uncommitted ID.
async fn lock_run(tx: &mut Tx, run: Uuid) -> Result<(), DbError> {
    sqlx::query("SELECT id FROM runs WHERE id = $1 FOR NO KEY UPDATE")
        .bind(run)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Writes what the sinks receive.
pub struct Ingest {
    db: Db,
    store: Arc<ArtifactStore>,
    observers: Arc<MetricObservers>,
    max_series: i64,
}

/// `mrl_telemetry_dropped_total{reason}` += 1.
pub(crate) fn dropped(reason: &str) {
    static C: OnceLock<Option<IntCounterVec>> = OnceLock::new();
    let c = C.get_or_init(|| {
        let c = IntCounterVec::new(
            Opts::new(
                "mrl_telemetry_dropped_total",
                "Relayed worker telemetry the server did not store, by reason.",
            ),
            &["reason"],
        )
        .ok()?;
        let _ = prometheus::default_registry().register(Box::new(c.clone()));
        Some(c)
    });
    if let Some(c) = c {
        c.with_label_values(&[reason]).inc();
    }
}

fn is_missing_partition(e: &DbError) -> bool {
    matches!(e, DbError::Query(sqlx::Error::Database(d)) if d.code().as_deref() == Some("23514")
        && d.message().contains("no partition"))
}

fn sink_error(sink: &'static str, e: impl std::fmt::Display) -> SinkError {
    SinkError {
        sink,
        detail: e.to_string(),
    }
}

/// The relay, worker message and body of an agent message, if it relays one.
fn relayed(item: &LaneItem) -> Option<(&WorkerRelay, &WorkerMessage, &WorkerBody)> {
    let Some(Body::Worker(relay)) = &item.message.body else {
        return None;
    };
    let msg = relay.message.as_ref()?;
    Some((relay, msg, msg.body.as_ref()?))
}

fn timestamp(t: Option<&prost_types::Timestamp>) -> Option<OffsetDateTime> {
    let t = t?;
    let nanos = i128::from(t.seconds) * 1_000_000_000 + i128::from(t.nanos);
    OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()
}

/// A point time the partitions can hold: within six days before and one day after `now`.
fn clamp_ts(t: Option<OffsetDateTime>, now: OffsetDateTime) -> OffsetDateTime {
    match t {
        Some(t) if t >= now - time::Duration::days(6) && t <= now + time::Duration::days(1) => t,
        _ => now,
    }
}

/// Text PostgreSQL can store, cut to `max` characters.
fn clean(s: &str, max: usize) -> String {
    s.chars().filter(|c| *c != '\0').take(max).collect()
}

/// One checked metric point.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Point {
    pub name: String,
    pub split: String,
    pub freshness: &'static str,
    pub declared: bool,
    pub step: i64,
    pub ts: OffsetDateTime,
    pub value: f64,
}

/// Checks relayed points: a name of 1–200 characters without NUL, a split of at most 50, a
/// step that fits, a finite value. Others are dropped (`invalid_point`).
pub(crate) fn prepare(
    points: &[MetricPoint],
    sent_at: Option<OffsetDateTime>,
    now: OffsetDateTime,
) -> Vec<Point> {
    let mut out = Vec::with_capacity(points.len());
    for p in points {
        let ok_name =
            !p.name.is_empty() && p.name.chars().count() <= MAX_NAME && !p.name.contains('\0');
        let ok_split = p.split.chars().count() <= MAX_SPLIT && !p.split.contains('\0');
        let Some(step) = i64::try_from(p.step).ok().filter(|s| *s <= MAX_STEP) else {
            dropped("invalid_point");
            continue;
        };
        if !ok_name || !ok_split || !p.value.is_finite() {
            dropped("invalid_point");
            continue;
        }
        let freshness = match MetricKind::try_from(p.kind) {
            Ok(MetricKind::Sampled) => "sampled",
            Ok(MetricKind::Delayed) => "delayed",
            _ => "live",
        };
        out.push(Point {
            name: p.name.clone(),
            split: p.split.clone(),
            freshness,
            // `resource/` is the platform's own prefix: never an undeclared engine metric.
            declared: !p.undeclared || p.name.starts_with(RESOURCE_PREFIX),
            step,
            ts: clamp_ts(timestamp(p.time.as_ref()).or(sent_at), now),
            value: p.value,
        });
    }
    out
}

/// Rollout statistics as `rollout/…` points at the trainer step.
pub(crate) fn rollout_points(r: &RolloutBatchSummary) -> Vec<MetricPoint> {
    let lag = r.lag_histogram.keys().max().copied().unwrap_or(0);
    let v = [
        ("rollout/n_prompts", f64::from(r.n_prompts)),
        ("rollout/n_samples", f64::from(r.n_samples)),
        ("rollout/tokens_generated", r.tokens_generated as f64),
        ("rollout/truncated_frac", r.truncated_frac),
        ("rollout/policy_version_max", r.policy_version_max as f64),
        ("rollout/max_policy_lag", f64::from(lag)),
    ]
    .into_iter()
    .map(|(n, v)| (n.to_owned(), v))
    .collect();
    system_points(v, r.trainer_step, "rollout")
}

/// Reward statistics as `reward/…` points; their step is the generation batch number.
pub(crate) fn reward_points(r: &RewardBatchStats) -> Vec<MetricPoint> {
    let mut v: Vec<(String, f64)> = [
        ("reward/mean", r.mean),
        ("reward/std", r.std),
        ("reward/min", r.min),
        ("reward/max", r.max),
        ("reward/zero_variance_frac", r.zero_variance_frac),
        ("reward/groups_dropped", f64::from(r.groups_dropped)),
    ]
    .into_iter()
    .map(|(n, v)| (n.to_owned(), v))
    .collect();
    v.extend(
        r.component_means
            .iter()
            .map(|(k, x)| (format!("reward/component/{k}"), *x)),
    );
    v.extend(
        r.failures_by_status
            .iter()
            .map(|(k, x)| (format!("reward/failures/{k}"), f64::from(*x))),
    );
    system_points(v, r.batch_no, "reward")
}

fn system_points(v: Vec<(String, f64)>, step: u64, split: &str) -> Vec<MetricPoint> {
    v.into_iter()
        .map(|(name, value)| MetricPoint {
            name,
            step,
            value,
            split: split.to_owned(),
            kind: MetricKind::Live as i32,
            ..MetricPoint::default()
        })
        .collect()
}

/// Reserved metric prefix for resource samples (Doc 07 §6.10).
pub const RESOURCE_PREFIX: &str = "resource/";
/// Resource signals, all in bytes.
pub const RESOURCE_SIGNALS: &[&str] = &[
    "torch_allocated_peak",
    "torch_reserved_peak",
    "process_peak",
    "host_rss_peak",
    "scratch_peak",
];

/// `resource/<signal>[/<gpu-uuid>]` points with a known signal: `(device, signal)`; the
/// device is empty for the whole host.
pub(crate) fn resource_signal(name: &str) -> Option<(String, String)> {
    let rest = name.strip_prefix(RESOURCE_PREFIX)?;
    let (signal, device) = rest.split_once('/').unwrap_or((rest, ""));
    RESOURCE_SIGNALS
        .contains(&signal)
        .then(|| (device.to_owned(), signal.to_owned()))
}

fn log_stream(s: i32) -> &'static str {
    match LogStream::try_from(s) {
        Ok(LogStream::Stdout) => "stdout",
        Ok(LogStream::Stderr) => "stderr",
        _ => "harness",
    }
}

impl Ingest {
    /// Writes into `db` and `store`; tells `observers` about new points.
    pub fn new(
        db: Db,
        store: Arc<ArtifactStore>,
        observers: Arc<MetricObservers>,
        max_series: u32,
    ) -> Self {
        Self {
            db,
            store,
            observers,
            max_series: i64::from(max_series),
        }
    }

    /// The attempt `relay` names, when `node` runs it and the epoch matches.
    pub async fn owner(&self, node: Uuid, relay: &WorkerRelay) -> Result<Option<Owner>, DbError> {
        let Some(r) = relay.attempt.as_ref() else {
            return Ok(None);
        };
        let Ok(attempt) = r.attempt_id.parse::<AttemptId>() else {
            return Ok(None);
        };
        let Ok(epoch) = i64::try_from(r.fencing_epoch) else {
            return Ok(None);
        };
        let row = sqlx::query(
            "SELECT a.org_id, j.run_id, a.attempt_no
               FROM job_attempts a JOIN jobs j ON j.id = a.job_id
              WHERE a.id = $1 AND a.epoch = $3 AND j.run_id IS NOT NULL
                AND (EXISTS (SELECT 1 FROM attempt_placements p
                              WHERE p.attempt_id = a.id AND p.node_id = $2)
                  OR EXISTS (SELECT 1 FROM agent_commands c
                              WHERE c.node_id = $2 AND c.attempt_id = a.id
                                AND c.kind = 'LaunchAttempt'))",
        )
        .bind(attempt.as_uuid())
        .bind(node)
        .bind(epoch)
        .fetch_optional(self.db.pool())
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(Owner {
            org: row.try_get("org_id")?,
            run: row.try_get("run_id")?,
            attempt: attempt.as_uuid(),
            attempt_no: row.try_get("attempt_no")?,
        }))
    }

    async fn owned(&self, item: &LaneItem) -> Result<Option<Owner>, DbError> {
        let Some((relay, _, _)) = relayed(item) else {
            return Ok(None);
        };
        let owner = self.owner(item.node_id, relay).await?;
        if owner.is_none() {
            dropped("unknown_attempt");
        }
        Ok(owner)
    }

    /// Stores one metrics delivery. Returns its batch ID, or `None` for a replay or empty batch.
    pub async fn metrics(
        &self,
        o: Owner,
        delivery: Delivery,
        sent_at: Option<OffsetDateTime>,
        points: &[MetricPoint],
    ) -> Result<Option<i64>, DbError> {
        let points = prepare(points, sent_at, OffsetDateTime::now_utc());
        if points.is_empty() {
            return Ok(None);
        }
        let first = self.write_metrics(o, delivery, &points).await;
        let written = match first {
            Err(e) if is_missing_partition(&e) => {
                super::rollup::ensure_partitions(&self.db, OffsetDateTime::now_utc()).await?;
                self.write_metrics(o, delivery, &points).await?
            }
            other => other?,
        };
        let Some((batch, observed)) = written else {
            return Ok(None);
        };
        self.observers.publish(IngestedBatch {
            run_id: RunId::from_uuid(o.run).to_string(),
            attempt_id: AttemptId::from_uuid(o.attempt).to_string(),
            batch_id: batch,
            points: observed,
        });
        Ok(Some(batch))
    }

    async fn write_metrics(
        &self,
        o: Owner,
        delivery: Delivery,
        points: &[Point],
    ) -> Result<Option<(i64, Vec<ObservedPoint>)>, DbError> {
        let mut tx = self.db.begin().await?;
        if !delivery.claim(&mut tx).await? {
            return Ok(None);
        }
        let batch: i64 = sqlx::query_scalar(
            "INSERT INTO metric_batches (org_id, run_id, attempt_id, agent_seq, point_count)
             VALUES ($1, $2, $3, $4, 0) RETURNING id",
        )
        .bind(o.org)
        .bind(o.run)
        .bind(o.attempt)
        .bind(i64::try_from(delivery.seq).unwrap_or(i64::MAX))
        .fetch_one(&mut *tx)
        .await?;
        let series = self.series(&mut tx, o, points).await?;
        let mut copy = String::new();
        let mut observed = Vec::new();
        let mut dirty: BTreeSet<(i64, &str, i64)> = BTreeSet::new();
        let mut peaks: BTreeMap<(String, String, String), f64> = BTreeMap::new();
        let fmt = &time::format_description::well_known::Rfc3339;
        for p in points {
            let Some(&series_id) = series.get(&(p.name.clone(), p.split.clone())) else {
                dropped("series_limit");
                continue;
            };
            let ts = p
                .ts
                .format(fmt)
                .map_err(|e| DbError::Query(sqlx::Error::Protocol(format!("point time: {e}"))))?;
            copy.push_str(&format!(
                "{series_id}\t{}\t{}\t{batch}\t{}\t{ts}\t{}\n",
                o.attempt, o.attempt_no, p.step, p.value
            ));
            dirty.insert((series_id, "step_bucket", p.step.div_euclid(STEP_BUCKET)));
            dirty.insert((series_id, "1m", p.ts.unix_timestamp().div_euclid(60)));
            if let Some((device, signal)) = resource_signal(&p.name) {
                let key = (device, p.split.clone(), signal);
                let peak = peaks.entry(key).or_insert(p.value);
                *peak = peak.max(p.value);
            }
            observed.push(ObservedPoint {
                series_id,
                name: p.name.clone(),
                split: p.split.clone(),
                step: p.step,
                value: p.value,
                ts: p.ts,
            });
        }
        if !observed.is_empty() {
            let mut c = tx
                .copy_in_raw(
                    "COPY metric_points (series_id, attempt_id, attempt_no, batch_id, step, ts, value) FROM STDIN",
                )
                .await?;
            c.send(copy.into_bytes()).await?;
            c.finish().await?;
        }
        let (ids, tiers, buckets): (Vec<i64>, Vec<&str>, Vec<i64>) = dirty.into_iter().fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut a, mut b, mut c), (s, t, k)| {
                a.push(s);
                b.push(t);
                c.push(k);
                (a, b, c)
            },
        );
        sqlx::query(
            "INSERT INTO metric_rollup_dirty (series_id, tier, bucket_start)
             SELECT * FROM unnest($1::bigint[], $2::text[], $3::bigint[])
             ON CONFLICT (series_id, tier, bucket_start) DO UPDATE SET bucket_start = EXCLUDED.bucket_start",
        )
        .bind(&ids)
        .bind(&tiers)
        .bind(&buckets)
        .execute(&mut *tx)
        .await?;
        for ((device, phase, signal), peak) in &peaks {
            sqlx::query(
                "INSERT INTO resource_actuals (attempt_id, org_id, run_id, device, phase, signal, peak)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (attempt_id, device, phase, signal)
                 DO UPDATE SET peak = GREATEST(resource_actuals.peak, EXCLUDED.peak), updated_at = now()",
            )
            .bind(o.attempt)
            .bind(o.org)
            .bind(o.run)
            .bind(device)
            .bind(phase)
            .bind(signal)
            .bind(peak)
            .execute(&mut *tx)
            .await?;
        }
        let steps = observed.iter().map(|p| p.step);
        sqlx::query(
            "UPDATE metric_batches SET point_count = $2, min_step = $3, max_step = $4 WHERE id = $1",
        )
        .bind(batch)
        .bind(i32::try_from(observed.len()).unwrap_or(i32::MAX))
        .bind(steps.clone().min())
        .bind(steps.max())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some((batch, observed)))
    }

    /// Series ids by `(name, split)`, creating missing ones while the run has fewer than
    /// `max_series`. Undeclared points flag their series.
    async fn series(
        &self,
        tx: &mut Tx,
        o: Owner,
        points: &[Point],
    ) -> Result<HashMap<(String, String), i64>, DbError> {
        // Sorted keys: concurrent batches of one run lock new series rows in the same order.
        let mut keys: BTreeMap<(String, String), (&'static str, bool)> = BTreeMap::new();
        for p in points {
            let e = keys
                .entry((p.name.clone(), p.split.clone()))
                .or_insert((p.freshness, p.declared));
            e.1 &= p.declared;
        }
        let names: Vec<&str> = keys.keys().map(|k| k.0.as_str()).collect();
        let splits: Vec<&str> = keys.keys().map(|k| k.1.as_str()).collect();
        // Lock before reading the series set so concurrent batches share the same cap.
        lock_run(tx, o.run).await?;
        let found = load_series(tx, o.run, &names, &splits).await?;
        let missing: Vec<_> = keys
            .iter()
            .filter(|(k, _)| !found.contains_key(*k))
            .collect();
        if !missing.is_empty() {
            let have: i64 =
                sqlx::query_scalar("SELECT count(*) FROM metric_series WHERE run_id = $1")
                    .bind(o.run)
                    .fetch_one(&mut **tx)
                    .await?;
            let room = usize::try_from((self.max_series - have).max(0)).unwrap_or(0);
            let new: Vec<_> = missing.into_iter().take(room).collect();
            sqlx::query(
                "INSERT INTO metric_series (org_id, run_id, name, split, freshness, declared)
                 SELECT $1, $2, n, s, f, d FROM unnest($3::text[], $4::text[], $5::text[], $6::bool[]) AS u(n, s, f, d)
                 ON CONFLICT (run_id, name, split, rank) DO NOTHING",
            )
            .bind(o.org)
            .bind(o.run)
            .bind(new.iter().map(|(k, _)| k.0.as_str()).collect::<Vec<_>>())
            .bind(new.iter().map(|(k, _)| k.1.as_str()).collect::<Vec<_>>())
            .bind(new.iter().map(|(_, v)| v.0).collect::<Vec<_>>())
            .bind(new.iter().map(|(_, v)| v.1).collect::<Vec<_>>())
            .execute(&mut **tx)
            .await?;
        }
        let all = load_series(tx, o.run, &names, &splits).await?;
        let undeclared: Vec<i64> = keys
            .iter()
            .filter(|(_, v)| !v.1)
            .filter_map(|(k, _)| all.get(k).copied())
            .collect();
        if !undeclared.is_empty() {
            sqlx::query(
                "UPDATE metric_series SET declared = false WHERE id = ANY($1) AND declared",
            )
            .bind(&undeclared)
            .execute(&mut **tx)
            .await?;
        }
        Ok(all)
    }

    async fn samples(
        &self,
        o: Owner,
        delivery: Delivery,
        b: &SampleBatch,
    ) -> Result<(), SinkError> {
        let err = |e: &dyn std::fmt::Display| sink_error("telemetry.metrics", e);
        let Ok(step) = i64::try_from(b.step) else {
            dropped("invalid_sample");
            return Ok(());
        };
        let rewards = b
            .samples
            .iter()
            .map(|s| s.reward_total)
            .filter(|r| r.is_finite());
        let (lo, hi) = (rewards.clone().reduce(f64::min), rewards.reduce(f64::max));
        let doc = json!({
            "step": step,
            "overflow_ref": b.overflow_ref,
            "samples": b.samples.iter().map(|s| json!({
                "sample_id": s.sample_id, "group_id": s.group_id,
                "prompt": s.prompt, "completion": s.completion,
                "reward_total": s.reward_total.is_finite().then_some(s.reward_total),
                "reward_components": s.reward_components.iter()
                    .filter(|(_, v)| v.is_finite()).collect::<BTreeMap<_, _>>(),
                "truncated": s.truncated, "completion_tokens": s.completion_tokens,
                "policy_version": s.policy_version,
            })).collect::<Vec<_>>(),
        });
        let bytes = serde_json::to_vec(&doc).map_err(|e| err(&e))?;
        let store = self
            .store
            .org_store(&storage_org(o.org))
            .await
            .map_err(|e| err(&e))?;
        let sha = sha256_hex(&bytes);
        let key = format!(
            "{}runs/{}/samples/{}-{}-{}-{}.json",
            store.keys().prefix(),
            RunId::from_uuid(o.run),
            delivery.node,
            delivery.stream as i32,
            delivery.seq,
            sha,
        );
        super::remember_object(self.db.pool(), o.org, &key)
            .await
            .map_err(|e| err(&e))?;
        let mut tx = self.db.begin().await.map_err(|e| err(&e))?;
        if !delivery.claim(&mut tx).await.map_err(|e| err(&e))? {
            return Ok(());
        }
        super::remember_object(&mut *tx, o.org, &key)
            .await
            .map_err(|e| err(&e))?;
        lock_run(&mut tx, o.run).await.map_err(|e| err(&e))?;
        store.put(&key, bytes.into()).await.map_err(|e| err(&e))?;
        sqlx::query(
            "INSERT INTO samples (org_id, run_id, attempt_id, worker_seq, step, object_key, count, reward_min, reward_max, sha256)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(o.org)
        .bind(o.run)
        .bind(o.attempt)
        .bind(i64::try_from(delivery.seq).unwrap_or(i64::MAX))
        .bind(step)
        .bind(&key)
        .bind(i32::try_from(b.samples.len()).unwrap_or(i32::MAX))
        .bind(lo)
        .bind(hi)
        .bind(&sha)
        .execute(&mut *tx)
        .await
        .map_err(|e| err(&e))?;
        tx.commit().await.map_err(|e| err(&e))?;
        Ok(())
    }

    /// Stores one relayed log chunk (split into pieces of at most 64 KiB).
    async fn log(&self, tx: &mut Tx, o: Owner, c: &LogChunk) -> Result<(), DbError> {
        let (Ok(rank), Ok(offset)) = (i32::try_from(c.rank), i64::try_from(c.offset)) else {
            dropped("invalid_log");
            return Ok(());
        };
        let stream = log_stream(c.stream);
        let mut offsets = Vec::new();
        let mut pieces = Vec::new();
        for (i, piece) in c.data.chunks(MAX_CHUNK_BYTES).enumerate() {
            let Some(at) = i64::try_from(i * MAX_CHUNK_BYTES)
                .ok()
                .and_then(|d| offset.checked_add(d))
            else {
                dropped("invalid_log");
                return Ok(());
            };
            offsets.push(at);
            pieces.push(piece.to_vec());
        }
        Self::insert_chunks(tx, o, rank, stream, &offsets, &pieces).await
    }

    /// Records that `count` log chunks were lost before this point (an `agent` line).
    async fn log_gap(tx: &mut Tx, o: Owner, at: u64, count: u64) -> Result<(), DbError> {
        let text = format!(
            "[masterrl] {count} log chunk(s) were dropped here because the connection was too slow.\n"
        );
        let at = i64::try_from(at).unwrap_or(i64::MAX);
        Self::insert_chunks(tx, o, 0, "agent", &[at], &[text.into_bytes()]).await
    }

    async fn insert_chunks(
        tx: &mut Tx,
        o: Owner,
        rank: i32,
        stream: &str,
        offsets: &[i64],
        pieces: &[Vec<u8>],
    ) -> Result<(), DbError> {
        if offsets.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO log_chunks (org_id, run_id, attempt_id, rank, stream, \"offset\", data)
             SELECT $1, $2, $3, $4, $5, o, d FROM unnest($6::bigint[], $7::bytea[]) AS u(o, d)
             ON CONFLICT (attempt_id, rank, stream, \"offset\") WHERE stream <> 'agent' DO NOTHING",
        )
        .bind(o.org)
        .bind(o.run)
        .bind(o.attempt)
        .bind(rank)
        .bind(stream)
        .bind(offsets)
        .bind(pieces)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Keeps the newest progress of the run: a later attempt, or a non-regressing step of the same attempt.
    async fn progress(&self, o: Owner, delivery: Delivery, p: &Progress) -> Result<(), DbError> {
        let mut tx = self.db.begin().await?;
        if !delivery.claim(&mut tx).await? {
            return Ok(());
        }
        let n = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
        let f = |v: f64| if v.is_finite() { v } else { 0.0 };
        sqlx::query(
            "INSERT INTO run_progress (run_id, org_id, attempt_id, attempt_no, worker_seq, step,
                    total_steps, epoch, phase, tokens_seen, eta_seconds, last_node_id, last_stream)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             ON CONFLICT (run_id) DO UPDATE SET
               attempt_id = EXCLUDED.attempt_id, attempt_no = EXCLUDED.attempt_no,
               worker_seq = EXCLUDED.worker_seq, step = EXCLUDED.step,
               total_steps = EXCLUDED.total_steps, epoch = EXCLUDED.epoch, phase = EXCLUDED.phase,
               tokens_seen = EXCLUDED.tokens_seen, eta_seconds = EXCLUDED.eta_seconds,
               updated_at = now(), xid = pg_current_xact_id(), last_node_id = EXCLUDED.last_node_id,
               last_stream = EXCLUDED.last_stream
             WHERE run_progress.attempt_no < EXCLUDED.attempt_no OR
               (run_progress.attempt_no = EXCLUDED.attempt_no AND
                 CASE WHEN (run_progress.last_node_id, run_progress.last_stream) =
                           (EXCLUDED.last_node_id, EXCLUDED.last_stream)
                      THEN run_progress.worker_seq < EXCLUDED.worker_seq AND run_progress.step <= EXCLUDED.step
                      ELSE run_progress.step <= EXCLUDED.step END)",
        )
        .bind(o.run)
        .bind(o.org)
        .bind(o.attempt)
        .bind(o.attempt_no)
        .bind(n(delivery.seq))
        .bind(n(p.step))
        .bind(n(p.total_steps))
        .bind(f(p.epoch))
        .bind(clean(&p.phase, 100))
        .bind(n(p.tokens_seen))
        .bind(f(p.eta_seconds))
        .bind(delivery.node)
        .bind(delivery.stream as i32)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

async fn load_series(
    tx: &mut Tx,
    run: Uuid,
    names: &[&str],
    splits: &[&str],
) -> Result<HashMap<(String, String), i64>, DbError> {
    let rows = sqlx::query(
        "SELECT s.id, s.name, s.split FROM metric_series s
           JOIN unnest($2::text[], $3::text[]) AS k(n, sp) ON s.name = k.n AND s.split = k.sp
          WHERE s.run_id = $1 AND s.rank IS NULL",
    )
    .bind(run)
    .bind(names)
    .bind(splits)
    .fetch_all(&mut **tx)
    .await?;
    rows.into_iter()
        .map(|r| Ok(((r.try_get("name")?, r.try_get("split")?), r.try_get("id")?)))
        .collect()
}

/// METRICS: metric batches, samples, rollout and reward statistics.
pub struct MetricsSink(pub Arc<Ingest>);

#[async_trait::async_trait]
impl LaneSink for MetricsSink {
    fn name(&self) -> &'static str {
        "telemetry.metrics"
    }

    fn lanes(&self) -> &'static [LaneKind] {
        &[LaneKind::Metrics]
    }

    async fn deliver(&self, item: LaneItem) -> Result<(), SinkError> {
        let err = |e: DbError| sink_error("telemetry.metrics", e);
        let Some(o) = self.0.owned(&item).await.map_err(err)? else {
            return Ok(());
        };
        let Some((_, msg, body)) = relayed(&item) else {
            return Ok(());
        };
        let sent = timestamp(msg.sent_at.as_ref());
        match body {
            WorkerBody::Metrics(b) => self
                .0
                .metrics(o, (&item).into(), sent, &b.points)
                .await
                .map_err(err)?,
            WorkerBody::RolloutBatch(r) => self
                .0
                .metrics(o, (&item).into(), sent, &rollout_points(r))
                .await
                .map_err(err)?,
            WorkerBody::RewardBatchStats(r) => self
                .0
                .metrics(o, (&item).into(), sent, &reward_points(r))
                .await
                .map_err(err)?,
            WorkerBody::Samples(s) => {
                self.0.samples(o, (&item).into(), s).await?;
                None
            }
            _ => None,
        };
        Ok(())
    }
}

/// LOGS: worker log chunks, plus a marker line where chunks were dropped.
pub struct LogsSink(pub Arc<Ingest>);

#[async_trait::async_trait]
impl LaneSink for LogsSink {
    fn name(&self) -> &'static str {
        "telemetry.logs"
    }

    fn lanes(&self) -> &'static [LaneKind] {
        &[LaneKind::Logs]
    }

    async fn deliver(&self, item: LaneItem) -> Result<(), SinkError> {
        let err = |e: DbError| sink_error("telemetry.logs", e);
        let Some(o) = self.0.owned(&item).await.map_err(err)? else {
            return Ok(());
        };
        let mut tx = self.0.db.begin().await.map_err(err)?;
        if !Delivery::from(&item).claim(&mut tx).await.map_err(err)? {
            return Ok(());
        }
        lock_run(&mut tx, o.run).await.map_err(err)?;
        if item.dropped_before > 0 {
            Ingest::log_gap(&mut tx, o, item.seq, item.dropped_before)
                .await
                .map_err(err)?;
        }
        if let Some((_, _, WorkerBody::Log(c))) = relayed(&item) {
            self.0.log(&mut tx, o, c).await.map_err(err)?;
        }
        tx.commit().await.map_err(|e| err(e.into()))?;
        Ok(())
    }
}

/// CONTROL `worker.progress`: the run's latest progress.
pub struct ProgressSink(pub Arc<Ingest>);

#[async_trait::async_trait]
impl LaneSink for ProgressSink {
    fn name(&self) -> &'static str {
        "telemetry.progress"
    }

    fn lanes(&self) -> &'static [LaneKind] {
        &[LaneKind::Control]
    }

    fn control_kinds(&self) -> &'static [&'static str] {
        &["worker.progress"]
    }

    async fn deliver(&self, item: LaneItem) -> Result<(), SinkError> {
        // Only an unreachable database is an error (CONTROL retries it); bad input is dropped.
        let err = |e: DbError| sink_error("telemetry.progress", e);
        let Some(o) = self.0.owned(&item).await.map_err(err)? else {
            return Ok(());
        };
        if let Some((_, _, WorkerBody::Progress(p))) = relayed(&item) {
            self.0.progress(o, (&item).into(), p).await.map_err(err)?;
        }
        Ok(())
    }
}

/// sha256 of `bytes` as hex.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(name: &str, step: u64, value: f64) -> MetricPoint {
        MetricPoint {
            name: name.into(),
            step,
            value,
            ..MetricPoint::default()
        }
    }

    #[test]
    fn bad_points_are_dropped_and_times_clamped() {
        let now = OffsetDateTime::now_utc();
        let mut far = point("loss", 1, 1.0);
        far.time = Some(prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        });
        far.undeclared = true;
        far.kind = MetricKind::Delayed as i32;
        let pts = [
            far,
            point("", 1, 1.0),
            point("nul\0", 1, 1.0),
            point(&"x".repeat(201), 1, 1.0),
            point("nan", 1, f64::NAN),
            point("big", u64::MAX, 1.0),
            point("ok", 2, 2.5),
            MetricPoint {
                undeclared: true,
                ..point("resource/process_peak", 3, 1.0)
            },
        ];
        let got = prepare(&pts, None, now);
        assert_eq!(got.len(), 3);
        assert!(
            got[2].declared,
            "resource/ is the platform's prefix, never undeclared"
        );
        assert_eq!(got[0].ts, now, "a time far in the past becomes now");
        assert!(!got[0].declared);
        assert_eq!(got[0].freshness, "delayed");
        assert_eq!((got[1].name.as_str(), got[1].step), ("ok", 2));
    }

    #[test]
    fn system_stats_become_named_points() {
        let r = RolloutBatchSummary {
            trainer_step: 7,
            n_samples: 4,
            lag_histogram: [(0, 2), (3, 2)].into_iter().collect(),
            ..RolloutBatchSummary::default()
        };
        let p = rollout_points(&r);
        assert!(p.iter().all(|p| p.step == 7 && p.split == "rollout"));
        let lag = p
            .iter()
            .find(|p| p.name == "rollout/max_policy_lag")
            .unwrap();
        assert_eq!(lag.value, 3.0);
        let s = RewardBatchStats {
            batch_no: 3,
            mean: 0.5,
            component_means: [("format".to_string(), 1.0)].into_iter().collect(),
            ..RewardBatchStats::default()
        };
        let p = reward_points(&s);
        assert!(p
            .iter()
            .any(|p| p.name == "reward/component/format" && p.step == 3));
    }

    #[test]
    fn resource_names_split_into_signal_and_device() {
        assert_eq!(
            resource_signal("resource/torch_allocated_peak/GPU-1f2a"),
            Some(("GPU-1f2a".into(), "torch_allocated_peak".into()))
        );
        assert_eq!(
            resource_signal("resource/made_up_peak"),
            None,
            "unknown signal"
        );
        assert_eq!(
            resource_signal("resource/host_rss_peak"),
            Some((String::new(), "host_rss_peak".into()))
        );
        assert_eq!(resource_signal("resource/"), None);
        assert_eq!(resource_signal("train/loss"), None);
    }
}

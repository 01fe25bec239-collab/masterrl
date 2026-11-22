//! Downsampling, partitions and retention (Doc 09 §3.11). Runs on the leader-elected
//! `telemetry.maintenance` task.
//!
//! - Rollups: ingest marks each `(series, tier, bucket)` it touched in `metric_rollup_dirty`
//!   (same transaction as the points); a pass takes marks (`SKIP LOCKED`) and rewrites those
//!   buckets from the raw points, so a rollup is always exact, "latest attempt wins" per step
//!   included. A bucket whose raw points are all gone keeps its rollup.
//! - Partitions: weekly `metric_points_wYYYYMMDD`, last week to two weeks ahead, made at start
//!   and every pass; a week older than `raw_retention_days` is dropped after the marks drained.
//! - Retention (hourly): receipts with the raw points, `1m` rollups, sealed log segments and
//!   sample files (object first, then the row).

use super::ingest::STEP_BUCKET;
use super::Telemetry;
use crate::db::{Db, DbError};
use crate::modules::jobs::JobsError;
use sqlx::Row;
use time::{Date, Duration, OffsetDateTime};
use uuid::Uuid;

/// Dirty marks rewritten per transaction.
const ROLLUP_BATCH: i64 = 1000;

/// What one maintenance pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Maintained {
    /// Rollup buckets rewritten.
    pub rolled: usize,
    /// Log segments sealed.
    pub sealed: usize,
    /// Raw partitions dropped.
    pub dropped_partitions: usize,
    /// Old rows removed (receipts, rollups, segments, samples).
    pub deleted: u64,
}

/// One pass: rollups, sealing, partitions; with `retention`, old data too.
pub async fn maintain(t: &Telemetry, retention: bool) -> Result<Maintained, JobsError> {
    let now = OffsetDateTime::now_utc();
    let mut done = Maintained::default();
    ensure_partitions(&t.db, now).await?;
    loop {
        let n = roll_up(&t.db, ROLLUP_BATCH).await?;
        done.rolled += n;
        if n < usize::try_from(ROLLUP_BATCH).unwrap_or(usize::MAX) {
            break;
        }
    }
    done.sealed = super::logs::seal(
        t,
        std::time::Duration::from_secs(t.config.log_seal_after_seconds),
    )
    .await?;
    if retention {
        done.dropped_partitions = drop_old_partitions(
            &t.db,
            now - Duration::days(t.config.raw_retention_days.into()),
        )
        .await?;
        done.deleted = expire(t, now).await?;
    }
    Ok(done)
}

/// Monday of the week holding `d`.
fn week_start(d: Date) -> Date {
    d - Duration::days(i64::from(d.weekday().number_days_from_monday()))
}

fn partition_name(from: Date) -> String {
    format!(
        "metric_points_w{:04}{:02}{:02}",
        from.year(),
        u8::from(from.month()),
        from.day()
    )
}

/// Creates the weekly partitions from last week to two weeks after `now` (idempotent; one
/// writer at a time across replicas).
pub async fn ensure_partitions(db: &Db, now: OffsetDateTime) -> Result<(), DbError> {
    let monday = week_start(now.date());
    let mut tx = db.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('mrl.metric_points.partitions'))")
        .execute(&mut *tx)
        .await?;
    for w in -1..=2 {
        let from = monday + Duration::weeks(w);
        let to = from + Duration::weeks(1);
        // Names and bounds come from dates only.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE IF NOT EXISTS {} PARTITION OF metric_points \
             FOR VALUES FROM ('{from} 00:00:00+00') TO ('{to} 00:00:00+00')",
            partition_name(from)
        )))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// The week a partition name starts, if it is one of ours.
fn partition_week(name: &str) -> Option<Date> {
    let d = name.strip_prefix("metric_points_w")?;
    if d.len() != 8 || !d.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let y: i32 = d[0..4].parse().ok()?;
    let m: u8 = d[4..6].parse().ok()?;
    let day: u8 = d[6..8].parse().ok()?;
    Date::from_calendar_date(y, time::Month::try_from(m).ok()?, day).ok()
}

/// Drops weekly partitions that end before `cutoff`. Call it after the rollup marks are drained:
/// ingest keeps point times within the last six days and retention is at least seven, so the
/// marks of a dropped week were rolled up days before.
pub async fn drop_old_partitions(db: &Db, cutoff: OffsetDateTime) -> Result<usize, DbError> {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
          WHERE i.inhparent = 'metric_points'::regclass",
    )
    .fetch_all(db.pool())
    .await?;
    let mut dropped = 0;
    for name in names {
        let Some(from) = partition_week(&name) else {
            continue;
        };
        if (from + Duration::weeks(1)).midnight().assume_utc() > cutoff {
            continue;
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP TABLE IF EXISTS {}",
            partition_name(from)
        )))
        .execute(db.pool())
        .await?;
        dropped += 1;
    }
    Ok(dropped)
}

/// Rewrites up to `limit` marked buckets from the raw points; returns how many marks it took.
pub async fn roll_up(db: &Db, limit: i64) -> Result<usize, DbError> {
    let mut tx = db.begin().await?;
    let rows = sqlx::query(
        "DELETE FROM metric_rollup_dirty d
          USING (SELECT series_id, tier, bucket_start FROM metric_rollup_dirty
                  ORDER BY series_id, tier, bucket_start LIMIT $1 FOR UPDATE SKIP LOCKED) s
          WHERE d.series_id = s.series_id AND d.tier = s.tier AND d.bucket_start = s.bucket_start
          RETURNING d.series_id, d.tier, d.bucket_start",
    )
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    let (mut step_s, mut step_b, mut min_s, mut min_b) = (vec![], vec![], vec![], vec![]);
    for r in &rows {
        let (s, b): (i64, i64) = (r.try_get("series_id")?, r.try_get("bucket_start")?);
        if r.try_get::<String, _>("tier")? == "1m" {
            min_s.push(s);
            min_b.push(b);
        } else {
            step_s.push(s);
            step_b.push(b);
        }
    }
    if !step_s.is_empty() {
        sqlx::query(
            "INSERT INTO metric_rollups (series_id, tier, bucket_start, min, max, avg, last, n)
             SELECT d.s, 'step_bucket', d.b, min(p.value), max(p.value), avg(p.value),
                    (array_agg(p.value ORDER BY p.step DESC))[1], count(*)::int
               FROM unnest($1::bigint[], $2::bigint[]) AS d(s, b)
              CROSS JOIN LATERAL (
                    SELECT DISTINCT ON (step) step, value FROM metric_points
                     WHERE series_id = d.s AND step >= d.b * $3 AND step < (d.b + 1) * $3
                     ORDER BY step, attempt_no DESC, ts DESC) p
              GROUP BY d.s, d.b
             ON CONFLICT (series_id, tier, bucket_start) DO UPDATE SET
               min = EXCLUDED.min, max = EXCLUDED.max, avg = EXCLUDED.avg,
               last = EXCLUDED.last, n = EXCLUDED.n",
        )
        .bind(&step_s)
        .bind(&step_b)
        .bind(STEP_BUCKET)
        .execute(&mut *tx)
        .await?;
    }
    if !min_s.is_empty() {
        sqlx::query(
            "INSERT INTO metric_rollups (series_id, tier, bucket_start, min, max, avg, last, n)
             SELECT d.s, '1m', d.b, min(p.value), max(p.value), avg(p.value),
                    (array_agg(p.value ORDER BY p.ts DESC))[1], count(*)::int
               FROM unnest($1::bigint[], $2::bigint[]) AS d(s, b)
               JOIN metric_points p ON p.series_id = d.s
                AND p.ts >= to_timestamp(d.b * 60) AND p.ts < to_timestamp((d.b + 1) * 60)
              GROUP BY d.s, d.b
             ON CONFLICT (series_id, tier, bucket_start) DO UPDATE SET
               min = EXCLUDED.min, max = EXCLUDED.max, avg = EXCLUDED.avg,
               last = EXCLUDED.last, n = EXCLUDED.n",
        )
        .bind(&min_s)
        .bind(&min_b)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(rows.len())
}

/// Removes data past its retention; returns the rows removed.
async fn expire(t: &Telemetry, now: OffsetDateTime) -> Result<u64, JobsError> {
    let c = &t.config;
    let raw_cutoff = now - Duration::days(c.raw_retention_days.into());
    let mut n = sqlx::query("DELETE FROM metric_batches WHERE received_at < $1")
        .bind(raw_cutoff)
        .execute(t.db.pool())
        .await?
        .rows_affected();
    let minute_cutoff =
        (now - Duration::days(c.rollup_1m_retention_days.into())).unix_timestamp() / 60;
    n += sqlx::query("DELETE FROM metric_rollups WHERE tier = '1m' AND bucket_start < $1")
        .bind(minute_cutoff)
        .execute(t.db.pool())
        .await?
        .rows_affected();
    n += expire_objects(
        t,
        "log_index",
        "sealed_at",
        now - Duration::days(c.log_retention_days.into()),
    )
    .await?;
    n += expire_objects(
        t,
        "samples",
        "received_at",
        now - Duration::days(c.sample_retention_days.into()),
    )
    .await?;
    Ok(n)
}

/// Deletes the objects, then the rows, of `table` older than `cutoff` (500 at a time).
async fn expire_objects(
    t: &Telemetry,
    table: &'static str,
    at: &'static str,
    cutoff: OffsetDateTime,
) -> Result<u64, JobsError> {
    let mut n = 0;
    loop {
        // Table and column names are constants.
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT id, org_id, object_key FROM {table} WHERE {at} < $1 ORDER BY id LIMIT 500"
        )))
        .bind(cutoff)
        .fetch_all(t.db.pool())
        .await?;
        if rows.is_empty() {
            return Ok(n);
        }
        let mut ids = Vec::with_capacity(rows.len());
        for r in &rows {
            let org: Uuid = r.try_get("org_id")?;
            let key: String = r.try_get("object_key")?;
            let store = t
                .store
                .org_store(&super::storage_org(org))
                .await
                .map_err(|e| super::stored_file_error("file", &e))?;
            store
                .delete(&key)
                .await
                .map_err(|e| super::stored_file_error("file", &e))?;
            ids.push(r.try_get::<i64, _>("id")?);
        }
        n += sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE id = ANY($1)"
        )))
        .bind(&ids)
        .execute(t.db.pool())
        .await?
        .rows_affected();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::date;

    #[test]
    fn weeks_start_on_monday_and_names_round_trip() {
        assert_eq!(week_start(date!(2026 - 11 - 22)), date!(2026 - 11 - 16));
        assert_eq!(week_start(date!(2026 - 11 - 16)), date!(2026 - 11 - 16));
        let name = partition_name(date!(2026 - 01 - 05));
        assert_eq!(name, "metric_points_w20260105");
        assert_eq!(partition_week(&name), Some(date!(2026 - 01 - 05)));
        assert_eq!(partition_week("metric_points_wx"), None);
        assert_eq!(partition_week("other"), None);
    }
}

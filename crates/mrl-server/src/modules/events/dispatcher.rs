//! The dispatcher: one leader per cluster delivers outbox events to every registered
//! [`EventConsumer`], at least once, with effects exactly once.
//!
//! **Tailing.** A row whose `xid` is below the snapshot's `xmin` (Doc 09 §5.1) belongs to a
//! finished transaction, so no new row can ever appear below that point. Each consumer's
//! cursor is a position in `(xid, id)` order and only moves over such rows. A transaction
//! that commits late with a lower `id` is still read: its `xid` is above the cursor.
//!
//! **Order.** Rows are handed over in `id` order. When a later event of an aggregate is
//! ready while an earlier one still waits on the `xmin` rule, the earlier one is delivered
//! first (it is visible, because the writers locked the aggregate row in turn).
//!
//! **Dedup.** Each delivery runs in one transaction that first inserts
//! `processed_events(consumer, event_id)`; a second delivery finds the row and skips the
//! handler. Side effects written in that transaction happen exactly once.
//!
//! **Failures.** A failing event is retried with exponential backoff; it blocks only its
//! consumer. After `max_attempts` it is quarantined (set aside, MRL-INTERNAL-115), the alert
//! hook runs and delivery goes on.

use super::metrics::EventMetrics;
use super::outbox::Event;
use crate::db::{codes, Db, DbError, LeaderElection, Tx};
use async_trait::async_trait;
use futures_util::FutureExt;
use sqlx::postgres::PgListener;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// What a consumer's handler returns on failure.
pub type ConsumerError = Box<dyn std::error::Error + Send + Sync>;

/// Something that reacts to events. Register it in `registry.rs` (`// <mrl:consumers>`).
#[async_trait]
pub trait EventConsumer: Send + Sync {
    /// Unique, stable name; it keys the cursor and dedup rows, so never rename it.
    fn name(&self) -> &'static str;

    /// Whether this consumer wants events of this type. Others are skipped.
    fn accepts(&self, event_type: &str) -> bool {
        let _ = event_type;
        true
    }

    /// Handles one event inside `tx`. Writes made through `tx` happen exactly once.
    /// External calls must be idempotent (derive ids from `event.event_id`).
    async fn handle(&self, tx: &mut Tx, event: &Event) -> Result<(), ConsumerError>;
}

/// An event set aside after too many failed deliveries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quarantined {
    /// The consumer that kept failing.
    pub consumer: String,
    /// The event.
    pub event_id: Uuid,
    /// Its type.
    pub event_type: String,
    /// Deliveries tried.
    pub attempts: i32,
    /// The last error.
    pub error: String,
}

/// Called once per quarantined event, after it was set aside.
pub type AlertHook = Arc<dyn Fn(&Quarantined) + Send + Sync>;

/// Dispatcher tuning.
#[derive(Debug, Clone)]
pub struct DispatcherOptions {
    /// Events read per consumer per round.
    pub batch_size: i64,
    /// Longest wait between rounds without a `NOTIFY`.
    pub poll_interval: Duration,
    /// Failed deliveries before an event is quarantined.
    pub max_attempts: i32,
    /// First retry delay; doubles per failure.
    pub retry_base: Duration,
    /// Longest retry delay.
    pub retry_max: Duration,
    /// Leader lease.
    pub lease: Duration,
}

impl Default for DispatcherOptions {
    fn default() -> Self {
        Self {
            batch_size: 100,
            poll_interval: Duration::from_secs(1),
            max_attempts: 5,
            retry_base: Duration::from_secs(1),
            retry_max: Duration::from_secs(60),
            lease: Duration::from_secs(10),
        }
    }
}

/// Delivers outbox events to consumers. Run it with [`Dispatcher::run`].
pub struct Dispatcher {
    db: Db,
    consumers: Vec<Arc<dyn EventConsumer>>,
    options: DispatcherOptions,
    alert: AlertHook,
    metrics: &'static EventMetrics,
}

/// Where a consumer has read to: every event at or before `(xid, id)` is done.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
struct Cursor {
    xid: i64,
    id: i64,
}

impl Cursor {
    fn of(e: &Event) -> Self {
        Self {
            xid: e.xid,
            id: e.id,
        }
    }
}

enum Round {
    /// Caught up.
    Idle,
    /// A full batch went through; more may be waiting.
    More,
    /// An event failed; try this consumer again after the delay.
    Retry(Duration),
}

macro_rules! columns {
    () => {
        "o.id, o.event_id, o.org_id, o.aggregate_kind, o.aggregate_id, o.aggregate_seq, \
         o.type, o.schema_version, o.payload, o.trace_context, o.created_at, \
         (o.xid::text)::bigint AS xid"
    };
}

impl Dispatcher {
    /// A dispatcher for these consumers with default options and a logging alert hook.
    pub fn new(db: Db, consumers: Vec<Arc<dyn EventConsumer>>) -> Self {
        Self {
            db,
            consumers,
            options: DispatcherOptions::default(),
            alert: Arc::new(|_| {}),
            metrics: EventMetrics::get(),
        }
    }

    /// Replaces the options.
    pub fn with_options(mut self, options: DispatcherOptions) -> Self {
        self.options = options;
        self
    }

    /// Calls `hook` for every quarantined event (after the log line and metric).
    pub fn with_alert_hook(mut self, hook: AlertHook) -> Self {
        self.alert = hook;
        self
    }

    /// The consumer names, in registration order.
    pub fn consumer_names(&self) -> Vec<&'static str> {
        self.consumers.iter().map(|c| c.name()).collect()
    }

    /// Campaigns for leadership and dispatches while leading, until `cancel` fires.
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let election =
            LeaderElection::new(self.db.clone(), "events-dispatcher", self.options.lease);
        election
            .run(cancel, |lead| {
                let me = self.clone();
                async move { me.lead(lead).await }
            })
            .await;
        self.metrics.set_lag(0.0);
    }

    async fn lead(&self, cancel: CancellationToken) -> Result<(), DbError> {
        let mut listener = PgListener::connect_with(self.db.pool()).await?;
        listener.listen(super::outbox::CHANNEL).await?;
        let mut retry_at: HashMap<&'static str, Instant> = HashMap::new();
        let result = loop {
            let mut more = false;
            for c in &self.consumers {
                if retry_at.get(c.name()).is_some_and(|t| *t > Instant::now()) {
                    continue;
                }
                match self.round(c.as_ref()).await {
                    Ok(Round::Idle) => {}
                    Ok(Round::More) => more = true,
                    Ok(Round::Retry(d)) => {
                        retry_at.insert(c.name(), Instant::now() + d);
                    }
                    Err(e) => {
                        tracing::warn!(consumer = c.name(), code = e.code(), error = %e, "event delivery round failed");
                    }
                }
                if cancel.is_cancelled() {
                    break;
                }
            }
            if let Err(e) = self.report_lag().await {
                tracing::warn!(error = %e, "could not measure outbox lag");
            }
            if cancel.is_cancelled() {
                break Ok(());
            }
            if more {
                continue;
            }
            let next_retry = retry_at
                .values()
                .filter_map(|t| t.checked_duration_since(Instant::now()))
                .min();
            let wait = next_retry.map_or(self.options.poll_interval, |d| {
                d.min(self.options.poll_interval)
            });
            tokio::select! {
                () = cancel.cancelled() => break Ok(()),
                r = listener.recv() => { r?; }
                () = tokio::time::sleep(wait) => {}
            }
        };
        self.metrics.set_lag(0.0);
        result
    }

    /// Delivers one batch to `c`.
    async fn round(&self, c: &dyn EventConsumer) -> Result<Round, DbError> {
        let name = c.name();
        let start = self.cursor(name).await?;
        let batch: Vec<Event> = sqlx::query_as(concat!(
            "SELECT ",
            columns!(),
            " FROM outbox_events o
              WHERE (o.xid, o.id) > (($1::bigint)::text::xid8, $2)
                AND o.xid < pg_snapshot_xmin(pg_current_snapshot())
              ORDER BY o.xid, o.id LIMIT $3"
        ))
        .bind(start.xid)
        .bind(start.id)
        .bind(self.options.batch_size)
        .fetch_all(self.db.pool())
        .await?;
        let Some(last) = batch.last().map(Cursor::of) else {
            return Ok(Round::Idle);
        };
        let full = i64::try_from(batch.len()).unwrap_or(i64::MAX) >= self.options.batch_size;
        let earlier = self.earlier_of_same_aggregates(&batch, last).await?;

        // Hand over in id order; the cursor follows (xid, id) order over the batch only.
        let order: Vec<Cursor> = batch.iter().map(Cursor::of).collect();
        let mut by_id: BTreeMap<i64, Event> = BTreeMap::new();
        for e in batch.into_iter().chain(earlier) {
            by_id.entry(e.id).or_insert(e);
        }
        let mut done: HashSet<Cursor> = HashSet::new();
        let mut outcome = if full { Round::More } else { Round::Idle };
        for e in by_id.values() {
            if c.accepts(&e.event_type) {
                if let Err(err) = self.deliver(c, e).await {
                    if let Some(delay) = self.failed(c, e, &err).await? {
                        outcome = Round::Retry(delay);
                        break;
                    }
                }
            }
            done.insert(Cursor::of(e));
        }
        if let Some(to) = order.iter().take_while(|p| done.contains(p)).last() {
            self.advance(name, *to).await?;
        }
        Ok(outcome)
    }

    /// Visible, not yet passed events of the batch's aggregates with a lower id than the
    /// batch's own event of that aggregate (they wait on the `xmin` rule or the batch limit).
    async fn earlier_of_same_aggregates(
        &self,
        batch: &[Event],
        last: Cursor,
    ) -> Result<Vec<Event>, DbError> {
        let mut max_id: HashMap<(&str, Uuid), i64> = HashMap::new();
        for e in batch {
            let slot = max_id
                .entry((&e.aggregate_kind, e.aggregate_id))
                .or_insert(e.id);
            *slot = (*slot).max(e.id);
        }
        let (kinds, (ids, max)): (Vec<&str>, (Vec<Uuid>, Vec<i64>)) =
            max_id.into_iter().map(|((k, a), m)| (k, (a, m))).unzip();
        Ok(sqlx::query_as(concat!(
            "SELECT ",
            columns!(),
            " FROM outbox_events o
              JOIN unnest($1::text[], $2::uuid[], $3::bigint[]) AS b(kind, agg, max_id)
                ON o.aggregate_kind = b.kind AND o.aggregate_id = b.agg AND o.id < b.max_id
              WHERE (o.xid, o.id) > (($4::bigint)::text::xid8, $5)"
        ))
        .bind(kinds)
        .bind(ids)
        .bind(max)
        .bind(last.xid)
        .bind(last.id)
        .fetch_all(self.db.pool())
        .await?)
    }

    /// One delivery: dedup row and handler in one transaction.
    async fn deliver(&self, c: &dyn EventConsumer, e: &Event) -> Result<(), ConsumerError> {
        let mut tx = self.db.begin().await?;
        let fresh = sqlx::query(
            "INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2)
             ON CONFLICT DO NOTHING",
        )
        .bind(c.name())
        .bind(e.event_id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        if !fresh {
            return Ok(());
        }
        // A panicking handler counts as a failure instead of stopping the dispatcher.
        match AssertUnwindSafe(c.handle(&mut tx, e)).catch_unwind().await {
            Ok(result) => result?,
            Err(_) => return Err("the handler panicked".into()),
        }
        tx.commit().await?;
        self.metrics.delivered(c.name());
        Ok(())
    }

    /// Records a failure. `Some(delay)`: retry later. `None`: quarantined, go on.
    async fn failed(
        &self,
        c: &dyn EventConsumer,
        e: &Event,
        err: &ConsumerError,
    ) -> Result<Option<Duration>, DbError> {
        let text: String = err.to_string().chars().take(2000).collect();
        let attempts: i32 = sqlx::query_scalar(
            "INSERT INTO outbox_failures (consumer, event_id, outbox_id, attempts, last_error)
             VALUES ($1, $2, $3, 1, $4)
             ON CONFLICT (consumer, event_id) DO UPDATE SET
               attempts = outbox_failures.attempts + 1, last_error = EXCLUDED.last_error,
               updated_at = now()
             RETURNING attempts",
        )
        .bind(c.name())
        .bind(e.event_id)
        .bind(e.id)
        .bind(&text)
        .fetch_one(self.db.pool())
        .await?;
        if attempts < self.options.max_attempts {
            let delay = backoff(&self.options, attempts);
            tracing::warn!(consumer = c.name(), event_id = %e.event_id, event_type = %e.event_type,
                attempts, retry_in_ms = delay.as_millis() as u64, error = %text, "event delivery failed");
            return Ok(Some(delay));
        }
        let mut tx = self.db.begin().await?;
        sqlx::query(
            "INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )
        .bind(c.name())
        .bind(e.event_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE outbox_failures SET quarantined_at = now() WHERE consumer = $1 AND event_id = $2",
        )
        .bind(c.name())
        .bind(e.event_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        self.metrics.quarantined(c.name());
        tracing::error!(code = codes::EVENT_QUARANTINED, consumer = c.name(), event_id = %e.event_id,
            event_type = %e.event_type, attempts, error = %text, "event set aside after too many failed deliveries");
        (self.alert)(&Quarantined {
            consumer: c.name().to_owned(),
            event_id: e.event_id,
            event_type: e.event_type.clone(),
            attempts,
            error: text,
        });
        Ok(None)
    }

    async fn cursor(&self, consumer: &str) -> Result<Cursor, DbError> {
        let row: Option<(i64, i64)> =
            sqlx::query_as("SELECT last_xid, last_id FROM consumer_cursors WHERE consumer = $1")
                .bind(consumer)
                .fetch_optional(self.db.pool())
                .await?;
        Ok(row.map_or(Cursor::default(), |(xid, id)| Cursor { xid, id }))
    }

    /// Moves the cursor forward only, so a zombie leader can never move it back.
    async fn advance(&self, consumer: &str, to: Cursor) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO consumer_cursors (consumer, last_id, last_xid, updated_at)
             VALUES ($1, $2, $3, now())
             ON CONFLICT (consumer) DO UPDATE SET
               last_id = EXCLUDED.last_id, last_xid = EXCLUDED.last_xid, updated_at = now()
             WHERE (consumer_cursors.last_xid, consumer_cursors.last_id)
                 < (EXCLUDED.last_xid, EXCLUDED.last_id)",
        )
        .bind(consumer)
        .bind(to.id)
        .bind(to.xid)
        .execute(self.db.pool())
        .await?;
        Ok(())
    }

    /// Sets `mrl_outbox_lag_seconds` from [`Dispatcher::lag`].
    async fn report_lag(&self) -> Result<(), DbError> {
        let lag = self.lag().await?;
        self.metrics.set_lag(lag);
        Ok(())
    }

    /// Seconds since the oldest event some consumer has not passed was written; 0 when
    /// every consumer is caught up or none is registered.
    pub async fn lag(&self) -> Result<f64, DbError> {
        let names = self.consumer_names();
        let lag: f64 = if names.is_empty() {
            0.0
        } else {
            sqlx::query_scalar(
                "SELECT COALESCE(MAX(EXTRACT(EPOCH FROM now() - oldest.at)), 0)::float8
                 FROM unnest($1::text[]) AS n(consumer)
                 LEFT JOIN consumer_cursors c USING (consumer)
                 CROSS JOIN LATERAL (
                   SELECT min(o.created_at) AS at FROM outbox_events o
                   WHERE (o.xid, o.id) > ((COALESCE(c.last_xid, 0))::text::xid8, COALESCE(c.last_id, 0))
                 ) AS oldest",
            )
            .bind(names)
            .fetch_one(self.db.pool())
            .await?
        };
        Ok(lag.max(0.0))
    }
}

fn backoff(o: &DispatcherOptions, attempts: i32) -> Duration {
    let shift = u32::try_from(attempts.saturating_sub(1))
        .unwrap_or(0)
        .min(20);
    o.retry_base.saturating_mul(1u32 << shift).min(o.retry_max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_up_to_the_cap() {
        let o = DispatcherOptions::default();
        assert_eq!(backoff(&o, 1), Duration::from_secs(1));
        assert_eq!(backoff(&o, 2), Duration::from_secs(2));
        assert_eq!(backoff(&o, 4), Duration::from_secs(8));
        assert_eq!(backoff(&o, 30), Duration::from_secs(60));
    }

    #[test]
    fn cursors_order_by_xid_then_id() {
        let a = Cursor { xid: 5, id: 9 };
        let b = Cursor { xid: 6, id: 1 };
        assert!(a < b);
        assert!(Cursor { xid: 5, id: 10 } > a);
    }
}

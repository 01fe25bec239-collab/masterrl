//! Retention: outbox rows older than 7 days once every consumer is past them;
//! `processed_events` older than 14 days once their consumer's cursor is past the event
//! (an event delivered early for aggregate order keeps its dedup row until then); recovered
//! failure rows older than 14 days; expired idempotency keys.

use crate::db::{sweep_idempotency_keys, Db, DbError};

/// What one cleanup pass removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swept {
    /// Outbox rows.
    pub outbox: u64,
    /// Dedup rows.
    pub processed: u64,
    /// Failure rows of events that were delivered later.
    pub failures: u64,
    /// Idempotency keys.
    pub idempotency: u64,
}

/// Runs every cleanup once. `consumers` are the registered consumer names; an outbox row
/// goes only when all of them are past it (a consumer without a cursor holds everything).
pub async fn sweep(db: &Db, consumers: &[&str]) -> Result<Swept, DbError> {
    let outbox = sqlx::query(
        "WITH slowest AS (
           SELECT COALESCE(c.last_xid, 0) AS xid, COALESCE(c.last_id, 0) AS id
           FROM unnest($1::text[]) AS n(consumer)
           LEFT JOIN consumer_cursors c USING (consumer)
           ORDER BY 1, 2 LIMIT 1)
         DELETE FROM outbox_events o
         WHERE o.created_at < now() - interval '7 days'
           AND (NOT EXISTS (SELECT 1 FROM slowest)
                OR (o.xid, o.id) <= ((SELECT (xid::text)::xid8 FROM slowest),
                                     (SELECT id FROM slowest)))",
    )
    .bind(consumers)
    .execute(db.pool())
    .await?
    .rows_affected();
    // Once the cursor is past an event (or the event is gone), the dispatcher never reads
    // it again, so its dedup row is no longer needed.
    let processed = sqlx::query(
        "DELETE FROM processed_events p
         WHERE p.processed_at < now() - interval '14 days'
           AND NOT EXISTS (
             SELECT 1 FROM outbox_events o
             LEFT JOIN consumer_cursors c ON c.consumer = p.consumer
             WHERE o.event_id = p.event_id
               AND (o.xid, o.id) > ((COALESCE(c.last_xid, 0))::text::xid8, COALESCE(c.last_id, 0)))",
    )
    .execute(db.pool())
    .await?
    .rows_affected();
    let failures = sqlx::query(
        "DELETE FROM outbox_failures
         WHERE quarantined_at IS NULL AND updated_at < now() - interval '14 days'",
    )
    .execute(db.pool())
    .await?
    .rows_affected();
    let idempotency = sweep_idempotency_keys(db).await?;
    Ok(Swept {
        outbox,
        processed,
        failures,
        idempotency,
    })
}

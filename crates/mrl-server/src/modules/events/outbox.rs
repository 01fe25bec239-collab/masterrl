//! Writing events: in the caller's transaction, with a payload-free `NOTIFY` (D-04-12).

use crate::db::{DbError, Tx};
use serde::Serialize;
use time::OffsetDateTime;
use uuid::Uuid;

/// The `NOTIFY` channel that wakes the dispatcher. Notifications carry no payload.
pub const CHANNEL: &str = "mrl_outbox";

/// An event to write, like `job.state_changed` for job `aggregate_id` at version `aggregate_seq`.
#[derive(Debug, Clone, PartialEq)]
pub struct NewEvent {
    /// Catalog type (Doc 09 §5.4), like `job.state_changed`.
    pub event_type: String,
    /// The org the aggregate belongs to.
    pub org_id: Option<Uuid>,
    /// Aggregate kind, like `job`.
    pub aggregate_kind: String,
    /// Aggregate id.
    pub aggregate_id: Uuid,
    /// The aggregate's version after this change; unique per aggregate.
    pub aggregate_seq: i64,
    /// Payload schema version.
    pub schema_version: i32,
    /// Event body. Events notify; the aggregate row stays the truth.
    pub payload: serde_json::Value,
    /// W3C `traceparent` of the request that caused it.
    pub trace_context: Option<String>,
}

impl NewEvent {
    /// An event with schema version 1, no org and no trace.
    pub fn new(
        event_type: impl Into<String>,
        aggregate_kind: impl Into<String>,
        aggregate_id: Uuid,
        aggregate_seq: i64,
        payload: impl Serialize,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            event_type: event_type.into(),
            org_id: None,
            aggregate_kind: aggregate_kind.into(),
            aggregate_id,
            aggregate_seq,
            schema_version: 1,
            payload: serde_json::to_value(payload)?,
            trace_context: None,
        })
    }

    /// Sets the org.
    pub fn org(mut self, org_id: Uuid) -> Self {
        self.org_id = Some(org_id);
        self
    }

    /// Sets the trace context.
    pub fn trace(mut self, traceparent: impl Into<String>) -> Self {
        self.trace_context = Some(traceparent.into());
        self
    }
}

/// A stored event, as consumers receive it.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Event {
    /// Outbox row id.
    pub id: i64,
    /// Stable event id.
    pub event_id: Uuid,
    /// The org.
    pub org_id: Option<Uuid>,
    /// Aggregate kind.
    pub aggregate_kind: String,
    /// Aggregate id.
    pub aggregate_id: Uuid,
    /// Aggregate version after the change.
    pub aggregate_seq: i64,
    /// Catalog type.
    #[sqlx(rename = "type")]
    pub event_type: String,
    /// Payload schema version.
    pub schema_version: i32,
    /// Event body.
    pub payload: serde_json::Value,
    /// W3C trace context.
    pub trace_context: Option<String>,
    /// When it was written.
    pub created_at: OffsetDateTime,
    /// Writing transaction id (`xid8` as a number); orders tailing.
    pub xid: i64,
}

/// Writes events into the outbox.
pub struct OutboxWriter;

impl OutboxWriter {
    /// Inserts `event` in `tx` and queues one `NOTIFY` for the commit; PostgreSQL merges
    /// identical notifications of a transaction, so many events still wake once.
    /// Returns the new event id.
    pub async fn write(tx: &mut Tx, event: &NewEvent) -> Result<Uuid, DbError> {
        let event_id = Uuid::now_v7();
        sqlx::query(
            "WITH ins AS (
               INSERT INTO outbox_events (event_id, org_id, aggregate_kind, aggregate_id,
                 aggregate_seq, type, schema_version, payload, trace_context)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id)
             SELECT ins.id FROM ins, LATERAL (SELECT pg_notify('mrl_outbox', '')) AS wake",
        )
        .bind(event_id)
        .bind(event.org_id)
        .bind(&event.aggregate_kind)
        .bind(event.aggregate_id)
        .bind(event.aggregate_seq)
        .bind(&event.event_type)
        .bind(event.schema_version)
        .bind(&event.payload)
        .bind(&event.trace_context)
        .fetch_one(&mut **tx)
        .await?;
        Ok(event_id)
    }
}

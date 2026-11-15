-- Events written in the same transaction as the change they describe (Doc 09 §3.10, §5.1).
CREATE TABLE outbox_events (
  id bigserial PRIMARY KEY,
  event_id uuid NOT NULL UNIQUE,
  org_id uuid NULL,
  aggregate_kind text NOT NULL,
  aggregate_id uuid NOT NULL,
  aggregate_seq bigint NOT NULL,
  type text NOT NULL,
  schema_version int NOT NULL DEFAULT 1,
  payload jsonb NOT NULL,
  trace_context text NULL,
  xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (aggregate_kind, aggregate_id, aggregate_seq)
);

-- The dispatcher tails by (xid, id): rows below the snapshot xmin never change again.
CREATE INDEX outbox_events_tail ON outbox_events (xid, id);

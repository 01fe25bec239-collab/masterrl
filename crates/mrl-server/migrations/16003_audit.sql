-- The audit log (Doc 09 §3, Doc 23 §3). Rows are written in the same transaction as the
-- action they record and are never changed. Monthly partitions; rows outside every month
-- partition land in `audit_events_default`. Ids keep their API prefixes here (text).
CREATE TABLE audit_events (
  id uuid NOT NULL,
  at timestamptz NOT NULL,
  org_id text NULL,
  project_id text NULL,
  actor text NOT NULL,
  action text NOT NULL,
  target text NOT NULL,
  outcome text NOT NULL CHECK (outcome IN ('success', 'denied', 'failed')),
  details jsonb NOT NULL DEFAULT '{}',
  trace_id text NOT NULL DEFAULT '',
  PRIMARY KEY (at, id)
) PARTITION BY RANGE (at);

CREATE TABLE audit_events_default PARTITION OF audit_events DEFAULT;
CREATE INDEX audit_events_org_at ON audit_events (org_id, at DESC);

CREATE FUNCTION audit_events_read_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'audit_events rows cannot be changed or deleted';
END $$;
CREATE TRIGGER audit_events_read_only BEFORE UPDATE OR DELETE ON audit_events
  FOR EACH ROW EXECUTE FUNCTION audit_events_read_only();

-- Creates the partition for the month that holds `day` (no-op if it exists).
CREATE FUNCTION audit_ensure_partition(day date) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  month_start date := date_trunc('month', day)::date;
  part text := 'audit_events_' || to_char(month_start, 'YYYY_MM');
BEGIN
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF audit_events FOR VALUES FROM (%L) TO (%L)',
    part, month_start, (month_start + interval '1 month')::date);
END $$;

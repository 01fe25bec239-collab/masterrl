-- P22: durable delivery receipts in the gateway's sequence space.
CREATE TABLE telemetry_receipts (
    node_id uuid NOT NULL,
    stream int NOT NULL,
    seq numeric(20, 0) NOT NULL,
    PRIMARY KEY (node_id, stream, seq)
);
ALTER TABLE metric_batches DROP CONSTRAINT metric_batches_attempt_id_agent_seq_key;
ALTER TABLE samples DROP CONSTRAINT samples_attempt_id_worker_seq_key;

-- Keep the winning value per archived step so a later attempt can replace just those
-- steps it reports. Raw receipts and multiple attempts are still subject to retention.
CREATE TABLE metric_step_archive (
    series_id bigint NOT NULL,
    step bigint NOT NULL,
    attempt_no int NOT NULL,
    ts timestamptz NOT NULL,
    value double precision NOT NULL,
    PRIMARY KEY (series_id, step)
);

-- Object writes and removal can each be retried after a process dies.
CREATE TABLE telemetry_objects (
    object_key text PRIMARY KEY,
    org_id uuid NOT NULL,
    touched_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO telemetry_objects (object_key, org_id)
    SELECT object_key, org_id FROM log_index
    UNION
    SELECT object_key, org_id FROM samples;
CREATE INDEX telemetry_objects_by_age ON telemetry_objects (touched_at);
CREATE INDEX samples_by_object ON samples (object_key);

-- Tail events survive an overwrite of the current progress or alert.
CREATE TABLE telemetry_progress_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id uuid NOT NULL,
    xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    payload jsonb NOT NULL,
    captured_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX telemetry_progress_age ON telemetry_progress_events (captured_at);
CREATE INDEX telemetry_progress_tail ON telemetry_progress_events (run_id, xid, id);
CREATE FUNCTION telemetry_capture_progress() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO telemetry_progress_events (run_id, payload) VALUES (NEW.run_id, to_jsonb(NEW));
    RETURN NULL;
END;
$$;
CREATE TRIGGER telemetry_capture_progress AFTER INSERT OR UPDATE ON run_progress
    FOR EACH ROW EXECUTE FUNCTION telemetry_capture_progress();

CREATE TABLE telemetry_alert_events (LIKE alerts);
ALTER TABLE telemetry_alert_events ADD COLUMN event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY;
ALTER TABLE telemetry_alert_events ADD COLUMN captured_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX telemetry_alert_age ON telemetry_alert_events (captured_at);
CREATE INDEX telemetry_alert_tail ON telemetry_alert_events (org_id, xid, event_id);
CREATE FUNCTION telemetry_capture_alert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO telemetry_alert_events SELECT NEW.*;
    RETURN NULL;
END;
$$;
CREATE TRIGGER telemetry_capture_alert AFTER INSERT OR UPDATE ON alerts
    FOR EACH ROW EXECUTE FUNCTION telemetry_capture_alert();

-- Exact high-water marks of deleted rows, scoped to what a subscriber can read.
CREATE TABLE telemetry_pruned (
    source text NOT NULL,
    scope_id uuid NOT NULL,
    xid xid8 NOT NULL,
    id bigint NOT NULL,
    PRIMARY KEY (source, scope_id)
);
CREATE FUNCTION telemetry_mark_pruned() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format(
        'INSERT INTO telemetry_pruned (source, scope_id, xid, id)
         SELECT DISTINCT ON (%1$I) %2$L, %1$I, xid, %3$I FROM removed
         %4$s ORDER BY %1$I, xid DESC, %3$I DESC
         ON CONFLICT (source, scope_id) DO UPDATE SET xid = EXCLUDED.xid, id = EXCLUDED.id
         WHERE (telemetry_pruned.xid, telemetry_pruned.id) < (EXCLUDED.xid, EXCLUDED.id)',
        TG_ARGV[1], TG_ARGV[0], TG_ARGV[2],
        CASE WHEN TG_TABLE_NAME = 'outbox_events' THEN
            'WHERE aggregate_kind IN (''job'', ''attempt'')'
        ELSE '' END
    );
    RETURN NULL;
END;
$$;
CREATE TRIGGER telemetry_prune_outbox AFTER DELETE ON outbox_events
    REFERENCING OLD TABLE AS removed FOR EACH STATEMENT
    EXECUTE FUNCTION telemetry_mark_pruned('o', 'aggregate_id', 'id');
CREATE TRIGGER telemetry_prune_metrics AFTER DELETE ON metric_batches
    REFERENCING OLD TABLE AS removed FOR EACH STATEMENT
    EXECUTE FUNCTION telemetry_mark_pruned('m', 'run_id', 'id');
CREATE TRIGGER telemetry_prune_logs AFTER DELETE ON log_chunks
    REFERENCING OLD TABLE AS removed FOR EACH STATEMENT
    EXECUTE FUNCTION telemetry_mark_pruned('l', 'run_id', 'id');
CREATE TRIGGER telemetry_prune_progress AFTER DELETE ON telemetry_progress_events
    REFERENCING OLD TABLE AS removed FOR EACH STATEMENT
    EXECUTE FUNCTION telemetry_mark_pruned('p', 'run_id', 'id');
CREATE TRIGGER telemetry_prune_alerts AFTER DELETE ON telemetry_alert_events
    REFERENCING OLD TABLE AS removed FOR EACH STATEMENT
    EXECUTE FUNCTION telemetry_mark_pruned('a', 'org_id', 'event_id');

ALTER TABLE run_progress ADD COLUMN last_node_id uuid;
ALTER TABLE samples ADD COLUMN sha256 text;

-- PostgreSQL rejects tiny nonzero numerics that round to zero in float8.
CREATE FUNCTION telemetry_float8(value numeric) RETURNS double precision
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT CASE WHEN abs(value) < 2.4703282292062328e-324::numeric
                THEN 0::float8 ELSE value::float8 END;
$$;

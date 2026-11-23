-- Keep revisions in row-lock order even when a transaction obtained its XID early.
-- While that earlier transaction is open, snapshot xmin holds back both revisions.
CREATE FUNCTION telemetry_order_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.xid := greatest(OLD.xid, NEW.xid);
    RETURN NEW;
END;
$$;
CREATE TRIGGER telemetry_order_progress BEFORE UPDATE ON run_progress
    FOR EACH ROW EXECUTE FUNCTION telemetry_order_revision();
CREATE TRIGGER telemetry_order_alert BEFORE UPDATE ON alerts
    FOR EACH ROW EXECUTE FUNCTION telemetry_order_revision();

CREATE OR REPLACE FUNCTION telemetry_capture_progress() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO telemetry_progress_events (run_id, xid, payload)
    VALUES (NEW.run_id, NEW.xid, to_jsonb(NEW));
    RETURN NULL;
END;
$$;

-- Synthetic gaps live in each sender's sequence space, independent of rank byte offsets.
ALTER TABLE log_chunks DROP CONSTRAINT log_chunks_attempt_id_rank_stream_offset_key;
CREATE UNIQUE INDEX log_chunks_per_offset ON log_chunks (attempt_id, rank, stream, "offset")
    WHERE stream <> 'agent';

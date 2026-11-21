-- The keychain session is published only when the matching sign-in transaction committed.
ALTER TABLE users ADD COLUMN signin_operation uuid;

-- Rows written for a month with no partition yet (the background role was down past the last
-- pre-created month) land in `audit_events_default`. Creating that month's partition later
-- would then fail ("would be violated by some row") and block startup. This version creates
-- the partition as a plain table, moves that month's rows out of the default partition into
-- it, then attaches it, all in the caller's transaction, so no audit row is lost.
--
-- The read-only trigger is switched off on the default partition only for the move, inside
-- the same transaction (DDL is transactional, so no other session ever sees it off).
CREATE OR REPLACE FUNCTION audit_ensure_partition(day date) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  month_start date := date_trunc('month', day)::date;
  month_end date := (date_trunc('month', day) + interval '1 month')::date;
  part text := 'audit_events_' || to_char(date_trunc('month', day), 'YYYY_MM');
  waiting bigint;
BEGIN
  IF to_regclass(part) IS NOT NULL THEN
    RETURN;
  END IF;
  -- Acquire the parent and default locks in the same order as partition attachment.
  LOCK TABLE ONLY audit_events IN SHARE UPDATE EXCLUSIVE MODE;
  IF to_regclass(part) IS NOT NULL THEN
    RETURN;
  END IF;
  EXECUTE format('CREATE TABLE %I (LIKE audit_events INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', part);
  -- Prevent the first row for this month arriving between the scan and attachment.
  LOCK TABLE audit_events_default IN ACCESS EXCLUSIVE MODE;
  SELECT count(*) INTO waiting FROM audit_events_default WHERE at >= month_start AND at < month_end;
  IF waiting > 0 THEN
    ALTER TABLE audit_events_default DISABLE TRIGGER audit_events_read_only;
    EXECUTE format(
      'WITH moved AS (DELETE FROM audit_events_default WHERE at >= %L AND at < %L RETURNING *) '
      'INSERT INTO %I SELECT * FROM moved',
      month_start, month_end, part);
    ALTER TABLE audit_events_default ENABLE TRIGGER audit_events_read_only;
  END IF;
  EXECUTE format(
    'ALTER TABLE audit_events ATTACH PARTITION %I FOR VALUES FROM (%L) TO (%L)',
    part, month_start, month_end);
END $$;

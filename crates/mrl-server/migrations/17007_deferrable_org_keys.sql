-- P17: the (org_id, …) foreign keys of the runs tables become deferrable, so a startup step
-- can move rows stored under the nil org to their project's org in one transaction
-- (constraints are checked at commit). They stay checked immediately unless deferred.
DO $$
DECLARE c record;
BEGIN
  FOR c IN
    SELECT conrelid::regclass AS tbl, conname
      FROM pg_constraint
     WHERE contype = 'f'
       AND conrelid::regclass::text IN
           ('experiments', 'runs', 'run_configs', 'jobs', 'job_attempts', 'attempt_placements')
  LOOP
    EXECUTE format('ALTER TABLE %s ALTER CONSTRAINT %I DEFERRABLE INITIALLY IMMEDIATE', c.tbl, c.conname);
  END LOOP;
END $$;

-- The snapshot stays immutable; only the org it is filed under may change (the same move).
CREATE OR REPLACE FUNCTION run_configs_refuse_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND (to_jsonb(NEW) - 'org_id') = (to_jsonb(OLD) - 'org_id') THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'run_configs rows are immutable (run %)', OLD.run_id
    USING ERRCODE = 'restrict_violation';
END $$;

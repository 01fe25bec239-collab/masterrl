ALTER TABLE reward_reports
  ADD COLUMN prepared_result jsonb,
  ADD COLUMN prepared_artifact_id uuid,
  ADD COLUMN idempotency_key text,
  ADD COLUMN request_sha256 text;

CREATE UNIQUE INDEX reward_reports_idempotency
  ON reward_reports (org_id, created_by, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE FUNCTION reward_test_case_insert_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1 FROM reward_versions WHERE id = NEW.version_id FOR SHARE;
  IF EXISTS (SELECT 1 FROM reward_versions WHERE id = NEW.version_id
             AND state IN ('PUBLISHED', 'DEPRECATED')) THEN
    RAISE EXCEPTION 'cannot add a case to published reward version %', NEW.version_id
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER reward_test_case_insert_guard BEFORE INSERT ON reward_test_cases
  FOR EACH ROW EXECUTE FUNCTION reward_test_case_insert_guard();

CREATE FUNCTION reward_deprecation_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.deprecated_at IS DISTINCT FROM OLD.deprecated_at
     AND NOT (OLD.state = 'PUBLISHED' AND NEW.state = 'DEPRECATED'
              AND NEW.deprecated_at IS NOT NULL) THEN
    RAISE EXCEPTION 'cannot change the deprecation timestamp of reward version %', OLD.id
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER reward_deprecation_guard BEFORE UPDATE ON reward_versions
  FOR EACH ROW EXECUTE FUNCTION reward_deprecation_guard();

ALTER TABLE reward_reports ADD COLUMN golden_samples jsonb;
ALTER TABLE reward_definitions ADD COLUMN golden_samples jsonb;

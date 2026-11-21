-- The immutable config snapshot of a run (FR-TR-8, Doc 09 §3.5).
CREATE TABLE run_configs (
  run_id uuid PRIMARY KEY,
  org_id uuid NOT NULL,
  submitted jsonb NOT NULL,
  resolved jsonb NOT NULL,
  resolved_sha256 bytea NOT NULL CHECK (length(resolved_sha256) = 32),
  dataset_version_ids uuid[] NOT NULL,
  seeds jsonb NOT NULL,
  -- Image digest or lock hash, once an engine validator pins it (P24).
  runtime_ref text NULL,
  estimate_id uuid NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (org_id, run_id) REFERENCES runs (org_id, id)
);

CREATE FUNCTION run_configs_refuse_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'run_configs rows are immutable (run %)', OLD.run_id
    USING ERRCODE = 'restrict_violation';
END $$;

CREATE TRIGGER run_configs_immutable BEFORE UPDATE OR DELETE ON run_configs
  FOR EACH ROW EXECUTE FUNCTION run_configs_refuse_change();

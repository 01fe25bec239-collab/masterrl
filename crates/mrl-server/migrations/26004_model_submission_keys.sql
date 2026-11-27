CREATE TABLE model_submission_keys (
  org_id uuid NOT NULL,
  project_id uuid NOT NULL,
  principal_id text NOT NULL,
  key text NOT NULL,
  fingerprint bytea NOT NULL CHECK (length(fingerprint) = 32),
  response jsonb,
  PRIMARY KEY (org_id, project_id, principal_id, key)
);

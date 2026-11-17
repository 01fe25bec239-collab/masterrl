-- Secret references (Doc 09 §3.10). Values are never stored here: `backend = 'secret_store'`
-- keeps the value in the server's SecretStore under `ref`; `backend = 'env'` reads the
-- environment variable `ref` when resolved (CLI use).
CREATE TABLE secrets_metadata (
  id uuid PRIMARY KEY,
  org_id text NOT NULL,
  project_id text NULL,
  name text NOT NULL,
  backend text NOT NULL CHECK (backend IN ('secret_store', 'env')),
  ref text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('hf_token', 'cloud_credentials', 's3', 'oidc', 'generic')),
  allowed_uses text[] NOT NULL,
  created_by text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  rotated_at timestamptz NULL,
  UNIQUE NULLS NOT DISTINCT (org_id, project_id, name)
);

-- Which secrets a run may receive, and how they are mounted.
CREATE TABLE run_secret_grants (
  run_id uuid NOT NULL,
  secret_id uuid NOT NULL REFERENCES secrets_metadata (id) ON DELETE CASCADE,
  mount text NOT NULL CHECK (mount IN ('file', 'env')),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, secret_id)
);

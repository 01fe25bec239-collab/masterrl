-- Generic artifacts (P19): eval reports, exports and other outputs, with their
-- artifact-manifest/v1. Files live under `prefix`. Two-phase: a PENDING row reserves the
-- name, the objects are checked against the manifest, then the row becomes COMMITTED (or FAILED).
CREATE TABLE artifacts (
  id uuid PRIMARY KEY,
  org_id text NOT NULL,
  project_id text NULL,
  name text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('model', 'dataset', 'checkpoint', 'eval', 'export', 'other')),
  prefix text NOT NULL,
  manifest jsonb NOT NULL,
  size_bytes bigint NOT NULL,
  state text NOT NULL CHECK (state IN ('PENDING', 'COMMITTED', 'FAILED')),
  created_by text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  committed_at timestamptz NULL
);

-- A name is taken by a pending or committed artifact; failed ones free it again.
CREATE UNIQUE INDEX artifacts_name ON artifacts (org_id, project_id, name)
  NULLS NOT DISTINCT WHERE state <> 'FAILED';
CREATE INDEX artifacts_org ON artifacts (org_id, id);

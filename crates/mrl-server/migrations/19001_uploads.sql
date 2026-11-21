-- Resumable uploads (P19). Parts are objects under `orgs/{org}/uploads/{id}/parts/`; the
-- finished file becomes the content-addressed blob `orgs/{org}/blobs/sha256/..`.
-- `org_id` is the caller's org as text (solo has no org uuid; it uses `local`).
CREATE TABLE uploads (
  id uuid PRIMARY KEY,
  org_id text NOT NULL,
  project_id text NULL,
  created_by text NOT NULL,
  filename text NOT NULL,
  size bigint NOT NULL CHECK (size >= 0),
  sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  part_size bigint NOT NULL CHECK (part_size > 0),
  state text NOT NULL CHECK (state IN ('OPEN', 'COMPLETED', 'ABORTED', 'EXPIRED')),
  blob_key text NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  finished_at timestamptz NULL
);

CREATE INDEX uploads_open_expiry ON uploads (expires_at) WHERE state = 'OPEN';
CREATE INDEX uploads_org ON uploads (org_id, id);

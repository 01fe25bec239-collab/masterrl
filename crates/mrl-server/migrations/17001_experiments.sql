-- Experiments group runs inside a project (Doc 09 §3.5).
-- project_id has no foreign key yet: the projects table belongs to P16 and lands in the same wave.
CREATE TABLE experiments (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL,
  project_id uuid NOT NULL,
  name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
  description text NULL,
  tags text[] NOT NULL DEFAULT '{}',
  version bigint NOT NULL DEFAULT 0,
  created_by text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, id),
  UNIQUE (org_id, project_id, name)
);
CREATE INDEX experiments_project ON experiments (org_id, project_id, id DESC);

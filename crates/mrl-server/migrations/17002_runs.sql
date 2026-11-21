-- One run per TRAIN/EVAL job: the ML record users browse (Doc 09 §3.5).
-- engine_version_id and base_model_version_id have no foreign keys yet (P24, P26 own those tables).
CREATE TABLE runs (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL,
  project_id uuid NOT NULL,
  experiment_id uuid NOT NULL,
  name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
  kind text NOT NULL CHECK (kind IN ('TRAIN','EVAL')),
  algorithm_id text NOT NULL,
  engine_version_id uuid NULL,
  base_model_version_id uuid NOT NULL,
  reward_version_id uuid NULL,
  parent_run_id uuid NULL,
  parent_checkpoint_id uuid NULL,
  latest_committed_checkpoint_id uuid NULL,
  final_checkpoint_id uuid NULL,
  -- Mirror of the job's state for listing; JobTransitions keeps it in step.
  status_summary text NOT NULL DEFAULT 'CREATED',
  -- Durable submit key: the same principal and key always mean the same run.
  submit_key text NOT NULL,
  submit_fingerprint bytea NOT NULL,
  created_by text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, id),
  UNIQUE (org_id, created_by, submit_key),
  FOREIGN KEY (org_id, experiment_id) REFERENCES experiments (org_id, id),
  FOREIGN KEY (org_id, parent_run_id) REFERENCES runs (org_id, id)
);
CREATE INDEX runs_project_created ON runs (org_id, project_id, id DESC);
CREATE INDEX runs_experiment ON runs (org_id, experiment_id, id DESC);

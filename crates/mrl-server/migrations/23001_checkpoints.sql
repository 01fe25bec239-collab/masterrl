-- P23: checkpoints and their files (Doc 09 §3.7), plus the object deletion queue.
-- Foreign keys use plain ids (not (org_id, id)), so P17's runs-rehome can move a run's org
-- without touching these rows; reads authorize through the run.
CREATE TABLE checkpoints (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL,
  -- The org text the objects are stored under (`orgs/{storage_org}/…`).
  storage_org text NOT NULL,
  run_id uuid NOT NULL REFERENCES runs (id),
  job_id uuid NOT NULL REFERENCES jobs (id),
  attempt_id uuid NOT NULL REFERENCES job_attempts (id),
  -- Fencing: a commit is refused unless this equals jobs.epoch at commit time.
  attempt_epoch bigint NOT NULL,
  global_step bigint NOT NULL CHECK (global_step >= 0),
  policy_version bigint NULL,
  kind text NOT NULL CHECK (kind IN ('full_resume','weights_only','adapter_only')),
  layout text NOT NULL,
  is_final bool NOT NULL DEFAULT false,
  state text NOT NULL CHECK (state IN ('WRITING','UPLOADED','VERIFYING','COMMITTED','CORRUPT','TOMBSTONED')),
  state_version bigint NOT NULL DEFAULT 0,
  verification_level text NULL CHECK (verification_level IN ('manifest_head','full_rehash')),
  manifest_key text NULL,
  manifest_sha256 bytea NULL CHECK (length(manifest_sha256) = 32),
  total_bytes bigint NULL,
  file_count int NULL,
  topology jsonb NULL,
  metrics_at_save jsonb NULL,
  corrupt_reason text NULL,
  -- The worker asked to commit (CheckpointCommitRequest).
  commit_requested_at timestamptz NULL,
  -- The UploadArtifacts command that carries the current upload tickets, and when they end.
  upload_command_id uuid NULL,
  tickets_expire_at timestamptz NULL,
  node_id uuid NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  committed_at timestamptz NULL,
  verified_at timestamptz NULL,
  tombstoned_at timestamptz NULL,
  UNIQUE (org_id, id)
);
CREATE INDEX ckpt_run_committed ON checkpoints (run_id, global_step DESC) WHERE state = 'COMMITTED';
CREATE UNIQUE INDEX one_final_per_run ON checkpoints (run_id) WHERE is_final AND state = 'COMMITTED';
CREATE INDEX ckpt_open ON checkpoints (created_at) WHERE state IN ('WRITING','UPLOADED','VERIFYING');

CREATE TABLE checkpoint_files (
  checkpoint_id uuid NOT NULL REFERENCES checkpoints (id),
  org_id uuid NOT NULL,
  path text NOT NULL,
  role text NOT NULL,
  object_key text NOT NULL,
  sha256 bytea NOT NULL CHECK (length(sha256) = 32),
  size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
  -- The stored object's version at commit (entity tag), when the backend reports one.
  storage_checksum text NULL,
  PRIMARY KEY (checkpoint_id, path)
);

-- Objects to delete (tombstones, stripped optimizer state). Rows go once the object is gone.
CREATE TABLE checkpoint_object_deletions (
  storage_org text NOT NULL,
  object_key text NOT NULL,
  checkpoint_id uuid NOT NULL REFERENCES checkpoints (id),
  queued_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (storage_org, object_key)
);

ALTER TABLE runs
  ADD CONSTRAINT runs_latest_committed_checkpoint_fk
    FOREIGN KEY (latest_committed_checkpoint_id) REFERENCES checkpoints (id),
  ADD CONSTRAINT runs_final_checkpoint_fk
    FOREIGN KEY (final_checkpoint_id) REFERENCES checkpoints (id);

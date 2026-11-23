-- Upload tickets target staging only. Published files have their own object keys.
ALTER TABLE checkpoint_files ADD COLUMN staging_key text;
ALTER TABLE checkpoints ADD COLUMN commit_checked_at timestamptz;
ALTER TABLE checkpoints ADD COLUMN tickets_checked_at timestamptz;

-- A commit may reach the core before its stage message.
CREATE TABLE checkpoint_early_commits (
    checkpoint_id uuid NOT NULL,
    attempt_id uuid NOT NULL REFERENCES job_attempts (id),
    epoch bigint NOT NULL,
    node_id uuid NOT NULL,
    digest bytea NOT NULL CHECK (length(digest) = 32),
    requested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (checkpoint_id, attempt_id, epoch, node_id)
);

ALTER TABLE checkpoint_object_deletions ADD COLUMN delete_after timestamptz NOT NULL DEFAULT now();

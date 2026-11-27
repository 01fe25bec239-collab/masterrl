-- A1 (INT-6): what a built-in job runs, and where its outputs went. One row per job; the
-- job row (P17) holds org, project and state, so the solo-to-team move needs nothing here.
CREATE TABLE builtin_jobs (
  job_id uuid PRIMARY KEY REFERENCES jobs (id) ON DELETE CASCADE,
  task text NOT NULL,
  inputs jsonb NOT NULL,
  -- Stored files staged for the task (InputFile list).
  files jsonb NOT NULL DEFAULT '[]',
  -- Secrets delivered as credentials (BuiltinGrant list).
  grants jsonb NOT NULL DEFAULT '[]',
  -- The latest outputs upload: attempt, org-relative storage folder, manifest, command.
  upload_attempt_id uuid NULL,
  upload_prefix text NULL,
  upload_manifest jsonb NULL,
  upload_command_id uuid NULL,
  -- The registered outputs, or why they could not be registered.
  artifact_id uuid NULL REFERENCES artifacts (id),
  failure text NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX builtin_jobs_pending ON builtin_jobs (upload_command_id)
  WHERE upload_command_id IS NOT NULL AND artifact_id IS NULL AND failure IS NULL;

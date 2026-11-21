-- One try at running a job (Doc 09 §3.5). Only JobTransitions changes `state`.
CREATE TABLE job_attempts (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL,
  job_id uuid NOT NULL,
  attempt_no int NOT NULL CHECK (attempt_no >= 1),
  epoch bigint NOT NULL,
  state text NOT NULL CHECK (state IN ('PENDING','LAUNCHING','RUNNING','EXITED_OK','EXITED_ERROR','KILLED','LOST')),
  state_version bigint NOT NULL DEFAULT 0,
  resume_from_checkpoint_id uuid NULL,
  lease_expires_at timestamptz NULL,
  exit_report jsonb NULL,
  failure_class text NULL,
  last_step bigint NULL,
  started_at timestamptz NULL,
  ended_at timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, id),
  UNIQUE (job_id, attempt_no),
  FOREIGN KEY (org_id, job_id) REFERENCES jobs (org_id, id)
);
-- Invariant: at most one live attempt per job.
CREATE UNIQUE INDEX one_live_attempt ON job_attempts (job_id)
  WHERE state IN ('PENDING','LAUNCHING','RUNNING');
CREATE INDEX attempts_lease ON job_attempts (lease_expires_at) WHERE state IN ('LAUNCHING','RUNNING');

ALTER TABLE jobs ADD FOREIGN KEY (org_id, current_attempt_id) REFERENCES job_attempts (org_id, id);

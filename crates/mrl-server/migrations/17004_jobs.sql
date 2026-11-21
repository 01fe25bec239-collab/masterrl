-- Scheduled work (Doc 09 §3.5). Only JobTransitions changes `state`.
-- pool_id has no foreign key yet (pools land with the scheduler, P21) and stays NULL until placed.
CREATE TABLE jobs (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL,
  project_id uuid NOT NULL,
  kind text NOT NULL CHECK (kind IN ('TRAIN','EVAL','DATA_PREP','IMPORT','REWARD_PREVIEW',
        'MERGE_ADAPTER','CONVERT','QUANTIZE','EXPORT','GENERATE')),
  run_id uuid NULL,
  subject_ref jsonb NULL,
  state text NOT NULL CHECK (state IN ('CREATED','VALIDATING','REJECTED','QUEUED','ALLOCATING',
        'PREPARING','RUNNING','PAUSING','PAUSED','RESUMING','FINALIZING','COMPLETED',
        'CANCELLING','CANCELLED','RETRY_WAIT','FAILED')),
  state_version bigint NOT NULL DEFAULT 0,
  -- Bumped by every change (state or priority); the ETag.
  version bigint NOT NULL DEFAULT 0,
  state_reason jsonb NULL,
  failure_class text NULL,
  epoch bigint NOT NULL DEFAULT 0,
  priority int NOT NULL DEFAULT 0,
  pool_id uuid NULL,
  resource_request jsonb NOT NULL,
  retry_policy jsonb NOT NULL,
  retries_used jsonb NOT NULL DEFAULT '{}',
  current_attempt_id uuid NULL,
  queued_at timestamptz NULL,
  started_at timestamptz NULL,
  finished_at timestamptz NULL,
  next_eligible_at timestamptz NULL,
  max_runtime_seconds bigint NULL CHECK (max_runtime_seconds > 0),
  created_by text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, id),
  FOREIGN KEY (org_id, run_id) REFERENCES runs (org_id, id),
  CHECK ((kind IN ('TRAIN','EVAL')) = (run_id IS NOT NULL))
);
CREATE UNIQUE INDEX jobs_one_per_run ON jobs (run_id) WHERE run_id IS NOT NULL;
CREATE INDEX jobs_queue ON jobs (pool_id, priority DESC, queued_at) WHERE state = 'QUEUED';
CREATE INDEX jobs_retry ON jobs (next_eligible_at) WHERE state = 'RETRY_WAIT';
CREATE INDEX jobs_nonterminal ON jobs (state, updated_at)
  WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED','REJECTED');
CREATE INDEX jobs_org_created ON jobs (org_id, id DESC);

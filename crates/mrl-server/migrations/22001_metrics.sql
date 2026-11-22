-- P22: metric history (Doc 09 §3.7). Series dictionary, weekly-partitioned raw points,
-- downsampled rollups and ingest receipts (the SSE tail cursor, D-04-07).
CREATE TABLE metric_series (
  id bigserial PRIMARY KEY,
  org_id uuid NOT NULL,
  run_id uuid NOT NULL,
  name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
  split text NOT NULL DEFAULT '' CHECK (length(split) <= 50),
  rank int NULL,                                             -- NULL = aggregate
  kind text NOT NULL DEFAULT 'gauge' CHECK (kind IN ('gauge','counter','histogram_summary')),
  freshness text NOT NULL CHECK (freshness IN ('live','sampled','delayed')),
  declared bool NOT NULL,                                    -- in the engine manifest?
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE NULLS NOT DISTINCT (run_id, name, split, rank)
);

-- One receipt per worker message: replays of the same message are dropped (UNIQUE).
CREATE TABLE metric_batches (
  id bigserial PRIMARY KEY,
  org_id uuid NOT NULL,
  run_id uuid NOT NULL,
  attempt_id uuid NOT NULL,
  agent_seq bigint NOT NULL,                                 -- WorkerMessage.seq
  point_count int NOT NULL,
  min_step bigint NULL,
  max_step bigint NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
  UNIQUE (attempt_id, agent_seq)
);
-- SSE tails a run's batches by (xid, id), like the outbox dispatcher.
CREATE INDEX metric_batches_tail ON metric_batches (run_id, xid, id);
CREATE INDEX metric_batches_received ON metric_batches (received_at);

-- Raw points, append-only, in weekly partitions the server creates ahead of time. Unique keys
-- on a partitioned table must hold the partition key, so replays are dropped by the batch
-- receipt (UNIQUE (attempt_id, agent_seq)) instead of a primary key. Both attempts' points of
-- a re-run step are kept; queries take, per step, the point of the latest attempt
-- (attempt_no) that reached it.
CREATE TABLE metric_points (
  series_id bigint NOT NULL,
  attempt_id uuid NOT NULL,
  attempt_no int NOT NULL,
  batch_id bigint NOT NULL,
  step bigint NOT NULL,
  ts timestamptz NOT NULL,
  value double precision NOT NULL
) PARTITION BY RANGE (ts);
CREATE INDEX metric_points_latest ON metric_points (series_id, step, attempt_no DESC, ts DESC);
CREATE INDEX metric_points_time ON metric_points (series_id, ts);
CREATE INDEX metric_points_batch ON metric_points (batch_id);

-- Downsampled tiers: '1m' (bucket_start = minute since the epoch), 'step_bucket'
-- (bucket_start = step / 10). Rewritten from raw points, so they follow "latest attempt wins".
CREATE TABLE metric_rollups (
  series_id bigint NOT NULL,
  tier text NOT NULL CHECK (tier IN ('1m','step_bucket')),
  bucket_start bigint NOT NULL,
  min double precision NOT NULL,
  max double precision NOT NULL,
  avg double precision NOT NULL,
  last double precision NOT NULL,
  n int NOT NULL,
  PRIMARY KEY (series_id, tier, bucket_start)
);

-- Buckets whose rollups must be rewritten: written with the points, cleared by the rollup pass.
CREATE TABLE metric_rollup_dirty (
  series_id bigint NOT NULL,
  tier text NOT NULL CHECK (tier IN ('1m','step_bucket')),
  bucket_start bigint NOT NULL,
  PRIMARY KEY (series_id, tier, bucket_start)
);

-- Latest progress of each run (CONTROL `worker.progress`): later attempts and later worker
-- messages win.
CREATE TABLE run_progress (
  run_id uuid PRIMARY KEY,
  org_id uuid NOT NULL,
  attempt_id uuid NOT NULL,
  attempt_no int NOT NULL,
  worker_seq bigint NOT NULL,
  step bigint NOT NULL,
  total_steps bigint NOT NULL,
  epoch double precision NOT NULL,
  phase text NOT NULL,
  tokens_seen bigint NOT NULL,
  eta_seconds double precision NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  xid xid8 NOT NULL DEFAULT pg_current_xact_id()
);

-- Peak resource use per attempt, device and phase (Doc 07 §6.10), from `resource/…` metrics.
CREATE TABLE resource_actuals (
  attempt_id uuid NOT NULL,
  org_id uuid NOT NULL,
  run_id uuid NOT NULL,
  device text NOT NULL,                                      -- '' = the whole host
  phase text NOT NULL,
  signal text NOT NULL,                                      -- torch_allocated_peak, process_peak, …
  peak double precision NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (attempt_id, device, phase, signal)
);
CREATE INDEX resource_actuals_run ON resource_actuals (run_id);

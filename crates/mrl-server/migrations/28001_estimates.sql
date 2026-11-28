-- P28: resource estimates from dry-run probes, and the calibration loop (Doc 07 §6.10–6.11).

-- One estimate per probe (a short CPU-only dry-run job on a connected machine). The engine's
-- range is stored as it came (GiB per GPU); calibration is applied when it is read.
CREATE TABLE resource_estimates (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL,
  project_id uuid NOT NULL,
  -- The probe job (kind TRAIN, no run). NULL never: every estimate comes from a probe.
  job_id uuid NOT NULL UNIQUE REFERENCES jobs (id),
  -- sha256 of the resolved config, the same bytes as run_configs.resolved_sha256.
  inputs_sha256 bytea NOT NULL CHECK (length(inputs_sha256) = 32),
  engine_version_id uuid NOT NULL REFERENCES engine_versions (id),
  algorithm_id text NOT NULL,
  -- engine × method × PEFT × GPU arch bucket (estimates::calibration_key).
  calibration_key text NOT NULL,
  -- `<engine>@<version>`: the code that produced the range.
  estimator_version text NOT NULL,
  -- What the probe launch needs (resolved config, model, datasets, seeds).
  probe_spec jsonb NOT NULL,
  state text NOT NULL CHECK (state IN ('PENDING', 'READY')),
  -- The machine that answered, and its answer.
  node_id uuid NULL,
  ok boolean NULL,
  issues jsonb NOT NULL DEFAULT '[]',
  gpu_mem_lo_gib double precision NULL CHECK (gpu_mem_lo_gib >= 0),
  gpu_mem_hi_gib double precision NULL CHECK (gpu_mem_hi_gib >= gpu_mem_lo_gib),
  total_steps bigint NULL CHECK (total_steps >= 0),
  wall_lo_seconds double precision NULL,
  wall_hi_seconds double precision NULL,
  assumptions jsonb NOT NULL DEFAULT '[]',
  created_at timestamptz NOT NULL DEFAULT now(),
  decided_at timestamptz NULL
);
CREATE INDEX resource_estimates_inputs
  ON resource_estimates (org_id, project_id, inputs_sha256, decided_at DESC)
  WHERE state = 'READY';

-- Per configuration class: n, Σx and Σx² of x = ln(actual peak / analytic expected).
-- Per installation (Doc 09 §3): one row per key, shared by every org.
CREATE TABLE estimator_calibration (
  key text PRIMARY KEY,
  n integer NOT NULL CHECK (n >= 0),
  sum_ln double precision NOT NULL,
  sum_ln_sq double precision NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- One sample per attempt, so a replayed event can't count twice; also the "recent misses"
-- list an admin page can show.
CREATE TABLE estimator_calibration_samples (
  attempt_id uuid PRIMARY KEY,
  key text NOT NULL,
  estimate_id uuid NOT NULL REFERENCES resource_estimates (id),
  ratio double precision NOT NULL CHECK (ratio > 0),
  at timestamptz NOT NULL DEFAULT now()
);

-- P24: the engine registry (Doc 09 §3, Doc 05 §10/§12.1).
-- First-party bundles are embedded in the server and upserted at startup; a version's
-- manifest never changes once stored (immutable per name + version).

CREATE TABLE training_engines (
  id uuid PRIMARY KEY,
  name text NOT NULL UNIQUE,                         -- metadata.name, like `trl`
  kind text NOT NULL CHECK (kind IN ('engine', 'builtin')),
  origin text NOT NULL CHECK (origin IN ('first_party', 'third_party')),
  scope text NOT NULL DEFAULT 'platform' CHECK (scope IN ('platform', 'org')),
  org_id uuid NULL,                                  -- set for org-scoped third-party engines
  dev_only boolean NOT NULL DEFAULT false,           -- the FakeEngine: shown only on dev servers
  listed boolean NOT NULL DEFAULT true,              -- false hides it from lists and checks
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE engine_versions (
  id uuid PRIMARY KEY,
  engine_id uuid NOT NULL REFERENCES training_engines(id),
  version text NOT NULL,                             -- metadata.version (adapter semver)
  manifest jsonb NOT NULL,                           -- merged manifest, as embedded
  schemas jsonb NOT NULL DEFAULT '{}',               -- config/UI schemas by manifest path
  manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
  runtime_ref text NULL,                             -- lock hash of the trainer environment
  worker_protocol text NULL,                         -- spec.adapter.workerProtocol range
  status text NOT NULL CHECK (status IN ('PENDING_APPROVAL', 'PROBING', 'ENABLED', 'DISABLED', 'REVOKED')),
  state_version bigint NOT NULL DEFAULT 0,           -- bumped on every status change (event seq)
  -- Per hardware class: {"cpu": {"status": "ENABLED"|"FAILED", "node_id", "evidence_id", "at"}}.
  probe_results jsonb NOT NULL DEFAULT '{}',
  approved_by text NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (engine_id, version)
);

-- Flattened from the manifest for queries (Doc 09 OQ-09-2: flattened for indexability).
CREATE TABLE algorithm_definitions (
  engine_version_id uuid NOT NULL REFERENCES engine_versions(id),
  algorithm_id text NOT NULL,
  display_name text NOT NULL,
  summary text NOT NULL,
  parent_trainer text NOT NULL,
  switch jsonb NOT NULL DEFAULT '{}',
  defaults jsonb NOT NULL DEFAULT '{}',
  maturity text NOT NULL CHECK (maturity IN ('stable', 'beta', 'experimental')),
  fidelity text NOT NULL CHECK (fidelity IN ('full', 'partial', 'approximation')),
  fidelity_note text NULL,
  required_schemas text[] NOT NULL,
  required_signals text[] NOT NULL,                  -- signals marked `required`
  config_schema jsonb NULL,
  ui_schema jsonb NULL,
  presets jsonb NOT NULL DEFAULT '[]',
  emitted_metrics jsonb NOT NULL DEFAULT '[]',
  verified_hw text[] NOT NULL DEFAULT '{}',          -- hardware classes with a passed probe
  PRIMARY KEY (engine_version_id, algorithm_id)
);
CREATE INDEX algorithm_definitions_algorithm ON algorithm_definitions (algorithm_id);

-- Evidence that an engine version works on a hardware class: probe answers from machines
-- (source `probe`) and CI conformance runs (source `ci`). A probe row is PENDING from the
-- moment its Probe command is written until the machine answers.
CREATE TABLE capability_evidence (
  id uuid PRIMARY KEY,                               -- for probes, also Probe.request_id
  engine_version_id uuid NOT NULL REFERENCES engine_versions(id),
  algorithm_id text NULL,
  hardware_class text NOT NULL CHECK (hardware_class IN ('cpu', 'nvidia', 'amd', 'apple')),
  source text NOT NULL CHECK (source IN ('probe', 'ci')),
  node_id uuid NULL,                                 -- the machine asked (probes)
  command_id uuid NULL,                              -- its agent_commands row (probes)
  test_id text NULL,
  result text NOT NULL CHECK (result IN ('PENDING', 'PASSED', 'FAILED')),
  detail jsonb NOT NULL DEFAULT '{}',                -- check outcomes and summaries
  ci_run_url text NULL,
  at timestamptz NOT NULL DEFAULT now(),
  decided_at timestamptz NULL,
  CHECK (source <> 'probe' OR node_id IS NOT NULL)
);
CREATE INDEX capability_evidence_version ON capability_evidence (engine_version_id, hardware_class, result);

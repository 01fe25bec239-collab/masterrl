-- P26: the model registry (Doc 09 §3.2): logical models, immutable versions, their files and
-- per-project license acknowledgements. Rows live under their project's org; files point at
-- content-addressed blobs (`orgs/{org}/blobs/sha256/…`), so a version never changes.

CREATE TABLE models (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL,
  project_id uuid NOT NULL,
  name text NOT NULL CHECK (length(name) BETWEEN 1 AND 128),
  family text NULL,                                  -- of the newest READY version
  created_by text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, project_id, name)
);

CREATE TABLE model_versions (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL,
  project_id uuid NOT NULL,
  model_id uuid NOT NULL REFERENCES models (id),
  seq int NOT NULL CHECK (seq >= 1),
  -- NULL until inspected.
  kind text NULL CHECK (kind IN ('full_weights', 'adapter', 'inference_only')),
  source_kind text NOT NULL CHECK (source_kind IN ('hub', 'upload', 'node_path', 'checkpoint', 'merge')),
  -- hub: {repo, revision, commit}; upload: {uploads}; node_path: {node, path};
  -- checkpoint: {checkpoint_id, run_id}; merge: {base, adapter, dtype}.
  source_ref jsonb NOT NULL,
  base_model_version_id uuid NULL REFERENCES model_versions (id),
  family text NULL,                                  -- decoder, decoder-moe, encoder, encoder-decoder, vlm
  architecture text NULL,
  model_type text NULL,
  param_count bigint NULL,
  dtypes text[] NOT NULL DEFAULT '{}',
  context_length int NULL,
  is_moe bool NOT NULL DEFAULT false,
  quantization text NULL,
  tokenizer_sha256 bytea NULL CHECK (length(tokenizer_sha256) = 32),
  chat_template_sha256 bytea NULL CHECK (length(chat_template_sha256) = 32),
  requires_remote_code bool NOT NULL DEFAULT false,
  -- Approval holds for exactly this revision (`remote_code_approved_for`).
  remote_code_approved_by text NULL,
  remote_code_approved_at timestamptz NULL,
  remote_code_approved_for text NULL,
  license_spdx text NULL,
  license_kind text NULL CHECK (license_kind IN ('spdx', 'custom', 'unknown')),
  license_sha256 bytea NULL CHECK (length(license_sha256) = 32),
  -- sha256 of the single weights file, or of the sorted `path\0sha256\n` list when sharded.
  weights_sha256 bytea NULL CHECK (length(weights_sha256) = 32),
  manifest_sha256 bytea NULL CHECK (length(manifest_sha256) = 32),
  state text NOT NULL CHECK (state IN ('IMPORTING', 'READY', 'INVALID', 'ARCHIVED')),
  stage text NOT NULL DEFAULT 'EXPERIMENTAL' CHECK (stage IN ('EXPERIMENTAL', 'CANDIDATE', 'APPROVED', 'ARCHIVED')),
  compatibility jsonb NOT NULL DEFAULT '{}',         -- per engine version, from P24's matcher
  inspection jsonb NULL,                             -- P15 ModelInspection (no tensor list)
  invalid_reason jsonb NULL,                         -- {code, message}
  job_id uuid NULL REFERENCES jobs (id),             -- the job that inspects or builds it
  created_by text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  ready_at timestamptz NULL,
  UNIQUE (model_id, seq),
  CHECK (kind IS DISTINCT FROM 'adapter' OR base_model_version_id IS NOT NULL),
  CHECK (state <> 'READY' OR (kind IS NOT NULL AND weights_sha256 IS NOT NULL))
);
CREATE INDEX model_versions_org_stage ON model_versions (org_id, stage);
CREATE UNIQUE INDEX model_versions_job ON model_versions (job_id) WHERE job_id IS NOT NULL;

CREATE TABLE model_files (
  model_version_id uuid NOT NULL REFERENCES model_versions (id),
  org_id uuid NOT NULL,
  path text NOT NULL CHECK (length(path) BETWEEN 1 AND 1024),
  role text NOT NULL CHECK (role IN ('weights', 'config', 'tokenizer', 'adapter', 'code', 'other')),
  object_key text NOT NULL,                          -- a content-addressed blob
  sha256 bytea NOT NULL CHECK (length(sha256) = 32),
  size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
  -- Pickle weights are never stored (Doc 09 §3.2): the import refuses them.
  format text NOT NULL CHECK (format IN ('safetensors', 'gguf', 'json', 'txt', 'other')),
  PRIMARY KEY (model_version_id, path)
);

CREATE TABLE license_acknowledgements (
  org_id uuid NOT NULL,
  project_id uuid NOT NULL,
  model_id uuid NOT NULL REFERENCES models (id),
  user_id text NOT NULL,
  license_text_sha256 bytea NOT NULL CHECK (length(license_text_sha256) = 32),
  at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, model_id)
);

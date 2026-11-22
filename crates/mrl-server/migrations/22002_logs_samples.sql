-- P22: hot log tail, sealed log segments and the sample index (Doc 09 §3.7, D-04-11).
CREATE TABLE log_chunks (
  id bigserial PRIMARY KEY,
  org_id uuid NOT NULL,
  run_id uuid NOT NULL,
  attempt_id uuid NOT NULL,
  rank int NOT NULL,
  stream text NOT NULL CHECK (stream IN ('stdout','stderr','harness','agent')),
  "offset" bigint NOT NULL,
  data bytea NOT NULL CHECK (length(data) <= 65536),          -- redacted by the harness
  received_at timestamptz NOT NULL DEFAULT now(),
  xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
  UNIQUE (attempt_id, rank, stream, "offset")
);
CREATE INDEX log_chunks_run ON log_chunks (run_id, id);
CREATE INDEX log_chunks_tail ON log_chunks (run_id, xid, id);
CREATE INDEX log_chunks_received ON log_chunks (received_at);

-- One sealed segment: the chunks of one attempt, rank and stream with ids id_lo..id_hi, as JSON
-- lines in object storage.
CREATE TABLE log_index (
  id bigserial PRIMARY KEY,
  org_id uuid NOT NULL,
  run_id uuid NOT NULL,
  attempt_id uuid NOT NULL,
  rank int NOT NULL,
  stream text NOT NULL,
  offset_lo bigint NOT NULL,
  offset_hi bigint NOT NULL,
  id_lo bigint NOT NULL,
  id_hi bigint NOT NULL,
  chunk_count int NOT NULL,
  object_key text NOT NULL UNIQUE,
  sha256 text NOT NULL,
  sealed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX log_index_run ON log_index (run_id, id_lo);

-- Generated samples: the text is in object storage, the index row here.
CREATE TABLE samples (
  id bigserial PRIMARY KEY,
  org_id uuid NOT NULL,
  run_id uuid NOT NULL,
  attempt_id uuid NOT NULL,
  worker_seq bigint NOT NULL,
  step bigint NOT NULL,
  object_key text NOT NULL,
  count int NOT NULL,
  reward_min double precision NULL,
  reward_max double precision NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (attempt_id, worker_seq)
);
CREATE INDEX samples_run_step ON samples (run_id, step, id);

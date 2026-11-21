-- P18: one-time join tokens (Doc 09 §3.6). Only the SHA-256 of a token is stored.
CREATE TABLE node_enrollment_tokens (
  id uuid PRIMARY KEY,
  pool_id uuid NOT NULL REFERENCES compute_pools(id),
  token_hash bytea NOT NULL UNIQUE,
  labels jsonb NOT NULL DEFAULT '{}',
  lifecycle text NOT NULL CHECK (lifecycle IN ('STATIC','EPHEMERAL')),
  endpoint jsonb NULL,                               -- set when the core dials the new machine
  expires_at timestamptz NOT NULL,
  used_at timestamptz NULL,
  used_by_node_id uuid NULL REFERENCES compute_nodes(id),
  created_by text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX enrollment_tokens_open ON node_enrollment_tokens (expires_at) WHERE used_at IS NULL;

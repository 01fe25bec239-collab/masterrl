-- Idempotency-Key records (Doc 09 §3.10). `scope` is the caller's principal id; the
-- fingerprint covers method, path and body, so method and path need no columns of their own.
-- `owner` names the server process running an IN_PROGRESS request: that process holds the
-- advisory lock (70110, owner) for as long as it lives.
CREATE TABLE idempotency_keys (
  scope text NOT NULL,
  key text NOT NULL,
  request_sha256 bytea NOT NULL,
  state text NOT NULL CHECK (state IN ('IN_PROGRESS', 'DONE')),
  owner int NULL,
  response_status int NULL,
  response_headers jsonb NULL,
  response_body bytea NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  PRIMARY KEY (scope, key)
);

CREATE INDEX idempotency_keys_expiry ON idempotency_keys (expires_at);

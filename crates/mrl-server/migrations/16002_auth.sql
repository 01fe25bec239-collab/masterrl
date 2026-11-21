-- Credentials (Doc 09 §2, Doc 11 §4). Token and session values are never stored: only hashes.

-- Personal access tokens for the CLI and SDK: `mrl_pat_<43 chars>`, shown once.
-- `token_hash` is HMAC-SHA256 with the server's pepper (kept in the OS keychain).
CREATE TABLE api_tokens (
  id uuid PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  org_id uuid NULL REFERENCES organizations (id) ON DELETE CASCADE,
  project_id uuid NULL REFERENCES projects (id) ON DELETE CASCADE,
  name text NOT NULL,
  token_prefix text NOT NULL,
  token_hash bytea NOT NULL UNIQUE,
  scopes text[] NOT NULL,
  expires_at timestamptz NOT NULL,
  last_used_at timestamptz NULL,
  revoked_at timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX api_tokens_user ON api_tokens (user_id);

-- Desktop renderer sessions (cookie `mrl_session` on 127.0.0.1). Both values are SHA-256 hashes
-- of 256-bit random strings.
CREATE TABLE sessions (
  id_hash bytea PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  csrf_hash bytea NOT NULL,
  idle_expires_at timestamptz NOT NULL,
  absolute_expires_at timestamptz NOT NULL,
  user_agent text NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX sessions_user ON sessions (user_id);

-- The Supabase signing keys last fetched, so tokens can be checked while offline.
CREATE TABLE auth_jwks (
  issuer text PRIMARY KEY,
  jwks jsonb NOT NULL,
  fetched_at timestamptz NOT NULL
);

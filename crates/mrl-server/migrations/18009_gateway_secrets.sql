-- P18: secrets shared by every replica (the CA key when `gateway.ca_store = database`), sealed
-- with XChaCha20-Poly1305 under a key-encryption key that never reaches the database.
CREATE TABLE gateway_secrets (
  name text PRIMARY KEY,
  nonce bytea NOT NULL,
  sealed bytea NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

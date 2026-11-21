-- Idempotent creation (P19 endpoints are never replayed by the generic middleware): a
-- retry with the same Idempotency-Key from the same caller in the same org finds the row it
-- made, after a fresh authorization check. `request_sha256` catches a different body.
ALTER TABLE uploads ADD COLUMN idempotency_key text NULL, ADD COLUMN request_sha256 text NULL;
CREATE UNIQUE INDEX uploads_idempotency ON uploads (org_id, created_by, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
ALTER TABLE artifacts ADD COLUMN idempotency_key text NULL, ADD COLUMN request_sha256 text NULL;
CREATE UNIQUE INDEX artifacts_idempotency ON artifacts (org_id, created_by, idempotency_key)
  WHERE idempotency_key IS NOT NULL AND state <> 'FAILED';
ALTER TABLE secrets_metadata ADD COLUMN idempotency_key text NULL, ADD COLUMN request_sha256 text NULL;
CREATE UNIQUE INDEX secrets_idempotency ON secrets_metadata (org_id, created_by, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

-- Artifacts whose files were all checked into blobs/sha256 when they committed. Rows from
-- before that (false) get each file checked on download.
ALTER TABLE artifacts ADD COLUMN files_checked boolean NOT NULL DEFAULT false;

-- Objects P19 is about to write. Written before the object, so a crash between the write
-- and the database commit that references it leaves a trace the cleanup job follows: after
-- 24 hours an intent for a blob nothing references deletes the blob; any intent also makes
-- its org's `uploads/` area visible to the cleanup job.
CREATE TABLE storage_intents (
  id uuid PRIMARY KEY,
  org_id text NOT NULL,
  key text NOT NULL,
  sha256 text NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX storage_intents_age ON storage_intents (created_at);

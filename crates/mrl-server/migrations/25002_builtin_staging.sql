CREATE TABLE builtin_output_staging (
  attempt_id uuid PRIMARY KEY REFERENCES job_attempts (id),
  storage_org_id uuid NOT NULL,
  prefix text NOT NULL,
  last_ticket_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX builtin_output_staging_age ON builtin_output_staging (last_ticket_at);

INSERT INTO builtin_output_staging (attempt_id, storage_org_id, prefix, last_ticket_at)
SELECT b.upload_attempt_id, j.org_id, b.upload_prefix, b.updated_at
  FROM builtin_jobs b JOIN jobs j ON j.id = b.job_id
 WHERE b.upload_attempt_id IS NOT NULL AND b.upload_prefix IS NOT NULL;

ALTER TABLE jobs DROP CONSTRAINT jobs_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_check CHECK (
  (kind IN ('TRAIN', 'EVAL')) = (run_id IS NOT NULL)
  OR (kind = 'TRAIN' AND run_id IS NULL AND (subject_ref ->> 'kind' = 'run_probe') IS TRUE)
);

ALTER TABLE resource_estimates DROP CONSTRAINT resource_estimates_state_check;
ALTER TABLE resource_estimates ADD CONSTRAINT resource_estimates_state_check
  CHECK (state IN ('PENDING', 'READY', 'EXPIRED'));
ALTER TABLE resource_estimates ADD COLUMN deadline timestamptz;
UPDATE resource_estimates SET deadline = created_at + interval '60 seconds';
ALTER TABLE resource_estimates ALTER COLUMN deadline SET NOT NULL;

CREATE TABLE estimator_calibration_pending (
  attempt_id uuid PRIMARY KEY,
  estimate_id uuid NOT NULL REFERENCES resource_estimates (id)
);

CREATE TABLE submission_probe_decisions (
  attempt_id uuid PRIMARY KEY,
  cancelled boolean NOT NULL,
  decided_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX resource_estimates_pending_deadline ON resource_estimates (deadline, job_id)
  WHERE state = 'PENDING';

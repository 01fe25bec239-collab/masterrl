-- P28: the settings check (`runs:validate`) runs as a short dry-run job of kind TRAIN with
-- no run (A1, Wave 6 prompt). P17's rule "TRAIN and EVAL jobs have a run" gets one
-- exception: a job whose subject is a run probe. Every other row keeps the old rule.
ALTER TABLE jobs DROP CONSTRAINT jobs_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_check CHECK (
  (kind IN ('TRAIN', 'EVAL')) = (run_id IS NOT NULL)
  OR (kind = 'TRAIN' AND run_id IS NULL AND subject_ref ->> 'kind' = 'run_probe')
);

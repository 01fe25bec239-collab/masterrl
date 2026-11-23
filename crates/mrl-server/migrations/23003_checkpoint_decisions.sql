-- INT-1: when the commit decision was sent to the agent, and the CheckpointRequest ids a
-- checkpoint answers. Rows decided before this are treated as sent.
ALTER TABLE checkpoints
  ADD COLUMN decision_sent_at timestamptz NULL,
  ADD COLUMN request_ids text[] NOT NULL DEFAULT '{}';
UPDATE checkpoints SET decision_sent_at = now() WHERE state IN ('COMMITTED','CORRUPT','TOMBSTONED');
CREATE INDEX checkpoints_undecided ON checkpoints (attempt_id)
  WHERE decision_sent_at IS NULL AND state IN ('COMMITTED','CORRUPT');

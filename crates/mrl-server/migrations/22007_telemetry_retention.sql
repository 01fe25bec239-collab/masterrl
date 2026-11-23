-- P22: receipts and archived steps expire (rollup::expire).
-- Receipts arrive in time order, so a BRIN index is enough to find the old ones.
ALTER TABLE telemetry_receipts ADD COLUMN received_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX telemetry_receipts_age ON telemetry_receipts USING brin (received_at);
CREATE INDEX metric_step_archive_age ON metric_step_archive (ts);

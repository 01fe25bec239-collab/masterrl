-- A `tight` estimate's safety settings change the config. The fingerprint of the adjusted
-- config finds the same estimate, so submitting `runs:validate`'s resolved_config as is keeps
-- its estimate (and the resolver does not apply the settings twice).
ALTER TABLE resource_estimates ADD COLUMN adjusted_sha256 bytea NULL
  CHECK (length(adjusted_sha256) = 32);

CREATE INDEX resource_estimates_adjusted
  ON resource_estimates (org_id, project_id, adjusted_sha256, decided_at DESC)
  WHERE state = 'READY' AND adjusted_sha256 IS NOT NULL;

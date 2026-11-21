-- Keep the original wire key for a rehomed run displaced by an existing org key.
ALTER TABLE runs ADD COLUMN legacy_submit_key text;
-- Recover keys already suffixed by the first rehome implementation.
UPDATE runs
SET legacy_submit_key = left(submit_key, -17), submit_key = 'legacy ' || id::text
WHERE right(submit_key, 17) = ' (local ' || right(replace(id::text, '-', ''), 8) || ')';

CREATE INDEX runs_legacy_submit_key ON runs (org_id, created_by, legacy_submit_key)
  WHERE legacy_submit_key IS NOT NULL;

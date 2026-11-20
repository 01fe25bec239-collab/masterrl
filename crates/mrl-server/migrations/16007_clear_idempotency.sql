-- Earlier P16 revisions let keyed requests to the secret-bearing endpoints reach the
-- idempotency store, which keeps a SHA-256 of the body (a fast check of a sync passphrase)
-- and the response for replay. Those endpoints now bypass the store; this removes what they
-- left, and nothing else:
-- - only rows written while P16 was installed (a database coming from wave-03 has none), and
-- - only rows whose kept response is one of those endpoints': a new token (`mrl_pat_`), a
--   session's `csrf_token`, a sync `recovery_key`, a restored `setup-profile/v1`, or the
--   MRL-IDEM-003 refusal (its fingerprint covers the passphrase).
-- Every other completed or running request keeps its replay protection.
DELETE FROM idempotency_keys
WHERE created_at >= (SELECT installed_on FROM _sqlx_migrations WHERE version = 16001)
  AND response_body IS NOT NULL
  AND (position(convert_to('mrl_pat_', 'UTF8') IN response_body) > 0
    OR position(convert_to('"csrf_token"', 'UTF8') IN response_body) > 0
    OR position(convert_to('"recovery_key"', 'UTF8') IN response_body) > 0
    OR position(convert_to('setup-profile/v1', 'UTF8') IN response_body) > 0
    OR position(convert_to('MRL-IDEM-003', 'UTF8') IN response_body) > 0);

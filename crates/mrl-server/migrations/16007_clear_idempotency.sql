-- Earlier P16 revisions let keyed requests to the secret-bearing endpoints (tokens, sessions,
-- sync setup and restore) reach the idempotency store, which keeps a SHA-256 of the body (a
-- fast check of a sync passphrase) and the response (a new token or recovery key). Those
-- endpoints now bypass the store; this clears what was kept. Rows only live 24 hours, so the
-- cost is that a retry in flight during the upgrade runs again instead of replaying.
DELETE FROM idempotency_keys;

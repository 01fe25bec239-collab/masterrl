-- The keychain name of the active sync key. A new key is saved under a new name first and
-- becomes active only when this row commits with its matching KDF settings; NULL means the
-- name `sync.key` used before this column existed.
ALTER TABLE sync_state ADD COLUMN key_ref text NULL;

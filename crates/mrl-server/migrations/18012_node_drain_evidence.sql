-- P18: what decommissioning waits for. `drained_at`: when the node started draining.
-- `running_reported_at`: the last heartbeat (only heartbeats; reconnects don't count) with its
-- running-attempt count. A node is removed only after a heartbeat newer than both the drain and
-- its last work start shows no attempts.
ALTER TABLE compute_nodes
  ADD COLUMN drained_at timestamptz NULL,
  ADD COLUMN running_reported_at timestamptz NULL;

-- The key a token was used with (SHA-256 of the public key in the request). A machine that lost
-- the answer may ask again with the same token and key while the token is valid and the node
-- has never connected; any other key is still refused.
ALTER TABLE node_enrollment_tokens ADD COLUMN used_key_sha256 text NULL;

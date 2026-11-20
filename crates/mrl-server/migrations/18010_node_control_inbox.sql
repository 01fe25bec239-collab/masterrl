-- P18: CONTROL-class agent messages accepted for a sink. Written in the transaction that checks
-- the session epoch (under the node row lock), delivered to the sink after that lock is released,
-- deleted once the sink took them. Undelivered rows are retried in order. A dispatcher leases the
-- node's oldest row (`leased_until`) instead of holding a connection while the sink runs.
CREATE TABLE node_control_inbox (
  id bigserial PRIMARY KEY,
  node_id uuid NOT NULL REFERENCES compute_nodes(id) ON DELETE CASCADE,
  stream smallint NOT NULL,
  seq bigint NOT NULL,
  session_epoch bigint NOT NULL,
  message bytea NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  leased_until timestamptz NULL,
  UNIQUE (node_id, stream, seq)
);
CREATE INDEX node_control_inbox_node ON node_control_inbox (node_id, id);

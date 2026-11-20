-- P18: a dispatcher leases the node's oldest kept CONTROL message instead of holding a
-- connection while the sink runs. `lease_token` names the dispatcher that holds the lease, so
-- only it may renew, release or delete the row.
ALTER TABLE node_control_inbox
  ADD COLUMN leased_until timestamptz NULL,
  ADD COLUMN lease_token uuid NULL;
CREATE INDEX node_control_inbox_node ON node_control_inbox (node_id, id);

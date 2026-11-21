-- The CONTROL epoch whose agent acknowledged the drain fence.
ALTER TABLE compute_nodes ADD COLUMN drain_ack_epoch bigint;

-- P18: clock skew from the last heartbeat (agent clock minus server clock), for node health.
ALTER TABLE compute_nodes ADD COLUMN clock_skew_ms bigint NULL;

-- When the node health check last ran, and when it last resumed after a core outage
-- (INT-3). Heartbeat ages count from `resumed_at` at the earliest.
CREATE TABLE node_health_ticks (
  id int PRIMARY KEY CHECK (id = 1),
  last_check_at timestamptz NOT NULL,
  resumed_at timestamptz NOT NULL
);

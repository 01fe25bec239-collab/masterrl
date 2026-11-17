-- P18: latest-wins GPU telemetry (TELEMETRY lane, D-04-05) and node incidents.
CREATE TABLE node_telemetry_latest (
  gpu_device_id uuid PRIMARY KEY REFERENCES gpu_devices(id),
  util_pct int NOT NULL,
  mem_used_bytes bigint NOT NULL,
  temp_c int NOT NULL,
  power_w double precision NULL,
  ecc_errors bigint NULL,
  xid_last int NULL,
  thermal_throttling boolean NOT NULL DEFAULT false,
  observed_at timestamptz NOT NULL
);

CREATE TABLE node_events (
  id uuid PRIMARY KEY,
  node_id uuid NOT NULL REFERENCES compute_nodes(id),
  gpu_device_id uuid NULL REFERENCES gpu_devices(id),
  kind text NOT NULL,
  severity text NOT NULL CHECK (severity IN ('info','warning','error')),
  details jsonb NOT NULL DEFAULT '{}',
  at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX node_events_node_at ON node_events (node_id, at DESC);

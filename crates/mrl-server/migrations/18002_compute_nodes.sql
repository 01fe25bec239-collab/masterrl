-- P18: machines and their GPUs (Doc 09 §3.6, node states §4.5).
CREATE TABLE compute_nodes (
  id uuid PRIMARY KEY,
  pool_id uuid NOT NULL REFERENCES compute_pools(id),
  hostname text NOT NULL,
  lifecycle text NOT NULL CHECK (lifecycle IN ('STATIC','EPHEMERAL')),
  status text NOT NULL CHECK (status IN ('ENROLLING','ONLINE','DEGRADED','OFFLINE','DRAINING','DECOMMISSIONED')),
  status_version bigint NOT NULL DEFAULT 0,
  status_reason text NULL,
  cert_serial text NULL, cert_expires_at timestamptz NULL,
  agent_version text NULL, protocol_version text NULL,
  session_epoch bigint NOT NULL DEFAULT 0,           -- bumped on every new CONTROL session
  session_holder text NULL,                          -- core id of the replica holding the CONTROL stream
  transport text NULL,
  endpoint jsonb NULL,                               -- how the core dials this node (core-dials transports)
  last_heartbeat_at timestamptz NULL, boot_id text NULL,
  running_attempts integer NOT NULL DEFAULT 0,       -- from the last heartbeat
  inventory jsonb NULL,
  telemetry jsonb NULL, telemetry_at timestamptz NULL,  -- latest-wins node telemetry
  hardware_fingerprint text NULL,
  labels jsonb NOT NULL DEFAULT '{}',
  quarantined_until timestamptz NULL, quarantine_reason text NULL,
  provider_instance_ref text NULL,
  -- Highest CONTROL AgentMessage.seq durably accepted (dedupe on node, lane, seq).
  control_rx_seq bigint NOT NULL DEFAULT 0,
  -- Last agent_commands.seq handed out for this node.
  command_seq bigint NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX nodes_pool_status ON compute_nodes (pool_id, status);

CREATE TABLE gpu_devices (
  id uuid PRIMARY KEY,
  node_id uuid NOT NULL REFERENCES compute_nodes(id),
  vendor text NOT NULL CHECK (vendor IN ('nvidia','amd','apple','intel','other')),
  device_uuid text NOT NULL,
  index_on_node int NOT NULL,
  model text NOT NULL, arch text NOT NULL,
  vram_total_bytes bigint NOT NULL,
  mig_profile text NULL, parent_device_id uuid NULL REFERENCES gpu_devices(id),
  nvlink_peers int[] NOT NULL DEFAULT '{}',
  health text NOT NULL DEFAULT 'OK' CHECK (health IN ('OK','DEGRADED','FAILED','QUARANTINED')),
  present boolean NOT NULL DEFAULT true,             -- false once the device left the inventory
  UNIQUE (node_id, device_uuid)
);

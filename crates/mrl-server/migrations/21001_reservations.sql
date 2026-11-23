-- P21: resources held for a job or a deployment (Doc 09 §3.6, Doc 20 item 13).
-- Only the scheduler's reservation writer changes `state`; it keeps
-- `reservation_devices.active` in step in the same transaction.
CREATE TABLE resource_reservations (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL,
  owner_kind text NOT NULL CHECK (owner_kind IN ('job','deployment')),
  job_id uuid NULL,
  -- No foreign key yet: deployments land later.
  deployment_revision_id uuid NULL,
  attempt_id uuid NULL,
  pool_id uuid NOT NULL REFERENCES compute_pools(id),
  node_id uuid NOT NULL REFERENCES compute_nodes(id),
  state text NOT NULL CHECK (state IN ('HELD','ACTIVE','RELEASING','RELEASED','EXPIRED','QUARANTINED')),
  state_version bigint NOT NULL DEFAULT 0,
  -- Fencing token: the job epoch of the attempt holding it (0 for a deployment).
  epoch bigint NOT NULL,
  cpu_millis bigint NOT NULL DEFAULT 0 CHECK (cpu_millis >= 0),
  mem_bytes bigint NOT NULL DEFAULT 0 CHECK (mem_bytes >= 0),
  -- GPU indices per role, like {"trainer": [0, 1]}.
  placement jsonb NOT NULL DEFAULT '{}',
  lease_expires_at timestamptz NOT NULL,
  reason text NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  released_at timestamptz NULL,
  CHECK ((owner_kind = 'job') = (job_id IS NOT NULL)),
  CHECK ((owner_kind = 'deployment') = (deployment_revision_id IS NOT NULL)),
  FOREIGN KEY (org_id, job_id) REFERENCES jobs (org_id, id),
  FOREIGN KEY (org_id, attempt_id) REFERENCES job_attempts (org_id, id)
);
-- A job holds at most one reservation it may still use (a quarantined one waits for the
-- machine to confirm the old processes are gone, while a retry holds a new one).
CREATE UNIQUE INDEX one_usable_reservation_per_job ON resource_reservations (job_id)
  WHERE state IN ('HELD','ACTIVE','RELEASING');
CREATE INDEX reservations_node_live ON resource_reservations (node_id)
  WHERE state IN ('HELD','ACTIVE','RELEASING','QUARANTINED');
CREATE INDEX reservations_attempt ON resource_reservations (attempt_id) WHERE attempt_id IS NOT NULL;

CREATE TABLE reservation_devices (
  reservation_id uuid NOT NULL REFERENCES resource_reservations(id),
  gpu_device_id uuid NOT NULL REFERENCES gpu_devices(id),
  active boolean NOT NULL DEFAULT true,
  PRIMARY KEY (reservation_id, gpu_device_id)
);
-- Safety net against double allocation, even by a zombie leader (D-04-08).
CREATE UNIQUE INDEX one_holder_per_gpu ON reservation_devices (gpu_device_id) WHERE active;

-- The highest CONTROL seq of an attempt status the scheduler applied, per attempt; a
-- reconcile report older than it says nothing about the attempt.
CREATE TABLE scheduler_attempt_reports (
  attempt_id uuid PRIMARY KEY,
  node_id uuid NOT NULL,
  seq bigint NOT NULL
);

-- The newest reconcile report applied per machine (older or replayed ones change nothing).
CREATE TABLE scheduler_node_reconciles (
  node_id uuid PRIMARY KEY REFERENCES compute_nodes(id),
  seq bigint NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);

-- The scheduler leader's last tick, to spot a control-plane gap (D-04-09).
CREATE TABLE scheduler_ticks (
  id smallint PRIMARY KEY CHECK (id = 1),
  last_tick_at timestamptz NOT NULL
);

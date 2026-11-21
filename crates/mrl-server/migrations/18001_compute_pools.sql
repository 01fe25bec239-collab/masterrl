-- P18: pools of machines (Doc 09 §3.6, Doc 20 item 13).
CREATE TABLE compute_pools (
  id uuid PRIMARY KEY,
  org_id uuid NULL,                                  -- NULL = platform pool shared by policy
  name text NOT NULL CHECK (length(name) BETWEEN 1 AND 100),
  provider_kind text NOT NULL DEFAULT 'agent'
    CHECK (provider_kind IN ('agent','kubernetes','slurm','cloud_vm')),
  provider_config jsonb NOT NULL DEFAULT '{}',       -- refs only; secrets via the secret store
  scheduling_policy jsonb NOT NULL DEFAULT '{}',
  flags text[] NOT NULL DEFAULT '{}',
  -- How long an attempt lease lasts without a renewing heartbeat.
  attempt_lease_ttl interval NOT NULL DEFAULT interval '90 seconds'
    CHECK (attempt_lease_ttl BETWEEN interval '10 seconds' AND interval '1 hour'),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE NULLS NOT DISTINCT (org_id, name)
);

-- P22: fired alerts (Doc 09 §3.8). Any module raises one through `AlertSink`.
CREATE TABLE alerts (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL,                                       -- nil = the local tenant / platform
  project_id uuid NULL,
  scope_kind text NOT NULL CHECK (scope_kind IN ('run','node','system')),
  scope_id uuid NULL,
  source text NOT NULL CHECK (source IN ('safety','node','budget','system','metric')),
  code text NOT NULL CHECK (length(code) BETWEEN 1 AND 100),
  severity text NOT NULL CHECK (severity IN ('info','warning','error','critical')),
  title text NOT NULL CHECK (length(title) BETWEEN 1 AND 300),
  details jsonb NOT NULL DEFAULT '{}',
  dedup_key text NOT NULL CHECK (length(dedup_key) BETWEEN 1 AND 300),
  state text NOT NULL CHECK (state IN ('OPEN','ACKED','RESOLVED')),
  version bigint NOT NULL DEFAULT 0,
  fired_at timestamptz NOT NULL DEFAULT now(),
  last_fired_at timestamptz NOT NULL DEFAULT now(),
  fire_count int NOT NULL DEFAULT 1,
  acked_at timestamptz NULL,
  acked_by text NULL,
  resolved_at timestamptz NULL,
  resolved_by text NULL,
  xid xid8 NOT NULL DEFAULT pg_current_xact_id()
);
-- At most one live alert per key: firing again updates it.
CREATE UNIQUE INDEX alerts_live ON alerts (org_id, dedup_key) WHERE state <> 'RESOLVED';
CREATE INDEX alerts_org_state ON alerts (org_id, state, id DESC);
CREATE INDEX alerts_tail ON alerts (xid, id);

-- P18: durable commands to agents (Doc 09 §3.10, D-04-06). Any replica writes a row; the replica
-- holding the node's CONTROL stream sends it. `payload` is the protobuf `agent.v1.Command`.
CREATE TABLE agent_commands (
  id uuid PRIMARY KEY,
  node_id uuid NOT NULL REFERENCES compute_nodes(id),
  seq bigint NOT NULL,                               -- per-node monotonic
  kind text NOT NULL CHECK (kind IN ('LaunchAttempt','StartAttempt','CancelAttempt','PauseAttempt',
        'CheckpointAttempt','StageArtifacts','UploadArtifacts','Cleanup','Drain','UpdateCredentials',
        'LaunchDeployment','StopDeployment','Probe')),
  payload bytea NOT NULL,
  attempt_id uuid NULL, epoch bigint NULL,
  state text NOT NULL CHECK (state IN ('PENDING','SENT','RECEIVED','COMPLETED','FAILED','EXPIRED','SUPERSEDED')),
  deadline_at timestamptz NOT NULL, result jsonb NULL,
  sent_at timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (node_id, seq)
);
CREATE INDEX agent_cmds_pending ON agent_commands (node_id, seq) WHERE state IN ('PENDING','SENT','RECEIVED');

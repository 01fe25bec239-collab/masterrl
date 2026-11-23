-- INT-1: the core's checkpoint commit decision reaches the agent as a command, and an upload's
-- outcome (the agent's second result for its UploadArtifacts) is kept apart from acceptance:
-- the first one per command id wins.
ALTER TABLE agent_commands DROP CONSTRAINT agent_commands_kind_check;
ALTER TABLE agent_commands ADD CONSTRAINT agent_commands_kind_check CHECK (kind IN (
  'LaunchAttempt','StartAttempt','CancelAttempt','PauseAttempt','CheckpointAttempt',
  'StageArtifacts','UploadArtifacts','Cleanup','Drain','UpdateCredentials','LaunchDeployment',
  'StopDeployment','Probe','CheckpointDecision'));
ALTER TABLE agent_commands ADD COLUMN outcome jsonb NULL, ADD COLUMN outcome_at timestamptz NULL;

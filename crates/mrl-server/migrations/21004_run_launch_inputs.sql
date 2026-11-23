ALTER TABLE run_configs ADD COLUMN launch_inputs jsonb NOT NULL DEFAULT '[]';
ALTER TABLE agent_commands ADD COLUMN not_after timestamptz;

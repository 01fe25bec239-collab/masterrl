-- P21 hands granted secrets to agents in UpdateCredentials commands. Once such a command can
-- never be sent again, the scheduler empties its payload, so secret values don't stay in the
-- database. This index finds the commands that still hold values.
CREATE INDEX agent_cmds_unscrubbed_credentials ON agent_commands (id)
  WHERE kind = 'UpdateCredentials' AND octet_length(payload) > 0;

-- One durable identity per (attempt, epoch, kind) for attempt commands (INT-2): at most one
-- live command each. Older live duplicates (written before this index) are withdrawn first.
UPDATE agent_commands c SET state = CASE WHEN state = 'PENDING' THEN 'SUPERSEDED' ELSE 'EXPIRED' END,
       updated_at = now()
 WHERE c.attempt_id IS NOT NULL
   AND c.kind IN ('LaunchAttempt','StageArtifacts','UpdateCredentials','CancelAttempt','PauseAttempt')
   AND c.state IN ('PENDING','SENT','RECEIVED')
   AND EXISTS (SELECT 1 FROM agent_commands n
                WHERE n.node_id = c.node_id AND n.kind = c.kind AND n.attempt_id = c.attempt_id
                  AND n.epoch IS NOT DISTINCT FROM c.epoch AND n.seq > c.seq
                  AND n.state IN ('PENDING','SENT','RECEIVED'));
CREATE UNIQUE INDEX agent_commands_one_live ON agent_commands (node_id, kind, attempt_id, COALESCE(epoch, -1))
 WHERE attempt_id IS NOT NULL
   AND kind IN ('LaunchAttempt','StageArtifacts','UpdateCredentials','CancelAttempt','PauseAttempt')
   AND state IN ('PENDING','SENT','RECEIVED');

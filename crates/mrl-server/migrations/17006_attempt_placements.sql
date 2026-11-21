-- Which node runs which rank range of an attempt (Doc 09 §3.5).
-- node_id has no foreign key yet (nodes land with the agent gateway, P18).
CREATE TABLE attempt_placements (
  attempt_id uuid NOT NULL,
  node_id uuid NOT NULL,
  org_id uuid NOT NULL,
  node_rank int NOT NULL CHECK (node_rank >= 0),
  rank_lo int NOT NULL CHECK (rank_lo >= 0),
  rank_hi int NOT NULL,
  role text NOT NULL DEFAULT 'trainer'
    CHECK (role IN ('trainer','rollout','reward','ray_head','ray_worker','service')),
  PRIMARY KEY (attempt_id, node_id, role),
  CHECK (rank_hi >= rank_lo),
  FOREIGN KEY (org_id, attempt_id) REFERENCES job_attempts (org_id, id)
);

-- P18: which CA every replica must use. The key itself is only in the secret store; this row
-- lets a replica whose secret store holds no key, or another one, refuse to start instead of
-- issuing certificates the other replicas don't trust.
CREATE TABLE gateway_ca (
  id int PRIMARY KEY CHECK (id = 1),
  fingerprint text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

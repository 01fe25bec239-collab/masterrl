-- P18: every node certificate the internal CA issued; the revocation list is revoked_at.
CREATE TABLE node_certificates (
  sha256 text PRIMARY KEY,                           -- hex SHA-256 of the DER certificate
  node_id uuid NOT NULL REFERENCES compute_nodes(id),
  serial text NOT NULL UNIQUE,
  not_after timestamptz NOT NULL,
  issued_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz NULL,
  revoked_reason text NULL CHECK (revoked_reason IN ('rotated','decommissioned','operator'))
);
CREATE INDEX node_certificates_node ON node_certificates (node_id);

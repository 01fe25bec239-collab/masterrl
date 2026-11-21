-- A secret being deleted: its value is being removed from the SecretStore. Rotation and
-- resolution refuse it; repeating the DELETE finishes an interrupted one.
ALTER TABLE secrets_metadata ADD COLUMN deleting boolean NOT NULL DEFAULT false;

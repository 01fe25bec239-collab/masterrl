-- A staged part becomes visible only when the open upload records its key.
CREATE TABLE upload_parts (
    upload_id uuid NOT NULL REFERENCES uploads (id) ON DELETE CASCADE,
    part bigint NOT NULL CHECK (part >= 0),
    object_key text NOT NULL,
    PRIMARY KEY (upload_id, part)
);

-- A rotation keeps one candidate or retired reference until it can delete it.
ALTER TABLE secrets_metadata ADD COLUMN rotation_ref text;

-- Keep the outcome of secret mutations across a process stop before audit delivery.
CREATE TABLE secret_mutation_audit (
    id uuid PRIMARY KEY,
    org_id text NOT NULL,
    actor text NOT NULL,
    project_id text,
    action text NOT NULL,
    target text NOT NULL,
    trace_id text NOT NULL,
    attempted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    outcome text CHECK (outcome IN ('success', 'failed')),
    finished_at timestamptz
);

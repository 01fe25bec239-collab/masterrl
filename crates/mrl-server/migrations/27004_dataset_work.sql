ALTER TABLE dataset_versions ADD COLUMN request_key text, ADD COLUMN request_hash text;
CREATE UNIQUE INDEX dataset_version_requests ON dataset_versions (dataset_id, created_by, request_key)
    WHERE request_key IS NOT NULL;

CREATE TABLE dataset_work (
    job_id uuid PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    version_id uuid NOT NULL REFERENCES dataset_versions(id),
    purpose text NOT NULL CHECK (purpose IN ('fetch','prepare','transform_preview','tokenize','jsonl','hub_push')),
    result jsonb,
    failure text,
    CHECK (result IS NULL OR failure IS NULL)
);
CREATE INDEX dataset_work_pending ON dataset_work(job_id) WHERE result IS NULL AND failure IS NULL;

CREATE TABLE dataset_create_requests (
    org_id      uuid NOT NULL,
    project_id  uuid NOT NULL,
    actor       text NOT NULL,
    key         text NOT NULL,
    fingerprint text NOT NULL,
    response    jsonb,
    PRIMARY KEY (org_id, project_id, actor, key)
);

CREATE INDEX dataset_versions_artifact ON dataset_versions (artifact_id) WHERE artifact_id IS NOT NULL;

ALTER TABLE dataset_versions ADD COLUMN inherited_restricted_pii boolean NOT NULL DEFAULT false;

CREATE FUNCTION dataset_version_is_restricted(version_id uuid) RETURNS boolean
LANGUAGE sql STABLE AS $$
    WITH RECURSIVE ancestors AS (
        SELECT id, org_id, dataset_id, parent_version_id, inherited_restricted_pii
        FROM dataset_versions WHERE id = version_id
        UNION
        SELECT v.id, v.org_id, v.dataset_id, v.parent_version_id, v.inherited_restricted_pii
        FROM ancestors a
        JOIN dataset_versions v ON v.id = a.parent_version_id AND v.org_id = a.org_id
    )
    SELECT EXISTS (
        SELECT 1 FROM ancestors a
        JOIN datasets d ON d.id = a.dataset_id AND d.org_id = a.org_id
        WHERE a.inherited_restricted_pii OR d.sensitivity = 'restricted-pii'
    );
$$;

UPDATE dataset_versions
SET inherited_restricted_pii = true
WHERE dataset_version_is_restricted(parent_version_id);

ALTER TABLE dataset_work ADD COLUMN request_key text, ADD COLUMN request_hash text,
    ADD COLUMN requested_by text;
CREATE UNIQUE INDEX dataset_work_requests ON dataset_work (version_id, requested_by, request_key)
    WHERE request_key IS NOT NULL;

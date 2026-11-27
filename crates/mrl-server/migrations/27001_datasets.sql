-- P27 Dataset Studio (Doc 09 §3.3): datasets, immutable versions, their files and eval overlap.
-- org_id is the project's org (uuid; nil for the solo tenant), like the runs tables.

CREATE TABLE datasets (
    id            uuid PRIMARY KEY,
    org_id        uuid NOT NULL,
    project_id    uuid NOT NULL,
    name          text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    description   text NOT NULL DEFAULT '',
    license_spdx  text NOT NULL DEFAULT 'unknown',
    sensitivity   text NOT NULL DEFAULT 'internal'
        CHECK (sensitivity IN ('public', 'internal', 'confidential', 'restricted-pii')),
    permitted_use text[] NOT NULL DEFAULT '{}',
    consent_basis text,
    source_urls   text[] NOT NULL DEFAULT '{}',
    card_md       text NOT NULL DEFAULT '',
    eval_set      boolean NOT NULL DEFAULT false,
    next_seq      integer NOT NULL DEFAULT 1,
    version       bigint NOT NULL DEFAULT 1,
    created_by    text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    deleted_at    timestamptz,
    UNIQUE (org_id, id)
);
-- A deleted dataset frees its name.
CREATE UNIQUE INDEX datasets_name ON datasets (org_id, project_id, name) WHERE deleted_at IS NULL;
CREATE INDEX datasets_project ON datasets (org_id, project_id, id) WHERE deleted_at IS NULL;

CREATE TABLE dataset_versions (
    id                    uuid PRIMARY KEY,
    org_id                uuid NOT NULL,
    dataset_id            uuid NOT NULL REFERENCES datasets (id),
    seq                   integer NOT NULL,
    schema_id             text NOT NULL,
    state                 text NOT NULL
        CHECK (state IN ('INGESTING', 'VALIDATING', 'READY', 'INVALID', 'ARCHIVED')),
    state_reason          jsonb,
    source_kind           text NOT NULL
        CHECK (source_kind IN ('upload', 'hub', 'remote_ref', 'transform', 'generated')),
    source_ref            jsonb NOT NULL DEFAULT '{}',
    parent_version_id     uuid REFERENCES dataset_versions (id),
    transform_spec        jsonb,
    row_count             bigint,
    byte_size             bigint,
    content_sha256        bytea CHECK (content_sha256 IS NULL OR length(content_sha256) = 32),
    mutable_external      boolean NOT NULL DEFAULT false,
    validation_report_key text,
    stats_key             text,
    validation            jsonb,
    stats                 jsonb,
    summary               jsonb,
    splits                jsonb,
    -- The job working on it now (IMPORT, then DATA_PREP), and the artifact it produced.
    job_id                uuid,
    artifact_id           uuid,
    created_by            text NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    ready_at              timestamptz,
    UNIQUE (dataset_id, seq),
    UNIQUE (org_id, id),
    CHECK ((state = 'READY') = (content_sha256 IS NOT NULL AND ready_at IS NOT NULL)
           OR state = 'ARCHIVED')
);
CREATE INDEX dataset_versions_content ON dataset_versions (org_id, content_sha256);
CREATE INDEX dataset_versions_job ON dataset_versions (job_id) WHERE job_id IS NOT NULL;

CREATE TABLE dataset_files (
    dataset_version_id uuid NOT NULL REFERENCES dataset_versions (id),
    org_id             uuid NOT NULL,
    path               text NOT NULL,
    split              text,
    role               text NOT NULL CHECK (role IN ('shard', 'original', 'derived')),
    object_key         text NOT NULL,
    sha256             bytea NOT NULL CHECK (length(sha256) = 32),
    size_bytes         bigint NOT NULL CHECK (size_bytes >= 0),
    row_count          bigint,
    PRIMARY KEY (dataset_version_id, path)
);

CREATE TABLE eval_contamination (
    dataset_version_id      uuid NOT NULL REFERENCES dataset_versions (id),
    eval_dataset_version_id uuid NOT NULL REFERENCES dataset_versions (id),
    org_id                  uuid NOT NULL,
    overlap_ratio           numeric NOT NULL CHECK (overlap_ratio BETWEEN 0 AND 1),
    overlapping_rows        bigint NOT NULL,
    method                  text NOT NULL CHECK (method IN ('exact', 'minhash')),
    computed_at             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_version_id, eval_dataset_version_id, method)
);

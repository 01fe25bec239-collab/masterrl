-- Lineage between any two entities (Doc 09 §3.10). Kinds are id prefixes (`run`, `dsv`,
-- `ckpt`, `art`, ...). `org_id` is text like the other P19 tables. Edges read
-- "from <relation> to", e.g. run trained_on dsv.
CREATE TABLE lineage_edges (
  org_id text NOT NULL,
  from_kind text NOT NULL, from_id uuid NOT NULL,
  relation text NOT NULL CHECK (relation IN ('derived_from', 'trained_on', 'rewarded_by', 'produced',
             'promoted_to', 'deployed_as', 'evaluated_by', 'exported_as', 'labelled_from', 'generated_by')),
  to_kind text NOT NULL, to_id uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (from_kind, from_id, relation, to_kind, to_id)
);

CREATE INDEX lineage_to ON lineage_edges (to_kind, to_id);

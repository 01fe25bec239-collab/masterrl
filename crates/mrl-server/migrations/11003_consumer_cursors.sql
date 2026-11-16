-- How far each consumer has read. Every event at or before (last_xid, last_id) is done.
CREATE TABLE consumer_cursors (
  consumer text PRIMARY KEY,
  last_id bigint NOT NULL,
  last_xid bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL
);

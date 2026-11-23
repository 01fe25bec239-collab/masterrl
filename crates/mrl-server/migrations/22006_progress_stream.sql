-- Existing rows have an unknown stream until the next accepted update.
ALTER TABLE run_progress ADD COLUMN last_stream int;

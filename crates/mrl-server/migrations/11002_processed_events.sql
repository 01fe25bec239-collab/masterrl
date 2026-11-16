-- One row per (consumer, event) handled or set aside; the dedup key for at-least-once delivery.
CREATE TABLE processed_events (
  consumer text NOT NULL,
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);

CREATE INDEX processed_events_age ON processed_events (processed_at);

-- Failed deliveries per consumer. A row with quarantined_at set was set aside after too many tries.
CREATE TABLE outbox_failures (
  consumer text NOT NULL,
  event_id uuid NOT NULL,
  outbox_id bigint NOT NULL,
  attempts int NOT NULL,
  last_error text NOT NULL,
  quarantined_at timestamptz NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);

CREATE INDEX outbox_failures_quarantined ON outbox_failures (quarantined_at) WHERE quarantined_at IS NOT NULL;

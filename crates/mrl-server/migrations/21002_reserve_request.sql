-- Direct reserve callers replay the request using the reservation id.
ALTER TABLE resource_reservations ADD COLUMN reserve_request jsonb NULL;

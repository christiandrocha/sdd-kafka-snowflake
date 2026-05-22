-- tests/payment_history_starts_with_created.sql
-- Every payment_id must have a 'created' event as its first event (sequence=1).
-- Fails (returns rows) if any payment starts with a different event name.
-- This validates that no events are missing from the beginning of the lifecycle.

SELECT
    payment_id,
    event_name,
    event_sequence
FROM {{ ref('silver_payment_events_history') }}
WHERE event_sequence = 1
  AND event_name != 'created'

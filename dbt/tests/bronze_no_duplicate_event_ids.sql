-- tests/bronze_no_duplicate_event_ids.sql
-- Bronze merge by event_id must produce exactly one row per event_id.
-- Fails (returns rows) if any event_id appears more than once.
-- Guards against merge strategy misconfiguration.

SELECT
    event_id,
    COUNT(*) AS cnt
FROM {{ ref('bronze_payment_events') }}
GROUP BY event_id
HAVING cnt > 1

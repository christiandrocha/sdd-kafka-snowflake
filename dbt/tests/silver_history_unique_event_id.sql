-- Fails if any event_id appears more than once in silver_payment_events_history.
-- History model merges by event_id — duplicates indicate a merge misconfiguration.

SELECT
    event_id,
    COUNT(*) AS cnt
FROM {{ ref('silver_payment_events_history') }}
GROUP BY event_id
HAVING cnt > 1

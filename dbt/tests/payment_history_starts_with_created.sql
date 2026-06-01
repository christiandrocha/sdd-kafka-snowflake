-- Every payment_id must have at least one 'created' event in its history.
-- Kafka out-of-order delivery means 'created' may not be the lowest timestamp,
-- but it must exist to represent a valid payment lifecycle start.

SELECT payment_id
FROM {{ ref('silver_payment_events_history') }}
GROUP BY payment_id
HAVING SUM(CASE WHEN event_name = 'created' THEN 1 ELSE 0 END) = 0

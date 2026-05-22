-- Fails if any deleted record (op='d') appears in silver_payment_events_history.
-- Silver history excludes DELETEs — only creates, updates, and snapshots are kept.

SELECT event_id
FROM {{ ref('silver_payment_events_history') }}
WHERE op = 'd'

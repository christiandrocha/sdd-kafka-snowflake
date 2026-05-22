-- tests/current_state_referential_integrity.sql
-- Every payment_id in silver_payment_current_state must exist in
-- silver_payment_events_history (referential integrity check).
-- Fails (returns rows) if any current state record has no history.

SELECT cs.payment_id
FROM {{ ref('silver_payment_current_state') }} cs
LEFT JOIN {{ ref('silver_payment_events_history') }} h
    ON cs.payment_id = h.payment_id
WHERE h.payment_id IS NULL

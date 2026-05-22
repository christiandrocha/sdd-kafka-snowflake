-- Fails if any payment_id appears more than once in silver_payment_current_state.
-- Current state model merges by payment_id — duplicates indicate a merge failure.

SELECT
    payment_id,
    COUNT(*) AS cnt
FROM {{ ref('silver_payment_current_state') }}
GROUP BY payment_id
HAVING cnt > 1

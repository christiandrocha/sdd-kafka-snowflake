{{
    config(
        materialized     = 'table',
        schema           = 'GOLD',
        on_schema_change = 'sync_all_columns'
    )
}}

-- Payment conversion funnel — one row per lifecycle stage.
-- Full table refresh (full-history aggregate, low cardinality).
-- Real lifecycle: created(1) → authorized(2) → captured(3)
--                → succeeded(4) → settled(5) → closed(6)
--                → refunded(4alt) → closed(6)

WITH stage_counts AS (
    SELECT
        event_name,
        COUNT(DISTINCT payment_id) AS payment_count
    FROM {{ ref('silver_payment_events_history') }}
    GROUP BY event_name
),

funnel_order AS (
    SELECT
        event_name,
        payment_count,
        CASE event_name
            WHEN 'created'    THEN 1
            WHEN 'authorized' THEN 2
            WHEN 'captured'   THEN 3
            WHEN 'succeeded'  THEN 4
            WHEN 'refunded'   THEN 5
            WHEN 'settled'    THEN 6
            WHEN 'closed'     THEN 7
            ELSE 99
        END AS stage_order
    FROM stage_counts
),

entry_count AS (
    SELECT payment_count AS total
    FROM funnel_order
    WHERE event_name = 'created'
)

SELECT
    f.stage_order,
    f.event_name                                                AS stage,
    f.payment_count,
    e.total                                                     AS entry_count,
    ROUND(100.0 * f.payment_count / NULLIF(e.total, 0), 2)    AS conversion_rate_pct,
    CURRENT_TIMESTAMP()                                         AS computed_at

FROM funnel_order f
CROSS JOIN entry_count e
ORDER BY f.stage_order

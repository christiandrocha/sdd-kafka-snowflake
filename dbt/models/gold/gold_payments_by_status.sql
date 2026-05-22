{{
    config(
        materialized         = 'incremental',
        schema               = 'GOLD',
        unique_key           = 'current_status',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Payment count grouped by current status.
-- One row per status value (low cardinality — 7 possible values).
-- Recomputes affected statuses on incremental runs.

SELECT
    current_status,
    COUNT(*)                                              AS total_payments,
    SUM(CASE WHEN is_closed        THEN 1 ELSE 0 END)    AS closed_count,
    SUM(CASE WHEN closed_via_refund THEN 1 ELSE 0 END)   AS refunded_count,
    SUM(CASE WHEN is_in_progress   THEN 1 ELSE 0 END)    AS in_progress_count,
    MAX(status_updated_at)                                AS last_updated_at,
    CURRENT_TIMESTAMP()                                   AS computed_at

FROM {{ ref('silver_payment_current_state') }}

{% if is_incremental() %}
WHERE current_status IN (
    SELECT DISTINCT current_status
    FROM {{ ref('silver_payment_current_state') }}
    WHERE source_ts_ms > (
        SELECT COALESCE(MAX(source_ts_ms), 0)
        FROM {{ ref('silver_payment_current_state') }}
        WHERE source_ts_ms <= (SELECT MAX(source_ts_ms) FROM {{ this }})
    )
)
{% endif %}

GROUP BY current_status

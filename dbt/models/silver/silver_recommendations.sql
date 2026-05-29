{{
    config(
        materialized         = 'incremental',
        schema               = 'SILVER',
        unique_key           = 'event_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Silver: ML recommendation events. One row per event_id.
-- Passthrough of bronze_recommendations filtering hard deletes.
-- Event types: view, click, purchase, dismiss.

SELECT
    event_id,
    user_id,
    product_id,
    event_type,
    recommendation_timestamp,
    dt_current_timestamp,
    op,
    source_ts_ms,
    kafka_created_at
FROM {{ ref('bronze_recommendations') }}
WHERE op != 'd'
{% if is_incremental() %}
AND source_ts_ms > (SELECT COALESCE(MAX(source_ts_ms), 0) FROM {{ this }})
{% endif %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'SILVER',
        unique_key           = 'search_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Silver: user search events. One row per search_id.
-- Passthrough of bronze_search_events filtering hard deletes.

SELECT
    search_id,
    user_id,
    query_text,
    filters,
    result_count,
    clicked_product_id,
    search_timestamp,
    op,
    source_ts_ms,
    kafka_created_at
FROM {{ ref('bronze_search_events') }}
WHERE op != 'd'
{% if is_incremental() %}
AND source_ts_ms > (SELECT COALESCE(MAX(source_ts_ms), 0) FROM {{ this }})
{% endif %}

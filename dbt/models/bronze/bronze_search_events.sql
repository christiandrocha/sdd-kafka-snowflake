{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'search_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: user search events from Snowpipe.
-- No dt_current_timestamp — uses timestamp as domain event time.
-- Bronze incremental filter uses kafka_created_at (Snowpipe CreateTime).
-- Merges by search_id for idempotency.

WITH source AS (
    SELECT
        RECORD_CONTENT:search_id::VARCHAR                    AS search_id,
        RECORD_CONTENT:user_id::INT                          AS user_id,
        RECORD_CONTENT:query_text::VARCHAR                   AS query_text,
        RECORD_CONTENT:filters::VARCHAR                      AS filters,
        RECORD_CONTENT:result_count::INT                     AS result_count,
        RECORD_CONTENT:clicked_product_id::VARCHAR           AS clicked_product_id,
        RECORD_CONTENT:timestamp::TIMESTAMP_NTZ              AS search_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'SEARCH_EVENTS') }}

    {% if is_incremental() %}
    WHERE RECORD_METADATA:CreateTime::BIGINT > (
        SELECT COALESCE(MAX(kafka_created_at), 0) FROM {{ this }}
    )
    {% endif %}
),

deduped AS (
    SELECT * EXCLUDE (_row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY search_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

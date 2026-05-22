{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'stock_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: inventory stock levels from Snowpipe.
-- No dt_current_timestamp — uses last_updated as domain timestamp.
-- Bronze incremental filter uses kafka_created_at (Snowpipe CreateTime).
-- Merges by stock_id.

WITH source AS (
    SELECT
        RECORD_CONTENT:stock_id::VARCHAR                     AS stock_id,
        RECORD_CONTENT:restaurant_id::INT                    AS restaurant_id,
        RECORD_CONTENT:product_id::VARCHAR                   AS product_id,
        RECORD_CONTENT:quantity_available::INT               AS quantity_available,
        RECORD_CONTENT:last_updated::TIMESTAMP_NTZ           AS last_updated,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'INVENTORY') }}

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
                PARTITION BY stock_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

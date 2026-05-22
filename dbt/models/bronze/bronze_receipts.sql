{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'receipt_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: receipts from Snowpipe.
-- No dt_current_timestamp — uses receipt_generated_at as domain timestamp.
-- Bronze incremental filter always uses kafka_created_at (Snowpipe CreateTime).
-- Merges by receipt_id.

WITH source AS (
    SELECT
        RECORD_CONTENT:receipt_id::VARCHAR                   AS receipt_id,
        RECORD_CONTENT:order_id::VARCHAR                     AS order_id,
        RECORD_CONTENT:payment_id::VARCHAR                   AS payment_id,
        RECORD_CONTENT:total_amount::FLOAT                   AS total_amount,
        RECORD_CONTENT:item_count::INT                       AS item_count,
        RECORD_CONTENT:receipt_generated_at::TIMESTAMP_NTZ   AS receipt_generated_at,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'RECEIPTS') }}

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
                PARTITION BY receipt_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

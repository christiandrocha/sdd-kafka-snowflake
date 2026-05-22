{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'order_item_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: order_items fact table from Snowpipe.
-- 110,001 records — 85% of total volume. Separate Snowflake Sink buffer (ADR-14).
-- Merges by order_item_id. Append-only semantics in practice (items are immutable).

WITH source AS (
    SELECT
        RECORD_CONTENT:order_item_id::VARCHAR                AS order_item_id,
        RECORD_CONTENT:order_id::VARCHAR                     AS order_id,
        RECORD_CONTENT:product_id::VARCHAR                   AS product_id,
        RECORD_CONTENT:restaurant_id::INT                    AS restaurant_id,
        RECORD_CONTENT:product_name::VARCHAR                 AS product_name,
        RECORD_CONTENT:product_type::VARCHAR                 AS product_type,
        RECORD_CONTENT:cuisine_type::VARCHAR                 AS cuisine_type,
        RECORD_CONTENT:unit_price::FLOAT                     AS unit_price,
        RECORD_CONTENT:quantity::INT                         AS quantity,
        RECORD_CONTENT:subtotal::FLOAT                       AS subtotal,
        RECORD_CONTENT:discount_applied::FLOAT               AS discount_applied,
        RECORD_CONTENT:modifiers::VARCHAR                    AS modifiers,
        RECORD_CONTENT:is_combo::BOOLEAN                     AS is_combo,
        RECORD_CONTENT:is_vegetarian::BOOLEAN                AS is_vegetarian,
        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'ORDER_ITEMS') }}

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
                PARTITION BY order_item_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

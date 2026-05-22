{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'order_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: orders hub table from Snowpipe.
-- Merges by order_id — idempotent on CDC updates and Snowpipe retries.
-- Hub table linking all domains via heterogeneous business keys:
-- user_key (CPF), restaurant_key (CNPJ), driver_key (string), payment_key (UUID), rating_key (UUID).

WITH source AS (
    SELECT
        RECORD_CONTENT:order_id::VARCHAR                     AS order_id,
        RECORD_CONTENT:order_date::TIMESTAMP_NTZ             AS order_date,
        RECORD_CONTENT:total_amount::FLOAT                   AS total_amount,
        RECORD_CONTENT:user_key::VARCHAR                     AS user_key,
        RECORD_CONTENT:restaurant_key::VARCHAR               AS restaurant_key,
        RECORD_CONTENT:driver_key::VARCHAR                   AS driver_key,
        RECORD_CONTENT:payment_key::VARCHAR                  AS payment_key,
        RECORD_CONTENT:rating_key::VARCHAR                   AS rating_key,
        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'ORDERS') }}

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
                PARTITION BY order_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

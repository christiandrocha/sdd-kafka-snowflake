{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'product_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: products from MySQL source via Snowpipe.
-- PK is product_id VARCHAR (not UUID). Merges by product_id.

WITH source AS (
    SELECT
        RECORD_CONTENT:product_id::VARCHAR                   AS product_id,
        RECORD_CONTENT:restaurant_id::INT                    AS restaurant_id,
        RECORD_CONTENT:name::VARCHAR                         AS name,
        RECORD_CONTENT:product_type::VARCHAR                 AS product_type,
        RECORD_CONTENT:cuisine_type::VARCHAR                 AS cuisine_type,
        RECORD_CONTENT:flavor_profile::VARCHAR               AS flavor_profile,
        RECORD_CONTENT:tags::VARCHAR                         AS tags,
        RECORD_CONTENT:price::FLOAT                          AS price,
        RECORD_CONTENT:unit_cost::FLOAT                      AS unit_cost,
        RECORD_CONTENT:calories::INT                         AS calories,
        RECORD_CONTENT:prep_time_min::INT                    AS prep_time_min,
        RECORD_CONTENT:is_vegetarian::BOOLEAN                AS is_vegetarian,
        RECORD_CONTENT:is_gluten_free::BOOLEAN               AS is_gluten_free,
        RECORD_CONTENT:created_at::TIMESTAMP_NTZ             AS created_at,
        RECORD_CONTENT:updated_at::TIMESTAMP_NTZ             AS updated_at,
        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'PRODUCTS') }}

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
                PARTITION BY product_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

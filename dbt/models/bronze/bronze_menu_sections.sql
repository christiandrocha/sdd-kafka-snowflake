{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'menu_section_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: menu sections from MySQL source via Snowpipe.
-- Merges by menu_section_id.

WITH source AS (
    SELECT
        RECORD_CONTENT:menu_section_id::VARCHAR              AS menu_section_id,
        RECORD_CONTENT:restaurant_id::INT                    AS restaurant_id,
        RECORD_CONTENT:name::VARCHAR                         AS name,
        RECORD_CONTENT:description::VARCHAR                  AS description,
        RECORD_CONTENT:active::BOOLEAN                       AS active,
        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'MENU_SECTIONS') }}

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
                PARTITION BY menu_section_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

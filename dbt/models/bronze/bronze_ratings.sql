{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'rating_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: ratings from MySQL source via Snowpipe.
-- rating_id is the PK; uuid is the FK referenced by orders.rating_key.
-- Merges by rating_id.

WITH source AS (
    SELECT
        RECORD_CONTENT:rating_id::VARCHAR                    AS rating_id,
        RECORD_CONTENT:uuid::VARCHAR                         AS uuid,
        RECORD_CONTENT:restaurant_identifier::VARCHAR        AS restaurant_identifier,
        RECORD_CONTENT:rating::FLOAT                         AS rating,
        RECORD_CONTENT:timestamp::TIMESTAMP_NTZ              AS rating_timestamp,
        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'RATINGS') }}

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
                PARTITION BY rating_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

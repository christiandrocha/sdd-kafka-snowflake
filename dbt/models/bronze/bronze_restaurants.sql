{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'uuid') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: restaurants from MySQL source via Snowpipe.
-- Merges by uuid. opening_time/closing_time kept as VARCHAR (TIME in Postgres).

WITH source AS (
    SELECT
        RECORD_CONTENT:uuid::VARCHAR                         AS uuid,
        RECORD_CONTENT:restaurant_id::INT                    AS restaurant_id,
        RECORD_CONTENT:cnpj::VARCHAR                         AS cnpj,
        RECORD_CONTENT:name::VARCHAR                         AS name,
        RECORD_CONTENT:address::VARCHAR                      AS address,
        RECORD_CONTENT:city::VARCHAR                         AS city,
        RECORD_CONTENT:country::VARCHAR                      AS country,
        RECORD_CONTENT:phone_number::VARCHAR                 AS phone_number,
        RECORD_CONTENT:cuisine_type::VARCHAR                 AS cuisine_type,
        RECORD_CONTENT:opening_time::VARCHAR                 AS opening_time,
        RECORD_CONTENT:closing_time::VARCHAR                 AS closing_time,
        RECORD_CONTENT:average_rating::FLOAT                 AS average_rating,
        RECORD_CONTENT:num_reviews::INT                      AS num_reviews,
        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'RESTAURANTS') }}

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
                PARTITION BY uuid
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'route_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: delivery routes from Snowpipe.
-- One route per order: start/end coordinates, distance, estimated duration.
-- Merges by route_id.

WITH source AS (
    SELECT
        RECORD_CONTENT:route_id::VARCHAR                     AS route_id,
        RECORD_CONTENT:order_id::VARCHAR                     AS order_id,
        RECORD_CONTENT:driver_id::VARCHAR                    AS driver_id,
        RECORD_CONTENT:start_lat::FLOAT                      AS start_lat,
        RECORD_CONTENT:start_lon::FLOAT                      AS start_lon,
        RECORD_CONTENT:end_lat::FLOAT                        AS end_lat,
        RECORD_CONTENT:end_lon::FLOAT                        AS end_lon,
        RECORD_CONTENT:distance_km::FLOAT                    AS distance_km,
        RECORD_CONTENT:estimated_duration_min::FLOAT         AS estimated_duration_min,
        RECORD_CONTENT:start_time::TIMESTAMP_NTZ             AS start_time,
        RECORD_CONTENT:end_time::TIMESTAMP_NTZ               AS end_time,
        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'ROUTES') }}

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
                PARTITION BY route_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'gps_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: GPS tracking events from Snowpipe.
-- 7,350 records across 5 files. High-frequency location data linked to order_id.
-- Merges by gps_id for idempotency. Append-only semantics (events are immutable).

WITH source AS (
    SELECT
        RECORD_CONTENT:gps_id::VARCHAR                       AS gps_id,
        RECORD_CONTENT:order_id::VARCHAR                     AS order_id,
        RECORD_CONTENT:lat::FLOAT                            AS lat,
        RECORD_CONTENT:lon::FLOAT                            AS lon,
        RECORD_CONTENT:altitude::FLOAT                       AS altitude,
        RECORD_CONTENT:speed_kph::FLOAT                      AS speed_kph,
        RECORD_CONTENT:direction_deg::FLOAT                  AS direction_deg,
        RECORD_CONTENT:accuracy_m::FLOAT                     AS accuracy_m,
        RECORD_CONTENT:duration_ms::FLOAT                    AS duration_ms,
        RECORD_CONTENT:timestamp::TIMESTAMP_NTZ              AS gps_timestamp,
        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'GPS_EVENTS') }}

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
                PARTITION BY gps_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

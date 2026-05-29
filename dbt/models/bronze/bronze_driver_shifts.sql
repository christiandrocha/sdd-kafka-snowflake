{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'shift_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: driver shift performance records from Snowpipe.
-- Shift metrics: earnings, distance, orders, rating, issues.
-- Merges by shift_id.

WITH source AS (
    SELECT
        RECORD_CONTENT:shift_id::VARCHAR                     AS shift_id,
        RECORD_CONTENT:driver_id::VARCHAR                    AS driver_id,
        RECORD_CONTENT:city::VARCHAR                         AS city,
        RECORD_CONTENT:region::VARCHAR                       AS region,
        RECORD_CONTENT:shift_type::VARCHAR                   AS shift_type,
        RECORD_CONTENT:login_method::VARCHAR                 AS login_method,
        RECORD_CONTENT:device_os::VARCHAR                    AS device_os,
        RECORD_CONTENT:start_time::TIMESTAMP_NTZ             AS start_time,
        RECORD_CONTENT:end_time::TIMESTAMP_NTZ               AS end_time,
        TRY_CAST(RECORD_CONTENT:shift_duration_min::VARCHAR AS INT)    AS shift_duration_min,
        TRY_CAST(RECORD_CONTENT:num_orders::VARCHAR        AS INT)    AS num_orders,
        TRY_CAST(RECORD_CONTENT:distance_covered_km::VARCHAR AS FLOAT) AS distance_covered_km,
        TRY_CAST(RECORD_CONTENT:earnings_brl::VARCHAR      AS FLOAT)  AS earnings_brl,
        TRY_CAST(RECORD_CONTENT:shift_rating::VARCHAR      AS FLOAT)  AS shift_rating,
        TRY_CAST(RECORD_CONTENT:issues_reported::VARCHAR   AS INT)    AS issues_reported,
        RECORD_CONTENT:available::BOOLEAN                    AS available,
        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'DRIVER_SHIFTS') }}

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
                PARTITION BY shift_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

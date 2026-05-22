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

-- Bronze: drivers from PostgreSQL source via Snowpipe.
-- Merges by uuid.

WITH source AS (
    SELECT
        RECORD_CONTENT:uuid::VARCHAR                         AS uuid,
        RECORD_CONTENT:driver_id::VARCHAR                    AS driver_id,
        RECORD_CONTENT:first_name::VARCHAR                   AS first_name,
        RECORD_CONTENT:last_name::VARCHAR                    AS last_name,
        RECORD_CONTENT:phone_number::VARCHAR                 AS phone_number,
        RECORD_CONTENT:city::VARCHAR                         AS city,
        RECORD_CONTENT:country::VARCHAR                      AS country,
        RECORD_CONTENT:date_birth::DATE                      AS date_birth,
        RECORD_CONTENT:license_number::VARCHAR               AS license_number,
        RECORD_CONTENT:vehicle_type::VARCHAR                 AS vehicle_type,
        RECORD_CONTENT:vehicle_make::VARCHAR                 AS vehicle_make,
        RECORD_CONTENT:vehicle_model::VARCHAR                AS vehicle_model,
        RECORD_CONTENT:vehicle_year::INT                     AS vehicle_year,
        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'DRIVERS') }}

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

{{
    config(
        materialized         = 'incremental',
        schema               = 'SILVER',
        unique_key           = 'driver_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Silver: latest driver entity state. One row per driver_id.
-- Deduplicates bronze_drivers on the business key driver_id (bronze dedupes on uuid).

WITH bronze_new AS (
    SELECT *
    FROM {{ ref('bronze_drivers') }}
    {% if is_incremental() %}
    WHERE source_ts_ms > (SELECT COALESCE(MAX(source_ts_ms), 0) FROM {{ this }})
    {% endif %}
),

latest AS (
    SELECT * EXCLUDE (_rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY source_ts_ms DESC) AS _rn
        FROM bronze_new
    )
    WHERE _rn = 1
)

SELECT
    driver_id,
    uuid,
    first_name,
    last_name,
    first_name || ' ' || last_name  AS full_name,
    phone_number,
    city,
    country,
    date_birth,
    license_number,
    vehicle_type,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    dt_current_timestamp,
    op,
    source_ts_ms,
    kafka_created_at
FROM latest
WHERE op != 'd'

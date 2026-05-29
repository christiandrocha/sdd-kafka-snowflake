{{
    config(
        materialized         = 'incremental',
        schema               = 'SILVER',
        unique_key           = 'shift_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Silver: driver shifts enriched with driver profile.
-- Joins bronze_driver_shifts with silver_drivers for name and vehicle info.
-- One row per shift_id.

WITH shifts AS (
    SELECT *
    FROM {{ ref('bronze_driver_shifts') }}
    {% if is_incremental() %}
    WHERE source_ts_ms > (SELECT COALESCE(MAX(source_ts_ms), 0) FROM {{ this }})
    {% endif %}
)

SELECT
    s.shift_id,
    s.driver_id,
    d.full_name                      AS driver_name,
    d.vehicle_type,
    d.vehicle_make,
    d.vehicle_model,
    d.vehicle_year,
    s.city,
    s.region,
    s.shift_type,
    s.login_method,
    s.device_os,
    s.start_time,
    s.end_time,
    s.shift_duration_min,
    s.num_orders                     AS shift_orders_reported,
    s.distance_covered_km,
    s.earnings_brl,
    s.shift_rating,
    s.issues_reported,
    s.available,
    s.dt_current_timestamp,
    s.op,
    s.source_ts_ms,
    s.kafka_created_at
FROM shifts s
LEFT JOIN {{ ref('silver_drivers') }} d ON s.driver_id = d.driver_id
WHERE s.op != 'd'

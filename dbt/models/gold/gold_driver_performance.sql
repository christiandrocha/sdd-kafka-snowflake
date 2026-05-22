{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

-- Gold: driver performance summary per shift.
-- Combines bronze_driver_shifts with silver_orders delivery counts
-- and the latest driver profile from bronze_drivers.

WITH latest_drivers AS (
    SELECT
        driver_id,
        first_name || ' ' || last_name       AS driver_name,
        vehicle_type,
        city                                 AS driver_city
    FROM (
        SELECT
            driver_id,
            first_name,
            last_name,
            vehicle_type,
            city,
            ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY source_ts_ms DESC) AS _rn
        FROM {{ ref('bronze_drivers') }}
    )
    WHERE _rn = 1
),

orders_per_driver AS (
    SELECT
        driver_key                           AS driver_id,
        COUNT(DISTINCT order_id)             AS delivered_orders,
        SUM(total_amount)                    AS total_order_value
    FROM {{ ref('silver_orders') }}
    WHERE op != 'd'
      AND driver_key IS NOT NULL
    GROUP BY driver_key
)

SELECT
    s.shift_id,
    s.driver_id,
    d.driver_name,
    d.vehicle_type,
    d.driver_city,
    s.city                                   AS shift_city,
    s.region,
    s.shift_type,
    s.start_time,
    s.end_time,
    s.shift_duration_min,
    s.num_orders                             AS shift_orders_reported,
    s.distance_covered_km,
    s.earnings_brl,
    s.shift_rating,
    s.issues_reported,
    o.delivered_orders                       AS total_delivered_orders,
    o.total_order_value,
    CASE
        WHEN s.shift_duration_min > 0
        THEN ROUND(s.earnings_brl / (s.shift_duration_min / 60.0), 2)
        ELSE NULL
    END                                      AS earnings_per_hour
FROM {{ ref('bronze_driver_shifts') }} s
LEFT JOIN latest_drivers d ON s.driver_id = d.driver_id
LEFT JOIN orders_per_driver o ON s.driver_id = o.driver_id
WHERE s.op != 'd'

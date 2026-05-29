{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

-- Gold: driver performance summary per shift.
-- Combines silver_driver_shifts (shifts + driver profile) with silver_orders
-- (cross-domain delivery counts).

WITH orders_per_driver AS (
    SELECT
        driver_key                           AS driver_id,
        COUNT(DISTINCT order_id)             AS delivered_orders,
        SUM(total_amount)                    AS total_order_value
    FROM {{ ref('silver_orders') }}
    WHERE driver_key IS NOT NULL
    GROUP BY driver_key
)

SELECT
    s.shift_id,
    s.driver_id,
    s.driver_name,
    s.vehicle_type,
    s.city                                   AS driver_city,
    s.city                                   AS shift_city,
    s.region,
    s.shift_type,
    s.start_time,
    s.end_time,
    s.shift_duration_min,
    s.shift_orders_reported,
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
FROM {{ ref('silver_driver_shifts') }} s
LEFT JOIN orders_per_driver o ON s.driver_id = o.driver_id

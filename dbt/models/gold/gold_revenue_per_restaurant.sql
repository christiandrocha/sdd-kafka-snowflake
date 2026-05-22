{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

-- Gold: revenue aggregation per restaurant.
-- Joins silver_orders (CNPJ key) with bronze_order_items (subtotals).
-- One row per restaurant per day.

WITH order_items AS (
    SELECT
        order_id,
        SUM(subtotal)                        AS items_subtotal,
        SUM(quantity)                        AS total_items,
        COUNT(DISTINCT order_item_id)        AS line_count,
        SUM(discount_applied)                AS total_discount
    FROM {{ ref('bronze_order_items') }}
    GROUP BY order_id
),

orders AS (
    SELECT
        order_id,
        restaurant_key,
        restaurant_cnpj_normalized,
        restaurant_name,
        DATE(order_date)                     AS order_day,
        total_amount
    FROM {{ ref('silver_orders') }}
    WHERE op != 'd'
)

SELECT
    o.restaurant_cnpj_normalized,
    o.restaurant_name,
    o.order_day,
    COUNT(DISTINCT o.order_id)               AS num_orders,
    SUM(o.total_amount)                      AS gross_revenue,
    SUM(i.items_subtotal)                    AS items_subtotal,
    SUM(i.total_discount)                    AS total_discount,
    SUM(i.total_items)                       AS total_items_sold,
    AVG(o.total_amount)                      AS avg_order_value
FROM orders o
LEFT JOIN order_items i ON o.order_id = i.order_id
GROUP BY
    o.restaurant_cnpj_normalized,
    o.restaurant_name,
    o.order_day

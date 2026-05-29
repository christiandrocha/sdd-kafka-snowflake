{{
    config(
        materialized = 'table',
        schema       = 'GOLD'
    )
}}

-- Gold: user behavior summary.
-- Aggregates orders, searches, and recommendations per unified CPF from silver_users.
-- One row per user (CPF).

WITH user_orders AS (
    SELECT
        user_cpf_normalized,
        COUNT(DISTINCT order_id)             AS num_orders,
        SUM(total_amount)                    AS total_spend,
        MIN(order_date)                      AS first_order_at,
        MAX(order_date)                      AS last_order_at
    FROM {{ ref('silver_orders') }}
    WHERE op != 'd'
      AND user_cpf_normalized IS NOT NULL
    GROUP BY user_cpf_normalized
),

user_searches AS (
    SELECT
        REGEXP_REPLACE(CAST(user_id AS VARCHAR), '[^0-9]', '') AS user_id_str,
        COUNT(DISTINCT search_id)            AS num_searches,
        AVG(result_count)                    AS avg_search_results,
        COUNT(CASE WHEN clicked_product_id IS NOT NULL THEN 1 END) AS searches_with_click
    FROM {{ ref('silver_search_events') }}
    GROUP BY user_id
),

user_recommendations AS (
    SELECT
        REGEXP_REPLACE(CAST(user_id AS VARCHAR), '[^0-9]', '') AS user_id_str,
        COUNT(CASE WHEN event_type = 'view' THEN 1 END)        AS rec_views,
        COUNT(CASE WHEN event_type = 'click' THEN 1 END)       AS rec_clicks,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END)    AS rec_purchases,
        COUNT(CASE WHEN event_type = 'dismiss' THEN 1 END)     AS rec_dismissals
    FROM {{ ref('silver_recommendations') }}
    GROUP BY user_id
)

SELECT
    u.cpf_normalized,
    u.uuid,
    u.email,
    u.phone_number,
    u.city,
    u.country,
    u.first_name,
    u.last_name,
    u.source                                 AS user_source,
    COALESCE(o.num_orders, 0)                AS num_orders,
    COALESCE(o.total_spend, 0)               AS total_spend,
    o.first_order_at,
    o.last_order_at,
    COALESCE(s.num_searches, 0)              AS num_searches,
    COALESCE(s.avg_search_results, 0)        AS avg_search_results,
    COALESCE(s.searches_with_click, 0)       AS searches_with_click,
    COALESCE(r.rec_views, 0)                 AS rec_views,
    COALESCE(r.rec_clicks, 0)                AS rec_clicks,
    COALESCE(r.rec_purchases, 0)             AS rec_purchases,
    COALESCE(r.rec_dismissals, 0)            AS rec_dismissals,
    CASE
        WHEN o.num_orders >= 10 THEN 'high'
        WHEN o.num_orders >= 3  THEN 'medium'
        WHEN o.num_orders >= 1  THEN 'low'
        ELSE 'inactive'
    END                                      AS engagement_tier
FROM {{ ref('silver_users') }} u
LEFT JOIN user_orders o ON u.cpf_normalized = o.user_cpf_normalized
LEFT JOIN user_searches s ON u.user_id::VARCHAR = s.user_id_str
LEFT JOIN user_recommendations r ON u.user_id::VARCHAR = r.user_id_str

{{
    config(
        materialized         = 'incremental',
        schema               = 'SILVER',
        unique_key           = 'order_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Silver: enriched orders. One row per order_id.
-- Joins to latest restaurant name (CNPJ), driver name, and user CPF from Bronze.
-- Incremental merge on order_id — updates propagate from Bronze.

WITH orders AS (
    SELECT *
    FROM {{ ref('bronze_orders') }}
    {% if is_incremental() %}
    WHERE kafka_created_at > (
        SELECT COALESCE(MAX(kafka_created_at), 0) FROM {{ this }}
    )
    {% endif %}
),

latest_restaurants AS (
    SELECT
        REGEXP_REPLACE(cnpj, '[^0-9]', '')  AS cnpj_normalized,
        name                                AS restaurant_name
    FROM (
        SELECT
            cnpj,
            name,
            ROW_NUMBER() OVER (PARTITION BY cnpj ORDER BY source_ts_ms DESC) AS _rn
        FROM {{ ref('bronze_restaurants') }}
    )
    WHERE _rn = 1
),

latest_drivers AS (
    SELECT
        driver_id,
        first_name || ' ' || last_name      AS driver_name
    FROM (
        SELECT
            driver_id,
            first_name,
            last_name,
            ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY source_ts_ms DESC) AS _rn
        FROM {{ ref('bronze_drivers') }}
    )
    WHERE _rn = 1
),

latest_users AS (
    SELECT
        REGEXP_REPLACE(cpf, '[^0-9]', '')   AS cpf_normalized,
        email                               AS user_email
    FROM (
        SELECT
            cpf,
            email,
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY source_ts_ms DESC) AS _rn
        FROM {{ ref('bronze_users_mongo') }}
    )
    WHERE _rn = 1
)

SELECT
    o.order_id,
    o.order_date,
    o.total_amount,
    o.user_key,
    REGEXP_REPLACE(o.user_key, '[^0-9]', '')            AS user_cpf_normalized,
    o.restaurant_key,
    REGEXP_REPLACE(o.restaurant_key, '[^0-9]', '')      AS restaurant_cnpj_normalized,
    o.driver_key,
    o.payment_key,
    o.rating_key,
    o.dt_current_timestamp,
    r.restaurant_name,
    d.driver_name,
    u.user_email,
    o.op,
    o.source_ts_ms,
    o.kafka_offset,
    o.kafka_partition,
    o.kafka_created_at
FROM orders o
LEFT JOIN latest_restaurants r
    ON REGEXP_REPLACE(o.restaurant_key, '[^0-9]', '') = r.cnpj_normalized
LEFT JOIN latest_drivers d
    ON o.driver_key = d.driver_id
LEFT JOIN latest_users u
    ON REGEXP_REPLACE(o.user_key, '[^0-9]', '') = u.cpf_normalized

{{
    config(
        materialized         = 'incremental',
        schema               = 'SILVER',
        unique_key           = 'order_item_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Silver: order items fact table. One row per order_item_id.
-- Passthrough of bronze_order_items filtering hard deletes.
-- Items are immutable in practice — no updates expected after creation.

SELECT
    order_item_id,
    order_id,
    product_id,
    restaurant_id,
    product_name,
    product_type,
    cuisine_type,
    unit_price,
    quantity,
    subtotal,
    discount_applied,
    modifiers,
    is_combo,
    is_vegetarian,
    dt_current_timestamp,
    op,
    source_ts_ms,
    kafka_created_at
FROM {{ ref('bronze_order_items') }}
WHERE op != 'd'
{% if is_incremental() %}
AND source_ts_ms > (SELECT COALESCE(MAX(source_ts_ms), 0) FROM {{ this }})
{% endif %}

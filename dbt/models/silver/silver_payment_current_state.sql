{{
    config(
        materialized         = 'incremental',
        schema               = 'SILVER',
        unique_key           = 'payment_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Current state of each payment — one row per payment_id.
-- Latest event by event_timestamp_ms per payment_id.
-- Merge by payment_id: state transitions overwrite previous state.
-- Terminal states: closed (normal or refunded path).
-- In-progress: created, authorized, captured, succeeded, settled.

WITH history_new AS (
    SELECT *
    FROM {{ ref('silver_payment_events_history') }}

    {% if is_incremental() %}
    WHERE source_ts_ms > (
        SELECT COALESCE(MAX(source_ts_ms), 0) FROM {{ this }}
    )
    {% endif %}
),

affected_payments AS (
    SELECT DISTINCT payment_id FROM history_new
),

latest_event AS (
    SELECT * EXCLUDE (_row_num)
    FROM (
        SELECT
            h.*,
            ROW_NUMBER() OVER (
                PARTITION BY h.payment_id
                ORDER BY h.event_timestamp_ms DESC
            ) AS _row_num
        FROM {{ ref('silver_payment_events_history') }} h
        INNER JOIN affected_payments ap USING (payment_id)
    )
    WHERE _row_num = 1
)

SELECT
    payment_id,
    event_id                        AS latest_event_id,
    event_name                      AS current_status,
    event_timestamp                 AS status_updated_at,
    event_timestamp_ms              AS status_updated_at_ms,
    source_ts_ms,
    kafka_created_at,

    -- Terminal: payment reached closed state
    current_status = 'closed'       AS is_closed,

    -- Path taken to closure
    is_refund                       AS closed_via_refund,

    -- In-progress: not yet closed
    current_status != 'closed'      AS is_in_progress,

    -- Stage grouping for funnel analysis
    CASE current_status
        WHEN 'created'    THEN 1
        WHEN 'authorized' THEN 2
        WHEN 'captured'   THEN 3
        WHEN 'succeeded'  THEN 4
        WHEN 'settled'    THEN 5
        WHEN 'refunded'   THEN 4
        WHEN 'closed'     THEN 6
        ELSE 0
    END                             AS stage_order

FROM latest_event

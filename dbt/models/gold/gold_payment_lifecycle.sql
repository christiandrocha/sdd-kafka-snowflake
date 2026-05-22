{{
    config(
        materialized         = 'incremental',
        schema               = 'GOLD',
        unique_key           = 'payment_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Payment lifecycle timing — one row per payment_id.
-- Pivots event history into stage timestamp columns.
-- Computes duration between key transitions in seconds.
-- Merge by payment_id — updated when new events arrive.
-- Use for: SLA monitoring, p50/p95 latency per stage, processing KPIs.
--
-- Note: payment_ids are reused across lifecycle cycles in real data.
-- This model reflects the LAST observed cycle per payment_id.

WITH history AS (
    SELECT
        payment_id,
        event_name,
        event_timestamp_ms,
        event_timestamp,
        is_refund,
        source_ts_ms
    FROM {{ ref('silver_payment_events_history') }}

    {% if is_incremental() %}
    WHERE payment_id IN (
        SELECT DISTINCT payment_id
        FROM {{ ref('silver_payment_events_history') }}
        WHERE source_ts_ms > (
            SELECT COALESCE(MAX(source_ts_ms), 0) FROM {{ this }}
        )
    )
    {% endif %}
),

-- Take the LAST occurrence of each stage per payment_id
-- (handles payment_id reuse across multiple lifecycle cycles)
pivoted AS (
    SELECT
        payment_id,
        MAX(CASE WHEN event_name = 'created'    THEN event_timestamp END) AS created_at,
        MAX(CASE WHEN event_name = 'authorized' THEN event_timestamp END) AS authorized_at,
        MAX(CASE WHEN event_name = 'captured'   THEN event_timestamp END) AS captured_at,
        MAX(CASE WHEN event_name = 'succeeded'  THEN event_timestamp END) AS succeeded_at,
        MAX(CASE WHEN event_name = 'settled'    THEN event_timestamp END) AS settled_at,
        MAX(CASE WHEN event_name = 'refunded'   THEN event_timestamp END) AS refunded_at,
        MAX(CASE WHEN event_name = 'closed'     THEN event_timestamp END) AS closed_at,
        MAX(CASE WHEN is_refund                 THEN true END)            AS had_refund,
        MAX(source_ts_ms)                                                  AS source_ts_ms
    FROM history
    GROUP BY payment_id
)

SELECT
    payment_id,
    created_at,
    authorized_at,
    captured_at,
    succeeded_at,
    settled_at,
    refunded_at,
    closed_at,
    COALESCE(had_refund, false)                AS had_refund,

    -- Stage durations (seconds)
    DATEDIFF('second', created_at,    authorized_at) AS created_to_authorized_sec,
    DATEDIFF('second', authorized_at, captured_at)   AS authorized_to_captured_sec,
    DATEDIFF('second', captured_at,   succeeded_at)  AS captured_to_succeeded_sec,
    DATEDIFF('second', succeeded_at,  settled_at)    AS succeeded_to_settled_sec,
    DATEDIFF('second', settled_at,    closed_at)     AS settled_to_closed_sec,
    DATEDIFF('second', captured_at,   refunded_at)   AS captured_to_refunded_sec,

    -- Total: created → closed
    DATEDIFF('second', created_at, closed_at)        AS total_lifecycle_sec,

    -- Current terminal state
    CASE
        WHEN closed_at     IS NOT NULL THEN 'closed'
        WHEN settled_at    IS NOT NULL THEN 'settled'
        WHEN refunded_at   IS NOT NULL THEN 'refunded'
        WHEN succeeded_at  IS NOT NULL THEN 'succeeded'
        WHEN captured_at   IS NOT NULL THEN 'captured'
        WHEN authorized_at IS NOT NULL THEN 'authorized'
        ELSE 'created'
    END                                              AS reached_stage,

    source_ts_ms,
    CURRENT_TIMESTAMP()                              AS computed_at

FROM pivoted

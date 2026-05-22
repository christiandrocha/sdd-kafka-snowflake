{{
    config(
        materialized         = 'incremental',
        schema               = 'SILVER',
        unique_key           = 'event_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Complete payment event history. One row per event_id.
-- Merge by event_id: corrected events replace originals.
-- DELETEs excluded — retracted events not shown.
-- Real lifecycle: created → authorized → captured → succeeded → settled → closed
--                                                 ↘ refunded → closed
-- Note: payment_ids are reused across lifecycle cycles in real data.
-- event_sequence resets to 1 at each new 'created' event per payment_id.

WITH bronze_new AS (
    SELECT *
    FROM {{ ref('bronze_payment_events') }}

    {% if is_incremental() %}
    WHERE source_ts_ms > (
        SELECT COALESCE(MAX(source_ts_ms), 0) FROM {{ this }}
    )
    {% endif %}
)

SELECT
    event_id,
    payment_id,
    event_name,
    event_timestamp_ms,
    event_timestamp,
    dt_current_timestamp,
    op,
    source_ts_ms,
    kafka_offset,
    kafka_created_at,

    -- Terminal state flag
    event_name IN ('closed') AS is_terminal,

    -- Success path flag
    event_name IN ('succeeded', 'settled', 'closed') AS is_success_path,

    -- Refund path flag
    event_name = 'refunded' AS is_refund,

    -- Global sequence across all events for this payment_id
    ROW_NUMBER() OVER (
        PARTITION BY payment_id
        ORDER BY event_timestamp_ms ASC
    ) AS event_sequence

FROM bronze_new
WHERE op != 'd'

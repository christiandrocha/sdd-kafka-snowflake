{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'status_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: order status events from Snowpipe.
-- Nested status JSONB: {status_name, timestamp} — same pattern as payment_events.event.
-- status_id is INTEGER (not UUID) — unique across all order status transitions.
-- Merges by status_id.

WITH source AS (
    SELECT
        RECORD_CONTENT:status_id::INT                        AS status_id,
        RECORD_CONTENT:order_identifier::VARCHAR             AS order_identifier,

        -- Expand nested status JSONB into flat columns
        RECORD_CONTENT:status:status_name::VARCHAR           AS status_name,

        -- Normalize timestamp: same int/float dual-format as payment_events
        CAST(
            RECORD_CONTENT:status:timestamp::FLOAT AS BIGINT
        )                                                    AS status_timestamp_ms,

        TO_TIMESTAMP_NTZ(
            CAST(RECORD_CONTENT:status:timestamp::FLOAT AS BIGINT) / 1000
        )                                                    AS status_timestamp,

        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'ORDER_STATUS') }}

    {% if is_incremental() %}
    WHERE RECORD_METADATA:CreateTime::BIGINT > (
        SELECT COALESCE(MAX(kafka_created_at), 0) FROM {{ this }}
    )
    {% endif %}
),

deduped AS (
    SELECT * EXCLUDE (_row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY status_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

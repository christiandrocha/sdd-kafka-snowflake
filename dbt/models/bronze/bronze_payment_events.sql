{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'event_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: payment events from Snowpipe.
-- Merges by event_id — idempotent on CDC updates and Snowpipe retries.
-- Normalizes event.timestamp: arrives as int OR float (scientific notation).
-- Real lifecycle: created → authorized → captured → succeeded → settled → closed
--                                                 ↘ refunded → closed

WITH source AS (
    SELECT
        RECORD_CONTENT:event_id::VARCHAR                     AS event_id,
        RECORD_CONTENT:payment_id::VARCHAR                   AS payment_id,
        PARSE_JSON(RECORD_CONTENT:event):event_name::VARCHAR AS event_name,

        -- Normalize: int (151/883) or float scientific (732/883) — both epoch ms
        -- CAST via FLOAT first handles both representations safely
        -- event field arrives as escaped JSON string — requires PARSE_JSON before path traversal
        CAST(
            PARSE_JSON(RECORD_CONTENT:event):timestamp::FLOAT AS BIGINT
        )                                                    AS event_timestamp_ms,

        TO_TIMESTAMP_NTZ(
            CAST(PARSE_JSON(RECORD_CONTENT:event):timestamp::FLOAT AS BIGINT) / 1000
        )                                                    AS event_timestamp,

        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'PAYMENT_EVENTS') }}

    {% if is_incremental() %}
    WHERE RECORD_METADATA:CreateTime::BIGINT > (
        SELECT COALESCE(MAX(kafka_created_at), 0) FROM {{ this }}
    )
    {% endif %}
),

-- Deduplicate within batch before merge
deduped AS (
    SELECT * EXCLUDE (_row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

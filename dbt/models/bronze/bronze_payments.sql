{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'payment_id') %}

{{
    config(
        materialized         = 'incremental',
        schema               = 'BRONZE',
        unique_key           = unique_key,
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

-- Bronze: payments from Snowpipe. 28 fields including card, method, provider.
-- Merges by payment_id.

WITH source AS (
    SELECT
        RECORD_CONTENT:payment_id::VARCHAR                   AS payment_id,
        RECORD_CONTENT:invoice_id::VARCHAR                   AS invoice_id,
        RECORD_CONTENT:order_key::VARCHAR                    AS order_key,
        RECORD_CONTENT:method::VARCHAR                       AS method,
        RECORD_CONTENT:provider::VARCHAR                     AS provider,
        RECORD_CONTENT:status::VARCHAR                       AS status,
        RECORD_CONTENT:amount::FLOAT                         AS amount,
        RECORD_CONTENT:net_amount::FLOAT                     AS net_amount,
        RECORD_CONTENT:tax_amount::FLOAT                     AS tax_amount,
        RECORD_CONTENT:platform_fee::FLOAT                   AS platform_fee,
        RECORD_CONTENT:provider_fee::FLOAT                   AS provider_fee,
        RECORD_CONTENT:refund_amount::FLOAT                  AS refund_amount,
        RECORD_CONTENT:currency::VARCHAR                     AS currency,
        RECORD_CONTENT:country::VARCHAR                      AS country,
        RECORD_CONTENT:captured::BOOLEAN                     AS captured,
        RECORD_CONTENT:refunded::BOOLEAN                     AS refunded,
        RECORD_CONTENT:card_brand::VARCHAR                   AS card_brand,
        RECORD_CONTENT:card_last4::VARCHAR                   AS card_last4,
        RECORD_CONTENT:card_exp_month::INT                   AS card_exp_month,
        RECORD_CONTENT:card_exp_year::INT                    AS card_exp_year,
        RECORD_CONTENT:wallet_provider::VARCHAR              AS wallet_provider,
        RECORD_CONTENT:failure_reason::VARCHAR               AS failure_reason,
        RECORD_CONTENT:receipt_url::VARCHAR                  AS receipt_url,
        RECORD_CONTENT:ip_address::VARCHAR                   AS ip_address,
        RECORD_CONTENT:user_agent::VARCHAR                   AS user_agent,
        RECORD_CONTENT:timestamp::TIMESTAMP_NTZ              AS payment_timestamp,
        RECORD_CONTENT:capture_timestamp::TIMESTAMP_NTZ      AS capture_timestamp,
        RECORD_CONTENT:dt_current_timestamp::VARCHAR         AS dt_current_timestamp,

        RECORD_CONTENT:__op::VARCHAR                         AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT                AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT                       AS kafka_offset,
        RECORD_METADATA:partition::INT                       AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT                   AS kafka_created_at

    FROM {{ source('bronze_raw', 'PAYMENTS') }}

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
                PARTITION BY payment_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)

SELECT * FROM deduped

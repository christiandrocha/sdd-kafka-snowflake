-- ──────────────────────────────────────────────────────────────────────────
-- sdd-kafka-snowflake — Snowflake governance setup
-- ADR-16: RESOURCE MONITOR | ADR-17: Time Travel
--
-- Run once as ACCOUNTADMIN after CDC_POC database and CDC_WH warehouse exist.
-- CDC_ROLE cannot execute this script (RESOURCE MONITOR requires ACCOUNTADMIN).
--
-- Sections:
--   1. RESOURCE MONITOR — prevents runaway credit consumption
--   2. TIME TRAVEL      — reduces BRONZE storage 3× vs 90-day Enterprise default
-- ──────────────────────────────────────────────────────────────────────────

USE ROLE ACCOUNTADMIN;


-- ════════════════════════════════════════════════════════════════════════════
-- 1. RESOURCE MONITOR (ADR-16)
--
-- Guards CDC_WH against runaway spend from:
--   - Dagster bronze_new_data_sensor looping on transient errors
--   - Accidental dbt run --full-refresh on 110k+ order_items
--   - Unfiltered Gold queries joining all domains
--
-- Trigger levels:
--   75%  → email notification only (warning, pipeline continues)
--   90%  → SUSPEND new queries when current ones finish (soft stop)
--   100% → SUSPEND_IMMEDIATE — no new queries started (hard stop)
--
-- CREDIT_QUOTA = 20 is conservative for a 400-credit trial running
-- intermittently. Adjust before sustained production use.
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE RESOURCE MONITOR cdc_poc_monitor
    WITH
        CREDIT_QUOTA    = 20
        FREQUENCY       = MONTHLY
        START_TIMESTAMP = IMMEDIATELY

    TRIGGERS
        ON 75  PERCENT DO NOTIFY
        ON 90  PERCENT DO SUSPEND
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE CDC_WH SET RESOURCE_MONITOR = cdc_poc_monitor;


-- ════════════════════════════════════════════════════════════════════════════
-- 2. TIME TRAVEL (ADR-17)
--
-- BRONZE = 1 day
--   Raw Snowpipe VARIANT tables are append-only CDC streams — no DELETEs.
--   Kafka retains 7 days of replay (LOG_RETENTION_MS in docker-compose).
--   The 90-day Enterprise default retains 90 micro-partition versions per
--   mutation, silently tripling storage on high-churn tables (order_items
--   has 110,001 records). 1 day is sufficient for same-day debugging.
--
-- SILVER = 7 days
-- GOLD   = 7 days
--   Computed dbt state rebuilt by Dagster. 7 days allows reverting a bad
--   dbt run (broken Silver merge, incorrect Gold join) without replaying
--   the full Kafka pipeline from scratch.
--
-- Schema-level ALTER sets the default for all NEW tables created later.
-- Per-table ALTERs below override existing tables immediately.
-- ════════════════════════════════════════════════════════════════════════════

USE DATABASE CDC_POC;

ALTER SCHEMA BRONZE SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER SCHEMA SILVER SET DATA_RETENTION_TIME_IN_DAYS = 7;
ALTER SCHEMA GOLD   SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- ── Raw Snowpipe VARIANT tables — 20 domains ──────────────────────────────
-- These are created automatically by the Snowflake Kafka Connector.
-- Each holds RECORD_METADATA (VARIANT) + RECORD_CONTENT (VARIANT) per row.
-- Kafka replay makes Snowflake Time Travel redundant beyond same-day use.

ALTER TABLE BRONZE.PAYMENT_EVENTS    SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.ORDERS            SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.PAYMENTS          SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.ORDER_ITEMS       SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.GPS_EVENTS        SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.ORDER_STATUS      SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.ROUTES            SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.RECEIPTS          SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.DRIVER_SHIFTS     SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.SEARCH_EVENTS     SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.RECOMMENDATIONS   SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.SUPPORT_TICKETS   SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.USERS_MONGO       SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.USERS_MSSQL       SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.RESTAURANTS       SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.DRIVERS           SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.PRODUCTS          SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.MENU_SECTIONS     SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.RATINGS           SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.INVENTORY         SET DATA_RETENTION_TIME_IN_DAYS = 1;


-- ════════════════════════════════════════════════════════════════════════════
-- Verification queries (AC-25, AC-26)
-- Run in a Snowflake worksheet after executing this script.
-- ════════════════════════════════════════════════════════════════════════════

-- AC-25: Resource monitor exists and is attached to CDC_WH
-- SHOW RESOURCE MONITORS LIKE 'cdc_poc_monitor';
-- Expected: 1 row, CREDIT_QUOTA = 20, FREQUENCY = MONTHLY

-- AC-26: Time Travel per schema
-- SELECT table_schema, MIN(retention_time) AS min_days, MAX(retention_time) AS max_days
-- FROM CDC_POC.information_schema.tables
-- WHERE table_schema IN ('BRONZE', 'SILVER', 'GOLD')
-- GROUP BY table_schema
-- ORDER BY table_schema;
-- Expected:
--   BRONZE  1  1
--   GOLD    7  7
--   SILVER  7  7

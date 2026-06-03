# KB: Snowflake — Ingestion, VARIANT, Snowpipe, Connector
# Knowledge base for ai-kafka-microbatch agents

## Snowflake Kafka Connector

The official Snowflake Kafka Connector (com.snowflake.kafka.connector.SnowflakeSinkConnector)
runs inside Kafka Connect and uses Snowpipe internally for continuous ingestion.

Key behaviors:
- Creates target tables automatically in the configured schema
- Each Kafka message becomes one row with two VARIANT columns
- Reads Avro schema from Schema Registry for deserialization
- Manages offsets in Kafka Connect (exactly-once delivery option available)

## What lands in Snowflake (BRONZE table structure)

The connector creates tables with this structure automatically:

```sql
CREATE TABLE CDC_POC.BRONZE.USUARIOS (
    RECORD_METADATA  VARIANT,  -- Kafka metadata: topic, partition, offset, timestamp
    RECORD_CONTENT   VARIANT   -- Event payload: all CDC fields including __op
);
```

Example rows:
```json
-- RECORD_METADATA
{
  "offset": 42,
  "partition": 0,
  "topic": "pg.public.usuarios",
  "CreateTime": 1715695200000
}

-- RECORD_CONTENT
{
  "id": 1,
  "nome": "Ana Lima",
  "email": "ana.lima@exemplo.com",
  "ativo": true,
  "criado_em": 1715695200000000,
  "__op": "u",
  "__source_ts_ms": 1715695200000
}
```

## Snowpipe ingestion model

Snowpipe is Snowflake's continuous ingestion service used internally by the connector:
- Latency: ~1 minute from Kafka message to queryable row in Snowflake
- Not configurable by the user — managed entirely by Snowflake
- Charged per credit consumed during loading (separate from query credits)
- Idempotent: replaying the same Kafka offset produces the same row

## Connector configuration placeholders

All credential fields use `${VAR}` syntax resolved by `envsubst` at runtime:

```json
{
  "name": "snowflake-sink",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "snowflake.url.name": "${SNOWFLAKE_URL}",
    "snowflake.user.name": "${SNOWFLAKE_USER}",
    "snowflake.private.key": "${SNOWFLAKE_PRIVATE_KEY}",
    "snowflake.database.name": "${SNOWFLAKE_DATABASE}",
    "snowflake.schema.name": "BRONZE",
    "snowflake.ingestion.method": "SNOWPIPE",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```

## Required environment variables (.env)

```bash
# PostgreSQL
POSTGRES_USER=poc_user
POSTGRES_PASSWORD=poc_pass
POSTGRES_DB=pocdb

# Snowflake
SNOWFLAKE_URL=<account>.snowflakecomputing.com
SNOWFLAKE_USER=<user>
SNOWFLAKE_PRIVATE_KEY=<base64-encoded-private-key>
SNOWFLAKE_DATABASE=CDC_POC
SNOWFLAKE_WAREHOUSE=CDC_WH
SNOWFLAKE_ROLE=CDC_ROLE

# Schema Registry
SCHEMA_REGISTRY_URL=http://schema-registry:8081
```

## Casting VARIANT to typed columns in Bronze models

Bronze dbt models cast VARIANT fields to native Snowflake types:

```sql
SELECT
    RECORD_CONTENT:id::INT              AS id,
    RECORD_CONTENT:nome::VARCHAR        AS nome,
    RECORD_CONTENT:email::VARCHAR       AS email,
    RECORD_CONTENT:ativo::BOOLEAN       AS ativo,
    RECORD_CONTENT:preco::FLOAT         AS preco,
    RECORD_CONTENT:__op::VARCHAR        AS op,
    RECORD_CONTENT:__source_ts_ms::BIGINT AS source_ts_ms,
    RECORD_METADATA:offset::BIGINT      AS kafka_offset,
    RECORD_METADATA:partition::INT      AS kafka_partition
FROM {{ source('bronze', 'usuarios') }}
```

## Snowflake database setup (run once before pipeline)

```sql
-- Run as ACCOUNTADMIN or SYSADMIN
CREATE DATABASE IF NOT EXISTS CDC_POC;
CREATE WAREHOUSE IF NOT EXISTS CDC_WH WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60;
CREATE ROLE IF NOT EXISTS CDC_ROLE;

CREATE SCHEMA IF NOT EXISTS CDC_POC.BRONZE;
CREATE SCHEMA IF NOT EXISTS CDC_POC.SILVER;
CREATE SCHEMA IF NOT EXISTS CDC_POC.GOLD;

GRANT USAGE ON DATABASE CDC_POC TO ROLE CDC_ROLE;
GRANT ALL ON SCHEMA CDC_POC.BRONZE TO ROLE CDC_ROLE;
GRANT ALL ON SCHEMA CDC_POC.SILVER TO ROLE CDC_ROLE;
GRANT ALL ON SCHEMA CDC_POC.GOLD TO ROLE CDC_ROLE;
GRANT USAGE ON WAREHOUSE CDC_WH TO ROLE CDC_ROLE;
```

## Governance: RESOURCE MONITOR + Time Travel (ADR-16, ADR-17)

Run `infra/scripts/snowflake_setup.sql` once as ACCOUNTADMIN after the
database and warehouse are created. Do not run as CDC_ROLE — RESOURCE MONITOR
creation requires ACCOUNTADMIN privilege.

### RESOURCE MONITOR (ADR-16)

Prevents runaway credit consumption from Dagster sensor loops, accidental
`dbt run --full-refresh`, or unfiltered Gold queries.

Applied at account level (not warehouse) so it tracks all warehouse usage.
FREQUENCY=NEVER means the quota never resets — it tracks total trial consumption.
All triggers are NOTIFY only (trial context — no auto-suspend).

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE RESOURCE MONITOR cdc_trial_monitor
    WITH
        CREDIT_QUOTA    = 348          -- 400 trial credits - 52 already consumed
        FREQUENCY       = NEVER
        START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON   8 PERCENT DO NOTIFY
        ON  50 PERCENT DO NOTIFY
        ON  75 PERCENT DO NOTIFY
        ON 100 PERCENT DO NOTIFY;

ALTER ACCOUNT SET RESOURCE_MONITOR = cdc_trial_monitor;
```

Verify: `SHOW RESOURCE MONITORS LIKE 'cdc_trial_monitor';` → 1 row, CREDIT_QUOTA=348.

### Time Travel reduction (ADR-17)

BRONZE tables are append-only CDC VARIANT streams. The 90-day Enterprise default
retains 90 micro-partition versions per mutation, tripling storage silently.
Kafka retains 7 days of replay — 1-day Time Travel is sufficient for same-day
debugging of raw tables.

SILVER/GOLD hold computed dbt state. 7 days allows reverting a bad dbt run
without replaying the full Kafka pipeline.

```sql
USE DATABASE CDC_POC;

-- Schema defaults (applies to all future tables in each schema)
ALTER SCHEMA BRONZE SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER SCHEMA SILVER SET DATA_RETENTION_TIME_IN_DAYS = 7;
ALTER SCHEMA GOLD   SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- Override each existing BRONZE raw table individually
ALTER TABLE BRONZE.PAYMENT_EVENTS    SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE BRONZE.ORDERS            SET DATA_RETENTION_TIME_IN_DAYS = 1;
-- ... (all 20 tables — see snowflake_setup.sql for full list)
```

Verify:
```sql
SELECT table_schema, MIN(retention_time), MAX(retention_time)
FROM CDC_POC.information_schema.tables
WHERE table_schema IN ('BRONZE','SILVER','GOLD')
GROUP BY table_schema;
-- BRONZE: 1,1 | SILVER: 7,7 | GOLD: 7,7
```

## CONFIG schema — metadata and observability tables

Beyond BRONZE/SILVER/GOLD, the project uses a `CONFIG` schema with two tables:

**`CONFIG.TABLE_METADATA`** — one row per domain, drives dbt macro config.
Fields: `table_name`, `table_type`, `cdc_strategy`, `unique_key`, `active`.
Populated by `scripts/bootstrap_metadata.sql` and kept in sync by `sync_metadata.py`
(triggered by `registry_new_subject_sensor` when new Avro subjects are detected).

**`CONFIG.PROCESSING_LOG`** — append-only log written by the Dagster
`log_processing_results` asset after each `dbt run`. One row per dbt model per run.
Fields: `table_name`, `layer`, `dbt_model`, `dbt_invocation_id`, `run_id`,
`status`, `rows_processed`, `started_at`, `finished_at`, `duration_seconds`,
`error_message`, `triggered_by`, `logged_at`.

## Authentication: private key (recommended over password)

Snowflake Kafka Connector requires key-pair authentication:

```bash
# Generate key pair
openssl genrsa -out snowflake_private_key.pem 2048
openssl rsa -in snowflake_private_key.pem -pubout -out snowflake_public_key.pem

# Extract base64 private key (remove header/footer)
PRIVATE_KEY=$(cat snowflake_private_key.pem | grep -v "BEGIN\|END" | tr -d '\n')

# Register public key in Snowflake
ALTER USER <user> SET RSA_PUBLIC_KEY='<contents of snowflake_public_key.pem>';
```

Add `PRIVATE_KEY` value to `.env` as `SNOWFLAKE_PRIVATE_KEY`.

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

## CONFIG schema — 3 tabelas de metadados e observabilidade

Criado por `scripts/bootstrap_metadata.sql` (run once). Grants para `CDC_ROLE`
incluem SELECT, INSERT, UPDATE em todas as tabelas do schema.

### CONFIG.TABLE_METADATA

Uma linha por domínio. Fonte de verdade para o macro `get_table_config()` no dbt.
Populada pelo bootstrap e mantida em sincronia pelo `sync_metadata.py`
(disparado por `registry_new_subject_sensor`).

| Campo | Tipo | Descrição |
|---|---|---|
| `table_name` | VARCHAR PK | Nome da tabela (ex: `orders`) |
| `topic` | VARCHAR | Kafka topic correspondente (ex: `pg.public.orders`) |
| `table_type` | VARCHAR | `entity`, `fact` ou `log` |
| `cdc_strategy` | VARCHAR | `upsert` (todos os 20 domínios) |
| `unique_key` | VARCHAR | PK da tabela usada no MERGE |
| `active` | BOOLEAN | `true` = processada pelo dbt |
| `registered_at` | TIMESTAMP_NTZ | Inserção inicial |
| `updated_at` | TIMESTAMP_NTZ | Última atualização |
| `source` | VARCHAR | `manual` (bootstrap) ou `schema_registry` (sync) |
| `previous_strategy` | VARCHAR | Valor anterior de `cdc_strategy` ao fazer update |
| `changed_by` | VARCHAR | Ex: `bootstrap`, `sync_metadata.py` |
| `notes` | VARCHAR | Descrição livre por domínio |

`sync_metadata.py` lê o campo `doc` do schema Avro no Registry
(formato `table_type=entity,cdc_strategy=upsert,unique_key=id`) e:
- **Novo subject**: insere com os valores do `doc` + defaults para campos ausentes
- **Subject existente com `doc`**: atualiza apenas os campos explicitamente declarados no `doc`
- **Subject existente sem `doc`**: preserva metadados atuais, não sobrescreve com defaults

### CONFIG.METADATA_HISTORY

Audit trail de cada insert/update em `TABLE_METADATA`.
Um registro por campo alterado.

| Campo | Tipo | Descrição |
|---|---|---|
| `history_id` | NUMBER AUTOINCREMENT PK | |
| `table_name` | VARCHAR | |
| `changed_at` | TIMESTAMP_NTZ | |
| `changed_by` | VARCHAR | Ex: `sync_metadata.py` |
| `change_type` | VARCHAR | `insert` ou `update` |
| `field_changed` | VARCHAR | Campo que mudou (ex: `cdc_strategy`) |
| `old_value` | VARCHAR | Valor anterior |
| `new_value` | VARCHAR | Novo valor |
| `source` | VARCHAR | `manual` ou `schema_registry` |

### CONFIG.PROCESSING_LOG

Append-only. Escrito pelo asset Dagster `log_processing_results` após cada `dbt run`.
Uma linha por modelo dbt por execução.

| Campo | Tipo | Descrição |
|---|---|---|
| `log_id` | NUMBER AUTOINCREMENT PK | |
| `table_name` | VARCHAR | Ex: `orders` |
| `layer` | VARCHAR | `bronze`, `silver` ou `gold` |
| `dbt_model` | VARCHAR | Ex: `bronze_orders` |
| `dbt_invocation_id` | VARCHAR | UUID do run do dbt |
| `run_id` | VARCHAR | ID do run do Dagster |
| `status` | VARCHAR | `success` ou `error` |
| `rows_processed` | NUMBER | COUNT(*) da tabela após o run |
| `started_at` / `finished_at` | TIMESTAMP_NTZ | Timing do modelo |
| `duration_seconds` | NUMBER(10,3) | |
| `error_message` | VARCHAR(2000) | Preenchido se `status=error` |
| `triggered_by` | VARCHAR | Ex: `sensor` |
| `logged_at` | TIMESTAMP_NTZ | |

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

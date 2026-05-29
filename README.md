# sdd-kafka-snowflake

> **Real-time CDC pipeline** — PostgreSQL → Debezium → Kafka → Snowflake, with Medallion Architecture (Bronze / Silver / Gold) via dbt Core, orchestrated by Dagster, and full observability with Prometheus + Grafana.

<p align="left">
  <img src="https://img.shields.io/badge/version-4.0.0-blue?style=flat-square" />
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/Apache_Kafka-Confluent_7.5-231F20?style=flat-square&logo=apache-kafka&logoColor=white" />
  <img src="https://img.shields.io/badge/Snowflake-Enterprise-29B5E8?style=flat-square&logo=snowflake&logoColor=white" />
  <img src="https://img.shields.io/badge/dbt_Core-1.7-FF694B?style=flat-square&logo=dbt&logoColor=white" />
  <img src="https://img.shields.io/badge/Dagster-1.6-7C3AED?style=flat-square" />
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white" />
  <img src="https://img.shields.io/badge/Prometheus_%2B_Grafana-observability-E6522C?style=flat-square&logo=prometheus&logoColor=white" />
</p>

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SOURCE LAYER                                        │
│   PostgreSQL 15 (WAL logical replication · REPLICA IDENTITY FULL)           │
│   20 tables · 4 simulated sources (Kafka, MongoDB, MySQL, PostgreSQL/MSSQL) │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │ pgoutput slot (debezium_slot)
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STREAMING LAYER                                     │
│   Debezium 2.4  ──►  Apache Kafka (Confluent 7.5)  ──►  Schema Registry     │
│   20 CDC topics · Avro serialization · BACKWARD compatibility               │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │ Kafka Connect Snowflake Sink
                               │ 2 connectors: sink (19 topics) · sinkitems (order_items)
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER — BRONZE (Snowflake)                │
│   Snowpipe · 20 raw tables (RECORD_CONTENT VARIANT + RECORD_METADATA)       │
│   20 stages · 20 pipes · RSA Key Pair authentication                        │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │ dbt Core 1.7 — incremental MERGE
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  TRANSFORMATION LAYER — dbt Medallion Architecture          │
│                                                                             │
│  Bronze (20 models)    Silver (9 models)        Gold (6 models)             │
│  ─────────────────     ─────────────────        ──────────────              │
│  Flatten VARIANT       Deduplicate by PK         Cross-domain analytics     │
│  Cast + normalize      Enrich via joins          Aggregations for BI        │
│  PARSE_JSON JSONB      CDC upsert / merge        Engagement tiers           │
│  Filter op != 'd'      CPF/CNPJ business keys    Revenue · Funnel · KPIs    │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │ Dagster 1.6 — sensor-driven orchestration
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATION + OBSERVABILITY                          │
│   Dagster · bronze_new_data_sensor (60s) · CONFIG.PROCESSING_LOG            │
│   Prometheus 2.49 · JMX Exporter · Grafana 10.2 (3 dashboards)              │
│   Kafka UI · Schema Registry UI · dbt lineage graph                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Pipeline at a Glance

| Metric | Value |
|--------|-------|
| Source domains | 20 tables across 4 systems |
| Total records | 129,353 |
| Largest table | `order_items` — 110,001 rows (85% of volume) |
| dbt models | 35 (20 Bronze · 9 Silver · 6 Gold) |
| End-to-end latency | < 60 seconds (PostgreSQL → Snowflake Gold) |
| CDC events captured | INSERT · UPDATE · DELETE (Debezium op flags) |
| Schema compatibility | BACKWARD enforced via Schema Registry |
| Snowflake objects | 20 raw tables · 20 stages · 20 pipes |

---

## Tech Stack

| Layer | Technology | Version | Role |
|-------|-----------|---------|------|
| Source DB | PostgreSQL | 15 | WAL logical replication, CDC source |
| CDC | Debezium | 2.4 | Reads WAL via `pgoutput` slot |
| Broker | Apache Kafka | Confluent 7.5 | Event streaming, 7-day retention |
| Schema | Confluent Schema Registry | 7.5 | Avro schema governance (BACKWARD) |
| Sink | Kafka Connect Snowflake Sink | 2.x | Snowpipe ingestion into VARIANT tables |
| Data Warehouse | Snowflake Enterprise | — | Snowpipe · Time Travel · Resource Monitor |
| Transformation | dbt Core | 1.7 | Medallion Architecture, incremental MERGE |
| Orchestration | Dagster | 1.6 | Sensor-driven dbt execution, lineage graph |
| Observability | Prometheus + Grafana | 2.49 / 10.2 | JMX metrics, consumer lag, dashboards |
| Containerization | Docker Compose | v2 | 11 services, health-checked startup chain |

---

## Domain Model (20 Tables)

| Type | Table | Simulated Source | PK | Records |
|------|-------|-----------------|-----|---------|
| event | payment_events | Kafka | event_id | 2,208 |
| event | gps_events | Kafka | gps_id | 7,350 |
| event | order_status | Kafka | status_id | 4,176 |
| event | search_events | Kafka | search_id | 202 |
| event | recommendations | MongoDB | event_id | 254 |
| fact | order_items | MongoDB | order_item_id | 110,001 |
| entity | orders | Kafka | order_id | 405 |
| entity | payments | Kafka | payment_id | 260 |
| entity | routes | Kafka | route_id | 410 |
| entity | receipts | Kafka | receipt_id | 377 |
| entity | driver_shifts | Kafka | shift_id | 468 |
| entity | support_tickets | MongoDB | ticket_id | 410 |
| entity | users_mongo | MongoDB | uuid | 411 |
| entity | users_mssql | MSSQL | uuid | 288 |
| entity | restaurants | MySQL | uuid | 461 |
| entity | drivers | PostgreSQL | uuid | 354 |
| entity | products | MySQL | product_id | 368 |
| entity | menu_sections | MySQL | menu_section_id | 362 |
| entity | ratings | MySQL | rating_id | 327 |
| entity | inventory | PostgreSQL | stock_id | 261 |

---

## dbt Medallion Architecture

35 models across 3 layers — strictly Bronze → Silver → Gold, no layer skipping.

### Bronze → Silver — cleaning & enrichment

| Silver Model | Source Bronze Models | What happens |
|---|---|---|
| `silver_orders` | `bronze_orders` + `bronze_restaurants` + `bronze_drivers` + `bronze_users_mongo` | Denormalize: resolve restaurant name (CNPJ), driver name, user email |
| `silver_users` | `bronze_users_mongo` + `bronze_users_mssql` | FULL OUTER JOIN on CPF — unifies two user systems into one entity |
| `silver_drivers` | `bronze_drivers` | Deduplicate on business key `driver_id` (Bronze dedupes on `uuid`) |
| `silver_driver_shifts` | `bronze_driver_shifts` + `silver_drivers` | Enrich shifts with driver name and vehicle profile |
| `silver_payment_events_history` | `bronze_payment_events` | `PARSE_JSON(event)` — JSONB field arrives as escaped string; extract `event_name` + timestamp |
| `silver_payment_current_state` | `silver_payment_events_history` | Latest lifecycle state per `payment_id` via window function |
| `silver_order_items` | `bronze_order_items` | Filter hard deletes (`op != 'd'`) |
| `silver_search_events` | `bronze_search_events` | Filter hard deletes |
| `silver_recommendations` | `bronze_recommendations` | Filter hard deletes |

### Silver → Gold — analytics

| Gold Model | Source Silver Models | Business question answered |
|---|---|---|
| `gold_payment_funnel` | `silver_payment_events_history` | What is the conversion rate at each payment lifecycle stage? |
| `gold_payment_lifecycle` | `silver_payment_events_history` | How long does each payment transition take (seconds)? |
| `gold_payments_by_status` | `silver_payment_current_state` | How many payments are in each status right now? |
| `gold_driver_performance` | `silver_driver_shifts` + `silver_orders` | What are earnings/hour and delivery count per shift? |
| `gold_revenue_per_restaurant` | `silver_orders` + `silver_order_items` | What is daily gross revenue and item breakdown per restaurant? |
| `gold_user_behavior` | `silver_users` + `silver_orders` + `silver_search_events` + `silver_recommendations` | What is each user's engagement tier, total spend, and search/recommendation activity? |

**Notable transformations:**
- `PARSE_JSON()` on JSONB fields — PostgreSQL JSONB columns are serialized by Debezium/Avro as escaped JSON strings, not nested objects; direct path traversal returns NULL
- CPF normalization — `REGEXP_REPLACE(cpf, '[^0-9]', '')` strips formatting before joining across systems
- Timestamp normalization — `CAST(::FLOAT AS BIGINT)` handles both integer and scientific notation epoch milliseconds
- All incremental models use `merge` strategy — safe for Snowpipe at-least-once delivery

---

## Services

| Service | Image | Port (dev) | Purpose |
|---------|-------|-----------|---------|
| `postgres` | postgres:15 | 5432 | Source database with WAL replication |
| `zookeeper` | confluentinc/cp-zookeeper:7.5.0 | 2181 | Kafka coordination |
| `kafka` | confluentinc/cp-kafka:7.5.0 | 9092 | Event broker |
| `schema-registry` | confluentinc/cp-schema-registry:7.5.0 | 8081 | Avro schema governance |
| `kafka-connect` | custom (Debezium + Snowflake Sink) | 8083 | CDC + sink connectors |
| `dagster` | custom (dbt + Dagster) | 3000 | Orchestration webserver |
| `dagster-daemon` | custom | — | Sensor + run executor |
| `kafka-ui` | provectuslabs/kafka-ui | 8080 | Topic / connector inspection |
| `jmx-exporter` | bitnami/jmx-exporter | 5556 | Kafka JMX metrics |
| `prometheus` | prom/prometheus:v2.49.0 | 9090 | Metrics scraping |
| `grafana` | grafana/grafana:10.2.0 | 3001 | Dashboards (admin/admin) |

> All services start in health-checked dependency order — Kafka Connect only starts after Kafka, PostgreSQL, and Schema Registry pass their healthchecks.

---

## Prerequisites

- Docker Desktop 4.x+ with **at least 8 GB RAM** allocated
- Docker Compose v2
- **Snowflake Enterprise Edition** account (Snowpipe requires Enterprise)
- `openssl` installed locally

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/christiandrocha/sdd-kafka-snowflake.git
cd sdd-kafka-snowflake/infra
```

### 2. Generate RSA keys for Snowflake authentication

```bash
bash scripts/generate_keys.sh
```

Register the public key in Snowflake:
```sql
ALTER USER <your_user> SET RSA_PUBLIC_KEY='<contents of snowflake_key_pub.pem>';
```

### 3. Configure Snowflake

Run `scripts/snowflake_setup.sql` as `ACCOUNTADMIN` in Snowsight to create:
- `CDC_ROLE` with least-privilege grants
- `CDC_WH` warehouse (XSMALL, AUTO_SUSPEND=60)
- `CDC_POC` database with BRONZE / SILVER / GOLD / CONFIG schemas
- Resource Monitor (20 credits/month cap)
- Time Travel: 1 day for Bronze, 7 days for Silver/Gold

### 4. Set environment variables

```bash
cp .env.example .env
# Fill in SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH, etc.
```

### 5. Start all services

```bash
docker compose up -d
docker compose logs -f kafka-connect  # wait for "Kafka Connect started"
```

### 6. Register CDC connectors

```bash
bash scripts/register_connectors.sh
```

Registers three connectors:
- **`debezium-postgres-cdc`** — reads PostgreSQL WAL via `pgoutput`
- **`sink`** — delivers 19 topics to Snowflake via Snowpipe
- **`sinkitems`** — dedicated connector for `order_items` (110k records, larger buffer)

### 7. Load test data and run dbt

```bash
# Load all 20 domains into PostgreSQL (CDC triggers automatically)
python3 tests/load_to_postgres.py --data-dir tests/data/ --batch all \
  --db-url $DATABASE_URL

# Once Bronze tables are populated, run dbt
docker exec dagster-webserver bash -c \
  "cd /opt/dagster/dbt && dbt run --select bronze silver gold \
   --profiles-dir /opt/dagster/dbt"
```

Or let Dagster's `bronze_new_data_sensor` trigger the pipeline automatically (polls every 60s).

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Unified PostgreSQL source (ADR-04) | All 4 simulated sources (Kafka, MongoDB, MySQL, MSSQL) are consolidated into PostgreSQL, enabling a single Debezium connector for 20 tables |
| Snowpipe via Kafka Connect (not Snowpipe REST) | Preserves Kafka as the system of record; connector manages staging files, pipes, and offset commits automatically |
| `generate_schema_name` macro | Overrides dbt's default `<target_schema>_<custom_schema>` naming to write directly to BRONZE / SILVER / GOLD |
| `PARSE_JSON()` for JSONB fields | PostgreSQL JSONB columns arrive as escaped JSON strings via Debezium/Avro — direct path traversal returns NULL |
| `unique_key` per model from TABLE_METADATA | CDC strategy and PK are driven by `CONFIG.TABLE_METADATA`, populated by `sync_metadata.py` from Schema Registry |
| Separate `sinkitems` connector | `order_items` (110k rows) uses a dedicated connector with 5000-record / 120s buffer to avoid blocking other topics |
| RSA Key Pair auth (no passwords) | Private key stored in container volume; never in environment variables or config files |

---

## Observability

### Grafana Dashboards (localhost:3001)
- **Kafka Overview** — throughput, topic lag, broker health
- **Kafka Connect** — connector state, task errors, sink throughput
- **Dagster Pipeline** — run history, asset materialization status

### Consumer Lag Check
```bash
docker exec kafka bash -c "
  kafka-consumer-groups --bootstrap-server localhost:9092 \
    --describe --group connect-sink | awk 'NR>2{sum+=\$6} END{print \"Total lag:\", sum}'"
```

### Snowflake Pipeline Health
```sql
-- Pipe ingestion status
SELECT SYSTEM$PIPE_STATUS('CDC_POC.BRONZE.SNOWFLAKE_KAFKA_CONNECTOR_SINK_PIPE_ORDERS_0');

-- Credits consumed (last 30 days)
SELECT WAREHOUSE_NAME, ROUND(SUM(CREDITS_USED), 3) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME;

-- dbt execution log
SELECT * FROM CDC_POC.CONFIG.PROCESSING_LOG ORDER BY started_at DESC LIMIT 20;
```

---

## Repository Structure

```
infra/
├── connectors/
│   ├── debezium.json               # Debezium PostgreSQL Source (20 tables, pgoutput)
│   ├── snowflake_sink.json         # Snowflake Sink "sink" (19 topics, VARIANT)
│   └── snowflake_sink_items.json   # Snowflake Sink "sinkitems" (order_items, large buffer)
├── dbt/
│   ├── macros/
│   │   ├── generate_schema_name.sql  # Writes directly to BRONZE/SILVER/GOLD
│   │   ├── get_table_config.sql      # Reads CDC strategy from TABLE_METADATA
│   │   └── resolve_cdc.sql           # upsert / append / log strategies
│   ├── models/
│   │   ├── bronze/                 # 20 models — flatten VARIANT, PARSE_JSON, dedup
│   │   ├── silver/                 # 9 models — enrich, join, CPF/CNPJ business keys
│   │   ├── gold/                   # 6 models — cross-domain analytics
│   │   └── config/                 # sources.yml (20 Bronze tables + CONFIG schema)
│   ├── dbt_project.yml
│   └── profiles.yml                # dev + prod targets via env_var()
├── dagster/
│   └── pipeline/
│       ├── assets.py               # cdc_dbt_assets + log_processing_results
│       ├── sensors.py              # bronze_new_data_sensor + registry_new_subject_sensor
│       ├── jobs.py                 # cdc_pipeline_job + sync_metadata_job
│       └── resources.py            # SnowflakeResource + DbtCliResource
├── observability/
│   ├── prometheus/                 # prometheus.yml + alert_rules.yml
│   └── grafana/                    # 3 provisioned dashboards
├── scripts/
│   ├── init.sql                    # 20 PostgreSQL tables + dbz_publication
│   ├── snowflake_setup.sql         # Roles, warehouse, Time Travel, Resource Monitor
│   ├── register_connectors.sh      # Registers 3 connectors via Connect REST API
│   ├── bootstrap_metadata.sql      # Seeds CONFIG.TABLE_METADATA (20 entries)
│   ├── sync_metadata.py            # Schema Registry → TABLE_METADATA sync
│   └── truncate_snowflake.py       # Full pipeline reset utility
├── tests/
│   ├── load_to_postgres.py         # 20-domain data loader (129,353 records)
│   └── data/                       # 100 JSON files across 20 domains
├── docker-compose.yml              # Base services (no exposed ports)
├── docker-compose.override.yml     # Dev: exposed ports + volume mounts
├── Dockerfile.connect              # Debezium + Snowflake Sink + Bouncy Castle
└── .env.example                    # All required variables (safe to commit)
```

---

## Security

- **RSA Key Pair authentication** — no passwords; private key mounted as container volume
- **Bouncy Castle** (`bc-fips` + `bcpkix-fips`) in `Dockerfile.connect` — FIPS-compliant PKCS8 key handling in the JVM
- **Least privilege** — `CDC_ROLE` scoped to `CDC_POC` only
- **Ports never exposed in base compose** — only `docker-compose.override.yml` (dev) exposes ports
- **`.env` gitignored** — `.env.example` is the only committed credentials file

---

## Author

**Christian D. Rocha** — [github.com/christiandrocha](https://github.com/christiandrocha)

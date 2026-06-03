# KB: dbt Core — Medallion Architecture, CDC Resolution, Dagster
# Knowledge base for ai-kafka-microbatch agents

## Medallion Architecture in dbt

Three schemas in Snowflake, each built by dbt:

```
BRONZE  →  Raw events from Snowpipe. VARIANT → typed. One row per Kafka event.
           Append-only. Never modified after creation.

SILVER  →  Current state per entity. One row per id.
           DELETEs resolved (filtered out). Deduplicated via resolve_cdc macro.
           Rebuilt fully on every dbt run (table materialization).

GOLD    →  Business-ready analytical models.
           Aggregations, joins, metrics. Built from Silver only.
           Views or tables depending on query frequency.
```

## CDC resolution — inline per Silver model

There is no shared `resolve_cdc` macro. Each Silver model implements CDC
resolution inline. All 20 tables use `cdc_strategy='upsert'` per
`CONFIG.TABLE_METADATA` (all entries in the `get_table_config` static_fallback
are set to `upsert`).

Common pattern: incremental merge on PK, filter incremental window by
`kafka_created_at` (Bronze timestamp), deduplicate within batch by `source_ts_ms`.

Exception: `silver_users` is `materialized='table'` (not incremental) because
a FULL OUTER JOIN between `users_mongo` and `users_mssql` on CPF is incompatible
with the dbt incremental merge strategy.

## Bronze model pattern

Bronze models are **incremental merge** (not views). Each model:
1. Reads from the raw Snowpipe VARIANT table via `source()`
2. Casts VARIANT fields to typed columns
3. Deduplicates within the current batch before the dbt merge
4. Filters to new rows only on incremental runs via `kafka_created_at`

```sql
-- models/bronze/bronze_orders.sql
{% set cfg = get_config_for(this.name) %}
{% set unique_key = cfg.get('unique_key', 'order_id') %}

{{ config(
    materialized         = 'incremental',
    schema               = 'BRONZE',
    unique_key           = unique_key,
    incremental_strategy = 'merge',
    on_schema_change     = 'sync_all_columns'
) }}

WITH source AS (
    SELECT
        RECORD_CONTENT:order_id::VARCHAR          AS order_id,
        RECORD_CONTENT:total_amount::FLOAT        AS total_amount,
        RECORD_CONTENT:user_key::VARCHAR          AS user_key,
        RECORD_CONTENT:__op::VARCHAR              AS op,
        RECORD_CONTENT:__source_ts_ms::BIGINT     AS source_ts_ms,
        RECORD_METADATA:offset::BIGINT            AS kafka_offset,
        RECORD_METADATA:partition::INT            AS kafka_partition,
        RECORD_METADATA:CreateTime::BIGINT        AS kafka_created_at
    FROM {{ source('bronze_raw', 'ORDERS') }}
    {% if is_incremental() %}
    WHERE RECORD_METADATA:CreateTime::BIGINT > (
        SELECT COALESCE(MAX(kafka_created_at), 0) FROM {{ this }}
    )
    {% endif %}
),
deduped AS (
    SELECT * EXCLUDE (_row_num)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY source_ts_ms DESC, kafka_offset DESC
            ) AS _row_num
        FROM source
    )
    WHERE _row_num = 1
)
SELECT * FROM deduped
```

Key points:
- Incremental filter uses `RECORD_METADATA:CreateTime::BIGINT` (`kafka_created_at`), not `source_ts_ms` — late-arriving events have old `source_ts_ms` but fresh `CreateTime`
- `on_schema_change='sync_all_columns'` handles Avro schema evolution automatically
- `deduped` CTE prevents duplicate PKs when the same record arrives twice in one batch

## Silver model pattern

Most Silver models are `incremental` (merge on PK). Example:

```sql
-- models/silver/silver_orders.sql
{{ config(
    materialized         = 'incremental',
    schema               = 'SILVER',
    unique_key           = 'order_id',
    incremental_strategy = 'merge',
    on_schema_change     = 'sync_all_columns'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('bronze_orders') }}
    {% if is_incremental() %}
    WHERE kafka_created_at > (
        SELECT COALESCE(MAX(kafka_created_at), 0) FROM {{ this }}
    )
    {% endif %}
),
-- ... JOIN to latest_restaurants, latest_drivers, latest_users
SELECT ... FROM orders o
LEFT JOIN latest_restaurants r ON ...
LEFT JOIN latest_drivers d ON ...
LEFT JOIN latest_users u ON ...
```

Exception — `silver_users` is `materialized='table'`:
```sql
-- FULL OUTER JOIN between users_mongo and users_mssql on CPF (digits only)
{{ config(materialized='table', schema='SILVER') }}
WITH mongo AS (...), mssql AS (...)
SELECT COALESCE(m.cpf_normalized, s.cpf_normalized) AS cpf_normalized, ...
FROM mongo m FULL OUTER JOIN mssql s ON m.cpf_normalized = s.cpf_normalized
```

## Gold model patterns

Gold models are `table` or `incremental` (no views). Examples:

```sql
-- gold_revenue_per_restaurant.sql  →  materialized='table'
{{ config(materialized='table', schema='GOLD') }}

WITH order_items AS (
    SELECT order_id, SUM(subtotal) AS items_subtotal, ...
    FROM {{ ref('silver_order_items') }} GROUP BY order_id
),
orders AS (
    SELECT order_id, restaurant_cnpj_normalized, restaurant_name, DATE(order_date) AS order_day, ...
    FROM {{ ref('silver_orders') }} WHERE op != 'd'
)
SELECT o.restaurant_cnpj_normalized, o.restaurant_name, o.order_day,
    COUNT(DISTINCT o.order_id) AS num_orders,
    SUM(o.total_amount) AS gross_revenue, ...
FROM orders o LEFT JOIN order_items i ON o.order_id = i.order_id
GROUP BY ...

-- gold_payment_lifecycle.sql  →  materialized='incremental'
{{ config(materialized='incremental', schema='GOLD', unique_key='payment_id', ...) }}
```

## profiles.yml — environment management

Profile name is `sdd_kafka_snowflake` (matches `dbt_project.yml`).
CI uses a separate dummy profile at `infra/dbt/.ci/profiles/profiles.yml`
with `password: ci-dummy` — avoids needing key-pair auth in GitHub Actions.

```yaml
# infra/dbt/profiles.yml
sdd_kafka_snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      schema: SILVER
      threads: 4

    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT_PROD') }}"
      user: "{{ env_var('SNOWFLAKE_USER_PROD') }}"
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"
      role: CDC_ROLE_PROD
      database: CDC_POC_PROD
      warehouse: CDC_WH_PROD
      schema: SILVER
      threads: 8
```

## dbt tests for CDC

```sql
-- tests/silver_usuarios_no_deletes.sql
-- Fails if any deleted record appears in Silver
SELECT id FROM {{ ref('silver_usuarios') }}
WHERE op = 'd'

-- tests/silver_usuarios_unique_id.sql
-- Fails if any id appears more than once in Silver
SELECT id, COUNT(*) AS cnt
FROM {{ ref('silver_usuarios') }}
GROUP BY id
HAVING cnt > 1
```

## dbt_project.yml structure

```yaml
name: sdd_kafka_snowflake
version: '2.1.0'
config-version: 2
profile: sdd_kafka_snowflake

model-paths: ["models"]
test-paths:  ["tests"]
macro-paths: ["macros"]

models:
  sdd_kafka_snowflake:
    bronze:
      +materialized: incremental
      +schema: BRONZE
      +incremental_strategy: merge
      +on_schema_change: sync_all_columns
      +tags: ["bronze", "cdc"]
    silver:
      +materialized: incremental
      +schema: SILVER
      +incremental_strategy: merge
      +on_schema_change: sync_all_columns
      +tags: ["silver", "cdc"]
    gold:
      +materialized: incremental
      +schema: GOLD
      +incremental_strategy: merge
      +on_schema_change: sync_all_columns
      +tags: ["gold", "analytics"]
    config:
      +materialized: ephemeral
      +tags: ["config"]
```

Individual models override these defaults when needed (e.g., `silver_users`
overrides to `table`; some gold models use `table` instead of `incremental`).

## Dagster integration with dagster-dbt

Dagster treats each dbt model as an asset. The `@dbt_assets` decorator
auto-discovers all models from the dbt manifest. After `dbt run`, it also
runs `dbt test` on the same selection, and then `log_processing_results`
writes one row per model to `CONFIG.PROCESSING_LOG` in Snowflake.

```python
# dagster/pipeline/assets.py
@dbt_assets(manifest=DBT_PROJECT_DIR / "target" / "manifest.json")
def cdc_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run", "--select", "bronze silver gold"], context=context).stream()
    yield from dbt.cli(["test", "--select", "bronze silver gold"], context=context).stream()

@asset(deps=[cdc_dbt_assets], group_name="observability")
def log_processing_results(context, snowflake: SnowflakeResource) -> None:
    # reads dbt target/run_results.json, inserts one row per model
    # into CDC_POC.CONFIG.PROCESSING_LOG (status, rows, duration, errors)
    ...
```

## Dagster pipeline trigger — sensor-driven, not scheduled

There is no cron schedule. The pipeline is triggered by two sensors:

**`bronze_new_data_sensor`** (`minimum_interval_seconds=60`):
- Queries `MAX(RECORD_METADATA:CreateTime::BIGINT)` across all 20 Bronze tables
- Fires `cdc_pipeline_job` only when new data arrived since last cursor
- Prevents redundant dbt runs when no new Snowpipe data

**`registry_new_subject_sensor`** (`minimum_interval_seconds=300`):
- Queries Schema Registry `/subjects` and compares against `CONFIG.TABLE_METADATA`
- Fires `sync_metadata_job` when new subjects are registered but not yet in TABLE_METADATA
- `sync_metadata_job` runs `sync_metadata.py` to populate TABLE_METADATA

## Running dbt manually

```bash
cd infra/dbt

# Install dependencies
dbt deps

# Generate manifest (required by Dagster)
dbt compile --target dev

# Run all layers
dbt run --target dev

# Run specific layer
dbt run --select bronze --target dev
dbt run --select silver --target dev
dbt run --select gold --target dev

# Run tests
dbt test --target dev
```

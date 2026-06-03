# sdd-kafka-snowflake v4.0.0

**Platform:** Uber Eats food delivery (Brazilian market)
**Pipeline:** JSON exports → PostgreSQL → Debezium → Kafka → Snowflake → dbt → Dagster → CI/CD → Prometheus + Grafana

## Platform at a glance

| Metric | Value |
|---|---|
| Source systems | 4 (Kafka, MongoDB, MySQL, PostgreSQL/MSSQL) |
| Domains | 20 |
| JSON files | 100 |
| Total records | 129,353 |
| Largest table | order_items (110,001 — 85% of volume) |
| Hub table | orders (links all via CPF, CNPJ, driver_id, UUID) |

## Project structure

```
CLAUDE.md                          ← this file
.github/workflows/
├── ci.yml                         ← PR: dbt compile, lint, .env guard
└── deploy.yml                     ← merge to main: connectors + dagster

.claude/
├── commands/
│   ├── brainstorm.md              → /brainstorm (8 sections, multi-domain)
│   ├── define.md                  → /define (23 ACs, 20 domains)
│   ├── design.md                  → /design (15 ADRs)
│   └── build.md                   → /build (10 agents)
├── kb/
│   ├── index.md                   → KB navigation (read first)
│   ├── kafka-cdc.md               → WAL, Debezium, 20-table CDC
│   ├── schema-registry.md         → Avro, BACKWARD, 20 subjects
│   ├── snowflake.md               → Snowpipe, VARIANT, 2 sink connectors
│   ├── dbt.md                     → Medallion, business key joins, CPF merge
│   ├── cicd.md                    → GitHub Actions, .gitignore, pre-commit
│   └── observability.md           → Prometheus, Grafana, 20-topic lag
├── 01_brainstorm.prompt           ← multi-source consolidation, 8 sections
├── 02_define.spec.yaml            ← v4.0.0: 20 domains, 23 ACs
├── 03_design.manifest.json        ← v4.0.0: 15 ADRs, domain_map
├── 04_build.delegation.md         ← 10 agents, 20-domain scope
├── 05_implementation_log.md       ← build log with v4.0.0 entry
├── 06_retrospective.md            ← iteration analysis
└── sdd/contracts/quality.md       ← quality contract

infra/
├── docker-compose.yml             ← base services (no host ports)
├── docker-compose.override.yml    ← local: exposed ports, volumes
├── docker-compose.prod.yml        ← production reference
├── Dockerfile.connect             ← Debezium + Snowflake Sink
├── .env.example                   ← all required variables
├── connectors/
│   ├── debezium.json              ← 20 tables in table.include.list
│   ├── snowflake_sink.json        ← 19 standard topics
│   └── snowflake_sink_items.json  ← order_items (larger buffer, 110k records)
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml               ← dev + prod via env_var()
│   ├── packages.yml               ← dbt-utils + dbt-expectations
│   ├── macros/
│   │   ├── get_table_config.sql   ← reads TABLE_METADATA once per run; get_config_for helper
│   │   └── generate_schema_name.sql ← overrides dbt schema naming (uses custom_schema_name as-is)
│   ├── models/
│   │   ├── bronze/                ← 20 models (one per domain) + schema.yml
│   │   ├── silver/                ← payment history/current, users (CPF merge), orders
│   │   ├── gold/                  ← 6 cross-domain analytical models
│   │   └── config/                ← sources.yml (20 Bronze + CONFIG schema)
│   └── tests/                     ← custom SQL tests
├── dagster/
│   ├── Dockerfile
│   ├── dagster.yaml
│   ├── workspace.yaml
│   └── pipeline/
│       ├── __init__.py            ← Definitions: assets, sensors, jobs, resources
│       ├── assets.py              ← cdc_dbt_assets + log_processing_results
│       ├── sensors.py             ← bronze_new_data_sensor (20 tables) + registry_new_subject_sensor
│       ├── jobs.py
│       └── resources.py           ← SnowflakeResource with retry
├── observability/
│   ├── prometheus/prometheus.yml
│   ├── prometheus/alert_rules.yml
│   ├── grafana/provisioning/
│   ├── grafana/dashboards/        ← kafka.json, kafka_connect.json, dagster.json
│   └── jmx/kafka-jmx-exporter.yml
├── scripts/
│   ├── init.sql                   ← 20 PostgreSQL tables + publication
│   ├── bootstrap_metadata.sql     ← 20 TABLE_METADATA entries
│   ├── sync_metadata.py           ← Registry → TABLE_METADATA sync
│   ├── register_connectors.sh     ← registers 3 connectors (2 sinks)
│   └── set_compatibility.sh
└── tests/
    ├── load_to_postgres.py        ← 20-domain loader (--batch initial/incremental)
    ├── simulate_cdc.sql           ← payment_events CDC simulation
    └── schema_evolution.sql       ← payment_events schema evolution

docs/
├── ARCHITECTURE.md
└── TRIAL_PLAN.md                  ← 14-day Snowflake trial plan
```

## Domain map (20 tables)

| Type | Table | Source | PK | Records |
|---|---|---|---|---|
| event | payment_events | kafka_events | event_id | 2,208 |
| event | gps_events | kafka_gps | gps_id | 7,350 |
| event | order_status | kafka_status | status_id (INT) | 4,176 |
| event | search_events | kafka_search | search_id | 202 |
| event | recommendations | mongodb_recommendations | event_id | 254 |
| fact | order_items | mongodb_items | order_item_id | 110,001 |
| entity | orders | kafka_orders | order_id | 405 |
| entity | payments | kafka_payments | payment_id | 260 |
| entity | routes | kafka_route | route_id | 410 |
| entity | receipts | kafka_receipts | receipt_id | 377 |
| entity | driver_shifts | kafka_shift | shift_id | 468 |
| entity | support_tickets | mongodb_support | ticket_id | 410 |
| entity | users_mongo | mongodb_users | uuid | 411 |
| entity | users_mssql | mssql_users | uuid | 288 |
| entity | restaurants | mysql_restaurants | uuid | 461 |
| entity | drivers | postgres_drivers | uuid | 354 |
| entity | products | mysql_products | product_id | 368 |
| entity | menu_sections | mysql_menu | menu_section_id | 362 |
| entity | ratings | mysql_ratings | rating_id | 327 |
| entity | inventory | postgres_inventory | stock_id | 261 |

## Hub table — orders foreign keys

| Field | Type | Resolves to |
|---|---|---|
| user_key | CPF (000.000.000-00) | users_mongo.cpf / users_mssql.cpf |
| restaurant_key | CNPJ (00.000.000/0000-00) | restaurants.cnpj |
| driver_key | string | drivers.driver_id |
| payment_key | UUID | payments.payment_id |
| rating_key | UUID | ratings.uuid |

## Key decisions (full ADRs in 03_design.manifest.json)

| ADR | Decision |
|---|---|
| ADR-04 | Unified PostgreSQL via load_to_postgres.py (not direct multi-source connectors) |
| ADR-05 | 80/20 split: 80 files initial (op='r') + 20 incremental (op='c') |
| ADR-06 | All tables upsert by PK (even events) — Snowpipe retries require idempotency |
| ADR-07 | Business key joins in Gold (CPF/CNPJ as VARCHAR) — CPF normalized in silver_users |
| ADR-13 | Timestamp normalization: CAST(::FLOAT AS BIGINT) — 17% int, 83% float |
| ADR-14 | order_items separate Snowflake Sink buffer (5000 records, 120s) |
| ADR-15 | silver_users unifies users_mongo + users_mssql on CPF |
| ADR-16 | RESOURCE MONITOR cdc_trial_monitor: 348 créditos, FREQUENCY=NEVER, aplicado em ALTER ACCOUNT |
| ADR-17 | Time Travel: BRONZE=1d (Kafka retém 7d), SILVER/GOLD=7d |

## Slash commands

| Command | Phase | Purpose |
|---|---|---|
| `/brainstorm` | 01 | Explore multi-source consolidation and domain model |
| `/define` | 02 | Review 23 ACs and 20-domain clarity gate |
| `/design` | 03 | Review 15 ADRs and domain_map |
| `/build` | 04 | Execute one of 10 delegated agent tasks |

## Services and ports (local with override)

| Service | Port | URL |
|---|---|---|
| PostgreSQL | 5432 | `$DATABASE_URL` |
| Kafka | 9092 | `localhost:9092` |
| Schema Registry | 8081 | `http://localhost:8081` |
| Kafka Connect | 8083 | `http://localhost:8083` |
| Kafka UI | 8080 | `http://localhost:8080` |
| Dagster | 3000 | `http://localhost:3000` |
| Prometheus | 9090 | `http://localhost:9090` |
| Grafana | 3001 | `http://localhost:3001` (admin/admin) |
| JMX Exporter | 5556 | `http://localhost:5556/metrics` |

## Load test commands

```bash
# Dry-run (no DB needed) — VALIDATED ✅
python3 tests/load_to_postgres.py --data-dir tests/data/ --dry-run

# Initial load (80%)
python3 tests/load_to_postgres.py --data-dir tests/data/ --batch initial --db-url $DATABASE_URL

# Incremental load (20%)
python3 tests/load_to_postgres.py --data-dir tests/data/ --batch incremental --db-url $DATABASE_URL

# Single domain
python3 tests/load_to_postgres.py --data-dir tests/data/ --domain kafka_orders --db-url $DATABASE_URL
```

## Continuous improvement

After every change:
- Update `.claude/05_implementation_log.md`
- Update `.claude/06_retrospective.md` at end of iteration
- Flag manifest divergences for /design review

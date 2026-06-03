# KB Index
# sdd-kafka-snowflake
# Read by the agent before any AgentSpec phase.
# Use this index to decide which KB files to load before acting.

## Available files

| File | Load when o agente precisar de... |
|---|---|
| `kafka-cdc.md` | WAL, Debezium, replication slot, ExtractNewRecordState, operações CDC (c/u/d/r), delete.handling.mode=rewrite, configurações críticas do conector (decimal/time/interval), lista dos 20 tópicos, roteamento sink/sinkitems, adicionar tabela |
| `schema-registry.md` | Avro, AvroConverter, modos de compatibilidade BACKWARD, evolução de schema, mapeamento PostgreSQL→Snowflake VARIANT, REST API do Registry |
| `snowflake.md` | Snowpipe, colunas VARIANT, config do Kafka Connector, estrutura das tabelas Bronze, casting VARIANT, setup SQL, RESOURCE MONITOR (cdc_trial_monitor), Time Travel, CONFIG schema (TABLE_METADATA, METADATA_HISTORY, PROCESSING_LOG) |
| `dbt.md` | Medallion Architecture, CDC inline por modelo, inventário de Silver (9 modelos) e Gold (6 modelos), padrão de dois modelos do payment_events, get_table_config macro, sources.yml (bronze_raw + config), 7 testes customizados, sensors Dagster, PROCESSING_LOG |
| `cicd.md` | GitHub Actions CI/CD, ci.yml, deploy.yml, profiles CI dummy, .gitignore, pre-commit, docker-compose.override.yml, GitHub Secrets, register_connectors.sh |
| `observability.md` | Prometheus scraping, consumer lag, Grafana dashboards, alert rules, JMX exporter, métricas dos 3 conectores (debezium-postgres-cdc, sink, sinkitems), docker-compose de observabilidade |

## Which KB to load per phase

### /brainstorm
Load ALL six files — broad exploration phase covering all cycles.

### /define
No KB needed — spec structuring phase with the user.

### /design
Load based on which ADRs are being reviewed:
- ADR-01 to ADR-03 → schema-registry.md
- ADR-04 to ADR-09 → snowflake.md + dbt.md
- ADR-10 → cicd.md
- ADR-11 → observability.md
- ADR-12 → cicd.md (docker-compose.override.yml pattern)
- ADR-13 → kafka-cdc.md + snowflake.md (timestamp float/int normalization)
- ADR-14 → snowflake.md (sinkitems buffer)
- ADR-15 → dbt.md (silver_users FULL OUTER JOIN)
- ADR-16 → snowflake.md (RESOURCE MONITOR cdc_trial_monitor)
- ADR-17 → snowflake.md (Time Travel por schema)
- Full design review → load all six files

### /build
Load only the KB relevant to the agent being executed:
- [1] agent-infra-base → cicd.md (override pattern), observability.md (new services)
- [2] agent-postgres → kafka-cdc.md
- [3] agent-kafka-stack → kafka-cdc.md, observability.md (JMX port)
- [4] agent-connect → kafka-cdc.md, schema-registry.md, snowflake.md
- [5] agent-dbt → dbt.md, snowflake.md
- [6] agent-dagster → dbt.md, snowflake.md
- [7] agent-tests → dbt.md
- [8] agent-validation → all six files
- [9] agent-cicd → cicd.md only
- [10] agent-observability → observability.md only

### /iterate
Load only the KB related to the requested change:
- Adding a table → kafka-cdc.md + dbt.md
- Schema change → schema-registry.md
- New Gold model → dbt.md only
- New CI check → cicd.md only
- Grafana dashboard change → observability.md only
- Alert threshold change → observability.md only

## KB to artifact traceability

```
kafka-cdc.md
    └─▶ connectors/debezium.json
    └─▶ scripts/init.sql
    └─▶ tests/simulate_cdc.sql

schema-registry.md
    └─▶ connectors/debezium.json          (value.converter + auto.register.schemas)
    └─▶ connectors/snowflake_sink.json    (schema.registry.url)
    └─▶ scripts/register_connectors.sh    (define BACKWARD compatibility)
    └─▶ scripts/set_compatibility.sh
    └─▶ tests/schema_evolution.sql

snowflake.md
    └─▶ connectors/snowflake_sink.json
    └─▶ connectors/snowflake_sink_items.json
    └─▶ docker-compose.yml                (Snowflake env vars)
    └─▶ .env.example
    └─▶ dbt/models/bronze/               (VARIANT casting)
    └─▶ scripts/snowflake_setup.sql      (RESOURCE MONITOR + Time Travel)
    └─▶ scripts/bootstrap_metadata.sql   (CONFIG schema + TABLE_METADATA)
    └─▶ scripts/sync_metadata.py         (Registry → TABLE_METADATA)
    └─▶ dagster/pipeline/assets.py       (PROCESSING_LOG)

dbt.md
    └─▶ dbt/dbt_project.yml
    └─▶ dbt/profiles.yml
    └─▶ dbt/packages.yml
    └─▶ dbt/macros/get_table_config.sql  (get_config_for helper)
    └─▶ dbt/macros/generate_schema_name.sql
    └─▶ dbt/models/bronze/              (20 modelos incremental merge)
    └─▶ dbt/models/silver/              (9 modelos: incremental + silver_users=table)
    └─▶ dbt/models/gold/               (6 modelos: table ou incremental)
    └─▶ dbt/models/config/sources.yml  (bronze_raw + config sources)
    └─▶ dbt/tests/                     (7 testes customizados)
    └─▶ dagster/pipeline/assets.py
    └─▶ dagster/pipeline/sensors.py
    └─▶ dagster/pipeline/jobs.py

cicd.md
    └─▶ .github/workflows/ci.yml
    └─▶ .github/workflows/deploy.yml
    └─▶ .gitignore
    └─▶ .pre-commit-config.yaml
    └─▶ dbt/.ci/profiles/profiles.yml
    └─▶ docker-compose.override.yml
    └─▶ docker-compose.prod.yml
    └─▶ scripts/register_connectors.sh

observability.md
    └─▶ docker-compose.yml             (prometheus, grafana, jmx-exporter services)
    └─▶ observability/prometheus/prometheus.yml
    └─▶ observability/prometheus/alert_rules.yml
    └─▶ observability/grafana/provisioning/
    └─▶ observability/grafana/dashboards/
    └─▶ observability/jmx/kafka-jmx-exporter.yml
```

## AgentSpec phase files

| File | Purpose |
|---|---|
| `../01_brainstorm.prompt` | Exploration questions for all 8 sections |
| `../02_define.spec.yaml` | 23 ACs, constraints, clarity gate |
| `../03_design.manifest.json` | 15 ADRs, architecture, type mappings |
| `../04_build.delegation.md` | 10 agents with responsibilities and criteria |
| `../05_implementation_log.md` | Chronological build log |
| `../06_retrospective.md` | Iteration analysis and technical debt |
| `commands/brainstorm.md` | /brainstorm command instructions |
| `commands/define.md` | /define command instructions |
| `commands/design.md` | /design command instructions |
| `commands/build.md` | /build command instructions |

## When to update 05 and 06

- `05_implementation_log.md` → during and after each /build task
- `06_retrospective.md` → at the end of each full iteration (after agent-validation)

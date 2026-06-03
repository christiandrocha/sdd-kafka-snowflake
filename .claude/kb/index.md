# KB Index
# sdd-kafka-snowflake
# Read by the agent before any AgentSpec phase.
# Use this index to decide which KB files to load before acting.

## Available files

| File | Load when the agent needs to... |
|---|---|
| `kafka-cdc.md` | understand WAL, Debezium, replication slot, ExtractNewRecordState, CDC operations (c/u/d/r), Kafka topics, adding tables |
| `schema-registry.md` | understand Avro, AvroConverter, compatibility modes, schema evolution, PostgreSQL→Snowflake type mapping, Registry REST API |
| `snowflake.md` | understand Snowpipe ingestion, VARIANT columns, Snowflake Kafka Connector config, Bronze table structure, VARIANT casting, Snowflake setup SQL |
| `dbt.md` | understand Medallion Architecture, resolve_cdc macro, Bronze/Silver/Gold model patterns, profiles.yml env vars, dbt-expectations tests, schema.yml documentation, Dagster-dbt integration |
| `cicd.md` | understand GitHub Actions CI/CD, ci.yml structure, deploy.yml, CI-safe dbt profiles, .gitignore, pre-commit hooks, docker-compose.override.yml pattern, GitHub Secrets |
| `observability.md` | understand Prometheus scraping, consumer lag metric, Grafana dashboards, alert rules, JMX exporter config, Kafka Connect metrics, docker-compose additions for observability |

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

## KB to infra artifact traceability

```
kafka-cdc.md
    └─▶ infra/connectors/debezium.json
    └─▶ infra/scripts/init.sql
    └─▶ infra/tests/simulate_cdc.sql

schema-registry.md
    └─▶ infra/connectors/debezium.json       (value.converter)
    └─▶ infra/connectors/snowflake_sink.json (schema.registry.url)
    └─▶ infra/scripts/register_connectors.sh
    └─▶ infra/scripts/set_compatibility.sh
    └─▶ infra/tests/schema_evolution.sql

snowflake.md
    └─▶ infra/connectors/snowflake_sink.json
    └─▶ infra/docker-compose.yml              (Snowflake env vars)
    └─▶ infra/.env.example
    └─▶ infra/dbt/models/bronze/              (VARIANT casting)
    └─▶ infra/scripts/bootstrap_metadata.sql
    └─▶ infra/scripts/sync_metadata.py

dbt.md
    └─▶ infra/dbt/dbt_project.yml
    └─▶ infra/dbt/profiles.yml
    └─▶ infra/dbt/packages.yml
    └─▶ infra/dbt/macros/resolve_cdc.sql
    └─▶ infra/dbt/macros/get_table_config.sql
    └─▶ infra/dbt/models/bronze/              (models + schema.yml)
    └─▶ infra/dbt/models/silver/              (models + schema.yml)
    └─▶ infra/dbt/models/gold/                (models + schema.yml)
    └─▶ infra/dbt/tests/
    └─▶ infra/dagster/

cicd.md
    └─▶ .github/workflows/ci.yml
    └─▶ .github/workflows/deploy.yml
    └─▶ .gitignore
    └─▶ .pre-commit-config.yaml
    └─▶ infra/dbt/.ci/profiles/profiles.yml
    └─▶ infra/docker-compose.override.yml
    └─▶ infra/docker-compose.prod.yml

observability.md
    └─▶ infra/docker-compose.yml              (prometheus, grafana, jmx-exporter services)
    └─▶ infra/observability/prometheus/prometheus.yml
    └─▶ infra/observability/prometheus/alert_rules.yml
    └─▶ infra/observability/grafana/provisioning/
    └─▶ infra/observability/grafana/dashboards/
    └─▶ infra/observability/jmx/kafka-jmx-exporter.yml
```

## AgentSpec phase files

| File | Purpose |
|---|---|
| `../01_brainstorm.prompt` | Exploration questions for all 8 sections |
| `../02_define.spec.yaml` | 25 ACs, constraints, clarity gate |
| `../03_design.manifest.json` | 12 ADRs, architecture, type mappings |
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

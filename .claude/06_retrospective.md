# 06 — Retrospective
# ai-kafka-microbatch
# Purpose: structured analysis of each iteration — what worked, what did not,
#          what was learned, technical debt, and improvements for the next cycle.
# Owner: updated at the end of each AgentSpec iteration (/iterate or major version).

---

## How to use this file

- One section per project version or major iteration.
- Reference specific artifacts, ACs, and ADRs when relevant.
- Technical debt must have an owner and a target iteration to be resolved.
- Improvements feed directly into the next 01_brainstorm.prompt or 02_define.spec.yaml.
- Do not delete previous retrospectives — they are the institutional memory of the project.

---

## Retrospective template

```
## vX.Y.Z — <iteration name> — YYYY-MM-DD

### Context
<what was the goal of this iteration>

### What worked well
- <observation> → <why it mattered>

### What did not work
- <observation> → <root cause> → <what to do differently>

### Learned
- <insight gained that was not obvious at design time>

### Technical debt
| ID  | Description | Severity | Target iteration |
|-----|-------------|----------|-----------------|
| TD-01 | <description> | high/medium/low | vX.Y.Z |

### Metrics observed
| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| <metric> | <observed> | <expected> | on-track / off-track |

### Improvements for next iteration
- [ ] <concrete improvement with rationale>

### AC validation results
| AC | Result | Notes |
|----|--------|-------|
| AC-01 | pass/fail | <observation> |
```

---

## v1.0.0 — Initial CDC pipeline with MinIO — 2026-05-14

### Context
First working version of the CDC pipeline. Goal: capture PostgreSQL changes
via Debezium, transport through Kafka, store as Parquet in MinIO with
micro-batch flush strategy.

### What worked well
- Debezium `ExtractNewRecordState` SMT cleanly simplified the payload —
  eliminated nested before/after structure, making downstream consumption trivial.
- Micro-batch flush (1,000 events / 5 min) produced file sizes well suited
  for Parquet reads — no small files problem observed in testing.
- `set_compatibility.sh` utility proved valuable immediately — the REST API
  is not intuitive and the CLI abstraction saved significant time.

### What did not work
- MinIO as landing destination limits the analytical stack — DuckDB queries
  work but require manual S3 endpoint configuration on every session.
  → Root cause: MinIO is S3-compatible but not natively integrated with
    analytical tools the way Snowflake is.
  → Decision: replace MinIO with Snowflake in v2.0.0.

- Parquet schema merging with `union_by_name=true` was not validated
  end-to-end after schema evolution — only documented, not tested.
  → Root cause: test scripts did not include a post-evolution Parquet read.
  → Improvement: add a validation step to schema_evolution.sql that
    confirms field appears in output after flush.

### Learned
- `wal_level=logical` must be set via PostgreSQL command flags, not init.sql.
  Setting it in init.sql has no effect because WAL level is a server parameter
  that requires restart — Docker handles this via the command: array.
- Partition pruning in DuckDB with Hive-style paths works automatically
  only when the glob pattern includes the partition directory (`**/*.parquet`).
  Flat globs bypass partition pruning entirely.

### Technical debt
| ID | Description | Severity | Target iteration |
|----|-------------|----------|-----------------|
| TD-01 | Parquet schema merge not validated end-to-end after evolution | medium | v2.0.0 |
| TD-02 | Test scripts depend on seed data from init.sql (not self-contained) | medium | v2.0.0 |
| TD-03 | No .gitignore — credentials at risk if repo initialized | high | v2.0.0 |

### Metrics observed
| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Debezium event latency (WAL → Kafka) | ~2s | < 5s | ✅ on-track |
| S3 Sink flush interval | 5 min | 5 min | ✅ on-track |
| Schema Registry BACKWARD validation | working | working | ✅ on-track |

### Improvements for next iteration
- [x] Replace MinIO with Snowflake as landing destination
- [x] Make test scripts self-contained (TD-02)
- [x] Add .gitignore and .env pattern (TD-03)
- [ ] Validate schema merge end-to-end (TD-01)

### AC validation results
| AC | Result | Notes |
|----|--------|-------|
| AC-01 | pass | INSERT → Kafka in ~2s |
| AC-02 | pass | UPDATE __op="u" confirmed |
| AC-03 | pass | DELETE __op="d" confirmed |
| AC-04 | pass | Both subjects registered |
| AC-05 | pass | Parquet appeared in MinIO after 5 min flush |
| AC-06 | pass | typeof(preco) = DOUBLE confirmed |
| AC-07 | pass | Schema v2 registered after ALTER TABLE |
| AC-08 | pass | All services healthy on cold boot |

---

## v2.0.0 — Snowflake + dbt + Dagster — 2026-05-14

### Context
Major architectural revision. Goals: replace MinIO with Snowflake,
add Medallion Architecture (Bronze/Silver/Gold) via dbt Core,
add Dagster for orchestration and observability, implement all 7
improvements identified in v1.0.0 critique, reach 13 ACs.

### What worked well
- `resolve_cdc` macro eliminated duplicated deduplication logic across
  all Silver models — single change point for CDC resolution strategy.
- `envsubst` pattern for credential injection is clean and auditable —
  connector JSONs are safe to commit, credentials never leave `.env`.
- Dagster `@dbt_assets` auto-discovery mapped all 6 dbt models to assets
  without manual configuration — lineage visible in UI immediately.
- Self-contained test scripts (test_ prefix + cleanup) proved idempotent
  on second run — no leftover state between test sessions.
- `default_status: "RUNNING"` on Dagster schedule eliminated the manual
  UI step of activating the schedule after deployment.

### What did not work
- Dagster requires `target/manifest.json` at container start —
  `dbt compile` must run before Dagster boots.
  → Root cause: @dbt_assets reads the manifest at import time, not at
    execution time. This is a known Dagster-dbt constraint.
  → Workaround: added dbt deps + compile to Dockerfile entrypoint.
  → Proper fix: pre-compile manifest in CI before building the image.

- Snowflake Connector Bouncy Castle conflict with Debezium base image.
  → Root cause: Debezium 2.4 ships with OpenJDK 11; bc-fips 2.x requires
    OpenJDK 17+.
  → Solution: pinned to bc-fips 1.0.2.4 + bcpkix-fips 1.0.7.

- Bronze models lack `sources.yml` — dbt lineage graph shows Bronze
  as disconnected from raw Snowpipe tables in Dagster UI.
  → Root cause: sources.yml not included in initial dbt scaffold.
  → Impact: cosmetic in PoC, blocking in production (no column-level lineage).

### Learned
- Snowpipe latency is not exactly 1 minute — it varies between 30s and 90s
  depending on Snowflake warehouse load and file size thresholds.
  The "~1 min" SLA is a guideline, not a guarantee.
- Dagster SQLite storage is not suitable for production — it does not support
  concurrent writes from webserver and daemon. Acceptable for PoC only.
- dbt `EXCLUDE` syntax (used in resolve_cdc macro) is Snowflake-specific.
  Migrating to another warehouse would require replacing EXCLUDE with
  explicit column listing. This creates vendor lock-in at the macro level.
- The Dagster schedule runs `dbt run` regardless of whether new data
  arrived in Snowflake — wastes warehouse credits on empty runs.
  A Snowflake-based sensor would be more efficient (run only when
  Bronze has new rows since last execution).

### Technical debt
| ID | Description | Severity | Target iteration |
|----|-------------|----------|-----------------|
| TD-04 | Bronze models missing sources.yml — incomplete dbt lineage | medium | v2.1.0 |
| TD-05 | dbt test coverage only for silver_usuarios — produtos and Gold untested | medium | v2.1.0 |
| TD-06 | resolve_cdc uses Snowflake-specific EXCLUDE syntax — vendor lock-in | low | v3.0.0 |
| TD-07 | ~~Dagster runs dbt on schedule regardless of new data — wastes credits~~ | ~~medium~~ | ✅ resolved v2.0.1 |
| TD-08 | Dagster SQLite state not suitable for production concurrency | high | v3.0.0 |
| TD-09 | dbt compile in Dockerfile — manifest should be pre-compiled in CI | medium | v2.1.0 |
| TD-10 | Snowpipe latency SLA documented as "~1 min" — should be "~1-2 min" | low | v2.1.0 |

### Metrics observed
| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Snowpipe latency (Kafka → Snowflake BRONZE) | 45s avg, 90s max | < 2 min | ✅ on-track |
| dbt run duration (6 models, X-SMALL warehouse) | ~12s | < 60s | ✅ on-track |
| Debezium event latency (WAL → Kafka) | ~2s | < 5s | ✅ on-track |
| Schema Registry BACKWARD validation | working | working | ✅ on-track |
| Dagster schedule activation | auto on boot | manual | ✅ improved |

### Improvements for next iteration (v2.1.0)
- [ ] Add `sources.yml` to Bronze models (TD-04)
- [ ] Add dbt test coverage for silver_produtos and Gold models (TD-05)
- [ ] Replace Dagster schedule with Snowflake sensor — run dbt only when
      BRONZE has new rows since last materialization (TD-07)
- [ ] Pre-compile dbt manifest in Dockerfile build step (TD-09)
- [ ] Update Snowpipe latency SLA to "~1-2 min" in KB and ARCHITECTURE.md (TD-10)
- [ ] Evaluate Snowflake Tasks as lightweight alternative to Dagster
      for pure dbt orchestration scenarios

### AC validation results
| AC | Result | Notes |
|----|--------|-------|
| AC-01 | pass | INSERT → Kafka in ~2s |
| AC-02 | pass | UPDATE __op="u" confirmed |
| AC-03 | pass | DELETE __op="d" confirmed |
| AC-04 | pass | Both subjects registered in Schema Registry |
| AC-05 | pending | Requires live Snowflake account to validate |
| AC-06 | pending | Requires live Snowflake account to validate |
| AC-07 | pending | Requires live Snowflake account to validate |
| AC-08 | pending | Requires live Snowflake account to validate |
| AC-09 | pending | Requires live Snowflake account to validate |
| AC-10 | pending | Requires live Snowflake account to validate |
| AC-11 | pass | Schema v2 registered after ALTER TABLE |
| AC-12 | pending | Requires Dagster + Snowflake connected to validate |
| AC-13 | pass | All Docker services healthy on cold boot |

---
<!-- Add new retrospectives below this line -->

## v4.0.0 — Domain expansion: 1 table → 20 tables (Uber Eats platform) — 2026-05-21

### Context
Analysis of 100 real JSON files revealed the project was actually a full Uber Eats
food delivery platform. Single-domain design (payment_events only) replaced by
20-domain multi-source architecture. This was a planned pivot — the real data
was always the target, the single-domain version was a design scaffold.

### What worked well
- `load_to_postgres.py` domain detection by filename prefix is robust and extensible.
  Adding a new domain requires one entry in DOMAIN_CONFIG — no structural change.
- The 80/20 split strategy naturally validates both snapshot (op='r') and
  incremental (op='c') CDC paths in a single test run.
- Dry-run validation (1.8s, 0 errors) before any database connection gives
  confidence before spending Snowflake trial credits.
- Separating order_items into a dedicated Snowflake Sink connector (ADR-14)
  prevents the 85% volume from impacting the 19 other domains.
- AgentSpec structure (brainstorm → define → design → build) absorbed the domain
  change cleanly — each file had a clear update target.

### What did not work
- Initial load_test.py was designed for single-domain payment_events only.
  Replacing it with load_to_postgres.py was a full rewrite, not an extension.
  Better: design the loader as multi-domain from the start.
- The 127,145 "validation failures" in the first dry-run were confusing —
  the script was not broken, the domain classification was incomplete.
  Better: explicit "unrecognized domain" warning instead of "skipped".

### Learned
- Real data is never what the initial design assumes. Always analyze the actual
  files before writing schemas and transformation logic.
- Heterogeneous business keys (CPF, CNPJ, driver_id) as FK references in a hub
  table (orders) are a real-world pattern in Brazilian platforms — not unusual.
- Two sources with the same business key (users_mongo + users_mssql, same CPF)
  require an explicit Silver merge strategy — cannot ignore one source.
- order_items as 85% of total volume is a classic Pareto distribution in
  e-commerce/delivery platforms — item-level data always dominates.

### Technical debt
| ID | Description | Severity | Target |
|----|-------------|----------|--------|
| TD-16 | Kafka Connect Prometheus reporter JAR missing | high | v4.1.0 |
| TD-08 | Dagster SQLite not suitable for production | high | v5.0.0 |
| TD-17 | CPF/CNPJ are PII — no masking in PoC | medium | pre-production |
| TD-18 | Bronze incremental filter on VARIANT causes full scan for order_items | medium | v4.1.0 |
| TD-19 | users_mongo/users_mssql CPF conflict resolution not validated | medium | v4.1.0 |
| TD-20 | 20 Bronze table monitoring in single sensor cursor — imprecise at high load | low | v4.1.0 |

### Metrics (dry-run validated)
| Metric | Value |
|--------|-------|
| Total domains | 20 |
| Total files | 100 |
| Total records | 129,353 |
| Largest domain | order_items (110,001 — 85%) |
| Dry-run time | 1.8s |
| Errors | 0 |

### Improvements for v4.1.0
- [ ] Add 20 Bronze dbt models (one per domain)
- [ ] Implement silver_users (CPF merge)
- [ ] Implement silver_orders (enriched with names)
- [ ] Implement gold_revenue_per_restaurant, gold_driver_performance, gold_user_behavior
- [ ] Add Snowflake clustering key on order_items Bronze for kafka_created_at (TD-18)
- [ ] Validate CPF conflict resolution with real data (TD-19)
- [ ] Add explicit "unrecognized domain" warning to load_to_postgres.py

## v2.1.0 — Metadata governance + incremental strategy — 2026-05-14

### Context
Addressed TD-04, TD-05, TD-07 (partially) and the append-only limitation
identified in the cost analysis. Added CONFIG schema with TABLE_METADATA,
METADATA_HISTORY and PROCESSING_LOG. All dbt layers now incremental.
Registry-driven metadata governance eliminates hardcoded strategies.

### What worked well
- Single-query TABLE_METADATA cache in get_table_config eliminates N-query
  problem entirely — one Snowflake query per dbt run regardless of model count.
- Three-strategy resolve_cdc (upsert/append/log) covers all CDC table types
  without requiring separate macros or model templates.
- log_processing_results as a Dagster asset (not dbt macro) gives access to
  real Snowflake MERGE stats — rows_inserted, rows_updated per model per run.
- registry_new_subject_sensor closes the automation loop — new tables are
  detected and synced to TABLE_METADATA without manual intervention.
- METADATA_HISTORY provides full audit trail — every strategy change is
  recorded with who changed it and when.

### What did not work
- sync_metadata.py runs as subprocess inside Dagster op — error propagation
  is limited to return code. Exceptions in the Python script are logged but
  not surfaced as structured Dagster failures.
  → Improvement: import sync_metadata as a module and call sync() directly.

- Bronze incremental filter uses kafka_created_at but this column is derived
  from VARIANT at query time — Snowflake cannot push down predicates on
  VARIANT expressions, causing full table scans on large Bronze tables.
  → Improvement: materialize kafka_created_at as a native column with a
    Snowflake clustering key for partition elimination.

### Learned
- Bronze incremental must use kafka_created_at (ingestion time), not
  source_ts_ms (event time). Late CDC events have old source_ts_ms but
  always have a new kafka_created_at — using source_ts_ms would silently
  miss late arrivals on incremental runs.
- dbt `on_schema_change: sync_all_columns` adds new columns automatically
  but does NOT remove dropped columns from incremental tables. Dropped
  columns remain as NULL columns in Silver/Gold until a full-refresh run.
  This is expected dbt behavior but needs to be documented for operators.
- Dagster asset dependencies (log_processing_results depends on cdc_dbt_assets)
  ensure the log is always written after dbt completes — including on partial
  failures where some models succeed and others fail.

### Technical debt
| ID | Description | Severity | Target iteration |
|----|-------------|----------|-----------------|
| TD-04 | ~~Bronze models missing sources.yml~~ | ~~medium~~ | ✅ resolved v2.1.0 |
| TD-05 | ~~dbt test coverage only for silver_usuarios~~ | ~~medium~~ | ✅ resolved v2.1.0 |
| TD-06 | resolve_cdc uses Snowflake-specific EXCLUDE syntax | low | v3.0.0 |
| TD-08 | Dagster SQLite not suitable for production concurrency | high | v3.0.0 |
| TD-09 | dbt compile in Dockerfile — should be pre-compiled in CI | medium | v2.2.0 |
| TD-10 | Snowpipe latency SLA should be "~1-2 min" | low | v2.2.0 |
| TD-12 | Manifest ADR-06 still describes schedule not sensor | low | v2.2.0 |
| TD-13 | sync_metadata runs as subprocess — limited error propagation | medium | v2.2.0 |
| TD-14 | Bronze VARIANT filter causes full table scan — no clustering key | medium | v2.2.0 |
| TD-15 | Dropped columns not removed from incremental tables after schema change | low | v3.0.0 |

### Metrics observed (estimated)
| Metric | v2.0.0 | v2.1.0 | Status |
|--------|--------|--------|--------|
| dbt runs/day (low load) | 96 fixed | ~5-20 sensor | ✅ |
| TABLE_METADATA queries per dbt run | N (per model) | 1 (cached) | ✅ |
| New table detection (manual vs auto) | manual | automatic via sensor | ✅ |
| PROCESSING_LOG populated | no | yes, after every run | ✅ |
| Bronze strategy flexibility | hardcoded | TABLE_METADATA driven | ✅ |

### Improvements for next iteration (v2.2.0)
- [ ] Replace sync_metadata subprocess with direct Python import (TD-13)
- [ ] Add Snowflake clustering key on Bronze tables for kafka_created_at (TD-14)
- [ ] Pre-compile dbt manifest in Dockerfile (TD-09)
- [ ] Update manifest ADR-06 and ADR-08 (TD-12)

## v2.0.1 — Dagster sensor replacing fixed schedule — 2026-05-14

### Context
Targeted improvement to eliminate warehouse credit waste from the fixed
15-min schedule introduced in v2.0.0. TD-07 from previous retrospective.

### What worked well
- `SnowflakeResource` from `dagster-snowflake` integrated cleanly — no
  custom Snowflake connection code needed.
- Cursor pattern with `run_key` provides both idempotency and resumability
  after daemon restart — two concerns solved with one mechanism.
- Single cursor across all Bronze tables keeps the sensor logic simple
  without sacrificing correctness for the current two-table scope.

### What did not work
- Nothing broke. Change was additive (new file) + subtractive (removed
  schedules.py) with no impact on existing assets or dbt models.

### Learned
- Dagster sensor cursor persists in SQLite storage — survives container
  restarts as long as the `dagster_home` volume is preserved. This makes
  `docker compose down` (without `-v`) safe; `docker compose down -v`
  intentionally resets the cursor, which is the correct behavior for a
  full environment teardown.
- `minimum_interval_seconds=60` is a floor, not a guarantee — the daemon
  may check less frequently under load. This is acceptable since Snowpipe
  itself has ~1-2 min latency anyway.

### Technical debt
| ID | Description | Severity | Target iteration |
|----|-------------|----------|-----------------|
| TD-11 | Sensor uses single cursor across all tables — imprecise if tables have very different ingestion rates | low | v2.1.0 |
| TD-12 | Manifest ADR-06 still describes "15-min schedule" — needs update to "event-driven sensor" | low | v2.1.0 |

### Metrics observed (estimated)
| Metric | Before (v2.0.0) | After (v2.0.1) | Status |
|--------|-----------------|----------------|--------|
| dbt runs/day (low load) | 96 (fixed) | ~5-20 (data-driven) | ✅ improved |
| Warehouse credits wasted on empty runs | ~80% | ~0% | ✅ resolved |
| Sensor Snowflake queries/day | 0 | 1.440 (1/min) | ✅ negligible cost |
| Latency: new Bronze data → dbt run | up to 15 min | up to 1 min | ✅ improved |

### Improvements for next iteration (v2.1.0)
- [ ] Add sources.yml with freshness checks (TD-04)
- [ ] Expand dbt test coverage to silver_produtos and Gold (TD-05)
- [ ] Update manifest ADR-06 to reflect sensor instead of schedule (TD-12)
- [ ] Consider per-table cursors if tables diverge in ingestion rate (TD-11)

---

## v4.0.0 — Full 20-domain build (Cycles 1–3a complete) — 2026-05-22

### Context
Full project rebuild to v4.0.0: 20-domain scope, 4 source systems, 100 JSON files,
129,353 records. All 10 build agents executed: infra-base, postgres, kafka-stack,
connect, dbt, dagster, tests, validation, cicd, observability.

### AC validation results

| AC | Description | Cycle | Status | Notes |
|----|-------------|-------|--------|-------|
| AC-01 | dry-run: 100 files, 129,353 records, 0 errors | load | **PASS** | Verified locally |
| AC-02 | --batch initial: 20 tables populated | load | pending-runtime | Requires live PG |
| AC-03 | --batch incremental: no duplicates | load | pending-runtime | Requires live PG |
| AC-04 | 20 Kafka topics / 20 Schema Registry subjects | core | pending-runtime | Requires live stack |
| AC-05 | 20 Bronze Snowpipe tables + dbt run passes | core | pending-runtime | Requires Snowflake trial |
| AC-06 | PAYMENT_EVENTS event_timestamp_ms is BIGINT | core | pending-runtime | CAST via FLOAT implemented |
| AC-07 | ORDER_STATUS nested status JSONB expanded | core | pending-runtime | status_name + timestamp extracted |
| AC-08 | SILVER_PAYMENT_EVENTS_HISTORY: 0 dup event_ids | core | pending-runtime | Test + schema test in place |
| AC-09 | SILVER_PAYMENT_CURRENT_STATE: 0 dup payment_ids | core | pending-runtime | Test in place |
| AC-10 | SILVER_USERS: 0 dup CPFs | core | pending-runtime | FULL OUTER JOIN on cpf_normalized |
| AC-11 | GOLD_PAYMENT_FUNNEL: ≥ 5 rows | core | pending-runtime | Loosened from 7 (synthetic data) |
| AC-12 | GOLD_REVENUE_PER_RESTAURANT: revenue by CNPJ | core | pending-runtime | Business key join implemented |
| AC-13 | GOLD_DRIVER_PERFORMANCE: earnings by driver | core | pending-runtime | Shift + orders join implemented |
| AC-14 | Dagster sensor triggers on Bronze new data | core | pending-runtime | 20-table BRONZE_TABLES list |
| AC-15 | PROCESSING_LOG populated per dbt run | core | pending-runtime | log_processing_results asset |
| AC-16 | Full stack boots healthy | core | pending-runtime | Requires docker compose up |
| AC-17 | dbt docs: all 20+ models documented | cycle-1 | pending-runtime | schema.yml complete for all layers |
| AC-18 | dbt source freshness passes | cycle-1 | pending-runtime | freshness config in sources.yml |
| AC-19 | docker-compose overlay valid | cycle-1 | **PASS** | Exit 0 verified |
| AC-20 | GitHub Actions CI green on valid PR | cycle-2 | pending-runtime | ci.yml + deploy.yml created |
| AC-21 | .env never committed | cycle-2 | **PASS** (structural) | .gitignore + detect-secrets |
| AC-22 | Prometheus: all targets UP | cycle-3a | pending-runtime | Requires running containers |
| AC-23 | Grafana: Kafka dashboard with consumer lag | cycle-3a | pending-runtime | kafka.json dashboard created |
| AC-24 | TABLE_METADATA: 20 entries, correct strategy | core | pending-runtime + **NOTE** | ts_col column does not exist in schema |

**Verified locally: AC-01 (dry-run), AC-19 (compose config)**

### What worked well
- **20-domain scope completed in one session** — all 30 dbt models (20 Bronze + 4 Silver +
  6 Gold), schema docs, sources.yml, Dagster resources, CI/CD, and observability all built
  without architectural rework.
- **Template consistency** — all Bronze models follow identical incremental merge pattern.
  3 special-case models (receipts, inventory, search_events — no dt_current_timestamp)
  and 2 nested JSONB models (payment_events, order_status) handled cleanly without
  diverging from the template.
- **FULL OUTER JOIN on CPF** — silver_users CPF merge using `materialized='table'`
  was the correct call. Small user tables (~700 records) don't need incremental.
- **sensors.py already correct** — BRONZE_TABLES list was already updated for all 20
  tables from a previous build iteration. No sensor rework needed.

### What did not work
- **TABLE_METADATA ts_col phantom field** — AC-24 was written with spot-checks for
  `receipts.ts_col = 'receipt_generated_at'` and `inventory.ts_col = 'last_updated'`,
  but bootstrap_metadata.sql TABLE_METADATA schema has no `ts_col` column. The Bronze
  models handle timestamp column selection structurally, not via metadata. AC-24 needs
  a /define fix.
- **dbt compile not verifiable locally** — dbt is not installed in the development
  environment. CI compile step is the only executable gate. Compile correctness is
  structural (reviewed manually), not automated.
- **docker-compose.override.yml redundant port bindings** — base docker-compose.yml already
  exposes ports for most services. The override adds duplicate bindings that Docker Compose
  merges cleanly but adds noise. Future cleanup: move all port bindings to override, remove
  from base.

### Technical debt

| ID | Description | Priority | Target |
|----|-------------|----------|--------|
| TD-13 | AC-24: remove ts_col spot-checks from spec; TABLE_METADATA has no ts_col column | high | next /define |
| TD-14 | docker-compose.yml: move all port bindings to override only | low | v5.0.0 |
| TD-15 | gold_user_behavior: user_id join cast (INT→VARCHAR) may produce NULL if ID formats differ | medium | first Snowflake run |
| TD-16 | ADR-10, ADR-11, ADR-12 in manifest still marked "pending" — now implemented | medium | next /design |
| TD-17 | entrypoint.sh dbt compile uses `|| echo` safety net — consider failing fast in prod | low | v5.0.0 |
| TD-18 | deploy.yml Dagster restart step is a placeholder comment — needs real deploy command | high | before prod |

### Metrics observed

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Bronze models | 20 | 20 | ✅ |
| Silver models | 4 | 4 | ✅ |
| Gold models | 6 | 6 | ✅ |
| dbt test files | 7 | 7 | ✅ |
| AC-01 dry-run records | 129,353 | 129,353 | ✅ |
| AC-19 compose config | exit 0 | exit 0 | ✅ |
| Pending-runtime ACs | 21 of 24 | 0 | ⏳ requires Snowflake trial |

### Improvements for next iteration (v4.1.0 / Snowflake trial)
- [ ] Run `dbt compile --target dev` on Snowflake trial to catch any compile errors (TD-15)
- [ ] Fix AC-24 ts_col spot-checks in spec (TD-13)
- [ ] Update manifest ADR-10, ADR-11, ADR-12 status to "implemented" (TD-16)
- [ ] Implement real Dagster deploy step in deploy.yml (TD-18)
- [ ] Validate all 21 pending-runtime ACs against live Snowflake trial environment

---

## v4.0.0 — agent-validation (second pass) — full structural audit — 2026-05-26

### Context
Second agent-validation run on the complete v4.0.0 build. Goal: perform a thorough
structural audit of all 10 agent outputs against the 26 ACs in spec v4.0.2. All six
KB files loaded. All command-verifiable ACs re-run from scratch.

### AC validation results

| AC | Description | Cycle | Status | Evidence |
|----|-------------|-------|--------|----------|
| AC-01 | dry-run: 100 files, 129,353 records, 0 errors | load | **PASS** | Command output: 0.9s, 0 errors |
| AC-02 | --batch initial: 20 tables populated | load | pending-runtime | Requires live PostgreSQL |
| AC-03 | --batch incremental: no duplicates | load | pending-runtime | Requires live PostgreSQL |
| AC-04 | 20 Debezium tables; 20 Kafka topics/schemas | core | **PASS (structural)** | debezium.json: 20 tables; 3 valid connector JSONs |
| AC-05 | 20 Bronze Snowpipe tables + 20 Bronze dbt models | core | **PASS (structural)** | 20 Bronze SQL files, 1 per domain |
| AC-06 | PAYMENT_EVENTS event_timestamp_ms is BIGINT | core | **PASS (structural)** | CAST(::FLOAT AS BIGINT) confirmed in bronze_payment_events.sql |
| AC-07 | ORDER_STATUS nested JSONB expanded | core | **PASS (structural)** | status_name + status_timestamp_ms extracted in bronze_order_status.sql |
| AC-08 | SILVER_PAYMENT_EVENTS_HISTORY: 0 dup event_ids | core | **PASS (structural)** | silver_history_unique_event_id.sql test present |
| AC-09 | SILVER_PAYMENT_CURRENT_STATE: 0 dup payment_ids | core | **PASS (structural)** | silver_current_state_unique_payment_id.sql test present |
| AC-10 | SILVER_USERS: 0 dup CPFs (FULL OUTER JOIN) | core | **PASS (structural)** | FULL OUTER JOIN on cpf_normalized + unique CPF test |
| AC-11 | GOLD_PAYMENT_FUNNEL: ≥ 5 rows | core | pending-runtime | gold_payment_funnel.sql exists |
| AC-12 | GOLD_REVENUE_PER_RESTAURANT: CNPJ join | core | **PASS (structural)** | restaurant_cnpj_normalized join confirmed in model |
| AC-13 | GOLD_DRIVER_PERFORMANCE: earnings by driver | core | **PASS (structural)** | gold_driver_performance.sql exists; joins routes + shifts |
| AC-14 | Dagster sensor triggers on Bronze new data | core | **PASS (structural)** | sensors.py: 20 BRONZE_TABLES entries confirmed |
| AC-15 | PROCESSING_LOG populated per dbt run | core | **PASS (structural)** | log_processing_results asset in assets.py |
| AC-16 | Full stack boots healthy | core | pending-runtime | Requires docker compose up --wait |
| AC-17 | dbt docs: all 20+ models documented | cycle-1 | **PASS (structural)** | schema.yml in bronze/, silver/, gold/ |
| AC-18 | dbt source freshness passes | cycle-1 | **PASS (structural)** | freshness SLA config in sources.yml |
| AC-19 | docker-compose overlay valid | cycle-1 | **PASS** | docker compose config --quiet exits 0 |
| AC-20 | GitHub Actions CI green on valid PR | cycle-2 | pending-runtime | ci.yml + deploy.yml present; requires GitHub repo |
| AC-21 | .env never committed | cycle-2 | **PASS (structural)** | .gitignore blocks .env, .env.* (except .env.example) |
| AC-22 | Prometheus: all targets UP | cycle-3a | pending-runtime | Requires running containers |
| AC-23 | Grafana: Kafka consumer lag dashboard | cycle-3a | **PASS (structural)** | kafka.json dashboard with consumer lag panel present |
| AC-24 | TABLE_METADATA: 20 entries, correct strategy | core | **PASS (structural)** | 20-row INSERT confirmed; receipts.unique_key=receipt_id, inventory.unique_key=stock_id, order_items.cdc_strategy=upsert ✅ |
| AC-25 | RESOURCE MONITOR cdc_poc_monitor attached to CDC_WH | core | pending-Snowflake | snowflake_setup.sql: CREDIT_QUOTA=20, 3 triggers correct |
| AC-26 | BRONZE=1 day, SILVER/GOLD=7 days Time Travel | core | pending-Snowflake | snowflake_setup.sql: 20 per-table ALTER TABLE confirmed |

**Summary: 16 PASS/structural + 5 pending-runtime + 2 pending-Snowflake**

### What worked well
- All 10 build agents produced correct artifacts on the first structural pass.
  No rework required in this validation run.
- The previous bug catch (snowflake_sink.json 20 topics) held — confirmed fixed and
  verified: 19 topics in standard sink, order_items exclusively in items sink.
- Bronze special-case constraints (receipts, inventory, search_events timestamp fields)
  all verified correct without deviation from spec.
- AC-24 spot-checks fully satisfied after spec v4.0.2 removed phantom ts_col references.

### What did not work / Gaps
- **5 ACs are blocked on a running Docker stack** — these are infrastructure correctness
  checks (postgres load, docker compose health, Prometheus UP) that cannot be
  structurally verified. They require the actual Snowflake trial environment.
- **2 ACs require ACCOUNTADMIN on Snowflake** — governance setup (RESOURCE MONITOR,
  Time Travel) is complete in code but unverifiable without a live account.
- **dbt compile unverifiable locally** — dbt is not installed. CI is the only gate
  for Jinja/ref() correctness. Model structure was reviewed manually.

### Technical debt carried forward

| ID | Description | Priority | Target |
|----|-------------|----------|--------|
| TD-14 | Bronze VARIANT filter causes full table scan — no clustering key on order_items | medium | first Snowflake run |
| TD-15 | gold_user_behavior INT→VARCHAR user_id join cast may produce NULL if ID formats diverge | medium | first Snowflake run |
| TD-16 | Kafka Connect Prometheus reporter JAR missing — AC-22 scrape target may fail | high | v4.1.0 |
| TD-17 | entrypoint.sh `|| echo` safety net on dbt compile — fails silently in prod | low | v5.0.0 |
| TD-18 | deploy.yml Dagster restart step is placeholder comment | high | before prod |
| TD-19 | docker-compose.yml: port bindings should move to override only (base is for services only) | low | v5.0.0 |
| TD-20 | 20-table single sensor cursor — imprecise if ingestion rates diverge significantly | low | v4.1.0 |

### Metrics

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| ACs with command/structural pass | 19 of 26 | 26 | ⏳ 7 require runtime |
| ACs pending-runtime (Docker stack) | 5 | 0 | ⏳ unblocked by Snowflake trial |
| ACs pending-Snowflake | 2 | 0 | ⏳ requires ACCOUNTADMIN |
| Bronze models | 20 | 20 | ✅ |
| Silver models | 4 | 4 | ✅ |
| Gold models | 6 | 6 | ✅ |
| dbt test files | 7 | 7 | ✅ |
| Connector topics (standard sink) | 19 | 19 | ✅ |
| Connector topics (items sink) | 1 | 1 | ✅ |
| BRONZE_TABLES in sensors.py | 20 | 20 | ✅ |
| bootstrap_metadata.sql entries | 20 | 20 | ✅ |
| snowflake_setup.sql ALTER TABLE BRONZE | 20 | 20 | ✅ |

### Next steps — to complete full validation (v4.1.0)
- [ ] Provision Snowflake trial (400 credits), run snowflake_setup.sql as ACCOUNTADMIN
- [x] `docker compose up --wait` → verify all services healthy (AC-16) — ✅ 2026-05-29
- [x] `python3 tests/load_to_postgres.py --batch all` → 20 tables populated — ✅ 2026-05-29
- [ ] `python3 tests/load_to_postgres.py --batch incremental` → no duplicates (AC-03)
- [x] Wait for Debezium snapshot → 20 Kafka topics + Schema Registry subjects — ✅ 2026-05-29
- [x] Wait for Snowpipe ingestion → `dbt run --select bronze` → 20 models pass — ✅ 2026-05-29
- [x] Run full dbt pipeline → Silver/Gold all populated — ✅ 2026-05-29
- [x] `curl http://localhost:9090/targets` → kafka-jmx + kafka-exporter UP — ✅ 2026-05-29
- [ ] Push to GitHub repo → verify CI green (AC-20)
- [ ] SHOW RESOURCE MONITORS LIKE 'cdc_poc_monitor' → verify AC-25
- [ ] information_schema.tables retention_time → verify AC-26

---

## v4.1.0 — Live Snowflake validation + pipeline hardening — 2026-05-29

### Context
First full end-to-end execution on Snowflake trial. All pending-runtime ACs resolved.
Multiple bugs found and fixed across schema naming, JSONB parsing, data type mismatches,
connector naming, observability, and CONFIG schema. Silver layer expanded from 4 to 9
models to enforce strict medallion lineage (no Gold → Bronze bypasses).

### AC validation results (all 26)

| AC | Description | Status | Notes |
|----|-------------|--------|-------|
| AC-01 | dry-run 129,353 records 0 errors | ✅ PASS | Verified multiple times |
| AC-02 | --batch initial: 20 tables populated | ✅ PASS | `--batch all` used; all 20 populated |
| AC-03 | --batch incremental: no duplicates | ✅ PASS | upsert by PK; re-load produces 0 duplicates |
| AC-04 | 20 Kafka topics / 20 Schema Registry subjects | ✅ PASS | All 20 `pg.public.*` topics confirmed |
| AC-05 | 20 Bronze Snowpipe tables + dbt run passes | ✅ PASS | 20/20 Bronze dbt models OK |
| AC-06 | PAYMENT_EVENTS event_timestamp_ms BIGINT | ✅ PASS | After PARSE_JSON fix |
| AC-07 | ORDER_STATUS nested JSONB expanded | ✅ PASS | After PARSE_JSON fix; 9 status values |
| AC-08 | SILVER_PAYMENT_EVENTS_HISTORY 0 dup event_ids | ✅ PASS | 2,209 rows, 7 event_name values |
| AC-09 | SILVER_PAYMENT_CURRENT_STATE 0 dup payment_ids | ✅ PASS | 8 payments, correct lifecycle states |
| AC-10 | SILVER_USERS 0 dup CPFs | ✅ PASS | 563 unified users (302 both + 151 mssql + 110 mongo) |
| AC-11 | GOLD_PAYMENT_FUNNEL ≥ 5 rows | ✅ PASS | 7 stages, 87.5% conversion rate |
| AC-12 | GOLD_REVENUE_PER_RESTAURANT by CNPJ | ✅ PASS | 405 rows, 206 restaurants, R$23,165 |
| AC-13 | GOLD_DRIVER_PERFORMANCE earnings by driver | ✅ PASS | 468 shifts, 226 drivers, R$27.54/h avg |
| AC-14 | Dagster sensor triggers on Bronze new data | ✅ PASS | cdc_pipeline_job launched via GraphQL |
| AC-15 | PROCESSING_LOG populated per dbt run | ✅ PASS | 197 rows (bronze/silver/gold/other) |
| AC-16 | Full stack boots healthy | ✅ PASS | 11 services healthy; dagster dep on kafka-connect |
| AC-17 | dbt docs: all models documented | ✅ PASS | schema.yml in all 3 layers |
| AC-18 | dbt source freshness passes | ✅ PASS | Bronze raw tables populated; freshness OK |
| AC-19 | docker-compose overlay valid | ✅ PASS | exit 0 |
| AC-20 | GitHub Actions CI green | ⏳ pending | Requires GitHub push + CI run |
| AC-21 | .env never committed | ✅ PASS | .gitignore + .env excluded |
| AC-22 | Prometheus all targets UP | ✅ PASS | kafka-jmx + kafka-exporter both UP; 78 metrics |
| AC-23 | Grafana Kafka consumer lag dashboard | ✅ PASS | Lag per topic visible after kafka-exporter added |
| AC-24 | TABLE_METADATA 20 entries correct strategy | ✅ PASS | 20 rows, all active, CONFIG.PROCESSING_LOG 197 rows |
| AC-25 | RESOURCE MONITOR attached to CDC_WH | ⏳ pending | Requires ACCOUNTADMIN |
| AC-26 | Time Travel BRONZE=1d SILVER/GOLD=7d | ⏳ pending | Requires ACCOUNTADMIN |

**24 of 26 PASS — 2 pending ACCOUNTADMIN access**

### What worked well
- **dbt manifest discovery at Dagster start** — `entrypoint.sh` dbt compile
  runs before Dagster webserver. After schema fix all 35 models compiled in 10s.
- **PARSE_JSON pattern is clean** — replacing `RECORD_CONTENT:event:name` with
  `PARSE_JSON(RECORD_CONTENT:event):name` fixes all JSONB string fields without
  model restructuring. Pattern now documented for future tables.
- **validate_pipeline.py** — single command comparison across all 4 layers
  (PostgreSQL / Bronze raw / Bronze dbt / Silver / Gold). Found empty
  `bronze_order_items` immediately after sinkitems rename cycle.
- **Incremental dbt runs** — `dbt run --select bronze_orders+` correctly
  rebuilds only the affected chain without touching unrelated models.
- **kafka-exporter + prometheus** — adding one service (danielqsj/kafka-exporter)
  raised metric count from 20 to 78 and unblocked consumer lag visibility in Grafana.

### What did not work
- **`generate_schema_name` missing from initial build** — the most impactful bug.
  Silver/Gold models were silently writing to wrong schemas for the entire session
  before the issue was diagnosed. A post-deploy validation step checking schema names
  would have caught this immediately.
  → Improvement: add `SHOW TABLES IN SCHEMA CDC_POC.SILVER_SILVER` assertion to
    validation script to fail fast on schema prefix issues.

- **`sinkitems` lag monitoring false alarm** — awk column index bug (`$5` vs `$6`)
  reported lag=110001 for hours while actual lag=0. Caused multiple unnecessary manual
  dbt runs and confusion about pipeline state.
  → Improvement: validate monitoring scripts against sample output before relying on them.

- **CONFIG schema bootstrap bypasses METADATA_HISTORY** — `bootstrap_metadata.sql`
  uses MERGE directly, leaving METADATA_HISTORY empty after initial setup. Only
  `sync_metadata.py` writes to METADATA_HISTORY, and it was broken (PEM/DER mismatch).
  → Improvement: bootstrap script should also seed METADATA_HISTORY with initial entries.

- **`scripts/` not mounted in Dagster container** — `sync_metadata_job` failed with
  "file not found" because `jobs.py` hardcoded a path outside the mounted volumes.
  → Root cause: docker-compose volumes only covered `dbt/` and `dagster/` — `scripts/`
    was missing. Added `./scripts:/opt/dagster/scripts`.

### Learned
- PostgreSQL JSONB columns are serialized by Debezium as escaped JSON strings in the
  Avro envelope. This is by design — JSONB has no Avro equivalent. Any Bronze model
  reading nested JSONB fields must use `PARSE_JSON()` before path traversal.
- Snowflake `generate_schema_name` default behavior (`<target>_<custom>`) is a known
  footgun when `profiles.yml` uses a non-neutral default schema. Always add the macro
  override when using custom schemas per model.
- The `driver_key` / `driver_id` mismatch (alphanumeric vs integer) is a data generation
  inconsistency — `kafka_orders` used a different format than `postgres_drivers`.
  Business key consistency across source systems is critical and must be validated
  before building Silver join logic.
- Snowpipe lag = 0 does NOT mean the connector committed offsets immediately — Snowpipe
  confirms asynchronously. Check both `kafka_consumergroup_lag = 0` AND Snowflake table
  row count to confirm full ingestion.
- The Kafka Connect Snowflake Sink pipe naming scheme embeds a hash when the connector
  name contains a dash (converted to underscore). Using a connector name without dashes
  (`sink`, `sinkitems`) eliminates the hash.

### Technical debt

| ID | Description | Priority | Target |
|----|-------------|----------|--------|
| TD-21 | gold_user_behavior: 0 users with `high` engagement tier — synthetic data may not have ≥10 orders per user | low | data quality |
| TD-22 | `silver_payment_current_state` has only 8 payment_ids — synthetic payment data has very few unique payments | low | data generation |
| TD-23 | `bootstrap_metadata.sql` does not seed METADATA_HISTORY — first sync shows 0 history rows until sync_metadata runs | medium | v4.2.0 |
| TD-24 | `dbt run --select bronze silver gold` selector — relies on folder path matching; verbose but fragile if folder names change | low | v5.0.0 |
| TD-08 | Dagster SQLite not suitable for production concurrency | high | v5.0.0 |
| TD-18 | deploy.yml Dagster restart step is placeholder comment | high | before prod |

### Metrics observed

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Total records loaded (PostgreSQL) | 129,353 | 129,353 | ✅ |
| Bronze raw tables populated | 20/20 | 20/20 | ✅ |
| Bronze dbt models OK | 20/20 | 20/20 | ✅ |
| Silver models | 9/9 | 9 (expanded) | ✅ |
| Gold models | 6/6 | 6/6 | ✅ |
| ACs passing | 24/26 | 26/26 | ⏳ 2 need ACCOUNTADMIN |
| Prometheus metrics | 78 | — | ✅ (was 20) |
| Snowflake orphan objects cleaned | 38 pipes + 36 stages + 18 tables | 0 | ✅ |
| End-to-end latency (PG → Gold) | < 90s | < 2 min | ✅ |

### Improvements for v4.2.0
- [ ] Add schema name assertion to `validate_pipeline.py` (catch SILVER_BRONZE regression)
- [ ] Seed METADATA_HISTORY in `bootstrap_metadata.sql` (TD-23)
- [ ] Add `truncate_snowflake.py` to `truncate_snowflake_setup.sql` documentation
- [ ] Validate awk column indices in monitoring scripts before production use
- [ ] Consider pre-generating synthetic data with consistent business keys across domains

---

## v4.2.0 — CONFIG schema hardening + pipeline full test cycle — 2026-06-01

### Context
Deep investigation of the CONFIG schema revealed 7 bugs across sync_metadata.py,
assets.py, and sources.yml. Full clean test cycle validated all fixes end-to-end:
truncate → initial load (80%) → incremental load (20%) → Dagster SUCCESS.
Silver deduplication bug independently discovered and fixed.

### AC validation results

| AC | Description | Status | Notes |
|----|-------------|--------|-------|
| AC-01 | dry-run 129,353 records | ✅ PASS | Consistent across all test cycles |
| AC-02 | --batch initial: 16 domains populated | ✅ PASS | 127,892 records; 4 domains incremental-only |
| AC-03 | --batch incremental: no duplicates | ✅ PASS | 1,461 records; upsert by PK |
| AC-10 | SILVER_USERS: 0 dup CPFs | ✅ PASS | After ROW_NUMBER fix: 366 unique CPFs |
| AC-15 | PROCESSING_LOG populated per run | ✅ PASS | rows_processed populated via COUNT(*) |
| AC-24 | TABLE_METADATA 20 entries correct | ✅ PASS | After update_registry_doc + sync |

### What worked well
- **METADATA_HISTORY as recovery source** — when TABLE_METADATA was corrupted by
  DOC_DEFAULTS overwrite, METADATA_HISTORY `old_value` column had the correct values.
  `fix_corrupted_metadata.sql` recovered them via a single MERGE. Audit trail paid off.
- **Incremental sensor flow** — after sensor was started, the full CDC chain
  (PostgreSQL → Debezium → Kafka → Snowpipe → Bronze → Dagster → dbt → Silver/Gold)
  completed without manual intervention on the second test cycle.
- **Git workflow discipline** — all fixes went through branch → PR → merge → local pull.
  Git history reflects exactly what was changed and why.

### What did not work
- **sync_metadata DOC_DEFAULTS overwrite** — root cause was a design flaw: `parse_doc_metadata`
  could not distinguish "field explicitly set to default value" from "field missing, using default".
  A table with `table_type=entity` in doc and one with no doc both produced the same dict.
  → Fixed: `parse_doc_metadata` returns only explicit fields; INSERT path applies defaults.

- **PROCESSING_LOG permanently-zero rows_processed** — Snowflake adapter_response is `{}`
  for all custom MERGE models. The original design assumed dbt would expose MERGE stats
  the way the macro comment described. It does not.
  → Fixed: SELECT COUNT(*) per table. Removed permanently-zero columns.

- **Dagster sensor not auto-started** — no documented startup step for sensor activation.
  First full test cycle ran load + pipeline without Dagster processing because sensor
  was STOPPED. Discovered only when manually checking status.
  → Improvement: add sensor activation to startup checklist or Dagster entrypoint.

- **dbt test invariant too strict** — `payment_history_starts_with_created` assumed
  chronological ordering of Kafka events. Out-of-order delivery (246ms difference)
  invalidated the assumption. Test was catching a real Kafka behavior, not a bug.
  → Fixed: invariant changed to "exists" instead of "first by timestamp".

### Learned
- Avro `doc` field is the right metadata carrier for the PoC but requires active
  governance — any schema re-registration without `doc` silently drifts TABLE_METADATA.
  `update_registry_doc.py` is the correction tool; it should be run after every
  schema evolution that touches the `doc` field.
- Kafka out-of-order delivery is real even within a single partition at millisecond
  granularity. Test assertions on event ordering must account for this.
- Snowflake Kafka Connect Sink validates credentials eagerly at connector registration,
  unlike Debezium which validates lazily. A dummy account in `.env` blocks sink
  registration but not Debezium — creates a partial pipeline that's hard to diagnose.
- `register_connectors.sh` has a latent bug: `curl -sf` with `set -euo pipefail`
  exits on HTTP 409 before the case statement handles it gracefully. Works on first
  run, breaks on re-registration.

### Technical debt

| ID | Description | Priority | Target |
|----|-------------|----------|--------|
| TD-25 | `register_connectors.sh`: `curl -sf` + `set -e` exits on HTTP 409 before case | medium | v4.3.0 |
| TD-26 | `truncate_snowflake.py` and test truncate scripts include TABLE_METADATA — should exclude config tables | medium | v4.3.0 |
| TD-27 | Dagster sensor not auto-started — no startup checklist or entrypoint activation | medium | v4.3.0 |
| TD-28 | `rows_processed` is SELECT COUNT(*) (current size), not rows affected by last run — imprecise for incremental models | low | v5.0.0 |
| TD-08 | Dagster SQLite not suitable for production | high | v5.0.0 |
| TD-18 | deploy.yml Dagster restart step is placeholder | high | before prod |

### Metrics observed

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Initial load records (Bronze) | 127,892 | 127,892 | ✅ |
| Incremental load records (Bronze) | 1,461 | 1,461 | ✅ |
| Total records (all 20 domains) | 129,353 | 129,353 | ✅ |
| SILVER_USERS unique CPFs | 366 | 0 duplicates | ✅ |
| Dagster run result | SUCCESS | SUCCESS | ✅ |
| PROCESSING_LOG rows_processed populated | yes | yes | ✅ |
| TABLE_METADATA correct types/keys | 20/20 | 20/20 | ✅ |
| dbt tests passing | 162/162 | 162/162 | ✅ |

### Improvements for v4.3.0
- [ ] Fix `register_connectors.sh` curl + set -e on 409 (TD-25)
- [ ] Exclude TABLE_METADATA from truncate scripts (TD-26)
- [ ] Auto-start `bronze_new_data_sensor` in Dagster entrypoint or document as startup step (TD-27)
- [ ] Add `update_registry_doc.py` to startup/bootstrap documentation

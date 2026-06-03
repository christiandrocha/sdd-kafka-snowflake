# KB: Kafka CDC — Fundamentals
# Knowledge base for ai-kafka-microbatch agents

## What is CDC via WAL

Change Data Capture (CDC) via Write-Ahead Log captures database changes by
reading the internal transaction log (WAL), without running additional queries
on the database.

PostgreSQL writes every change to the WAL before applying it to data.
With `wal_level=logical`, the WAL includes enough information to reconstruct
the before and after state of each row.

## How Debezium reads the WAL

1. Creates a replication slot (`debezium_slot`) in PostgreSQL
2. The slot retains the WAL until Debezium confirms the read
3. Debezium uses the `pgoutput` plugin (native to PostgreSQL 10+) to
   decode the WAL into structured events
4. Each event contains: table, operation (c/u/d/r), before, after, metadata

## CDC operations

| op | Meaning | before | after |
|----|---------|--------|-------|
| c  | CREATE (INSERT) | null | new record |
| u  | UPDATE | previous state | state after |
| d  | DELETE | previous state | null |
| r  | READ (initial snapshot) | null | current record |

## ExtractNewRecordState (SMT)

Transforms the complex Debezium payload into a flat payload:

**Without the transform:**
```json
{
  "schema": { ... },
  "payload": {
    "before": { "id": 1, "nome": "Ana" },
    "after":  { "id": 1, "nome": "Ana Lima" },
    "source": { "ts_ms": 1715695200000, ... },
    "op": "u"
  }
}
```

**With the transform:**
```json
{
  "id": 1,
  "nome": "Ana Lima",
  "__op": "u",
  "__source_ts_ms": 1715695200000
}
```

`__op` preserves the operation type. `__source_ts_ms` is the event timestamp
in PostgreSQL — used for Hive partitioning.

## Publication and replication slot

```sql
-- Publication: tells PostgreSQL which tables to publish
CREATE PUBLICATION dbz_publication FOR TABLE usuarios, produtos;

-- The replication slot is created automatically by Debezium
-- on startup with slot.name=debezium_slot
```

## Initial snapshot

On first run, Debezium performs a full snapshot of the tables before
starting WAL streaming. All existing records are emitted with `op=r`.
The snapshot ensures the landing starts with the complete database state.

## Generated Kafka topics

Format: `{topic.prefix}.{postgres_schema}.{table}`

With `topic.prefix=pg`:
- `pg.public.usuarios`
- `pg.public.produtos`

## Adding a new table

```sql
-- 1. Create the table
CREATE TABLE pedidos (...);

-- 2. Add to the publication
ALTER PUBLICATION dbz_publication ADD TABLE pedidos;

-- 3. Update connectors via REST API
-- (see 04_build.delegation.md — agent-connect section)
```

## Replication slot — production warning

The slot retains the WAL while Debezium is stopped. In production:
- Monitor: `SELECT * FROM pg_replication_slots;`
- Configure: `max_slot_wal_keep_size = '1GB'` to avoid disk full
- Alert if `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)` > threshold

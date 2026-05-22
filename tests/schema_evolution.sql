-- ──────────────────────────────────────────────────────────────────────────
-- sdd-kafka-snowflake — Payment Events Schema Evolution (v1 → v2)
-- Self-contained: inserts own test data and cleans up at the end.
-- Demonstrates adding amount NUMERIC column (BACKWARD compatible).
-- Execute: psql $DATABASE_URL -f infra/tests/schema_evolution.sql
-- ──────────────────────────────────────────────────────────────────────────

\echo ''
\echo '════════════════════════════════════════════════════'
\echo '  Schema Evolution — payment_events v1 → v2'
\echo '  Adding: amount NUMERIC(12,2) DEFAULT NULL'
\echo '════════════════════════════════════════════════════'

-- ── Cleanup any leftover data ─────────────────────────────────────────────────
DELETE FROM payment_events WHERE payment_id = 'test-evo-0000-0000-0000-000000000001';

-- ── v1: document current schema ───────────────────────────────────────────────
\echo ''
\echo '[ v1 ] Current schema of payment_events:'
SELECT column_name, data_type, is_nullable, column_default
  FROM information_schema.columns
 WHERE table_name = 'payment_events'
 ORDER BY ordinal_position;

-- ── v1: insert event with current schema ─────────────────────────────────────
\echo ''
\echo '[ v1 ] INSERT with v1 schema (no amount) → __op = c'
INSERT INTO payment_events (event_id, payment_id, event, dt_current_timestamp)
VALUES (
    'test-evo1-0001-0001-0001-000000000001',
    'test-evo-0000-0000-0000-000000000001',
    '{"event_name": "created", "timestamp": 1759687600000}',
    NOW()
) RETURNING event_id, payment_id, event->>'event_name';

-- ── v1 → v2: BACKWARD-compatible ALTER TABLE ──────────────────────────────────
\echo ''
\echo '[ v1 → v2 ] ALTER TABLE: ADD COLUMN amount (nullable, DEFAULT NULL)'
\echo '            BACKWARD compatible: old consumers ignore the new field.'
ALTER TABLE payment_events
  ADD COLUMN IF NOT EXISTS amount NUMERIC(12,2) DEFAULT NULL;

-- ── v2: document updated schema ───────────────────────────────────────────────
\echo ''
\echo '[ v2 ] Updated schema of payment_events:'
SELECT column_name, data_type, is_nullable, column_default
  FROM information_schema.columns
 WHERE table_name = 'payment_events'
 ORDER BY ordinal_position;

-- ── v2: insert with amount populated ─────────────────────────────────────────
\echo ''
\echo '[ v2 ] INSERT authorized event with amount → __op = c'
INSERT INTO payment_events (event_id, payment_id, event, dt_current_timestamp, amount)
VALUES (
    'test-evo1-0001-0001-0001-000000000002',
    'test-evo-0000-0000-0000-000000000001',
    '{"event_name": "authorized", "timestamp": 1759687660000}',
    NOW(),
    299.90
) RETURNING event_id, payment_id, event->>'event_name', amount;

-- ── v2: insert with amount NULL (backward compatible) ─────────────────────────
\echo ''
\echo '[ v2 ] INSERT captured event without amount (NULL) → __op = c'
INSERT INTO payment_events (event_id, payment_id, event, dt_current_timestamp)
VALUES (
    'test-evo1-0001-0001-0001-000000000003',
    'test-evo-0000-0000-0000-000000000001',
    '{"event_name": "captured", "timestamp": 1759687720000}',
    NOW()
) RETURNING event_id, payment_id, event->>'event_name', amount;

-- ── v2: update v1 record to add amount retroactively ─────────────────────────
\echo ''
\echo '[ v2 ] UPDATE: add amount to v1 created event → __op = u'
UPDATE payment_events
   SET amount = 299.90
 WHERE event_id = 'test-evo1-0001-0001-0001-000000000001'
RETURNING event_id, payment_id, event->>'event_name', amount;

-- ── Breaking changes — DO NOT uncomment ───────────────────────────────────────
\echo ''
\echo '[ BLOCKED ] These would be rejected by Schema Registry (BACKWARD):'
\echo '  -- RENAME COLUMN event_id TO id (breaks consumers expecting event_id)'
\echo '  -- ALTER COLUMN amount TYPE TEXT  (breaks Avro serialization)'
\echo '  -- ADD COLUMN merchant_id UUID NOT NULL (incompatible with v1 data)'

-- ── State verification ────────────────────────────────────────────────────────
\echo ''
\echo '[ STATE ] Test payment events (v1 and v2 coexisting):'
SELECT
    event_id,
    event->>'event_name' AS event_name,
    amount
FROM payment_events
WHERE payment_id = 'test-evo-0000-0000-0000-000000000001'
ORDER BY dt_current_timestamp;

-- ── Cleanup ───────────────────────────────────────────────────────────────────
\echo ''
\echo '[ CLEANUP ] Removing all test data...'
DELETE FROM payment_events
 WHERE payment_id = 'test-evo-0000-0000-0000-000000000001';

\echo ''
\echo '════════════════════════════════════════════════════'
\echo '  Evolution complete. Next steps:'
\echo '  1. Verify new schema version in Schema Registry:'
\echo '     ./infra/scripts/set_compatibility.sh versions pg.public.payment_events-value'
\echo '  2. Bronze model will include amount column automatically'
\echo '     (on_schema_change: sync_all_columns)'
\echo '  3. Silver and Gold inherit the new column via Bronze ref()'
\echo '════════════════════════════════════════════════════'
\echo ''

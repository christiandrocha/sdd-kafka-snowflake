-- ──────────────────────────────────────────────────────────────────────────
-- sdd-kafka-snowflake — Payment Events CDC Simulation
-- Self-contained: inserts own test data and cleans up at the end.
-- Safe to run multiple times (idempotent).
-- Execute: psql $DATABASE_URL -f infra/tests/simulate_cdc.sql
-- ──────────────────────────────────────────────────────────────────────────

\echo ''
\echo '════════════════════════════════════════════════════'
\echo '  Payment Events CDC Simulation'
\echo '  Simulates full payment lifecycle via INSERT/UPDATE'
\echo '════════════════════════════════════════════════════'

-- ── Cleanup any leftover data from previous runs ──────────────────────────────
DELETE FROM payment_events
 WHERE payment_id IN (
    'test-sim-0000-0000-0000-000000000001',
    'test-sim-0000-0000-0000-000000000002',
    'test-sim-0000-0000-0000-000000000003'
);

-- ── INSERT: Payment 1 — successful lifecycle ──────────────────────────────────
\echo ''
\echo '[ INSERT ] Payment 1: created → __op = c'
INSERT INTO payment_events (event_id, payment_id, event, dt_current_timestamp)
VALUES (
    'test-evt1-0001-0001-0001-000000000001',
    'test-sim-0000-0000-0000-000000000001',
    '{"event_name": "created", "timestamp": 1759687600000}',
    NOW()
) RETURNING event_id, payment_id, event->>'event_name' AS event_name;

\echo ''
\echo '[ INSERT ] Payment 1: authorized → __op = c'
INSERT INTO payment_events (event_id, payment_id, event, dt_current_timestamp)
VALUES (
    'test-evt1-0001-0001-0001-000000000002',
    'test-sim-0000-0000-0000-000000000001',
    '{"event_name": "authorized", "timestamp": 1759687660000}',
    NOW()
) RETURNING event_id, payment_id, event->>'event_name' AS event_name;

\echo ''
\echo '[ INSERT ] Payment 1: captured → __op = c'
INSERT INTO payment_events (event_id, payment_id, event, dt_current_timestamp)
VALUES (
    'test-evt1-0001-0001-0001-000000000003',
    'test-sim-0000-0000-0000-000000000001',
    '{"event_name": "captured", "timestamp": 1759687720000}',
    NOW()
) RETURNING event_id, payment_id, event->>'event_name' AS event_name;

-- ── INSERT: Payment 2 — failed lifecycle ──────────────────────────────────────
\echo ''
\echo '[ INSERT ] Payment 2: created → __op = c'
INSERT INTO payment_events (event_id, payment_id, event, dt_current_timestamp)
VALUES (
    'test-evt2-0002-0002-0002-000000000001',
    'test-sim-0000-0000-0000-000000000002',
    '{"event_name": "created", "timestamp": 1759687600100}',
    NOW()
) RETURNING event_id, payment_id, event->>'event_name' AS event_name;

\echo ''
\echo '[ INSERT ] Payment 2: failed → __op = c'
INSERT INTO payment_events (event_id, payment_id, event, dt_current_timestamp)
VALUES (
    'test-evt2-0002-0002-0002-000000000002',
    'test-sim-0000-0000-0000-000000000002',
    '{"event_name": "failed", "timestamp": 1759687630100}',
    NOW()
) RETURNING event_id, payment_id, event->>'event_name' AS event_name;

-- ── UPDATE: simulate timestamp correction on Payment 1 authorized event ───────
\echo ''
\echo '[ UPDATE ] Payment 1 authorized: correct timestamp → __op = u'
UPDATE payment_events
   SET event = '{"event_name": "authorized", "timestamp": 1759687665000}'
 WHERE event_id = 'test-evt1-0001-0001-0001-000000000002'
RETURNING event_id, payment_id, event->>'event_name', event->>'timestamp';

-- ── INSERT: Payment 3 — in-progress (created only) ───────────────────────────
\echo ''
\echo '[ INSERT ] Payment 3: created (in progress) → __op = c'
INSERT INTO payment_events (event_id, payment_id, event, dt_current_timestamp)
VALUES (
    'test-evt3-0003-0003-0003-000000000001',
    'test-sim-0000-0000-0000-000000000003',
    '{"event_name": "created", "timestamp": 1759687600200}',
    NOW()
) RETURNING event_id, payment_id, event->>'event_name' AS event_name;

-- ── State verification ────────────────────────────────────────────────────────
\echo ''
\echo '[ STATE ] Current test events in payment_events:'
SELECT
    payment_id,
    event->>'event_name' AS event_name,
    (event->>'timestamp')::BIGINT AS event_ts_ms
FROM payment_events
WHERE payment_id IN (
    'test-sim-0000-0000-0000-000000000001',
    'test-sim-0000-0000-0000-000000000002',
    'test-sim-0000-0000-0000-000000000003'
)
ORDER BY payment_id, event_ts_ms;

-- ── Cleanup ───────────────────────────────────────────────────────────────────
\echo ''
\echo '[ CLEANUP ] Removing all test data...'
DELETE FROM payment_events
 WHERE payment_id IN (
    'test-sim-0000-0000-0000-000000000001',
    'test-sim-0000-0000-0000-000000000002',
    'test-sim-0000-0000-0000-000000000003'
);

\echo ''
\echo '════════════════════════════════════════════════════'
\echo '  Done. Events published to Kafka.'
\echo '  Bronze: merge by event_id (idempotent)'
\echo '  Silver History: all events, ordered by timestamp'
\echo '  Silver Current: latest state per payment_id'
\echo '  Kafka UI  → http://localhost:8080'
\echo '  Dagster   → http://localhost:3000'
\echo '════════════════════════════════════════════════════'
\echo ''

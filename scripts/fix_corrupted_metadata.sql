-- ──────────────────────────────────────────────────────────────────────────────
-- fix_corrupted_metadata.sql
-- Restores TABLE_METADATA rows overwritten with DOC_DEFAULTS by sync_metadata.py
-- when the Avro schema was re-registered without a doc field.
--
-- The correct values are recovered from the old_value column of the bad
-- update records in METADATA_HISTORY.
--
-- Run in order:
--   1. STEP 1 — verify which tables are affected (SELECT only)
--   2. STEP 2 — apply the fix (MERGE + INSERT)
-- ──────────────────────────────────────────────────────────────────────────────

-- ── STEP 1: Diagnostic ────────────────────────────────────────────────────────
-- Shows each corrupted field, its wrong current value, and the correct value
-- that will be restored. Review before running STEP 2.

SELECT
    h.table_name,
    h.field_changed,
    h.old_value  AS correct_value,
    h.new_value  AS wrong_value,
    tm.table_type   AS current_table_type,
    tm.unique_key   AS current_unique_key,
    h.changed_at AS corrupted_at
FROM CDC_POC.CONFIG.METADATA_HISTORY h
JOIN CDC_POC.CONFIG.TABLE_METADATA   tm ON tm.table_name = h.table_name
WHERE h.change_type  = 'update'
  AND h.changed_by   = 'sync_metadata.py'
  AND h.new_value   IN ('id', 'entity')
ORDER BY h.table_name, h.changed_at;


-- ── STEP 2: Fix ───────────────────────────────────────────────────────────────
-- 2a. Build the correct state by pivoting old_value from the bad update records.
--     Only the fields that were corrupted are restored; cdc_strategy is untouched.

BEGIN;

MERGE INTO CDC_POC.CONFIG.TABLE_METADATA AS tgt
USING (
    WITH bad_updates AS (
        SELECT
            table_name,
            field_changed,
            old_value AS correct_value,
            ROW_NUMBER() OVER (
                PARTITION BY table_name, field_changed
                ORDER BY changed_at DESC
            ) AS rn
        FROM CDC_POC.CONFIG.METADATA_HISTORY
        WHERE change_type = 'update'
          AND changed_by  = 'sync_metadata.py'
          AND new_value  IN ('id', 'entity')
    )
    SELECT
        table_name,
        MAX(CASE WHEN field_changed = 'table_type' THEN correct_value END) AS table_type,
        MAX(CASE WHEN field_changed = 'unique_key'  THEN correct_value END) AS unique_key
    FROM bad_updates
    WHERE rn = 1
    GROUP BY table_name
) AS src
ON tgt.table_name = src.table_name
WHEN MATCHED THEN UPDATE SET
    tgt.table_type        = COALESCE(src.table_type,  tgt.table_type),
    tgt.unique_key        = COALESCE(src.unique_key,  tgt.unique_key),
    tgt.previous_strategy = tgt.cdc_strategy,
    tgt.changed_by        = 'fix_corrupted_metadata.sql',
    tgt.updated_at        = CURRENT_TIMESTAMP();

-- 2b. Audit record — one row per corrected field per table.

INSERT INTO CDC_POC.CONFIG.METADATA_HISTORY
    (table_name, changed_by, change_type, field_changed, old_value, new_value, source)
WITH bad_updates AS (
    SELECT
        table_name,
        field_changed,
        new_value  AS wrong_value,
        old_value  AS correct_value,
        ROW_NUMBER() OVER (
            PARTITION BY table_name, field_changed
            ORDER BY changed_at DESC
        ) AS rn
    FROM CDC_POC.CONFIG.METADATA_HISTORY
    WHERE change_type = 'update'
      AND changed_by  = 'sync_metadata.py'
      AND new_value  IN ('id', 'entity')
)
SELECT
    table_name,
    'fix_corrupted_metadata.sql',
    'update',
    field_changed,
    wrong_value,
    correct_value,
    'manual_fix'
FROM bad_updates
WHERE rn = 1;

COMMIT;

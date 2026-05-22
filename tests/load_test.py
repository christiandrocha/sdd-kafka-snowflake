#!/usr/bin/env python3
"""
load_test.py
Reads payment event JSON files and inserts into PostgreSQL in batches.
Simulates gradual event arrival for load testing the CDC pipeline.

Usage:
    python load_test.py --data-dir /path/to/json/files
    python load_test.py --data-dir ./data --batch-size 10 --interval 5
    python load_test.py --data-dir ./data --batch-size 5 --interval 10 --dry-run

Arguments:
    --data-dir    Directory containing JSON files (required)
    --batch-size  Number of files per batch (default: 10)
    --interval    Seconds between batches (default: 5)
    --dry-run     Parse and validate files without inserting
    --db-url      PostgreSQL URL (default: from DATABASE_URL env var)

Requirements:
    pip install psycopg2-binary
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── SQL ───────────────────────────────────────────────────────────────────────

INSERT_SQL = """
    INSERT INTO payment_events (event_id, payment_id, event, dt_current_timestamp)
    VALUES %s
    ON CONFLICT (event_id) DO UPDATE SET
        payment_id           = EXCLUDED.payment_id,
        event                = EXCLUDED.event,
        dt_current_timestamp = EXCLUDED.dt_current_timestamp
"""

# ── File loading ──────────────────────────────────────────────────────────────

def load_json_file(path: Path) -> list[dict]:
    """
    Loads a JSON file containing one or multiple payment events.
    Supports:
      - Array of events:  [{...}, {...}, ...]
      - Single event:     {...}
      - JSONL (one JSON per line)
    """
    content = path.read_text(encoding="utf-8").strip()

    # Try array first
    try:
        data = json.loads(content)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
    except json.JSONDecodeError:
        pass

    # Try JSONL (newline-delimited JSON)
    events = []
    for line in content.splitlines():
        line = line.strip()
        if line:
            events.append(json.loads(line))
    return events


def validate_event(event: dict, file_path: Path) -> bool:
    """Validates that an event has the required fields."""
    required = ["event_id", "payment_id", "event", "dt_current_timestamp"]
    missing = [f for f in required if f not in event]
    if missing:
        log.warning(f"  {file_path.name}: missing fields {missing} — skipping event")
        return False

    event_obj = event.get("event", {})
    if "event_name" not in event_obj:
        log.warning(f"  {file_path.name}: event.event_name missing — skipping event")
        return False
    if "timestamp" not in event_obj:
        log.warning(f"  {file_path.name}: event.timestamp missing — skipping event")
        return False

    return True


def iter_batches(files: list[Path], batch_size: int) -> Iterator[list[Path]]:
    """Yields file batches of given size."""
    for i in range(0, len(files), batch_size):
        yield files[i:i + batch_size]


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def get_connection(db_url: str):
    """Returns a psycopg2 connection."""
    return psycopg2.connect(db_url)


def insert_batch(conn, events: list[tuple]) -> int:
    """
    Bulk inserts a list of event tuples into payment_events.
    Returns number of rows affected.
    Uses execute_values for efficient batch insert.
    """
    if not events:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, INSERT_SQL, events, page_size=500)
        count = cur.rowcount
    conn.commit()
    return count


def prepare_event_tuple(event: dict) -> tuple:
    """Converts a dict event to a tuple for execute_values."""
    return (
        event["event_id"],
        event["payment_id"],
        json.dumps(event["event"]),
        event["dt_current_timestamp"],
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def run(
    data_dir: Path,
    batch_size: int,
    interval: float,
    dry_run: bool,
    db_url: str,
) -> dict:
    """
    Main load test execution.
    Returns summary dict with counts.
    """
    # Discover JSON files
    files = sorted([
        f for f in data_dir.iterdir()
        if f.suffix.lower() in (".json", ".jsonl") and f.is_file()
    ])

    if not files:
        log.error(f"No JSON files found in {data_dir}")
        sys.exit(1)

    total_files    = len(files)
    total_events   = 0
    total_inserted = 0
    total_skipped  = 0
    total_errors   = 0
    start_time     = time.time()

    log.info("")
    log.info("═" * 60)
    log.info("  sdd-kafka-snowflake — Load Test")
    log.info("═" * 60)
    log.info(f"  Data dir   : {data_dir}")
    log.info(f"  Files      : {total_files}")
    log.info(f"  Batch size : {batch_size} files")
    log.info(f"  Interval   : {interval}s between batches")
    log.info(f"  Dry run    : {dry_run}")
    if not dry_run:
        log.info(f"  DB URL     : {db_url[:40]}...")
    log.info("═" * 60)

    conn = None if dry_run else get_connection(db_url)

    batches = list(iter_batches(files, batch_size))
    total_batches = len(batches)

    for batch_num, batch_files in enumerate(batches, start=1):
        batch_events = []
        batch_skipped = 0

        log.info(f"\n[Batch {batch_num}/{total_batches}] "
                 f"Processing {len(batch_files)} files...")

        for file_path in batch_files:
            try:
                events = load_json_file(file_path)
                valid_events = [e for e in events if validate_event(e, file_path)]
                batch_skipped += len(events) - len(valid_events)
                batch_events.extend(valid_events)
                log.info(f"  ✅ {file_path.name}: {len(valid_events)} events")
            except Exception as exc:
                log.error(f"  ✖  {file_path.name}: {exc}")
                total_errors += 1

        total_events  += len(batch_events)
        total_skipped += batch_skipped

        if not dry_run and batch_events:
            tuples   = [prepare_event_tuple(e) for e in batch_events]
            inserted = insert_batch(conn, tuples)
            total_inserted += inserted
            log.info(f"  → Inserted {inserted} events into PostgreSQL")
        elif dry_run:
            log.info(f"  → [DRY RUN] Would insert {len(batch_events)} events")

        # Progress
        elapsed   = time.time() - start_time
        files_done = batch_num * batch_size
        pct        = min(100, round(100 * files_done / total_files))
        log.info(f"  Progress: {pct}% | "
                 f"Events so far: {total_events} | "
                 f"Elapsed: {elapsed:.1f}s")

        # Wait before next batch (skip after last batch)
        if batch_num < total_batches:
            log.info(f"  Waiting {interval}s before next batch...")
            time.sleep(interval)

    if conn:
        conn.close()

    elapsed = time.time() - start_time

    log.info("")
    log.info("═" * 60)
    log.info("  Load Test Complete")
    log.info("═" * 60)
    log.info(f"  Files processed : {total_files}")
    log.info(f"  Events parsed   : {total_events}")
    log.info(f"  Events inserted : {total_inserted}")
    log.info(f"  Events skipped  : {total_skipped} (validation failures)")
    log.info(f"  Errors          : {total_errors} (file read failures)")
    log.info(f"  Total time      : {elapsed:.1f}s")
    log.info(f"  Avg rate        : {total_events / elapsed:.1f} events/s")
    log.info("═" * 60)
    log.info("")

    if not dry_run:
        log.info("  Next steps:")
        log.info("  1. Kafka UI    → http://localhost:8080")
        log.info("     Topics → pg.public.payment_events → messages flowing")
        log.info("  2. Grafana     → http://localhost:3001")
        log.info("     Kafka dashboard → consumer lag rising then settling")
        log.info("  3. Dagster     → http://localhost:3000")
        log.info("     Assets → Bronze/Silver/Gold materializing")
        log.info("  4. Snowflake   → query CDC_POC.SILVER.SILVER_PAYMENT_CURRENT_STATE")
        log.info(f"     Expected: ~{total_events} distinct events processed")
        log.info("")

    return {
        "files":    total_files,
        "events":   total_events,
        "inserted": total_inserted,
        "skipped":  total_skipped,
        "errors":   total_errors,
        "elapsed":  elapsed,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load test: insert JSON payment events into PostgreSQL"
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        type=Path,
        help="Directory containing JSON files with payment events",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of files per batch (default: 10)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Seconds to wait between batches (default: 5)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate files without inserting into PostgreSQL",
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("DATABASE_URL"),
        help="PostgreSQL connection URL (default: DATABASE_URL env var)",
    )

    args = parser.parse_args()

    if not args.data_dir.is_dir():
        log.error(f"Data directory not found: {args.data_dir}")
        sys.exit(1)

    if not args.dry_run and not args.db_url:
        log.error(
            "No database URL provided. "
            "Set DATABASE_URL env var or use --db-url."
        )
        sys.exit(1)

    summary = run(
        data_dir   = args.data_dir,
        batch_size = args.batch_size,
        interval   = args.interval,
        dry_run    = args.dry_run,
        db_url     = args.db_url,
    )

    sys.exit(1 if summary["errors"] > 0 else 0)

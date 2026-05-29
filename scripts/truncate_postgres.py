#!/usr/bin/env python3
"""
truncate_postgres.py
Truncates all 20 CDC source tables in PostgreSQL for a full reset.

Does NOT drop tables or the dbz_publication — only empties the rows.
After running this script:
  - Re-load data with load_to_postgres.py
  - Debezium will capture the new INSERTs and send them through the pipeline

Usage:
    python3 scripts/truncate_postgres.py --db-url $DATABASE_URL

    # Or with explicit URL:
    python3 scripts/truncate_postgres.py \
        --db-url postgresql://poc_user:poc_pass123@localhost:5432/pocdb
"""
import argparse, os, sys
import psycopg2

TABLES = [
    # Events (high volume)
    "payment_events",
    "gps_events",
    "order_status",
    "search_events",
    "recommendations",
    # Fact
    "order_items",
    # Entities
    "orders",
    "payments",
    "routes",
    "receipts",
    "driver_shifts",
    "support_tickets",
    "users_mongo",
    "users_mssql",
    "restaurants",
    "drivers",
    "products",
    "menu_sections",
    "ratings",
    "inventory",
]

parser = argparse.ArgumentParser(description="Truncate all CDC tables in PostgreSQL")
parser.add_argument(
    "--db-url",
    default=os.getenv("DATABASE_URL"),
    help="PostgreSQL connection URL (default: DATABASE_URL env var)",
)
args = parser.parse_args()

if not args.db_url:
    print("ERROR: No DB URL. Set DATABASE_URL or use --db-url.")
    sys.exit(1)

conn = psycopg2.connect(args.db_url)
conn.autocommit = False
cur = conn.cursor()

errors = 0
print(f"Truncating {len(TABLES)} tables...", flush=True)
try:
    for t in TABLES:
        cur.execute(f"TRUNCATE TABLE {t} CASCADE")
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        n = cur.fetchone()[0]
        print(f"  OK   {t:<25} ({n} rows remaining)", flush=True)
    conn.commit()
    print(f"\nDone. All {len(TABLES)} tables truncated.", flush=True)
except Exception as e:
    conn.rollback()
    print(f"\nERROR: {e}", flush=True)
    errors += 1
finally:
    cur.close()
    conn.close()

sys.exit(1 if errors else 0)

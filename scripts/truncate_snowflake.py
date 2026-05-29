#!/usr/bin/env python3
"""
truncate_snowflake.py
Truncates all pipeline tables in Snowflake for a full reset.

Clears:
  - 20 Bronze raw tables   (VARIANT — written by Kafka Connect / Snowpipe)
  - 20 Bronze dbt models   (flattened / typed)
  - 9  Silver dbt models   (enriched / deduplicated)
  - 6  Gold dbt models     (analytical aggregations)

Does NOT drop stages or pipes — those are managed by the Kafka connector.
Run truncate_stages_pipes() separately if needed.

Usage (from inside the dagster container):
    python3 /opt/dagster/truncate_snowflake.py

Usage (from host via docker exec):
    docker exec dagster-webserver python3 /opt/dagster/truncate_snowflake.py
"""
import os, sys
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import snowflake.connector

with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as f:
    pk = load_pem_private_key(f.read(), password=None)

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key=pk,
    database=os.environ["SNOWFLAKE_DATABASE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    role=os.environ["SNOWFLAKE_ROLE"],
)
cur = conn.cursor()
db  = os.environ["SNOWFLAKE_DATABASE"]

TABLES = [
    # ── Bronze raw (Snowpipe VARIANT) ────────────────────────────────────────
    "BRONZE.PAYMENT_EVENTS",  "BRONZE.ORDERS",         "BRONZE.PAYMENTS",
    "BRONZE.ORDER_ITEMS",     "BRONZE.GPS_EVENTS",     "BRONZE.ORDER_STATUS",
    "BRONZE.ROUTES",          "BRONZE.RECEIPTS",       "BRONZE.DRIVER_SHIFTS",
    "BRONZE.SEARCH_EVENTS",   "BRONZE.RECOMMENDATIONS","BRONZE.SUPPORT_TICKETS",
    "BRONZE.USERS_MONGO",     "BRONZE.USERS_MSSQL",    "BRONZE.RESTAURANTS",
    "BRONZE.DRIVERS",         "BRONZE.PRODUCTS",       "BRONZE.MENU_SECTIONS",
    "BRONZE.RATINGS",         "BRONZE.INVENTORY",
    # ── Bronze dbt models ────────────────────────────────────────────────────
    "BRONZE.BRONZE_DRIVER_SHIFTS",    "BRONZE.BRONZE_DRIVERS",
    "BRONZE.BRONZE_GPS_EVENTS",       "BRONZE.BRONZE_INVENTORY",
    "BRONZE.BRONZE_MENU_SECTIONS",    "BRONZE.BRONZE_ORDER_ITEMS",
    "BRONZE.BRONZE_ORDERS",           "BRONZE.BRONZE_ORDER_STATUS",
    "BRONZE.BRONZE_PAYMENT_EVENTS",   "BRONZE.BRONZE_PAYMENTS",
    "BRONZE.BRONZE_PRODUCTS",         "BRONZE.BRONZE_RATINGS",
    "BRONZE.BRONZE_RECEIPTS",         "BRONZE.BRONZE_RECOMMENDATIONS",
    "BRONZE.BRONZE_RESTAURANTS",      "BRONZE.BRONZE_ROUTES",
    "BRONZE.BRONZE_SEARCH_EVENTS",    "BRONZE.BRONZE_SUPPORT_TICKETS",
    "BRONZE.BRONZE_USERS_MONGO",      "BRONZE.BRONZE_USERS_MSSQL",
    # ── Silver dbt models ────────────────────────────────────────────────────
    "SILVER.SILVER_ORDERS",
    "SILVER.SILVER_USERS",
    "SILVER.SILVER_DRIVERS",
    "SILVER.SILVER_DRIVER_SHIFTS",
    "SILVER.SILVER_PAYMENT_EVENTS_HISTORY",
    "SILVER.SILVER_PAYMENT_CURRENT_STATE",
    "SILVER.SILVER_ORDER_ITEMS",
    "SILVER.SILVER_SEARCH_EVENTS",
    "SILVER.SILVER_RECOMMENDATIONS",
    # ── Gold dbt models ──────────────────────────────────────────────────────
    "GOLD.GOLD_DRIVER_PERFORMANCE",
    "GOLD.GOLD_PAYMENT_FUNNEL",
    "GOLD.GOLD_PAYMENT_LIFECYCLE",
    "GOLD.GOLD_PAYMENTS_BY_STATUS",
    "GOLD.GOLD_REVENUE_PER_RESTAURANT",
    "GOLD.GOLD_USER_BEHAVIOR",
]

errors = 0
print(f"Truncating {len(TABLES)} tables in {db}...", flush=True)
for t in TABLES:
    try:
        cur.execute(f"TRUNCATE TABLE IF EXISTS {db}.{t}")
        print(f"  OK   {t}", flush=True)
    except Exception as e:
        print(f"  ERR  {t}  ({e})", flush=True)
        errors += 1

cur.close()
conn.close()
print(f"\nDone. {len(TABLES) - errors} truncated, {errors} errors.", flush=True)
sys.exit(1 if errors else 0)

#!/usr/bin/env python3
"""
validate_pipeline.py
Compares row counts across all pipeline layers:
  PostgreSQL → Bronze raw (Snowpipe) → Bronze dbt → Silver → Gold

Run inside the dagster container:
  python3 /opt/dagster/validate_pipeline.py
"""
import os
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import snowflake.connector
import psycopg2

# ── Snowflake ──────────────────────────────────────────────────────────────────
with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as f:
    pk = load_pem_private_key(f.read(), password=None)
sf = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key=pk,
    database=os.environ["SNOWFLAKE_DATABASE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    role=os.environ["SNOWFLAKE_ROLE"],
)
cur = sf.cursor()
db = os.environ["SNOWFLAKE_DATABASE"]

# ── PostgreSQL ─────────────────────────────────────────────────────────────────
pg = psycopg2.connect(os.environ["DATABASE_URL"])
pgc = pg.cursor()

TABLES = [
    "payment_events", "gps_events",    "order_status",  "search_events",
    "recommendations","order_items",   "orders",        "payments",
    "routes",         "receipts",      "driver_shifts", "support_tickets",
    "users_mongo",    "users_mssql",   "restaurants",   "drivers",
    "products",       "menu_sections", "ratings",       "inventory",
]

def sf_count(schema, table):
    cur.execute("SELECT COUNT(*) FROM {}.{}.{}".format(db, schema, table.upper()))
    return cur.fetchone()[0]

def pg_count(table):
    try:
        pgc.execute("SELECT COUNT(*) FROM {}".format(table))
        return pgc.fetchone()[0]
    except Exception:
        pg.rollback()
        return -1

print("\n{:<22} {:>8} {:>8} {:>10} {:>8}  {}".format(
    "TABLE", "PG", "RAW", "BRONZE_DBT", "DRIFT%", "STATUS"))
print("=" * 68)

total_pg = total_raw = total_dbt = 0
issues = []
for t in TABLES:
    pg_n  = pg_count(t)
    raw_n = sf_count("BRONZE", t)
    dbt_n = sf_count("BRONZE", "bronze_" + t)
    if pg_n >= 0: total_pg += pg_n
    total_raw += raw_n
    total_dbt += dbt_n
    # drift = how much Bronze dbt diverges from PostgreSQL
    drift = round((dbt_n - pg_n) / max(pg_n, 1) * 100, 1) if pg_n > 0 else 0
    status = "OK"
    if dbt_n == 0:
        status = "VAZIO"
        issues.append(t)
    elif abs(drift) > 5 and t not in ("order_status",):
        status = "DRIFT"
        issues.append(t)
    print("  {:<20} {:>8,} {:>8,} {:>10,} {:>7.1f}%  {}".format(
        t, pg_n, raw_n, dbt_n, drift, status))

print("=" * 68)
print("  {:<20} {:>8,} {:>8,} {:>10,}".format("TOTAL", total_pg, total_raw, total_dbt))

print("\n─── SILVER ──────────────────────────────────────")
SILVER = [
    "silver_orders", "silver_users", "silver_drivers", "silver_driver_shifts",
    "silver_payment_events_history", "silver_payment_current_state",
    "silver_order_items", "silver_search_events", "silver_recommendations",
]
for t in SILVER:
    n = sf_count("SILVER", t)
    status = "OK" if n > 0 else "VAZIO"
    if n == 0: issues.append(t)
    print("  {:<40} {:>8,}  {}".format(t, n, status))

print("\n─── GOLD ────────────────────────────────────────")
GOLD = [
    "gold_payment_funnel", "gold_payment_lifecycle", "gold_payments_by_status",
    "gold_driver_performance", "gold_revenue_per_restaurant", "gold_user_behavior",
]
for t in GOLD:
    n = sf_count("GOLD", t)
    status = "OK" if n > 0 else "VAZIO"
    if n == 0: issues.append(t)
    print("  {:<40} {:>8,}  {}".format(t, n, status))

print()
if issues:
    print("ISSUES: " + ", ".join(issues))
else:
    print("Pipeline OK — all layers populated.")

cur.close(); sf.close()
pgc.close(); pg.close()

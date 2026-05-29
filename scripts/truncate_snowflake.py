#!/usr/bin/env python3
"""Truncates all Snowflake tables (Bronze raw + dbt models) for a full pipeline reset."""
import os
import sys
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import snowflake.connector

with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as f:
    private_key = load_pem_private_key(f.read(), password=None)

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key=private_key,
    database=os.environ["SNOWFLAKE_DATABASE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    role=os.environ["SNOWFLAKE_ROLE"],
)
print("Connected to Snowflake OK", flush=True)

db  = os.environ["SNOWFLAKE_DATABASE"]
cur = conn.cursor()

tables = [
    # Raw Snowpipe VARIANT tables (written by Kafka Connect)
    "BRONZE.PAYMENT_EVENTS", "BRONZE.ORDERS",      "BRONZE.PAYMENTS",
    "BRONZE.ORDER_ITEMS",    "BRONZE.GPS_EVENTS",   "BRONZE.ORDER_STATUS",
    "BRONZE.ROUTES",         "BRONZE.RECEIPTS",     "BRONZE.DRIVER_SHIFTS",
    "BRONZE.SEARCH_EVENTS",  "BRONZE.RECOMMENDATIONS", "BRONZE.SUPPORT_TICKETS",
    "BRONZE.USERS_MONGO",    "BRONZE.USERS_MSSQL",  "BRONZE.RESTAURANTS",
    "BRONZE.DRIVERS",        "BRONZE.PRODUCTS",     "BRONZE.MENU_SECTIONS",
    "BRONZE.RATINGS",        "BRONZE.INVENTORY",
    # dbt Bronze models
    "BRONZE.BRONZE_DRIVER_SHIFTS",   "BRONZE.BRONZE_DRIVERS",       "BRONZE.BRONZE_GPS_EVENTS",
    "BRONZE.BRONZE_INVENTORY",       "BRONZE.BRONZE_MENU_SECTIONS", "BRONZE.BRONZE_ORDER_ITEMS",
    "BRONZE.BRONZE_ORDERS",          "BRONZE.BRONZE_ORDER_STATUS",  "BRONZE.BRONZE_PAYMENT_EVENTS",
    "BRONZE.BRONZE_PAYMENTS",        "BRONZE.BRONZE_PRODUCTS",      "BRONZE.BRONZE_RATINGS",
    "BRONZE.BRONZE_RECEIPTS",        "BRONZE.BRONZE_RECOMMENDATIONS", "BRONZE.BRONZE_RESTAURANTS",
    "BRONZE.BRONZE_ROUTES",          "BRONZE.BRONZE_SEARCH_EVENTS", "BRONZE.BRONZE_SUPPORT_TICKETS",
    "BRONZE.BRONZE_USERS_MONGO",     "BRONZE.BRONZE_USERS_MSSQL",
    # dbt Silver models
    "SILVER.SILVER_ORDERS", "SILVER.SILVER_PAYMENT_CURRENT_STATE",
    "SILVER.SILVER_PAYMENT_EVENTS_HISTORY", "SILVER.SILVER_USERS",
    # dbt Gold models
    "GOLD.GOLD_DRIVER_PERFORMANCE",  "GOLD.GOLD_PAYMENT_FUNNEL",
    "GOLD.GOLD_PAYMENT_LIFECYCLE",   "GOLD.GOLD_PAYMENTS_BY_STATUS",
    "GOLD.GOLD_REVENUE_PER_RESTAURANT", "GOLD.GOLD_USER_BEHAVIOR",
]

errors = 0
for t in tables:
    try:
        cur.execute(f"TRUNCATE TABLE IF EXISTS {db}.{t}")
        print(f"  TRUNCATED  {t}", flush=True)
    except Exception as e:
        print(f"  SKIP       {t}  ({e})", flush=True)
        errors += 1

cur.close()
conn.close()
print(f"\nDone. Errors: {errors}", flush=True)
sys.exit(1 if errors > 0 else 0)

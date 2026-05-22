import os
import requests

from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)
from dagster_snowflake import SnowflakeResource

from .jobs import cdc_pipeline_job, sync_metadata_job

SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "CDC_POC")
REGISTRY_URL       = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

BRONZE_TABLES = [
    f"{SNOWFLAKE_DATABASE}.BRONZE.PAYMENT_EVENTS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.ORDERS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.PAYMENTS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.ORDER_ITEMS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.GPS_EVENTS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.ORDER_STATUS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.ROUTES",
    f"{SNOWFLAKE_DATABASE}.BRONZE.RECEIPTS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.DRIVER_SHIFTS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.SEARCH_EVENTS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.RECOMMENDATIONS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.SUPPORT_TICKETS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.USERS_MONGO",
    f"{SNOWFLAKE_DATABASE}.BRONZE.USERS_MSSQL",
    f"{SNOWFLAKE_DATABASE}.BRONZE.RESTAURANTS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.DRIVERS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.PRODUCTS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.MENU_SECTIONS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.RATINGS",
    f"{SNOWFLAKE_DATABASE}.BRONZE.INVENTORY",
]


def _get_max_create_time(conn, table: str) -> int:
    try:
        result = conn.cursor().execute(f"""
            SELECT COALESCE(MAX(RECORD_METADATA:CreateTime::BIGINT), 0)
            FROM {table}
        """).fetchone()
        return result[0] if result else 0
    except Exception:
        return 0


def _get_registered_subjects() -> set[str]:
    """Returns all value subjects currently in Schema Registry."""
    try:
        resp = requests.get(f"{REGISTRY_URL}/subjects", timeout=5)
        resp.raise_for_status()
        return {s for s in resp.json() if s.endswith("-value")}
    except Exception:
        return set()


def _get_synced_tables(conn) -> set[str]:
    """Returns table names already present in TABLE_METADATA."""
    try:
        rows = conn.cursor().execute(f"""
            SELECT table_name FROM {SNOWFLAKE_DATABASE}.CONFIG.TABLE_METADATA
        """).fetchall()
        return {row[0] for row in rows}
    except Exception:
        return set()


# ── Sensor 1: Bronze new data → trigger dbt run ───────────────────────────────

@sensor(
    job=cdc_pipeline_job,
    minimum_interval_seconds=60,
    description=(
        "Monitors Bronze tables for new Snowpipe data. "
        "Triggers dbt run only when new rows detected since last cursor."
    ),
)
def bronze_new_data_sensor(
    context: SensorEvaluationContext,
    snowflake: SnowflakeResource,
) -> SensorResult:
    last_cursor  = int(context.cursor or "0")
    current_max  = 0

    with snowflake.get_connection() as conn:
        for table in BRONZE_TABLES:
            table_max = _get_max_create_time(conn, table)
            if table_max > current_max:
                current_max = table_max

    if current_max <= last_cursor:
        return SensorResult(
            skip_reason=SkipReason(
                f"No new Bronze data. "
                f"Last cursor: {last_cursor}, current max: {current_max}."
            )
        )

    context.log.info(
        f"New Bronze data detected. "
        f"Cursor: {last_cursor} → {current_max}. Triggering dbt run."
    )

    return SensorResult(
        run_requests=[RunRequest(
            run_key=str(current_max),
            tags={
                "triggered_by": "bronze_new_data_sensor",
                "bronze_max_create_time": str(current_max),
            },
        )],
        cursor=str(current_max),
    )


# ── Sensor 2: New Registry subjects → trigger metadata sync ──────────────────

@sensor(
    job=sync_metadata_job,
    minimum_interval_seconds=300,   # check Registry every 5 minutes
    description=(
        "Monitors Schema Registry for new subjects not yet in TABLE_METADATA. "
        "Triggers sync_metadata.py when unregistered subjects are detected."
    ),
)
def registry_new_subject_sensor(
    context: SensorEvaluationContext,
    snowflake: SnowflakeResource,
) -> SensorResult:
    subjects = _get_registered_subjects()
    if not subjects:
        return SensorResult(
            skip_reason=SkipReason("Schema Registry unreachable or empty.")
        )

    subject_tables = {
        s.removesuffix("-value").split(".")[-1]
        for s in subjects
    }

    with snowflake.get_connection() as conn:
        synced_tables = _get_synced_tables(conn)

    new_tables = subject_tables - synced_tables

    if not new_tables:
        return SensorResult(
            skip_reason=SkipReason(
                f"All {len(subject_tables)} Registry subjects already in TABLE_METADATA."
            )
        )

    context.log.info(
        f"New subjects detected: {new_tables}. Triggering sync_metadata job."
    )

    return SensorResult(
        run_requests=[RunRequest(
            run_key=f"sync_{sorted(new_tables)}",
            tags={
                "triggered_by": "registry_new_subject_sensor",
                "new_tables": str(sorted(new_tables)),
            },
        )]
    )

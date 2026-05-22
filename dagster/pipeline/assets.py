import os
import json
from datetime import datetime, timezone
from pathlib import Path

from dagster import AssetExecutionContext, asset
from dagster_dbt import dbt_assets
from dagster_snowflake import SnowflakeResource

from .resources import dbt_resource, DBT_PROJECT_DIR

SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "CDC_POC")


# ── dbt assets (Bronze → Silver → Gold) ───────────────────────────────────────

@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
)
def cdc_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Runs all dbt models in dependency order: bronze → silver → gold.
    After completion, writes execution results to PROCESSING_LOG via
    the log_processing_results asset (downstream dependency).
    """
    dbt_invocation = dbt.cli(
        ["run", "--select", "bronze silver gold"],
        context=context,
    )
    yield from dbt_invocation.stream()

    # Store run results in context metadata for log_processing_results
    try:
        run_results_path = DBT_PROJECT_DIR / "target" / "run_results.json"
        if run_results_path.exists():
            context.add_output_metadata({
                "run_results_path": str(run_results_path)
            })
    except Exception:
        pass

    yield from dbt.cli(
        ["test", "--select", "bronze silver gold"],
        context=context,
    ).stream()


# ── Processing log asset ───────────────────────────────────────────────────────

@asset(
    deps=[cdc_dbt_assets],
    description=(
        "Reads dbt run_results.json after each pipeline execution and "
        "inserts one row per model into CONFIG.PROCESSING_LOG. "
        "Captures status, row counts, duration and errors."
    ),
    group_name="observability",
)
def log_processing_results(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> None:
    run_results_path = DBT_PROJECT_DIR / "target" / "run_results.json"

    if not run_results_path.exists():
        context.log.warning("run_results.json not found — skipping processing log.")
        return

    with open(run_results_path) as f:
        run_results = json.load(f)

    invocation_id = run_results.get("metadata", {}).get("invocation_id", "unknown")
    elapsed_time  = run_results.get("elapsed_time", 0)
    results       = run_results.get("results", [])

    rows_to_insert = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    for result in results:
        node_name    = result.get("unique_id", "")     # model.project.bronze_usuarios
        model_name   = node_name.split(".")[-1]        # bronze_usuarios
        status       = result.get("status", "unknown")
        adapter_resp = result.get("adapter_response", {})
        timing       = result.get("timing", [])

        # Determine layer from model name prefix
        if model_name.startswith("bronze_"):
            layer = "bronze"
        elif model_name.startswith("silver_"):
            layer = "silver"
        elif model_name.startswith("gold_"):
            layer = "gold"
        else:
            layer = "other"

        table_name = (
            model_name
            .replace("bronze_", "")
            .replace("silver_", "")
            .replace("gold_", "")
        )

        # Row counts from adapter response (Snowflake MERGE stats)
        rows_inserted = adapter_resp.get("rows_inserted", 0)
        rows_updated  = adapter_resp.get("rows_updated", 0)
        rows_deleted  = adapter_resp.get("rows_deleted", 0)
        rows_affected = adapter_resp.get("rows_affected",
                        rows_inserted + rows_updated + rows_deleted)

        # Timing
        started_at  = None
        finished_at = None
        duration    = result.get("execution_time", 0)
        for t in timing:
            if t.get("name") == "execute":
                started_at  = t.get("started_at")
                finished_at = t.get("completed_at")

        error_message = None
        if status in ("error", "fail"):
            error_message = str(result.get("message", ""))[:2000]

        rows_to_insert.append((
            table_name, layer, model_name, invocation_id,
            str(id(context)),          # run_id (Dagster run id)
            "success" if status == "success" else "error",
            rows_affected, rows_inserted, rows_updated, rows_deleted,
            started_at, finished_at, round(duration, 3),
            error_message, "sensor",
        ))

    if not rows_to_insert:
        context.log.info("No model results to log.")
        return

    insert_sql = f"""
        INSERT INTO {SF_DATABASE}.CONFIG.PROCESSING_LOG (
            table_name, layer, dbt_model, dbt_invocation_id, run_id,
            status, rows_processed, rows_inserted, rows_updated, rows_deleted,
            started_at, finished_at, duration_seconds,
            error_message, triggered_by
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s)
    """

    with snowflake.get_connection() as conn:
        conn.cursor().executemany(insert_sql, rows_to_insert)
        conn.commit()

    context.log.info(
        f"Logged {len(rows_to_insert)} model results to PROCESSING_LOG "
        f"(invocation: {invocation_id})."
    )

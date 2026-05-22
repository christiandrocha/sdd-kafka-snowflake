from dagster import define_asset_job, AssetSelection, job, op
import subprocess
import os


# ── Job 1: dbt pipeline (Bronze → Silver → Gold) ─────────────────────────────

cdc_pipeline_job = define_asset_job(
    name="cdc_pipeline_job",
    selection=AssetSelection.all(),
    description="Runs all dbt models: bronze → silver → gold.",
)


# ── Job 2: sync_metadata (Registry → TABLE_METADATA) ─────────────────────────

@op(description="Runs sync_metadata.py to sync Schema Registry → TABLE_METADATA")
def run_sync_metadata(context):
    script_path = "/opt/dagster/app/../scripts/sync_metadata.py"
    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(f"sync_metadata.py failed with code {result.returncode}")
    context.log.info("sync_metadata.py completed successfully.")


@job(description="Triggered by registry_new_subject_sensor when new subjects detected.")
def sync_metadata_job():
    run_sync_metadata()

#!/bin/bash
# Generates dbt manifest before Dagster starts — required by @dbt_assets decorator.
# Uses `|| true` so container starts even if Snowflake is unreachable at boot time;
# the manifest from a previous compile is sufficient for asset graph loading.
set -e

cd /opt/dagster/dbt
echo "[entrypoint] Running dbt deps..."
dbt deps --quiet

echo "[entrypoint] Running dbt compile (target: ${DBT_TARGET:-dev})..."
dbt compile --target "${DBT_TARGET:-dev}" --quiet || echo "[entrypoint] dbt compile failed — using existing manifest if present"

exec "$@"

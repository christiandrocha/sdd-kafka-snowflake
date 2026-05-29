import os

from dagster import RetryPolicy, Backoff
from dagster_dbt import DbtCliResource
from dagster_snowflake import SnowflakeResource
from pathlib import Path

DBT_PROJECT_DIR = Path("/opt/dagster/dbt")
DBT_TARGET      = os.getenv("DBT_TARGET", "dev")

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    target=DBT_TARGET,
)

snowflake_resource = SnowflakeResource(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key_path=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
    role=os.environ.get("SNOWFLAKE_ROLE", "CDC_ROLE"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "CDC_WH"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "CDC_POC"),
)

# Retry policy for transient Snowflake/network failures
SNOWFLAKE_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=30,
    backoff=Backoff.EXPONENTIAL,
)

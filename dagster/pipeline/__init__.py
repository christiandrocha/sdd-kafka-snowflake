from dagster import Definitions

from .assets import cdc_dbt_assets, log_processing_results
from .sensors import bronze_new_data_sensor, registry_new_subject_sensor
from .jobs import cdc_pipeline_job, sync_metadata_job
from .resources import dbt_resource, snowflake_resource

defs = Definitions(
    assets=[cdc_dbt_assets, log_processing_results],
    resources={
        "dbt":       dbt_resource,
        "snowflake": snowflake_resource,
    },
    sensors=[
        bronze_new_data_sensor,
        registry_new_subject_sensor,
    ],
    jobs=[
        cdc_pipeline_job,
        sync_metadata_job,
    ],
)

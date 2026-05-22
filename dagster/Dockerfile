FROM python:3.11-slim

WORKDIR /opt/dagster

RUN pip install --no-cache-dir \
    dagster==1.6.* \
    dagster-webserver==1.6.* \
    dagster-dbt==0.22.* \
    dagster-snowflake==0.22.* \
    dbt-snowflake==1.7.* \
    dbt-core==1.7.* \
    snowflake-connector-python==3.6.* \
    requests==2.31.* \
    cryptography==41.*

COPY dagster/dagster.yaml     /opt/dagster/dagster_home/dagster.yaml
COPY dagster/workspace.yaml   /opt/dagster/app/workspace.yaml
COPY dagster/pipeline         /opt/dagster/app/pipeline
COPY dagster/entrypoint.sh    /opt/dagster/entrypoint.sh
COPY dbt                      /opt/dagster/dbt

RUN chmod +x /opt/dagster/entrypoint.sh

ENTRYPOINT ["/opt/dagster/entrypoint.sh"]

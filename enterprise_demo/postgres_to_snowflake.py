import time
from datetime import timedelta
import prefect
from prefect import task, Flow, Parameter
from prefect.core import parameter
from prefect.storage import GitHub
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefect.run_configs import DockerRun
from prefect.engine import signals

@task
def connect_to_postgres():
    time.sleep(5)

@task
def execute_query(client, table_name):
    logger = prefect.context.get("logger")
    logger.info(f"Table Name: {table_name}")
    if table_name == "Users":
        time.sleep(9)
        raise signals.FAIL(message="TableNotFound: The table specified does not exist.")
    else:
        time.sleep(19)
    return table_name

@task
def create_df(data):
    time.sleep(14)

@task
def connect_to_snowflake():
    time.sleep(4)

@task
def upload_to_snowflake(client, data):
    time.sleep(14)

with Flow(
    "ETL PostgreSQL to Snowflake",
    storage=GitHub(
        repo="kmoonwright/utility_flows", 
        path="enterprise_demo/postgres_to_snowflake.py",
        access_token_secret="GITHUB_ACCESS_TOKEN"
    ),
    # schedule=Schedule(clocks=[IntervalClock(timedelta(minutes=2))]),
    run_config=DockerRun(
        labels=["production"],
        image="prefecthq/prefect:latest"
    )
) as flow:
    postgres_table = Parameter(name="Table Name", default="User")
    pg_client = connect_to_postgres()
    query = execute_query(pg_client, postgres_table)
    df = create_df(query)
    sf_client = connect_to_snowflake()
    update_warehouse = upload_to_snowflake(sf_client, df)

flow.register(project_name="production-flows")
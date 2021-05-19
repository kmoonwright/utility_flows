from random import randrange
import prefect
from prefect import task, Flow
from prefect.storage import GitHub

@task
def extract():
    return randrange(1, 100)

@task
def transform(data):
    return data * 10

@task
def load(data):
    logger = prefect.context.get("logger")
    logger.info(f"\nHere's your data: {data}")

with Flow(
    "Evolving ETL",
    storage=GitHub(
        repo="kmoonwright/utility_flows",
        path="logging_demo/hello_etl.py",
        access_token_secret="GITHUB_ACCESS_TOKEN"
    )
) as flow:
    e = extract()
    t = transform(e)
    l = load(t)

flow.register(project_name="logging-demo")
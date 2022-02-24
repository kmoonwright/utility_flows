from github import Github
from prefect import task, Flow
import prefect
from prefect.engine.results import LocalResult
from prefect.storage import GitHub

# export PREFECT__FLOWS__CHECKPOINTING=False

@task(result=LocalResult(dir="/Users/kylemw/Downloads/results"))
def task_1(x=1, y=1):
    return x + y

@task
def task_2(x):
    logger = prefect.context.get("logger")
    logger.info(x)
    raise ValueError("THIS IS THE ERROR")


with Flow("my handled flow!") as flow:
    result_1 = task_1()
    result_2 = task_2(result_1)

flow.storage = GitHub(
    repo="kmoonwright/utility_flows",                           # name of repo
    path="sandbox/localresult.py",                   # location of flow file in repo
    access_token_secret="GITHUB_ACCESS_TOKEN"  # name of personal access token secret
)

flow.register(project_name="Test")
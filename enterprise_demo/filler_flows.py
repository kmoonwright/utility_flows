import time
from datetime import timedelta
from random import randrange

import prefect
from prefect import task, Flow
from prefect.storage import GitHub
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefect.run_configs import LocalRun, DockerRun, ECSRun, KubernetesRun
from prefect.engine import signals

@task
def task_1():
    logger = prefect.context.get("logger")
    interval = randrange(0, 60)
    logger.info(interval)
    time.sleep(interval)

@task
def task_2():
    logger = prefect.context.get("logger")
    interval = randrange(0, 60)
    logger.info(interval)
    time.sleep(interval)
    if interval > 40:
        logger.info("Skipping task...")
        raise signals.SKIP()

@task
def task_3():
    logger = prefect.context.get("logger")
    interval = randrange(0, 60)
    logger.info(interval)
    time.sleep(interval)
    if interval > 40:
        logger.info("Failing flow...")
        raise signals.FAIL()

with Flow(
    "Data Warehouse ETL",
    storage=GitHub(
        repo="kmoonwright/utility_flows", 
        path="enterprise_demo/filler_flows.py",
        access_token_secret="GITHUB_ACCESS_TOKEN"
    ),
    schedule=Schedule(clocks=[IntervalClock(timedelta(minutes=2))]),
    run_config=LocalRun(labels=["local"])
) as flow1:
    task1 = task_1()
    task2 = task_2()
    task3 = task_3()
    task2.set_upstream(task1)
    task3.set_upstream(task2)
flow1.register(project_name="data-warehouse")

with Flow(
    "Dev Environment ML Training",
    storage=GitHub(
        repo="kmoonwright/utility_flows", 
        path="enterprise_demo/filler_flows.py",
        access_token_secret="GITHUB_ACCESS_TOKEN"
    ),
    schedule=Schedule(clocks=[IntervalClock(timedelta(minutes=2))]),
    run_config=LocalRun(labels=["developer"])
) as flow2:
    task1 = task_1()
    task2 = task_2()
    task3 = task_3()
    task2.set_upstream(task1)
    task3.set_upstream(task2)
flow2.register(project_name="developer-flows")

with Flow(
    "Staging Environment ML Training",
    storage=GitHub(
        repo="kmoonwright/utility_flows", 
        path="enterprise_demo/filler_flows.py",
        access_token_secret="GITHUB_ACCESS_TOKEN"
    ),
    schedule=Schedule(clocks=[IntervalClock(timedelta(minutes=2))]),
    run_config=LocalRun(labels=["staging"])
) as flow3:
    task1 = task_1()
    task2 = task_2()
    task3 = task_3()
    task2.set_upstream(task1)
    task3.set_upstream(task2)
flow3.register(project_name="staging-flows")

with Flow(
    "Production Environment Pipeline",
    storage=GitHub(
        repo="kmoonwright/utility_flows", 
        path="enterprise_demo/filler_flows.py",
        access_token_secret="GITHUB_ACCESS_TOKEN"
    ),
    schedule=Schedule(clocks=[IntervalClock(timedelta(minutes=2))]),
    run_config=DockerRun(labels=["production"])
) as flow4:
    task1 = task_1()
    task2 = task_2()
    task3 = task_3()
    task2.set_upstream(task1)
    task3.set_upstream(task2)
flow4.register(project_name="production-flows")
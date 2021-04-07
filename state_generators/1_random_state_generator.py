import time
from datetime import timedelta
import random

import prefect
from prefect import task, Flow, Parameter, Client
from prefect.tasks.secrets import PrefectSecret
from prefect.engine.signals import SUCCESS, FAIL, SKIP, ENDRUN, LOOP
from prefect.engine import state
from prefect.tasks.prefect import StartFlowRun
from prefect.artifacts import create_link, create_markdown
from prefect.storage import Local, GitHub


# ---------------------------------------- #
# RANDOM STUFF FLOW
# ---------------------------------------- #
@task
def generate_randomness():
    return random.randrange(1, 100)

@task
def log_random_number(number):
    logger = prefect.context.get("logger")
    logger.info(f"Your randomly generated number is: {number}")
    return number

@task
def wait():
    time.sleep(4)

@task
def generate_flow_run_id():
    flow_run_id = prefect.context.get("flow_run_id")
    return flow_run_id

initiate_flow_run_changer = StartFlowRun(
    project_name="Demos", 
    flow_name="Previous Flow Run State Changer",
)


with Flow("Random State Generator") as flow1:
    t1 = generate_randomness()
    t2 = log_random_number(t1)
    t3 = wait()
    t4 = generate_flow_run_id()
    t5 = initiate_flow_run_changer(
        new_flow_context={
            "prev_flow_run_id": t4,
            "prev_random_number": t2
        })
    create_link(f"You activated a subsequent flow: {t5}")

flow1.add_edge(t1, t2)
flow1.add_edge(t2, t3)
flow1.add_edge(t3, t4)
flow1.add_edge(t4, t5)

flow1.storage = GitHub(
    repo="kmoonwright/utility_flows",
    path="state_generators/1_random_state_generator.py",
    access_token_secret="GITLAB_ACCESS_TOKEN"
)
flow1.register(project_name="Demos")
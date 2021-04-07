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
# FLOW RUN GENERATOR
# ---------------------------------------- #

# ATTEMPT 1
# @task
# def create_flow_runs(num_of_flows):
#     loop_count = prefect.context.get("task_loop_count", 1)

#     if loop_count < num_of_flows:
#         logger = prefect.context.get("logger")
#         logger.info(f"Starting FlowRun number: {loop_count + 1} of {num_of_flows}")
#         raise LOOP(
#             result=StartFlowRun(project_name="Demos", flow_name="Random State Generator").run()
#         )

# with Flow("Flow Run Generator") as flow3:
#     num_of_flows = Parameter("num_of_flows", default=5)
#     create_flow_runs(num_of_flows)

# flow3.storage = Local(add_default_labels=False)
# flow3.register(project_name="Demos")

# ATTEMPT 2
# @task
# def generate_list(length):
#     return [f"Flow Run {num + 1}" for num in range(length)]

# @task
# def create_flow_runs(num_of_flow_runs):
#     return StartFlowRun(
#         project_name="Demos", 
#         flow_name="Random State Generator"
#     )(task_run_name=num_of_flow_runs)

# with Flow("Flow Run Generator") as flow3:
#     num_of_flows = Parameter("num_of_flows", default=5)
#     my_flow_runs = generate_list(num_of_flows)
#     create_flow_runs.map(my_flow_runs)

# flow3.storage = Local(add_default_labels=False)
# flow3.register(project_name="Demos")

# ATTEMPT 3
@task
def generate_list(length):
    return ["Random State Generator" for name in range(length)]

create_flow_runs = StartFlowRun(
    project_name="State Generators", 
    flow_name="Random State Generator"
)

with Flow("Flow Run Generator") as flow3:
    num_of_flows = Parameter("num_of_flows", default=5)
    my_flow_runs = generate_list(num_of_flows)
    create_flow_runs.map(my_flow_runs)

flow3.storage = GitHub(
    repo="kmoonwright/utility_flows",
    path="state_generators/3_flow_run_generator.py",
    access_token_secret="GITHUB_ACCESS_TOKEN"
)

flow3.register(project_name="State Generators")
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
from prefect.storage import Local, GitLab


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

# flow1.storage = Local(add_default_labels=False)
flow1.storage = GitLab(
    repo="kmoonwright/demo_flows",                           # name of repo
    path="flows/state_generator_set.py",                   # location of flow file in repo
    access_token_secret="GITLAB_ACCESS_TOKEN"  # name of personal access token secret
)
flow1.register(project_name="Demos")


# ---------------------------------------- #
# FLOW RUN STATE CHANGER
# ---------------------------------------- #
@task
def log_prev_flow_run_id():
    logger = prefect.context.get("logger")
    prev_flow_run_id = prefect.context.get("prev_flow_run_id")
    logger.info(f"Your prev_flow_run_id is: {prev_flow_run_id}")

@task
def log_prev_num():
    logger = prefect.context.get("logger")
    prev_random_number = prefect.context.get("prev_random_number")
    logger.info(f"Your prev_random_number is: {prev_random_number}")
    return prev_random_number

@task
def change_prev_flow_state(number):
    logger = prefect.context.get("logger")
    prev_random_number = prefect.context.get("prev_random_number")
    prev_flow_run_id = prefect.context.get("prev_flow_run_id")

    logger.info(f"The number retrieved from Context was: {prev_random_number}")
    logger.info(f"The number submitted to this Task was: {number}")

    client = Client()
    client.login_to_tenant(tenant_slug="default")

    if number is None:
        logger.info("Outcome: Your number was None")
        pass
    elif number < 50:
        logger.info(f"Outcome: Your number was {number}, which is < 50")
        pass
    elif number < 70:
        set_flow_run_state = client.graphql(
            query="""
                mutation SetFlowRunStates($flowRunId: UUID!, $state: JSON!) {
                    set_flow_run_states(
                        input: {
                        states: [{ flow_run_id: $flowRunId, state: $state }]
                        }
                    ) {
                        states {
                        id
                        status
                        message
                        }
                    }
                }
            """,
            variables={
                "flowRunId": prev_flow_run_id,
                "state": {"type": "Cancelled"}
            }
        )
    elif number < 90:
        set_flow_run_state = client.graphql(
            query="""
                mutation SetFlowRunStates($flowRunId: UUID!, $state: JSON!) {
                    set_flow_run_states(
                        input: {
                        states: [{ flow_run_id: $flowRunId, state: $state }]
                        }
                    ) {
                        states {
                        id
                        status
                        message
                        }
                    }
                }
            """,
            variables={
                "flowRunId": prev_flow_run_id,
                "state": {"type": "Finished"}
            }
        )    
    else:
        set_flow_run_state = client.graphql(
            query="""
                mutation SetFlowRunStates($flowRunId: UUID!, $state: JSON!) {
                    set_flow_run_states(
                        input: {
                        states: [{ flow_run_id: $flowRunId, state: $state }]
                        }
                    ) {
                        states {
                        id
                        status
                        message
                        }
                    }
                }
            """,
            variables={
                "flowRunId": prev_flow_run_id,
                "state": {"type": "Skipped"}
            }
        )

        
with Flow("Previous Flow Run State Changer") as flow2:
    t1 = log_prev_flow_run_id()
    t2 = log_prev_num()
    t3 = change_prev_flow_state(t2)
    create_link(prefect.context.get("prev_flow_run_id"))

flow2.add_edge(t1, t2)
flow2.storage = GitLab(
    repo="kmoonwright/demo_flows",                           # name of repo
    path="flows/state_generator_set.py",                   # location of flow file in repo
    access_token_secret="GITLAB_ACCESS_TOKEN"  # name of personal access token secret
)
flow2.register(project_name="Demos")


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

create_flow_runs = StartFlowRun(project_name="Demos", flow_name="Random State Generator")

with Flow("Flow Run Generator") as flow3:
    num_of_flows = Parameter("num_of_flows", default=5)
    my_flow_runs = generate_list(num_of_flows)
    create_flow_runs.map(my_flow_runs)

flow3.storage = GitLab(
    repo="kmoonwright/demo_flows",                           # name of repo
    path="flows/state_generator_set.py",                   # location of flow file in repo
    access_token_secret="GITLAB_ACCESS_TOKEN"  # name of personal access token secret
)
flow3.register(project_name="Demos")
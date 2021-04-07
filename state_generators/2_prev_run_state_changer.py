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
    client.login_to_tenant(tenant_slug="kmw-cloud")

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
flow2.storage = GitHub(
    repo="kmoonwright/utility_flows",
    path="state_generators/2_prev_run_state_changer.py",
    access_token_secret="GITHUB_ACCESS_TOKEN"
)
flow2.register(project_name="State Generators")
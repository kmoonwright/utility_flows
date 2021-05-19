import prefect
from prefect import task, Flow, Parameter, Client
from prefect.tasks.secrets import PrefectSecret
from prefect.engine.signals import SUCCESS, FAIL, SKIP, ENDRUN, LOOP
from prefect.engine import state
from prefect.tasks.prefect import StartFlowRun

@task
def start_client():
    client = Client()
    client.login_to_tenant(tenant_slug="default")

@task
def initiate_flow_run(client, flow_id):
    client.graphql(
        """
        {
            "mutation": {
                "create_flow_run"(
                    "input": {
                        "flow_id": flow_id,
                    }
                ) {
                    "id", 
                    "name",
                    "scheduled_start_time"
                }
            }
        }
        """
    )

with Flow("Azure Functions GQL Call") as flow:
    flow_id = Parameter(name="Flow ID", default="")
    client = start_client()
    start_flow = initiate_flow_run(client, flow_id)

if __name__ == "__main__":
    flow.run()
import pendulum
import prefect
from prefect import task, Flow, Parameter
from prefect.schedules import CronSchedule
from prefect.engine.signals import SUCCESS, FAIL
from prefect.triggers import always_run
from prefect.tasks.prefect.flow_run import StartFlowRun
from prefect.artifacts import create_link, create_markdown

job_1 = StartFlowRun(
    flow_name="My Event Triggered ETL",
    project_name="Azure",
    wait=True
)

job_2 = StartFlowRun(
    flow_name="My Event Triggered ETL",
    project_name="Azure",
    wait=True
)

job_3 = StartFlowRun(
    flow_name="My Event Triggered ETL",
    project_name="Azure",
    wait=True
)

@task
def fail_task():
    raise FAIL()

@task(trigger=always_run)
def log_success():
    logger = prefect.context.get("logger")
    logger.info("All jobs completed...")

weekday_schedule = CronSchedule(
    "30 9 * * 1-5", start_date=pendulum.now(tz="US/Eastern")
)

with Flow("Orchestrator Flow", schedule=weekday_schedule) as flow:
    db_check = Parameter(name="DB Check", default=True, required=False)

    a = job_1(upstream_tasks=[db_check])
    b = job_2(upstream_tasks=[a])
    c = job_3(upstream_tasks=[a])
    d = fail_task(upstream_tasks=[a])

    final = log_success(upstream_tasks=[b, c, d])
    create_link(f"cloud.prefect.io/km-inc/flow-run/{a}")
    create_link(f"cloud.prefect.io/km-inc/flow-run/{b}")
    create_link(f"cloud.prefect.io/km-inc/flow-run/{c}")

if __name__ == "__main__":
    flow.register(project_name="Azure")
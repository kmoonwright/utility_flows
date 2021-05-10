from prefect import task, Flow, Parameter
from prefect.schedules import CronSchedule
from prefect.tasks.prefect.flow_run import StartFlowRun

job_1 = StartFlowRun(
    flow_name="My Event Triggered ETL",
    project_name="Azure Examples",
    wait=True
)

job_2 = StartFlowRun(
    flow_name="My Event Triggered ETL",
    project_name="Azure Examples",
    wait=True
)

job_3 = StartFlowRun(
    flow_name="My Event Triggered ETL",
    project_name="Azure Examples",
    wait=True
)

@task
def log_success():
    logger = context.get("logger")
    logger.info("All jobs completed...")

weekday_schedule = CronSchedule(
    "30 9 * * 1-5", start_date=pendulum.now(tz="US/Eastern")
)

with Flow("parent-flow", schedule=weekday_schedule) as flow:
    dynamic_val = Parameter(name="DB Check", default=True, required=False)

    a = job_1(upstream_tasks=[dynamic_val])
    b = job_2(upstream_tasks=[a])
    c = job_3(upstream_tasks=[a])

    final = log_success(upstream_tasks=[b, c])

if __name__ == "__main__":
    flow.run() 
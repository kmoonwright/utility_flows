import logging
import prefect
from prefect import task, Flow
from prefect.storage import GitHub

class AwesomeFilter(logging.Filter):
    def filter(self, record):
        filtered = ['sensitive', 'banana', 'papaya']
        if any(x in record.msg for x in filtered):
            return 0
        # you may need to filter based on `getMessage()` if
        # you can't find the information in the pre-formatted msg field
        return 1

def my_logger(record):
    logger = prefect.context.get("logger")
    logger.addFilter(AwesomeFilter())
    return logger.info(record)

@task
def first_task():
    my_logger("This is normal data")
    my_logger("This is sensitive data")
    return 1

@task
def second_task():
    my_logger("This is normal data")
    my_logger("This is sensitive data")
    return 1

@task
def third_task():
    from prefect.engine.signals import FAIL
    raise FAIL(message=my_logger("This is sensitive data"))

with Flow(
    "Filtered Logging Demo",
    storage=GitHub(
        repo="kmoonwright/utility_flows",
        path="logging_demo/log_filter.py",
        access_token_secret="GITHUB_ACCESS_TOKEN"
    )
) as flow:
    first = first_task()
    second = second_task(upstream_tasks=[first])
    third_task(upstream_tasks=[second])

flow.register(project_name="logging-demo")
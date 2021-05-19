import prefect
from prefect import task, Flow
import logging

class AwesomeFilter(logging.Filter):
    def filter(self, record):
        filtered = ['sensitive', 'banana']
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

with Flow("test") as flow:
    first_task()
    second_task()
    third_task()

flow.run()
from random import randrange
from prefect import task, Task, Flow

# TASK DEFINITIONS
# FUNCTIONAL API
@task
def extract():
    return randrange(1, 100)

# IMPERATIVE API
class Extract(Task):
    def run(self):
        # Extract the data
        return randrange(1, 10)

@task
def transform(num1, num2):
    return (num1 + num2) * 10

@task
def load(data):
    print(f"\nHere's your data: {data}")

# FLOW DEFINITION
with Flow("My Event Triggered ETL") as flow:
    extracted_data_1 = extract()
    extracted_data_2 = Extract()
    t = transform(extracted_data_1, extracted_data_2)
    l = load(t)

if __name__ == "__main__":
    flow.register(project_name="Azure")
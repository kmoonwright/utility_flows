from random import randrange
from prefect import task, Flow

@task
def extract():
    return randrange(1, 100)

@task
def transform(data):
    return data * 10

@task
def load(data):
    print(f"\nHere's your data: {data}")

with Flow("My Event Triggered ETL") as flow:
    e = extract()
    t = transform(e)
    l = load(t)

if __name__ == "__main__":
    flow.run()
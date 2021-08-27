from prefect import task, Flow, Parameter
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.aws import S3Download, LambdaCreate

"""
Tasks
    Connect boto3 client
        Secrets for credentials
    S3Download data to memory
        map over multiple buckets
        Parameterize the list of buckets
    Create event Lambda function
    Transformation of data
    Upload to Redshift
        Artifacts to Redshift Warehouse locations

Flow
    Parameterized Scheduling
    Docker Storage - ECR
        one image per flow
    ECS RUN
        Infra can be abstracted to another file

Execution
    Run on ECS Agent
    One K8s cluster, two agents
    1 - ECS Agent to submit Fargate Tasks, "serverless" labels 
    2 - **Kubernetes Agent to submit K8s Jobs, "eks" labels 
    
"""
my_s3_key = PrefectSecret(name="Secret Key")
buckets = Parameter(name="Bucket List", default=["users", "transactions"])

upload_data = S3Download(key=my_s3_key)
event_trigger = LambdaCreate()

@task
def transform_data(data):
    pass

@task
def upload_to_redshift(data):
    pass

with Flow("S3 to Redshift") as flow:
    upload_data()
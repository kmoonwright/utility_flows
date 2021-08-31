import boto3
from botocore.exceptions import ClientError
import json
import datetime
from dotenv import load_dotenv, dotenv_values

def load_settings():
    import prefect
    if prefect.context.get("flow_run_id"):
        # know we're in cloud
        return prefect.client.Secret("DICTIONARY_OF_SECRETs").get()
    else:
        load_dotenv()
        settings = dotenv_values("aws/.env")
        settings_dict = json.loads(json.dumps(settings))
        return settings_dict

settings_dict = load_settings()
AWS_REGION=settings_dict["AWS_REGION"]
AWS_ACCESS_KEY_ID=settings_dict["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY=settings_dict["AWS_SECRET_ACCESS_KEY"]
SESSION_TOKEN=settings_dict["SESSION_TOKEN"]

import logging
logging.basicConfig(level=logging.INFO)

import pathlib
BASE_DIR = pathlib.Path(__file__).parent.resolve()


def create_client():
    """Start a boto3 client"""
    client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        # aws_session_token=SESSION_TOKEN,
        region_name=AWS_REGION,
    )
    return client


def list_buckets(client):
    response = client.list_buckets()
    print("Listing Amazon S3 Buckets:")
    for bucket in response['Buckets']:
        print(f"-- {bucket['Name']}")


def upload_to_s3(
    client,
    file_name, 
    bucket, 
    object_name=None
):
    # Upload the file
    if object_name is None:
        object_name = file_name
    try:
        client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    print(f"'{file_name}' has been uploaded to '{bucket}'")


def download_from_s3_to_dir(
    client,
    bucket,
    file_name
):
    # download_file method accepts the names of the bucket and 
    # object to download and the filename to save the file to.
    download = client.download_file(
        bucket,
        file_name,
        f"{BASE_DIR}/downloaded--{datetime.datetime.now()}.txt",
    )
    print(f"DOWNLOAD IS:\n{download}")


def download_from_s3_to_memory(
    client,
    bucket,
    file_name
):
    obj = client.get_object(Bucket=bucket, Key=file_name)
    return obj

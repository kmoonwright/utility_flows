import logging
from pathlib import Path
from prefect import task, Flow, Parameter, unmapped

import s3_utils as s3

logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(__file__).parent.parent.resolve()

files_to_download = Parameter(
    name="File List", 
    default=["data/test_data.csv", "data/user_data.csv", "data/event_data.csv"]
)

@task
def create_filepath(filename):
    return Path(BASE_DIR) / Path(filename)

@task
def connect_to_s3():
    return s3.create_client()

@task
def upload_to_s3(s3_client, file_path):
    s3.upload_to_s3(
        client=s3_client, 
        file_name=str(file_path), 
        bucket="loading-store-1",
        object_name=str(file_path.name),
    )

with Flow("Upload to S3") as flow:
    conn = connect_to_s3()
    files_to_download = create_filepath.map(files_to_download)
    upload_to_s3.map(s3_client=unmapped(conn), file_path=files_to_download)

flow.run()
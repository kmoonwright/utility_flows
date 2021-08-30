import logging
from pathlib import Path
import datetime

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import utils.s3 as s3

logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(__file__).parent.parent.resolve()
file1_path = Path(BASE_DIR) / Path("data/test_data.csv")

def upload_many_to_s3():
    s3_client = s3.create_client()

    file1_path = Path(BASE_DIR) / Path("data/test_data.csv")
    file2_path = Path(BASE_DIR) / Path("data/user_data.csv")
    file3_path = Path(BASE_DIR) / Path("data/event_data.csv")

    # LOAD TO BUCKET: "loading-store-1"
    s3.upload_to_s3(
        client=s3_client, 
        file_name=str(file1_path), 
        bucket="loading-store-1",
        object_name=f"{datetime.datetime.now()}--{file1_path.name}"
    )
    s3.upload_to_s3(
        client=s3_client, 
        file_name=str(file2_path), 
        bucket="loading-store-1",
        object_name=f"{datetime.datetime.now()}--{file2_path.name}"
    )
    s3.upload_to_s3(
        client=s3_client, 
        file_name=str(file3_path), 
        bucket="loading-store-1",
        object_name=f"{datetime.datetime.now()}--{file3_path.name}"
    )

    # LOAD TO BUCKET: "loading-store-2"
    s3.upload_to_s3(
        client=s3_client, 
        file_name=str(file1_path), 
        bucket="loading-store-2",
        object_name=f"{datetime.datetime.now()}--{file1_path.name}"
    )
    s3.upload_to_s3(
        client=s3_client, 
        file_name=str(file2_path), 
        bucket="loading-store-2",
        object_name=f"{datetime.datetime.now()}--{file2_path.name}"
    )
    s3.upload_to_s3(
        client=s3_client, 
        file_name=str(file3_path), 
        bucket="loading-store-2",
        object_name=f"{datetime.datetime.now()}--{file3_path.name}"
    )

    print("\n<<<<DONEZO>>>>")

if __name__ == "__main__":
    upload_many_to_s3()
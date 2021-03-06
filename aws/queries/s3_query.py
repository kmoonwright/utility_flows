import datetime
import pandas as pd
import logging
import pathlib

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import utils.s3 as s3

logging.basicConfig(level=logging.INFO)
BASE_DIR = pathlib.Path(__file__).parent.resolve()

def main():
    s3_client = s3.create_client()

    s3.list_buckets(s3_client)

    # s3.upload_to_s3(
    #     client=s3_client, 
    #     file_name=f"{BASE_DIR}/test_data.csv", 
    #     bucket="return-box",
    #     object_name=f"uploads/test_data.csv"
    # )

    # s3.download_from_s3_to_dir(
    #     s3_client, 
    #     "return-box", 
    #     "uploads/test_data.txt"
    # )

    # downloaded = s3.download_from_s3_to_memory(
    #     client=s3_client,
    #     bucket="return-box",
    #     file_name="uploads/test_data.csv"
    # )
    # # Create dataframe from csv
    # df = pd.read_csv(downloaded['Body'])
    # print(df)

    print("\n<<<<DONEZO>>>>")

if __name__ == "__main__":
    main()
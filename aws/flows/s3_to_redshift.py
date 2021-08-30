from prefect import task, Flow, Parameter

import psycopg2
import pandas as pd
from pathlib import Path
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import utils.s3 as s3
import utils.redshift as rs

@task
def connect_to_s3():
    return s3.create_client()

Parameter("File to download")

@task
def download_from_s3(client, bucket, file_name):
    return s3.download_from_s3_to_memory(client, bucket, file_name)

@task
def local_data(file_name):
    file_path = Path(__file__).parent.parent.resolve() / Path(f"data/{file_name}")
    return file_path

@task
def create_dataframe(csv_file):
    test_df = pd.read_csv(csv_file)

@task
def transform(df):
    return df

print("----STAGE 2.....")

Parameter("DBname", default="suppliers")

@task
def connect_to_rs(dbname):
    conn = rs.create_conn(dbname)
    return conn

print("----STAGE 2.....")

@task
def insert_df(client, df, table):
    rs.insert_df(client, df, table)
    return

print("----STAGE 3.....")

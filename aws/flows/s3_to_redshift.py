from prefect import task, Flow, Parameter

import pandas as pd

import utils.s3 as s3
import utils.redshift as rs

s3.create_client()

Parameter("File to download")

s3.download_from_s3_to_memory()

@task
def create_dataframe(csv):
    df = pd.read_csv(csv['Body'])
    return df

@task
def transform(df):
    return df

rs.create_conn

@task
def load_to_redshift(df):
    return df

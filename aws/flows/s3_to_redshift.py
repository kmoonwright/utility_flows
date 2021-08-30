import psycopg2
import pandas as pd
from pathlib import Path
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import task, Flow, Parameter
from prefect.storage import Docker
from prefect.tasks.notifications.slack_task import SlackTask

import utils.s3 as s3
import utils.redshift as rs

# ----STAGE 1----

@task
def connect_to_s3():
    return s3.create_client()

@task
def download_from_s3(client, bucket, file_name):
    return s3.download_from_s3_to_memory(client, bucket, file_name)

@task
def local_data(file_name):
    file_path = Path(__file__).parent.parent.resolve() / Path(f"data/{file_name}")
    return file_path

# ----STAGE 2----

@task
def create_dataframe(csv_file):
    return pd.read_csv(csv_file)

@task
def transform(df):
    return df

# ----STAGE 3----

@task
def connect_to_rs(dbname):
    conn = rs.create_conn(dbname)
    return conn

@task
def insert_df(client, df, table):
    rs.insert_df(client, df, table)
    return

# ----STAGE 4----
slack_notification = SlackTask(
    message="Everything is alright, alright, alright...",
    webhook_secret="SLACK_WEBHOOK_URL_MHQ",
)

# ----Flow Configurations----
storage = Docker(
    files={
        # absolute path source -> destination in image
        str(Path(__file__).parent.parent.resolve()) / Path("utils/s3.py"): "/modules/s3.py",
        str(Path(__file__).parent.parent.resolve()) / Path("utils/redshift.py"): "/modules/redshift.py",
    },
    env_vars={
        # append modules directory to PYTHONPATH
        "PYTHONPATH": "$PYTHONPATH:modules/"
    },
)


with Flow("S3 to Redshift") as flow:
    # ----STAGE 1----
    conn = connect_to_s3()
    file_to_download = Parameter("File to download")
    s3_bucket = Parameter("S3 Bucket", default="blockbuster")
    downloaded = download_from_s3(conn, s3_bucket, file_to_download)

    # ----STAGE 2----
    # test_data = local_data()
    dataframe = create_dataframe(downloaded)
    transformed = transform(dataframe)

    # ----STAGE 3----
    dbname = Parameter("DBname", default="suppliers")
    redshift = connect_to_rs(dbname)
    insert_df(redshift, transformed, dbname)

    # ----STAGE 4----
    slack_notification()
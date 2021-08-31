import psycopg2
import pandas as pd
import datetime, pendulum
from pathlib import Path

from prefect import task, Flow, Parameter, unmapped
from prefect.storage import Docker
from prefect.tasks.notifications.slack_task import SlackTask
from prefect.artifacts import create_markdown, create_link
from prefect.run_configs import ECSRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

import s3_utils as s3
import redshift_utils as rs

# ----STAGE 1----

@task
def connect_to_s3():
    return s3.create_client()

@task
def download_from_s3(client, bucket, file_name):
    downloaded = s3.download_from_s3_to_memory(client, bucket, file_name)
    return downloaded

@task
def create_bucket_link(bucket, file_path):
    create_link(f"https://{bucket}.s3.us-west-2.amazonaws.com/{file_path}")

@task
def local_data(file_name):
    file_path = Path(__file__).parent.parent.resolve() / Path(f"data/{file_name}")
    return file_path

# ----STAGE 2----

@task
def create_dataframe(csv_file):
    return pd.read_csv(csv_file['Body'])

@task
def transform(df):
    return df

@task
def create_df_artifact(data):
    create_markdown(data)

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
    registry_url="kmoonwright",
    image_name="flows",
    image_tag="aws-s3-to-redshift",
    files={
        # absolute path source -> destination in image
        str(Path(__file__).parent.resolve()) / Path("s3_utils.py"): "/modules/s3_utils.py",
        str(Path(__file__).parent.resolve()) / Path("redshift_utils.py"): "/modules/redshift_utils.py",
    },
    env_vars={
        # append modules directory to PYTHONPATH
        "PYTHONPATH": "$PYTHONPATH:modules/"
    },
    python_dependencies=[
        "python-dotenv",
        "psycopg2-binary",
        "boto3",
        "botocore"
    ],
    ignore_healthchecks=True,
    # only an extreme poweruser should use this ^
)
run_config = ECSRun(
    env={"sample_key": "sample_value"},
    labels=["staging"]
)
schedule = Schedule(
    clocks=[
        CronClock(
            "0 12 * * 1-5", 
            start_date=pendulum.now(tz="US/Pacific"), 
            parameter_defaults={"Table Name": "users"}
        ),
        CronClock(
            "0 12 * * 1-5", 
            start_date=pendulum.now(tz="US/Pacific"),
            parameter_defaults={"Table Name": "events"}
        ),
    ]
)


with Flow(
    "S3 to Redshift",
    storage=storage,
    # run_config=run_config,
    # schedule=schedule
) as flow:
    # ----STAGE 1----
    conn = connect_to_s3()
    file_to_download = Parameter("File to download", default="user_data.csv")
    # s3_buckets = Parameter("S3 Bucket", default=["loading-store-1", "loading-store-2"])
    # downloaded = download_from_s3.map(unmapped(conn), s3_buckets, unmapped(file_to_download))
    # create_bucket_link.map(s3_bucket, unmapped(file_to_download))
    s3_bucket = Parameter("S3 Bucket", default="loading-store-1")
    downloaded = download_from_s3(conn, s3_bucket, file_to_download)
    create_bucket_link(s3_bucket, file_to_download)

    # ----STAGE 2----
    # test_data = local_data()
    dataframe = create_dataframe(downloaded)
    transformed = transform(dataframe)
    create_df_artifact(transformed)

    # ----STAGE 3----
    dbname = Parameter("DB Name", default="suppliers")
    tablename = Parameter("Table Name", default="users")
    redshift = connect_to_rs(dbname)
    insert_df(redshift, transformed, tablename)

    # ----STAGE 4----
    # slack_notification()

# flow.run()
flow.register(project_name="AWS")
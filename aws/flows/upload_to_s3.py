from pathlib import Path
from prefect import task, Flow, Parameter, unmapped
from prefect.storage import Docker
from prefect.run_configs import DockerRun

import s3_utils as s3

BASE_DIR = Path(__file__).parent.parent.resolve()

@task
def create_filepath(filename):
    return Path(filename)

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


storage = Docker(
    registry_url="kmoonwright",
    image_name="flows",
    image_tag="upload-to-s3",
    files={
        # absolute path source -> destination in image
        str(Path(__file__).parent.resolve()) / Path("s3_utils.py"): "/modules/s3_utils.py",
        str(Path(__file__).parent.parent.resolve()) / Path("data/test_data.csv"): "/data/test_data.csv",
        str(Path(__file__).parent.parent.resolve()) / Path("data/user_data.csv"): "/data/user_data.csv",
        str(Path(__file__).parent.parent.resolve()) / Path("data/event_data.csv"): "/data/event_data.csv",
    },
    env_vars={
        # append modules directory to PYTHONPATH
        "PYTHONPATH": "$PYTHONPATH:modules/"
    },
    python_dependencies=[
        "python-dotenv",
        "boto3",
        "botocore",
    ],
    ignore_healthchecks=True,
    # only an extreme poweruser should use this ^
)
run_config = DockerRun(
    env={"sample_key": "sample_value"},
    labels=["docker"],
)

with Flow(
    "Upload to S3", 
    storage=storage, 
    run_config=run_config
) as flow:
    files_to_download = Parameter(
        name="File List", 
        default=["data/test_data.csv", "data/user_data.csv", "data/event_data.csv"]
    )
    conn = connect_to_s3()
    upload_to_s3.map(
        s3_client=unmapped(conn), 
        file_path=create_filepath.map(files_to_download)
    )

# flow.run()
flow.register(project_name="AWS")
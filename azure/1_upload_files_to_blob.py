import os
import datetime
from utilities import logger_helper
from prefect import task, Flow, Parameter, context, case
from prefect.tasks.secrets import EnvVarSecret
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings

# TASK DEFINITIONS
@task
def get_image_container():
    return "incomingdata"
 
@task
def get_path():
    return "/Users/kyle/projects/utility_flows/azure"
 
@task
def start_azure_client(connection):
    return BlobServiceClient.from_connection_string(connection)

@task
def file_count_check():
    logger = context.get("logger")
    if isinstance(1, str):
        logger.info("Uploading multiple files...")
        return False
    else:
        logger.info("Uploading single file...")
        return True

@task
def upload_all_images_in_folder(client, path, container):
    # Get all files with jpg extension and exclude directories
    all_file_names = [f for f in os.listdir(path)
                    if os.path.isfile(os.path.join(path, f)) and ".jpg" in f]

    for file_name in all_file_names:
        logger = context.get("logger")
        logger.info(f"Uploading file - {file_name}")
        client.upload_image(file_name)

@task
def upload_image(client, file_name, container, path):
    blob_client = client.get_blob_client(container=container, blob=f"{file_name}_{datetime.datetime.now()}")
    upload_file_path = os.path.join(path, file_name)
    image_content_setting = ContentSettings(content_type='image/jpeg')
    logger, add_utility = context.get("logger"), logger_helper()
    logger.info(f"Uploading file - {file_name}")

    with open(upload_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True, content_settings=image_content_setting)
    
# FLOW DEFINITIONS
with Flow("Upload to Azure") as flow:
    file_name = Parameter(name="Upload File Name", default="prefect_icon.png")
    file_path = Parameter(name="Upload File Path", default="/Users/kyle/projects/utility_flows/azure")
    connection = EnvVarSecret("BLOB_STORAGE_KEY")
    client = start_azure_client(connection)
    container = get_image_container()

    single_or_multiple = file_count_check()
    with case(single_or_multiple, True):
        upload_image(client=client, file_name=file_name, container=container, path=file_path)
    with case(single_or_multiple, False):
        upload_all_images_in_folder(client=client, container=container, path=file_path)

if __name__ == "__main__":
    flow.register(project_name="Azure")
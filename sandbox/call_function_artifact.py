import prefect
import test_config as tconfig

@prefect.task
def do_stuff():
    tconfig.write_error_df_to_artifact()

with prefect.Flow(
    "Test Flow",
    prefect.storage.GitHub(
        repo="kmoonwright/utility_flows",
        path="sandbox/call_function_artifact.py",
        access_token_secret="GITHUB_ACCESS_TOKEN")
    # storage=prefect.storage.Docker(
    #     registry_url="kmoonwright",
    #     image_name="flows",
    #     image_tag="artifacts-test",
    #     files={
    #     # absolute path source -> destination in image
    #     "/Users/kyle/projects/utility_flows/sandbox/test_config.py": "/sandbox/test_config.py",
    # },
    # env_vars={
    #     # append modules directory to PYTHONPATH
    #     "PYTHONPATH": "$PYTHONPATH:sandbox/"
    # },
) as flow:
    do_stuff()
    
if __name__ == '__main__':
    flow.register(project_name="test-flows")
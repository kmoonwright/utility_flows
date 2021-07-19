import prefect
from sandbox.test_config import write_error_df_to_artifact

@prefect.task
def do_stuff():
    write_error_df_to_artifact()

with prefect.Flow(
    "Test Flow",
    prefect.storage.GitHub(
        repo="kmoonwright/utility_flows",
        path="sandbox/call_function_artifact.py",
        access_token_secret="GITHUB_ACCESS_TOKEN")
) as flow:
    do_stuff()

flow.register(project_name="test-flows")
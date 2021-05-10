import prefect
import os

def logger_helper():
    TOKEN = os.getenv('AZURE_TMP_TOKEN')
    client = prefect.Client(api_token=TOKEN)
    client.login_to_tenant(tenant_slug="km-inc")
    client.graphql(
        """
        mutation {
            create_flow_run(
                input: {
                    flow_id: "28e02bb3-b145-4eca-89a7-8b41281fb9cc",
                }
            ) {
                id
            }
        }
        """
    )
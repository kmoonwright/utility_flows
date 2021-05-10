import prefect
import os
import time

def logger_helper():
    time.sleep(6)
    TOKEN = os.getenv('AZURE_TMP_TOKEN')
    client = prefect.Client(api_token=TOKEN)
    client.login_to_tenant(tenant_slug="km-inc")
    client.graphql(
        """
        mutation {
            create_flow_run(
                input: {
                    flow_id: "61d6071b-cd81-4505-877b-18081c129b4b",
                }
            ) {
                id
            }
        }
        """
    )
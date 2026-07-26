from google.cloud import bigquery
from google.oauth2 import service_account
import os

CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS", "credentials.json")
SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]
def get_bq_client():
    if os.path.exists(CREDENTIALS_PATH):
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH,
            SCOPES
        )
        bq_client = bigquery.Client(credentials=credentials,project=credentials.project_id)
    else:
        bq_client = bigquery.Client()
    project_id = bq_client.project
    return bq_client, project_id

from google.cloud import bigquery
import os

CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS", "credentials.json")
if os.path.exists(CREDENTIALS_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

def get_bq_client():
    bq_client = bigquery.Client()
    project_id = bq_client.project
    return bq_client, project_id

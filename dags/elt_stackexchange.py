from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)
from datetime import datetime, timedelta
from google.oauth2 import service_account
from google.cloud import bigquery
import requests
import boto3
from botocore.client import Config
import os
import io
import json
import gzip


MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadminpassword")
MINIO_CONFIG = {
    "endpoint_url": "http://minio-storage:9000",
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "config": Config(
        signature_version="s3v4",
        connect_timeout=60,  
        read_timeout=60,
    ),
    "region_name": "us-east-1",
}

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2026,7,17),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

# Time Config
yesterday = datetime.now() - timedelta(days=1)
target_date = datetime(yesterday.year, yesterday.month, yesterday.day, 0,0,0)
formatted_hour = target_date.hour

# Format file
formatted_time = target_date.strftime("%Y-%m-%d")
file_name =f"{formatted_time}-{formatted_hour}-stackexchange.json.gz"

# File directory
current_dir = os.path.dirname(os.path.abspath(__file__))
sql_path = os.path.abspath(
    os.path.join(current_dir, "..", "include", "sql")
)

def get_bq_client():
    credentials_path = os.path.join(current_dir, "..", "include","credentials.json")
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    bq_client = bigquery.Client(project=credentials.project_id,credentials=credentials)
    return bq_client, credentials.project_id


def extract_and_load_to_storage(**kwargs):
    SE_API_KEY=os.getenv("STACKEXCHANGE_API_KEY","")
    url=f"https://api.stackexchange.com/2.3/questions"

    params = {
        "site": "stackoverflow",
        "pagesize": 100,        
        "sort": "hot",           
        "key": SE_API_KEY,      
        "filter": "default"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"Failed to extract from API Stack Exchange, status code : {response.status_code}")

    data = response.json()
    raw_items = data.get("items",[])

    ndjson_content = "\n".join([json.dumps(item) for item in raw_items])
    gzip_bytes = gzip.compress(ndjson_content.encode("utf-8"))
    file_data = io.BytesIO(gzip_bytes)

    s3_client = boto3.client('s3',**MINIO_CONFIG)
    s3_client.put_object(
        Bucket="stackexchange-raw-data",
        Key=file_name,
        Body=file_data
    )
    print(f"File {file_name} successfuly loaded")

def load_to_bq_staging(**kwargs):
    s3_client = boto3.client('s3',**MINIO_CONFIG)
    target_obj = s3_client.get_object(Bucket="stackexchange-raw-data",Key=file_name)
    file_content = target_obj["Body"].read()

    bq_client, project_id = get_bq_client()
    table_id = f"{project_id}.stackexchange_analytics.staging_stackexchange_events"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    data_stream = io.BytesIO(file_content)
    load_job = bq_client.load_table_from_file(data_stream,table_id,job_config=job_config)
    load_job.result()
    print("Raw data successfully streamed to BigQuery")


with DAG(
    'elt_stackexchange_pipeline',
    default_args=default_args,
    description="Pipeline ELT StackExchange Analytics",
    schedule=None,
    catchup=False,
    template_searchpath=[sql_path]
) as dag:
    task_extract_stackexchange= PythonOperator(
        task_id="extract_stackexchange_to_storage",
        python_callable=extract_and_load_to_storage,
    )

    task_load_staging_stackexchange = PythonOperator(
        task_id="load_stackexchange_staging_bq",
        python_callable=load_to_bq_staging
    )

    task_transform_stackexchange = BigQueryInsertJobOperator(
        task_id="transform_stackexchange",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query":{
                "query" : "transform_stackexchange.sql",
                "useLegacySql" : False
            }
        }
    )

    task_mart_stackexchange = BigQueryInsertJobOperator(
        task_id="create_mart_stackexchange",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query":{
                "query":"create_mart_stackexchange.sql",
                "useLegacySql": False
            }
        }
    )

task_extract_stackexchange >> task_load_staging_stackexchange >> task_transform_stackexchange >> task_mart_stackexchange
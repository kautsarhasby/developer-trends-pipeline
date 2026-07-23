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
import io
import os
import time
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
file_name =f"{formatted_time}-{formatted_hour}.json.gz"

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
    url=f"https://data.gharchive.org/{file_name}"

    print(f"Downloading from url : {url}")
    response = requests.get(url, stream=True)

    if response.status_code != 200:
        raise Exception(f"Failed to download on {formatted_time}, status code : {response.status_code}")
    
    file_data = io.BytesIO(response.content)
    s3_client = boto3.client('s3',**MINIO_CONFIG)

    s3_client.put_object(
        Bucket="github-raw-data",
        Key=f"github-{file_name}",
        Body=file_data
    )
    print(f"File {file_name} successfuly loaded")

def load_to_bq_staging(**kwargs):
    s3_client = boto3.client('s3',**MINIO_CONFIG)
    target_obj = s3_client.get_object(Bucket="github-raw-data",Key=f"github-{file_name}",)
    file_content = target_obj["Body"].read()
    decompressed = gzip.decompress(file_content)

    wrapped_lines = []
    for line in decompressed.splitlines():
        if not line.strip():
            continue
        event = json.loads(line)
        wrapped_lines.append(json.dumps({"raw_content": event}))

    data_stream = io.BytesIO("\n".join(wrapped_lines).encode("utf-8"))

    bq_client, project_id = get_bq_client()
    table_id = f"{project_id}.github_analytics.staging_github_events"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,
        schema=[
            bigquery.SchemaField("raw_content","JSON")
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    load_job = bq_client.load_table_from_file(data_stream,table_id,job_config=job_config)
    load_job.result()
    print("Raw data successfully streamed to BigQuery")

def fetch_github_api(**kwargs):
    bq_client, project_id = get_bq_client()
    query=f"SELECT repo_name,repo_url FROM `{project_id}.github_analytics.staging_top_repos` LIMIT 20"
    rows = bq_client.query(query).result()

    repo_details = []
    github_token = os.getenv('GITHUB_TOKEN', '')
    headers = {}
    if github_token:
        headers["Authorization"] = f"token {github_token}"

    NON_PRIMARY_LANGS = {"HTML", "CSS", "Tcl", "Shell", "Makefile", "Dockerfile", "TeX"}
    for row in rows:
        repo_name = row.repo_name
        api_url = row.repo_url
        lang_url = f"{api_url}/languages"
        detected_lang = "Unknown"

        try:
            lang_res = requests.get(lang_url,headers=headers)
            if lang_res.status_code == 200:
                lang_data= lang_res.json()
                primary_lang=[l for l in lang_data.keys() if l not in NON_PRIMARY_LANGS]
                if primary_lang:
                    detected_lang = primary_lang[0]
                elif lang_data:
                    detected_lang = list(lang_data.keys())[0] 
                else:
                    detected_lang = "Unknown"

            

            response = requests.get(api_url, headers=headers)
            if response.status_code == 200:
                data = response.json()

                if detected_lang == "Unknown" and data.get("language"):
                    detected_lang = data.get("language")
                    
                repo_details.append({
                    "repo_name": repo_name,
                    "language" : detected_lang,
                    "stargazers_count": data.get("stargazers_count"),
                    "open_issues_count" : data.get("open_issues_count")
                })
        except Exception as e:
            print(f"Error requesting {api_url} : {e}")
        time.sleep(1.5)
        
    table_id = f"{project_id}.github_analytics.staging_repo_details"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = bq_client.load_table_from_json(repo_details,table_id,job_config=job_config)
    job.result()
    print("Metadata Github API successfully loaded to BigQuery")


with DAG(
    'elt_github_global_pipeline',
    default_args=default_args,
    description="Pipeline ELT Global Open Source Analytics",
    schedule=None,
    catchup=False,
    template_searchpath=[sql_path]
) as dag:
    task_extract_github = PythonOperator(
        task_id="extract_gharchive_to_storage",
        python_callable=extract_and_load_to_storage,
    )

    task_load_staging_github = PythonOperator(
        task_id="load_to_bq_staging",
        python_callable=load_to_bq_staging
    )
    
    task_fetch_github_api = PythonOperator(
        task_id="fetch_github_api",
        python_callable=fetch_github_api,
    )

    task_filter_top_repo = BigQueryInsertJobOperator(
        task_id="filter_top_repos",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query" : {
                "query" : "filter_top_repos.sql",
                "useLegacySql": False
            }
        }
    )

    task_transform_github = BigQueryInsertJobOperator(
        task_id="transform_data_bq",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query":{
                "query":"transform_github_data.sql",
                "useLegacySql": False
            }
        }
    )
    task_mart_github = BigQueryInsertJobOperator(
        task_id="create_mart_github",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query":{
                "query":"create_mart_github.sql",
                "useLegacySql": False
            }
        }
    )

task_extract_github >> task_load_staging_github >> task_filter_top_repo >> task_fetch_github_api >> task_transform_github >> task_mart_github 
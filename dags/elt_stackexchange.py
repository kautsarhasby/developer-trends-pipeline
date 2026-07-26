from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)
from utils.bq_client import get_bq_client
from utils.gcs_client import (upload_to_gcs,load_from_gcs)
from datetime import datetime, timedelta
from google.cloud import bigquery
import requests
import os
import io
import json
import gzip


default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2026,7,17),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

# File directory
current_dir = os.path.dirname(os.path.abspath(__file__))
sql_path = os.path.abspath(
    os.path.join(current_dir, "..", "include", "sql")
)

def extract_and_load_to_storage(**kwargs):
    logical_date = kwargs.get("logical_date") or (datetime.now() - timedelta(days=1))
    logical_date = kwargs.get("logical_date") or (
        datetime.now() - timedelta(days=1)
    )
    formatted_time = logical_date.strftime("%Y-%m-%d")
    formatted_hour = logical_date.hour
    file_name = f"{formatted_time}-{formatted_hour}-stackexchange.json.gz"
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

    upload_to_gcs(file_data,f"raw/stackexchange/{file_name}")
    print(f"File {file_name} successfuly loaded")

def load_to_bq_staging(**kwargs):
    logical_date = kwargs.get("logical_date") or (datetime.now() - timedelta(days=1))
    logical_date = kwargs.get("logical_date") or (
            datetime.now() - timedelta(days=1)
        )
    formatted_time = logical_date.strftime("%Y-%m-%d")
    formatted_hour = logical_date.hour
    file_name = f"{formatted_time}-{formatted_hour}-stackexchange.json.gz"
    blob_name = f"github-{file_name}"
    file_bytes = load_from_gcs(blob_name)
    bq_client, project_id = get_bq_client()
    table_id = f"{project_id}.stackexchange_analytics.staging_stackexchange_events"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    data_stream = io.BytesIO(file_bytes)
    load_job = bq_client.load_table_from_file(data_stream,table_id,job_config=job_config)
    load_job.result()
    print("Raw data successfully streamed to BigQuery")


with DAG(
    'elt_stackexchange_pipeline',
    default_args=default_args,
    description="Pipeline ELT StackExchange Analytics",
    schedule='@daily',
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
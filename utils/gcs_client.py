import os
from google.cloud import storage
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GCS_CREDENTIALS","")

storage_client = storage.Client()
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME","elt-data")

def upload_to_gcs(source_file_path,destination_blob_name):
    try:
        bucket = storage_client.bucket(bucket_name=BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)

        if hasattr(source_file_path, 'read'):
            source_file_path.seek(0)
            blob.upload_from_file(source_file_path)
        else:
            blob.upload_from_filename(source_file_path)
        print(f"File {source_file_path} succeed add uploaded to GCS")
    except Exception as e:
        print(f"Failed to upload to GCS")
        raise e

def load_from_gcs(source_blob_name):
    try:
        bucket = storage_client.bucket(bucket_name=BUCKET_NAME)
        blob = bucket.blob(source_blob_name)
        content = blob.download_as_bytes()
        print(f"File {source_blob_name} succeed load from GCS")
        return content
    except Exception as e:
        print(f"Failed load from GCS")
        raise e
    

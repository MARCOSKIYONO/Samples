import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import json
from bson import json_util

def upload_csv(PROJECT_ID: str, CREDENTIALS: dict, bucket_name: str, file_name: str, df: pd.DataFrame):
    if CREDENTIALS == None:
        storage_client = storage.Client(project=PROJECT_ID)
    else:
        storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=service_account.Credentials.from_service_account_info(
        CREDENTIALS)
        )

    bucket = storage_client.bucket(bucket_name)
    bucket.blob(file_name).upload_from_string(
        df.to_csv(index=False),
        'text/csv'
    )
    print(f'Document uploaded {file_name} in {bucket_name}')
    return


def upload_json(PROJECT_ID: str, CREDENTIALS: dict, bucket_name: str, file_name: str, data: dict):
    if CREDENTIALS == None:
        storage_client = storage.Client(project=PROJECT_ID)
    else:
        storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=service_account.Credentials.from_service_account_info(
        CREDENTIALS)
        )

    bucket = storage_client.bucket(bucket_name)
    bucket.blob(file_name).upload_from_string(
        json.dumps(data, default=json_util.default),
        'text/son'
    )
    print(f'Document uploaded {file_name} in {bucket_name}')
    return


def download_csv_string(PROJECT_ID: str, CREDENTIALS: dict, bucket_name: str, file_name: str):
    if CREDENTIALS == None:
        storage_client = storage.Client(project=PROJECT_ID)
    else:
        storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=service_account.Credentials.from_service_account_info(
        CREDENTIALS)
        )

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = (blob.download_as_string())
    print(f'Document downloaded {file_name} from {bucket_name}')
    return data


def download_csv_stringIO(PROJECT_ID: str, CREDENTIALS: dict, bucket_name: str, file_name: str):
    if CREDENTIALS == None:
        storage_client = storage.Client(project=PROJECT_ID)
    else:
        storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=service_account.Credentials.from_service_account_info(
        CREDENTIALS)
      )

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = io.BytesIO((blob.download_as_string()))
    print(f'Document downloaded {file_name} from {bucket_name}')
    return data
  
def copy_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
    """Copies a blob from one bucket to another with a new name."""

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )

    print(
        "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )  
    
def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print("Blob {} deleted.".format(blob_name))    
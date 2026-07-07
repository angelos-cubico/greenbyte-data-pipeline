import truststore
truststore.inject_into_ssl()

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv("API_key.env")

CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

blob_service = BlobServiceClient.from_connection_string(CONN_STR)

def upload_file(file_path, container_name, blob_name=None):

    if blob_name is None:
        blob_name = os.path.basename(file_path)

    with open(file_path, "rb") as data:
        blob_service.get_blob_client(
            container=container_name,
            blob=blob_name
        ).upload_blob(data, overwrite=True)

    print(f"Uploaded: {blob_name}")
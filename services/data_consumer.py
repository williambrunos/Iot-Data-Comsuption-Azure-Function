import json
import os
from azure.storage.blob import BlobServiceClient, ContainerClient
import logging
import re
import base64

logger = logging.getLogger(__name__)

def get_data_from_storage_account(year: int, month: int, day: int) -> dict:
    """Get data from Azure Storage Account
    
    Keyword arguments:
    year -- year
    month -- month 
    day -- day
    Return: return_payload -- dictionary with the data
    """
    
    data_list = []
    container_name = 'messages'
    conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    if not conn_str:
        logger.error('AZURE_STORAGE_CONNECTION_STRING environment variable not set')
        return {}

    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service_client.get_container_client(container_name)
        sub_folder_name = f'azr-iot-hub/00/{year}/{month:02}/{day:02}'
        
        blob_list = container_client.list_blobs(name_starts_with=sub_folder_name)
        
        for blob in blob_list:
            logger.info(f'Reading blob: {blob.name}')
            blob_client = container_client.get_blob_client(blob.name)
            data = blob_client.download_blob().readall().decode('utf-8')
            json_objects = re.findall(r'\{.*?\}(?=\s*\{|\s*$)', data, re.DOTALL)
            
            for json_object in json_objects:                
                logger.info(f'Processing JSON object: {json_object}')
                try:
                    json_data = json.loads(json_object)
                    body_decoded = base64.b64decode(json_data["Body"]).decode('utf-8')
                    json_data["Body"] = json.loads(body_decoded)
                    data_list.append(json_data)
                except json.JSONDecodeError as e:
                    logger.error(f'Error decoding JSON object from blob {blob.name}: {e}')
        
    except Exception as e:
        logger.error(f'Error occurred: {e}')
        return {}

    return_payload = {
        'history': data_list
    }
    
    return return_payload
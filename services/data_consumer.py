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


def get_most_recent_data_from_storage_account() -> dict:
    """Get the most recent data from Azure Storage Account
    
    Return: return_payload -- dictionary with the most recent data
    """
    
    container_name = 'messages'
    conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    if not conn_str:
        logger.error('AZURE_STORAGE_CONNECTION_STRING environment variable not set')
        return {}

    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service_client.get_container_client(container_name)
        
        blob_list = container_client.list_blobs()
        sorted_blobs = sorted(blob_list, key=lambda b: b.last_modified, reverse=True)
        
        if not sorted_blobs:
            logger.info('No blobs found in the container')
            return {}

        most_recent_blob = sorted_blobs[0]
        logger.info(f'Reading most recent blob: {most_recent_blob.name}')
        blob_client = container_client.get_blob_client(most_recent_blob.name)
        data = blob_client.download_blob().readall().decode('utf-8')
        
        # Use regular expressions to find all JSON objects in the blob content
        json_objects = re.findall(r'\{.*?\}(?=\s*\{|\s*$)', data, re.DOTALL)
        
        if not json_objects:
            logger.info('No JSON objects found in the most recent blob')
            return {}

        # Process the most recent JSON object
        most_recent_json_object = json_objects[-1]
        try:
            logger.info(f'Processing JSON object: {most_recent_json_object}')
            json_data = json.loads(most_recent_json_object)
            # Decode the base64 encoded Body field
            body_decoded = base64.b64decode(json_data["Body"]).decode('utf-8')
            json_data["Body"] = json.loads(body_decoded)
            return_payload = {
                'mostRecentData': json_data
            }
            return return_payload
        except json.JSONDecodeError as e:
            logger.error(f'Error decoding JSON object: {e}')
            logger.error(f'JSON content: {most_recent_json_object}')
            return {}
        except Exception as e:
            logger.error(f'Error processing blob {most_recent_blob.name}: {e}')
            return {}

    except Exception as e:
        logger.error(f'Error occurred: {e}')
        return {}
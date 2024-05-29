import os
import csv
import logging
import boto3
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Extract bucket name and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    processed_key = f'processed/{object_key}'

    # Download the file from S3
    csv_content = download_s3_file(bucket_name, object_key)
    
    # Extract subscriber IDs
    sub_ids = extract_sub_ids_from_csv(csv_content)
    
    # Process subscriber IDs concurrently
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_sub_id, sub_id): sub_id for sub_id in sub_ids}
        for future in futures:
            sub_id = futures[future]
            try:
                future.result()
            except Exception as exc:
                logging.error(f"Error processing sub_id {sub_id}: {exc}")

    # Upload the file back to S3
        copy_and_delete_file(bucket_name, object_key, processed_key)
    
    return {
        'statusCode': 200,
        'body': f'Processed file {processed_key} uploaded to bucket {bucket_name}'
    }

def download_s3_file(bucket_name, object_key):
    try:
        s3_response = s3.get_object(Bucket=bucket_name, Key=object_key)
        csv_content = s3_response['Body'].read().decode('utf-8').splitlines()
        return csv_content
    except Exception as e:
        logging.error(f"Error downloading S3 file: {e}")
        raise

def extract_sub_ids_from_csv(csv_content):
    logging.info("J1ExtractStart")
    sub_ids = []
    
    current_date_format = datetime.now().strftime("%Y%m%d")

    try:
        csvreader = csv.reader(csv_content)
        for row in csvreader:
            if current_date_format in row[0]:  
                sub_id = row[0].strip()
                sub_ids.append(sub_id)
    except Exception as e:
        logging.error(f"Error reading or processing the files: {e}")
    
    return sub_ids

def process_sub_id(sub_id):
    executecom = refresh(sub_id)
    logging.info(f"Packet Made for Request JONE {executecom}")
    resp = send(executecom)
    logging.info(f"Connection Done {resp['request']}")
    hard_resp = {
        'details': resp,
        'status': 'True'
    }
    logging.info(f"RefreshJOne : {hard_resp['status']}")
    if hard_resp['status'] == 'True':
        hard_resp['statusMessage'] = 'Successful'
    else:
        hard_resp['statusMessage'] = 'Failed'
    logging.info(f"Processed sub_id: {sub_id}")

def refresh(subscriber_id):
    fixed_part = "0003M00410001010100"
    variable_part = datetime.now().strftime("%Y%m%d%H%M%S")
    action = "V"
    command = f"{fixed_part}{variable_part}0001SIN{subscriber_id}{action}".strip()
    logging.info(f"RefreshJOne Command Made: {command}")
    return command



def copy_and_delete_file(bucket_name, source_key, destination_key):
    try:
        # Copy the object to the new location
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)
        logging.info(f"File copied to: {destination_key}")
        
        # Delete the original object
        s3.delete_object(Bucket=bucket_name, Key=source_key)
        logging.info(f"Original file deleted: {source_key}")
    except Exception as e:
        logging.error(f"Error copying and deleting file in S3: {e}")
        raise  # Re-throw the exception
    
def send(packet):
    api_url = "http://your-ec2-instance-url/api-endpoint"
    
    try:
        # Make the HTTP GET request to the API
        response = requests.get(api_url, params={'packet': packet})
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Get the response content
        response_content = response.json()
        
        # Log the response content
        logging.info(f"Response from API: {response_content}")
        
        return {
            'statusCode': 200,
            'request': packet,
            'response': response_content
        }
    except requests.exceptions.RequestException as e:
        # Log the error
        logging.error(f"Error making request to API: {str(e)}")
        
        return {
            'statusCode': 500,
            'request': packet,
            'response': str(e)
        }
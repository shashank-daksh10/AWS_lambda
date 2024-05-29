import os
import csv
import logging
import boto3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize S3 client
s3 = boto3.client('s3')


def lambda_handler(event, context):
    # Extract bucket name and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    # Temporary local file paths
    download_path = f'/tmp/{object_key}'
    processed_key = f'processed/{object_key}'
    upload_path = f'/tmp/processed_{object_key}'

    # Download the CSV file from S3
    s3.download_file(bucket_name, object_key, download_path)

    # Extract subscriber IDs
    sub_ids = extract_sub_ids_from_csv(download_path)

    # Process subscriber IDs concurrently
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_sub_id, sub_id): sub_id for sub_id in sub_ids}
        for future in futures:
            sub_id = futures[future]
            try:
                future.result()
            except Exception as exc:
                logging.error(f"Error processing sub_id {sub_id}: {exc}")

    # Upload processed file to S3
    s3.upload_file(download_path, bucket_name, processed_key)

    return {
        'statusCode': 200,
        'body': f'Processed file {processed_key} uploaded to bucket {bucket_name}'
    }


def extract_sub_ids_from_csv(file_path):
    logging.info("J1ExtractStart")
    sub_ids = []

    current_date_format = datetime.now().strftime("%Y%m%d")

    try:
        with open(file_path, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                if current_date_format in row[0]:  # Assuming the date is part of the first column
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


def send(packet):
    return

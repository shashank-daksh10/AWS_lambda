import json
import boto3
import logging
from datetime import datetime

# Initialize the S3 client
s3 = boto3.client('s3')
logging.basicConfig(level=logging.INFO)

def lambda_handler(event, context):
    # Extract the VC number from the event
    vcNum = event.get('vcNum', '')

    # Validate the VC number length
    if len(vcNum) not in [11, 12]:
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid VC number format')
        }
    if len(vcNum) == 12:
        vcNum = vcNum[:11]

    logging.info("AMSCheck_Hit")
    logging.info("VC Number Received: %s", vcNum)
    logging.info("Adjusted VC Number: %s", vcNum)

    # Define S3 bucket name and object key path
    bucket_name = 'your-bucket-name'
    current_date = datetime.now().strftime("%Y%m%d")
    object_key = f'VCNUM/DAILY_AMSRECORDS_{current_date}.csv'

    # Read CSV file from S3
    csv_content = read_csv_from_s3(bucket_name, object_key)

    # Check if VC number is present in the CSV 
    is_present = check_vc_number_in_csv(csv_content, vcNum)

    # Prepare the response
    response = {
        'status': is_present,
        'statusMessage': 'Successful' if is_present else 'Failed',
        'details': {}
    }
    
    logging.info("VCNumber Status: %s", response['statusMessage'])
    
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }

def read_csv_from_s3(bucket_name, object_key):
    try:
        # Retrieve the CSV file from S3
        obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        # Read and decode the file content
        csv_content = obj['Body'].read().decode('utf-8').splitlines()
        return csv_content
    except Exception as e:
        logging.error(f"Error reading CSV from S3: {e}")
        return []

def check_vc_number_in_csv(csv_content, vcNum):
    # Iterate through the CSV lines and check for the VC number
    for line in csv_content:
        if vcNum in line:
            logging.info(f"The VCNumber is present and response sent: {vcNum}")
            return True 
    
    logging.info("VC Number is not present in the CSV")
    return False

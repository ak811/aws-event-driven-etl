import boto3
import csv
import io
from datetime import datetime, timedelta
import urllib.parse

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Lambda triggered by S3 event.")
    
    # Get the S3 bucket and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key_from_event = event['Records'][0]['s3']['object']['key']  
    raw_key = urllib.parse.unquote_plus(key_from_event, encoding='utf-8')
    file_name = raw_key.split('/')[-1]

    print(f"Incoming file: {raw_key}")
    
    try:
        # Download raw CSV from S3
        response = s3.get_object(Bucket=bucket_name, Key=raw_key)
        raw_csv = response['Body'].read().decode('utf-8').splitlines()
        print(f"Successfully read file from S3: {file_name}")
    except Exception as e:
        print(f"Error reading file from S3: {e}")
        raise e

    reader = csv.DictReader(raw_csv)
    filtered_rows = []
    original_count = 0
    filtered_out_count = 0
    cutoff_date = datetime.now() - timedelta(days=30)

    print("Processing records...")
    for row in reader:
        original_count += 1
        order_status = row['Status'].strip().lower()
        order_date = datetime.strptime(row['OrderDate'], "%Y-%m-%d")

        # Check if the order should be kept
        if order_status not in ['pending', 'cancelled'] or order_date > cutoff_date:
            filtered_rows.append(row)
        else:
            filtered_out_count += 1

    print(f"Total records processed: {original_count}")
    print(f"Records filtered out: {filtered_out_count}")
    print(f"Records kept: {len(filtered_rows)}")

    # Write the filtered rows to memory
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
    writer.writeheader()
    writer.writerows(filtered_rows)

    # Save to processed/ folder
    processed_key = f"processed/filtered_{file_name}"
    
    try:
        s3.put_object(Bucket=bucket_name, Key=processed_key, Body=output.getvalue())
        print(f"Filtered file successfully written to S3: {processed_key}")
    except Exception as e:
        print(f"Error writing filtered file to S3: {e}")
        raise e

    return {
        'statusCode': 200,
        'body': f"Filtered {len(filtered_rows)} rows and saved to {processed_key}"
    }


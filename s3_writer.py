import boto3
import json
import datetime
import os

class S3Writer:
    def __init__(self, bucket_name, prefix=""):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.s3 = boto3.client('s3')

    def upload(self, data, data_type, created_at):
        """
        Uploads data to S3 with partitioning key.
        Partition format: s3://<bucket>/<prefix>/raw/<data_type>/dt=<YYYY-MM-DD>/<filename>
        """
        # Parse created_at to get the date for partitioning
        if isinstance(created_at, str):
            dt = created_at.split('T')[0] # Assumes ISO format YYYY-MM-DD...
        else:
            # Fallback to today if parsing fails or invalid type
            dt = datetime.datetime.now().strftime('%Y-%m-%d')

        partition_key = f"dt={dt}"
        
        # Generate a unique filename or use ID if available (assuming data is a dict)
        item_id = data.get('id', 'unknown')
        filename = f"{data_type}_{item_id}.json"
        
        key = f"{self.prefix}/raw/{data_type}/{partition_key}/{filename}"
        # Clean up path separators
        key = key.replace('//', '/')
        if key.startswith('/'):
            key = key[1:]

        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            # print(f"Uploaded {key}")
        except Exception as e:
            print(f"Failed to upload {key}: {e}")

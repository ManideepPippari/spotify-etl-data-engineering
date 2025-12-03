import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from config import S3_BUCKET_NAME, S3_PROCESSED_PREFIX

def upload_to_s3(local_path: str = "tracks_transformed.csv") -> str:
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File not found locally: {local_path}")

    s3 = boto3.client("s3")
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_name = f"tracks_transformed_{timestamp}.csv"
    s3_key = f"{S3_PROCESSED_PREFIX}/{file_name}"

    try:
        print(f"Uploading to s3://{S3_BUCKET_NAME}/{s3_key} ...")
        s3.upload_file(local_path, S3_BUCKET_NAME, s3_key)
        print("Upload successful!")
        return s3_key
    except ClientError as e:
        print("Upload failed:", e)
        raise

if __name__ == "__main__":
    key = upload_to_s3()
    print(f"Uploaded file S3 key: {key}")
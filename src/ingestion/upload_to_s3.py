import os
import boto3

from config import S3_BUCKET_NAME, S3_PROCESSED_PREFIX

# Re-use a single S3 client
s3_client = boto3.client("s3")


def upload_csv_to_s3(
    local_csv_path: str,
    bucket: str | None = None,
    key: str | None = None,
) -> str:
    """
    Upload a local CSV file to S3.

    If bucket/key are not provided, it will use:
      - S3_BUCKET_NAME from src.config
      - S3_PROCESSED_PREFIX + filename
    Returns the full s3://... uri.
    """
    if bucket is None:
        bucket = S3_BUCKET_NAME

    if key is None:
        filename = os.path.basename(local_csv_path)
        key = f"{S3_PROCESSED_PREFIX}/{filename}"

    print(f"▶ Uploading {local_csv_path} -> s3://{bucket}/{key}")
    s3_client.upload_file(local_csv_path, bucket, key)
    print(f"✅ Uploaded to s3://{bucket}/{key}")

    return f"s3://{bucket}/{key}"
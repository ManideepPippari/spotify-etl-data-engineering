import boto3
import csv
import io
import os
import urllib.parse
from datetime import datetime

s3 = boto3.client("s3")

# These will come from Lambda environment variables
BUCKET_NAME = os.environ.get("BUCKET_NAME", "mani-spotify-etl-data")
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "spotify/processed/")
TRANSFORMED_PREFIX = os.environ.get("TRANSFORMED_PREFIX", "spotify/transformed/")


def transform_rows(rows):
    """
    Take an iterable of CSV rows (dicts), add derived columns,
    and yield transformed rows.
    """
    load_ts = datetime.utcnow().isoformat()

    for row in rows:
        # Convert duration_ms to minutes (float with 2 decimals)
        duration_ms_str = row.get("duration_ms") or "0"
        try:
            duration_ms = int(float(duration_ms_str))
            duration_min = round(duration_ms / 60000.0, 2)
        except ValueError:
            duration_min = None

        row["duration_min"] = duration_min
        row["load_timestamp_utc"] = load_ts

        yield row


def lambda_handler(event, context):
    """
    Entry point for Lambda. Triggered by S3 PUT event on
    mani-spotify-etl-data / spotify/processed/*.csv
    """
    print("Received event:", event)

    # Support multiple records, but we usually care about the first one
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        print(f"Processing object: s3://{bucket}/{key}")

        # Ignore anything that isn't our processed prefix (safety)
        if not key.startswith(PROCESSED_PREFIX) or not key.endswith(".csv"):
            print("Skipping key (not in processed prefix or not a CSV):", key)
            continue

        # 1) Download the CSV from S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")

        # 2) Read CSV into DictReader
        input_buffer = io.StringIO(body)
        reader = csv.DictReader(input_buffer)

        # 3) Transform rows
        transformed_rows = list(transform_rows(reader))

        if not transformed_rows:
            print("No rows found in input CSV, skipping.")
            continue

        # 4) Prepare output CSV
        output_buffer = io.StringIO()

        # Include all original columns + new ones
        fieldnames = list(transformed_rows[0].keys())
        writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transformed_rows)

        # 5) Decide output key in transformed folder
        base_name = os.path.basename(key)  # e.g., tracks_from_airflow.csv
        name_without_ext, _ = os.path.splitext(base_name)
        out_key = f"{TRANSFORMED_PREFIX}{name_without_ext}_transformed.csv"

        # 6) Upload back to S3
        s3.put_object(
            Bucket=bucket,
            Key=out_key,
            Body=output_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )

        print(f"✅ Wrote transformed file to s3://{bucket}/{out_key}")

    return {"status": "ok"}
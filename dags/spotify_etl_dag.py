from datetime import datetime, timedelta
import os
import sys
import time
import json   # 👈 NEW

import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.http.hooks.http import HttpHook  # 👈 NEW

# ────────────────────────────────────────────────────────────
#  Make your project importable inside the container
#  Container layout:
#    /opt/airflow/dags/spotify_etl_dag.py  ← this file
#    /opt/airflow/src/...                  ← your src package
# ────────────────────────────────────────────────────────────
DAGS_DIR = os.path.dirname(__file__)                     # /opt/airflow/dags
PROJECT_ROOT = os.path.abspath(os.path.join(DAGS_DIR, ".."))   # /opt/airflow
SRC_DIR = os.path.join(PROJECT_ROOT, "src")              # /opt/airflow/src

for path in (PROJECT_ROOT, SRC_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

# ✅ Import using ingestion.* (because src is on sys.path and
#    your code lives in src/ingestion/*.py)
from ingestion.extract_local import extract
from ingestion.upload_to_s3 import upload_csv_to_s3

# ────────────────────────────────────────────────────────────
#  CONFIG VALUES
# ────────────────────────────────────────────────────────────

# S3 config
S3_BUCKET_NAME = "mani-spotify-etl-data"
S3_PROCESSED_PREFIX = "spotify/processed"

# Glue crawler config
GLUE_CRAWLER_NAME = "spotify-etl-crawler"

# Athena config
ATHENA_WORKGROUP = "spotify-analytics-wg"
ATHENA_DATABASE = "spotify_etl_db"
ATHENA_OUTPUT_LOCATION = "s3://mani-spotify-etl-data/spotify/athena-results/"

# ✅ Athena validation query – counts rows + DISTINCT album_name
ATHENA_VALIDATION_QUERY = """
SELECT
  COUNT(*) AS athena_rows,
  COUNT(DISTINCT album_name) AS athena_albums
FROM processed;
"""

# AWS region
AWS_REGION = "us-east-2"


# ────────────────────────────────────────────────────────────
#  PYTHON CALLABLES
# ────────────────────────────────────────────────────────────

def run_spotify_extract(**context):
    """
    Run local Spotify extract and write CSV to container.
    """
    df = extract()

    output_dir = "/opt/airflow/dags/output"
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "tracks_raw_from_airflow.csv")
    df.to_csv(output_path, index=False)
    print(f"✅ Saved {len(df)} rows to {output_path}")

    context["ti"].xcom_push(key="local_csv_path", value=output_path)


def run_upload_to_s3(**context):
    """
    Read CSV path from XCom and upload to S3 under spotify/processed/.
    """
    ti = context["ti"]
    local_csv_path = ti.xcom_pull(
        key="local_csv_path",
        task_ids="extract_spotify_tracks",
    )
    if not local_csv_path:
        raise ValueError("No CSV path found in XCom from extract_spotify_tracks")

    s3_key = f"{S3_PROCESSED_PREFIX}/tracks_from_airflow.csv"

    upload_csv_to_s3(
        local_csv_path=local_csv_path,
        bucket=S3_BUCKET_NAME,
        key=s3_key,
    )
    print(f"✅ Uploaded {local_csv_path} to s3://{S3_BUCKET_NAME}/{s3_key}")


def run_glue_crawler_boto3(**context):
    """
    Start existing Glue crawler that points at spotify/processed/ in S3.
    """
    glue = boto3.client("glue", region_name=AWS_REGION)

    print(f"▶ Starting Glue crawler '{GLUE_CRAWLER_NAME}' in region '{AWS_REGION}'")
    glue.start_crawler(Name=GLUE_CRAWLER_NAME)

    while True:
        status = glue.get_crawler(Name=GLUE_CRAWLER_NAME)["Crawler"]["State"]
        print(f"   Glue crawler state = {status}")
        if status in ["READY", "STOPPING"]:
            break
        time.sleep(10)

    print("✅ Glue crawler finished / ready again.")


def run_athena_validation(**context):
    """
    Run Athena query that returns:
      - athena_rows (tracks)
      - athena_albums (distinct album_name)
    and push both to XCom.
    """
    athena = boto3.client("athena", region_name=AWS_REGION)

    print(f"▶ Running Athena validation query on DB '{ATHENA_DATABASE}'")

    response = athena.start_query_execution(
        QueryString=ATHENA_VALIDATION_QUERY,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
        WorkGroup=ATHENA_WORKGROUP,
    )

    query_execution_id = response["QueryExecutionId"]
    print(f"   Athena QueryExecutionId = {query_execution_id}")

    # Wait for completion
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = result["QueryExecution"]["Status"]["State"]
        print(f"   Athena query state = {state}")
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(3)

    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena query did not succeed. Final state = {state}")

    # Get first data row (row 0 = header)
    results = athena.get_query_results(QueryExecutionId=query_execution_id)
    rows = results["ResultSet"]["Rows"]
    data_row = rows[1]["Data"]

    total_rows = int(data_row[0]["VarCharValue"])
    total_albums = int(data_row[1]["VarCharValue"])

    print(f"✅ Athena rows = {total_rows}, albums = {total_albums}")

    ti = context["ti"]
    ti.xcom_push(key="athena_row_count", value=total_rows)
    ti.xcom_push(key="athena_album_count", value=total_albums)


def trigger_databricks_job(**context):
    """
    Trigger the Databricks job that loads bronze/silver/gold.
    Uses the HTTP connection databricks_spotify.
    """

    DATABRICKS_JOB_ID = int(os.environ["DATABRICKS_JOB_ID"])

    hook = HttpHook(http_conn_id="databricks_spotify", method="POST")

    # Get token from Airflow connection
    conn = hook.get_connection(hook.http_conn_id)
    token = conn.password

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payload = {"job_id": DATABRICKS_JOB_ID}

    response = hook.run(
        endpoint="/api/2.1/jobs/run-now",
        headers=headers,
        data=json.dumps(payload),
    )

    print(f"▶ Databricks run-now status: {response.status_code}")
    print(f"   Response: {response.text}")


# ────────────────────────────────────────────────────────────
#  DATA QUALITY HELPERS
# ────────────────────────────────────────────────────────────

def get_snowflake_gold_count(**context):
    """
    Query Snowflake GOLD table:
      - gold_tracks (sum of TRACK_COUNT)
      - gold_albums (DISTINCT ALBUM_NAME)
    and push both to XCom.
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_spotify")

    sql = """
        SELECT
            SUM(TRACK_COUNT) AS gold_tracks,
            COUNT(DISTINCT ALBUM_NAME) AS gold_albums
        FROM SPOTIFY_TRACKS_GOLD;
    """

    records = hook.get_first(sql)
    gold_tracks = int(records[0])
    gold_albums = int(records[1])

    print(f"✅ Snowflake GOLD tracks = {gold_tracks}, albums = {gold_albums}")

    ti = context["ti"]
    ti.xcom_push(key="snowflake_gold_track_count", value=gold_tracks)
    ti.xcom_push(key="snowflake_gold_album_count", value=gold_albums)


def compare_athena_vs_snowflake(**context):
    """
    Data quality rule:

    - Athena = raw / processed layer (more albums)
    - Snowflake GOLD = curated album-level layer (subset of albums)

    Rule:
      1) GOLD albums must be > 0
      2) GOLD albums must be <= Athena albums
    """
    ti = context["ti"]

    athena_rows = ti.xcom_pull(
        task_ids="run_athena_validation", key="athena_row_count"
    )
    athena_albums = ti.xcom_pull(
        task_ids="run_athena_validation", key="athena_album_count"
    )
    gold_tracks = ti.xcom_pull(
        task_ids="get_snowflake_gold_count", key="snowflake_gold_track_count"
    )
    gold_albums = ti.xcom_pull(
        task_ids="get_snowflake_gold_count", key="snowflake_gold_album_count"
    )

    print(
        f"🔎 Counts → Athena: rows={athena_rows}, albums={athena_albums} | "
        f"Snowflake GOLD: tracks={gold_tracks}, albums={gold_albums}"
    )

    if gold_albums is None or gold_albums <= 0:
        raise ValueError("❌ GOLD has no albums – check Snowflake pipeline.")

    if gold_albums > athena_albums:
        raise ValueError(
            f"❌ DQ failed: GOLD albums ({gold_albums}) > Athena albums ({athena_albums})"
        )

    print("✅ DQ passed: GOLD album count is a valid subset of Athena.")


# ────────────────────────────────────────────────────────────
#  DAG DEFINITION
# ────────────────────────────────────────────────────────────

default_args = {
    "owner": "manideep",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email": ["Mani.pippari@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="spotify_local_etl",
    default_args=default_args,
    description="Local Spotify extract orchestrated by Airflow",
    start_date=datetime(2025, 11, 27),
    schedule_interval=None, 
    catchup=False,
    max_active_runs=1,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_spotify_tracks",
        python_callable=run_spotify_extract,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=run_upload_to_s3,
        provide_context=True,
    )

    glue_task = PythonOperator(
        task_id="run_glue_crawler",
        python_callable=run_glue_crawler_boto3,
        provide_context=True,
    )

    athena_task = PythonOperator(
        task_id="run_athena_validation",
        python_callable=run_athena_validation,
        provide_context=True,
    )

    refresh_spotify_gold = SnowflakeOperator(
        task_id="refresh_spotify_gold",
        snowflake_conn_id="snowflake_spotify",
        sql=[
            "USE DATABASE SPOTIFY_ETL_DB;",
            "USE SCHEMA PUBLIC;",
            "CALL SPOTIFY_REFRESH_GOLD();",
        ],
    )

    get_gold_count_task = PythonOperator(
        task_id="get_snowflake_gold_count",
        python_callable=get_snowflake_gold_count,
        provide_context=True,
    )

    dq_compare_task = PythonOperator(
        task_id="compare_athena_vs_snowflake",
        python_callable=compare_athena_vs_snowflake,
        provide_context=True,
    )

    databricks_bronze_silver_gold = PythonOperator(
        task_id="run_databricks_bronze_silver_gold",
        python_callable=trigger_databricks_job,
        provide_context=True,
    )

    # ── Orchestration ───────────────────────────────────────

    # 1) Spotify → local CSV
    extract_task >> upload_task

    # 2) After upload:
    #    a) Trigger Databricks bronze/silver/gold
    #    b) Run Glue → Athena → Snowflake → DQ
    upload_task >> databricks_bronze_silver_gold
    upload_task >> glue_task >> athena_task \
        >> refresh_spotify_gold >> get_gold_count_task >> dq_compare_task
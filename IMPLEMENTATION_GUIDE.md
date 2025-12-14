1. Project Overview

This project implements an end-to-end data pipeline using Spotify data, covering:

 ingestion from the Spotify API

 storage in AWS S3

 transformation and validation using Python and SQL

 optional processing via Databricks and Snowflake

 analytics and visualization using Power BI

The pipeline is intentionally designed to demonstrate multiple real-world patterns:

 local development vs production ingestion

 serverless ingestion (AWS Lambda)

 batch orchestration (Airflow)

 cloud query engines (Athena, Snowflake)

 BI-ready curated outputs

In real data platforms, these approaches often coexist rather than replacing one another.


2. Repository Structure (High Level)

spotify-etl-data-engineering/
│
├── src/                # Core Python logic (extract, transform, upload)
├── lambda/             # AWS Lambda handlers for ingestion & transformation
├── dags/               # Airflow DAG for orchestration and validation
├── s3/                 # S3 folder structure reference
├── glue/               # Glue crawler definitions and notes
├── athena/             # Athena validation queries
├── databricks/         # Databricks notebooks / outputs (optional path)
├── snowflake/          # Snowflake DDL and validation SQL
├── powerbi/            # Power BI dashboard, data extracts, screenshots
├── architecture/       # Pipeline architecture diagram
└── tests/              # Unit tests (pytest)



3. Design Decisions (Why things are built this way)

Some key decisions made in this project:

	•	CSV is used as an interchange format
CSV works well across Athena, Snowflake, Databricks, and Power BI and keeps the pipeline simple and transparent.

	•	Both Lambda and Airflow are present intentionally
Lambda demonstrates serverless ingestion, while Airflow demonstrates scheduled batch orchestration and validation.

	•	Transformations are done in Python
This keeps logic portable between local execution, Lambda, and Databricks.

	•	Data quality checks are lightweight but meaningful
Instead of heavy row-level checks, aggregation-level checks are used to keep costs low and logic simple.

	•	The dataset is intentionally curated
The project focuses on a selected set of popular artists to avoid unnecessary API usage and rate-limit issues.

These trade-offs reflect how real systems evolve over time.


4. Local Development Flow

Local execution is useful for:
	•	testing transformations
	•	validating schemas
	•	debugging ingestion logic before deploying to AWS

4.1 Extract Spotify Data Locally

Implemented in:
src/ingestion/extract_local.py

This script:
	•	authenticates using Spotify credentials from environment variables
	•	pulls track and album metadata for a curated artist list
	•	returns a pandas DataFrame

Environment variables required:

SPOTIFY_CLIENT_ID
SPOTIFY_CLIENT_SECRET

4.2 Transform Data Locally

Implemented in:
src/transform/transform.py

This step:
	•	converts duration from milliseconds to minutes
	•	derives length_category (Short / Medium / Long)
	•	standardizes column names
	•	produces a BI-friendly final schema

The same logic is reused downstream in Lambda / Databricks paths.

4.3 Upload to S3

Implemented in:
src/ingestion/upload_to_s3.py

This step uploads transformed CSVs to:
s3://<bucket>/spotify/processed/

This processed layer becomes the source for Athena, Snowflake, and Power BI.


5. Serverless Ingestion (AWS Lambda)

Two Lambda functions are provided:

5.1 Spotify Ingest Lambda

Location:
lambda/spotify_lambda_ingest.py

Responsibilities:
	•	authenticate with Spotify
	•	fetch curated artist data
	•	apply lightweight transformation
	•	write CSV output to S3 processed layer

Configuration is done entirely via environment variables, not hard-coded secrets.

5.2 Transform-on-Upload Lambda

Location:
lambda/spotify_lambda_transform_ingest.py

Triggered by:
	•	S3 PUT events on the processed folder

Responsibilities:
	•	enrich data (duration_minutes, timestamps)
	•	write transformed output to:
s3://<bucket>/spotify/transformed/

This pattern mirrors real event-driven pipelines.


6. Batch Orchestration with Airflow

Airflow DAG:
dags/spotify_etl_dag.py

The DAG orchestrates:
	1.	ingestion
	2.	S3 upload
	3.	Glue crawler execution
	4.	Athena validation queries
	5.	optional Databricks job trigger
	6.	Snowflake validation

This demonstrates how batch orchestration fits into a broader data platform.


7. Data Validation and Quality Checks

Validation is intentionally simple and practical:
	•	row count checks
	•	album count sanity checks
	•	cross-system comparison (Athena vs Snowflake)

SQL used for validation lives under:
athena/
snowflake/

These checks ensure the pipeline produces consistent and trustworthy outputs without unnecessary complexity.


8. Analytics and Visualization (Power BI)

The final outputs are consumed by Power BI dashboards located under:
powerbi/

The dashboards focus on:
	•	artist output comparison
	•	song duration patterns
	•	explicit vs clean content
	•	album contribution analysis

The goal is decision-ready analytics, not just data movement.


9. Testing Approach

Tests are written using pytest and focus on:
	•	ingestion logic
	•	transformation correctness
	•	S3 write behavior

Tests intentionally avoid external calls by mocking:
	•	Spotify API responses
	•	AWS services

This keeps tests fast, reliable, and meaningful.


10. What This Project Demonstrates

This project is designed to show:
	•	real-world data engineering patterns
	•	trade-offs between multiple tools
	•	incremental pipeline evolution
	•	data quality awareness
	•	analytics-driven output

It reflects how production systems are built, extended, and maintained over time, rather than a single “perfect” design.
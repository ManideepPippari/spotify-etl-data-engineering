🎵 Spotify End-to-End Data Engineering Pipeline (Production-Grade)


AWS S3 | Lambda | EventBridge | Glue | Athena | Docker Airflow | Snowflake | Databricks (PySpark) | Power BI

This project demonstrates a full, enterprise-level ETL + ELT pipeline, ingesting Spotify artist track data, transforming it across multi-cloud systems, and loading it into analytics and BI layers.

Built using 11 industry technologies, this system simulates what a real Data Engineering team builds for scalable analytics.



🧠 High-Level Architecture

![Architecture Diagram](architecture/spotify_etl_architecture.png)

Full Workflow
	1.	Spotify API → Lambda (Ingestion)
	2.	Lambda → S3 (Raw/Processed Data)
	3.	Glue Crawler → Athena (Schema inference + querying)
	4.	Airflow (Docker) → Orchestrates Athena, Glue, Databricks, Snowflake jobs
	5.	Databricks (PySpark) → Bronze → Silver → Gold tables
	6.	Snowflake → ELT transformations + stored procedures + tasks
	7.	Power BI → Final visualization layer



    🛠 Tech Stack

Cloud: AWS S3, Lambda, EventBridge, Glue, Athena
Orchestration: Apache Airflow (Docker)
Processing: Databricks, PySpark
Data Warehouse: Snowflake
Analytics: Power BI
Programming: Python 3
CI/CD: Git + GitHub


📂 Repository Structure


spotify-etl-data-engineering/
│
├── src/                       # Local Spotify ingestion + transform scripts
│   ├── ingestion/
│   ├── transform/
│   └── config.py
│
├── lambda/                    # Lambda ingestion + transform code + configs
│
├── Eventbridge/               # EventBridge schedule + setup docs
│
├── glue/
│   ├── crawler_config.json
│   ├── processed_schema.json
│   ├── transformed_schema.json
│   └── s3_samples/
│
├── athena/
│   ├── tables/
│   ├── workgroup_config.json
│   ├── results_samples/
│   └── top10_artist_stats_query.sql
│
├── databricks/
│   ├── notebooks/
│   ├── queries/
│   ├── tables/
│   └── result_samples/
│
├── snowflake/
│   ├── sql/
│   ├── queries/
│   ├── procedures/
│   ├── tables/
│   └── results_samples/
│
├── dags/                      # Airflow DAG + CSV sample
│
├── s3/                        # Local copies of S3 objects
│
└── powerbi/                  



🎧 Spotify Ingestion

Python | Spotify API

Features
	•	Fetches Top Tracks from multiple artists
	•	Gets Access Token automatically
	•	Saves tracks_raw.csv and tracks_processed.csv
	•	Environment-variable driven (no secrets in repo)

AWS Lambda Ingest Function
	•	Calls Spotify API
	•	Writes processed tracks to S3 → spotify/processed/

EventBridge Integration
	•	Runs Lambda every hour
	•	JSON config included:
	•	eventbridge_schedule.json
	•	Full setup instructions


☁ AWS Layer

spotify/
    raw/
    processed/
    transformed/
    athena-results/

    🟣  AWS Glue & Athena (Data Discovery + SQL Analytics)

Glue Crawler
	•	Auto-creates schema for processed and transformed tables
	•	All configurations stored:
	•	crawler_config.json
	•	processed_schema.json
	•	transformed_schema.json

Athena
	•	Workgroup: spotify-analytics-wg
	•	Output path: /spotify/athena-results/
	•	Includes:
	•	DDL for both tables
	•	Artist analytics SQL
	•	Top 10 artist stats result CSV


⚙ Orchestration
    🟢 4. Airflow (Docker) Orchestration

Airflow DAG does:
	1.	Run local Spotify extract → CSV
	2.	Upload CSV to S3
	3.	Trigger Glue Crawler
	4.	Run Athena Query
	5.	Trigger Databricks workflow
	6.	Run Snowflake procedures
	7.	Run Data Quality Checks
	8.	Final summary output

Folder: dags/spotify_etl_dag.py
Includes:
	•	XCom usage
	•	SnowflakeOperator
	•	HTTP Hook for Databricks
	•	Retry & error handling
	•	Full documentation


🔥 Databricks
PySpark | Delta-Lake

Notebook:

✔ Spotify_Spark_Processing.ipynb
✔ .py version
✔ HTML export version


🥉 BRONZE — Raw ingest
	•	Created table: spotify_tracks_bronze
	•	COPY INTO from S3 processed

🥈 SILVER — Cleaned & typed
	•	Deduping
	•	Type casting
	•	Calculated fields
	•	Table: spotify_tracks_silver

🥇 GOLD — Analytics-ready
	•	Table: spotify_tracks_gold
	•	Aggregates
	•	Feature engineering
	•	Joins
	•	Popularity summary table

Databricks job config also included:
	•	cluster config markdown
	•	job markdown
	•	SQL queries
	•	Result samples


❄ Snowflake
Warehouse | ELT | Tasks

✔ Database
✔ Schema
✔ Warehouse
✔ File formats
✔ Stages
✔ Tables (Bronze, Silver, Gold, Raw, History)
✔ Views
✔ Stored Procedures
✔ Tasks (scheduled pipelines)
✔ DQ checks
✔ Result samples


📊 Power BI
Visualization

/powerbi/
    spotify_dashboard.pbix
    screenshots/

	•	Top Artists by Popularity
	•	Track Duration Distribution
	•	Explicit vs Non-Explicit Analysis
	•	Bronze → Silver → Gold Row Counts
	•	Release Timeline


 🧪 8. Data Quality Checks (Across Platforms)

You included:
	•	Athena row count
	•	Snowflake row count
	•	Cross-system DQ check
	•	Gold table summary
	•	Popularity stats analytics


🎬 9. Demo Video

/demo/
   pipeline_demo.mp4



🧩 10. How to Run This Project (Locally / Cloud)

Local (Airflow + Docker)
docker-compose up -d

Upload DAG:
dags/spotify_etl_dag.py

Check UI:
localhost:
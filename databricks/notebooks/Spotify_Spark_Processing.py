# Databricks notebook source
# MAGIC %md
# MAGIC # 🎵 Spotify End-to-End Data Pipeline using Databricks (PySpark) + Snowflake + AWS S3
# MAGIC
# MAGIC This notebook is part of an end-to-end ETL project that ingests raw Spotify track data from the Spotify API into AWS S3, applies transformations using Databricks (PySpark), and loads the final curated dataset into Snowflake for analytics and BI reporting.
# MAGIC
# MAGIC *Tech Stack:* Python, PySpark, Databricks, AWS S3, Snowflake, SQL  
# MAGIC *Pipeline Objective:* Build a scalable, production-style data pipeline for music analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📌 Step 1 — Load processed CSV from AWS S3 (via secure pre-signed URL)

# COMMAND ----------

import pandas as pd

presigned_url = "https://mani-spotify-etl-data.s3.us-east-2.amazonaws.com/spotify/processed/tracks_transformed_20251119_214719.csv

df_pd.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Convert Pandas DataFrame to Spark DataFrame
# MAGIC We convert the dataset into a distributed Spark DataFrame to enable scalable processing and parallel computation.

# COMMAND ----------

df = spark.createDataFrame(df_pd)
df.show(10)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Add New Columns — Feature Engineering
# MAGIC We derive new attributes for better analytics:
# MAGIC - duration_minutes = duration_ms converted to minutes
# MAGIC - length_category = Long / Medium / Short based on duration

# COMMAND ----------

from pyspark.sql.functions import *

df = df.withColumn("duration_minutes", col("duration_ms") / 60000)
df = df.withColumn(
    "length_category",
    when(col("duration_minutes") > 5, "Long (>5 min)")
    .when(col("duration_minutes") >= 3, "Medium (3-5 min)")
    .otherwise("Short (<3 min)")
)

df.show(10)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 📊 Step 4 — Exploratory Spark Analysis
# MAGIC
# MAGIC Track count and popularity by length category

# COMMAND ----------

(
    df.groupBy("length_category")
      .agg(
          count("*").alias("track_count"),
          round(avg("duration_minutes"), 2).alias("avg_duration_min"),
          round(avg("album_popularity_rank"), 2).alias("avg_album_pop_rank")
      )
      .orderBy("length_category")
      .show()
)

# COMMAND ----------

(
    df.select("artist", "album_name", "track_name", "duration_minutes", "length_category")
      .orderBy(col("duration_minutes").desc())
      .show(10, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ❄ Step 5 — (Planned) Write to Snowflake — Production Version
# MAGIC
# MAGIC Snowflake writeback uses the Snowflake Spark Connector, which is not supported on Databricks Free Serverless.
# MAGIC However, below is the production-ready code (to be run on a non-serverless cluster):

# COMMAND ----------

# sf_options = {
#     "sfURL": "<account>.snowflakecomputing.com",
#     "sfUser": "<user>",
#     "sfPassword": "<password>",
#     "sfWarehouse": "SPOTIFY_WH",
#     "sfDatabase": "SPOTIFY_ETL_DB",
#     "sfSchema": "SPOTIFY_SCHEMA"
# }

# df.write \
#   .format("snowflake") \
#   .options(**sf_options) \
#   .option("dbtable", "SPOTIFY_TRACKS_PROCESSED") \
#   .mode("overwrite") \
#   .save()

# COMMAND ----------

# MAGIC %md
# MAGIC 📌 *Note — Snowflake Load Already Completed Outside Databricks*
# MAGIC
# MAGIC In this project, the final load into Snowflake was executed earlier using the Snowflake COPY INTO command to ingest data directly from the AWS S3 bucket.
# MAGIC
# MAGIC Therefore, the df.write.format("snowflake") block is commented out in this notebook to avoid duplicate loads.
# MAGIC
# MAGIC ```sql
# MAGIC COPY INTO SPOTIFY_ETL_DB.SPOTIFY_SCHEMA.SPOTIFY_TRACKS_PROCESSED
# MAGIC FROM 's3://mani-spotify-etl-data/spotify/processed/'
# MAGIC FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1)
# MAGIC PATTERN = 'tracks_transformed_.*.csv'
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✔ Pipeline Completed Successfully
# MAGIC
# MAGIC This notebook completes the Databricks transformation stage of the pipeline.
# MAGIC We securely loaded transformed Spotify track data from AWS S3, engineered new features
# MAGIC (duration_minutes, length_category), and analyzed track characteristics at scale using PySpark.
# MAGIC
# MAGIC Next stage — Data is made available in Snowflake for BI dashboards and analytics (Power BI / Tableau).

# COMMAND ----------


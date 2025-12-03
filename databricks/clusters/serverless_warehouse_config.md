# Databricks Job: spotify_bronze_loader

- *Type:* Lakehouse Job (Tasks workflow)
- *Schedule:* Every 6 hours (paused during demo)
- *Warehouse:* Serverless Starter Warehouse (2X-Small)
- *Catalog:* spotify_lake

## Tasks

1. *load_spotify_bronze*
   - SQL: job_load_spotify_bronze.sql
   - Writes to: spotify_lake.bronze.spotify_tracks_bronze

2. *load_spotify_silver*
   - Depends on: load_spotify_bronze
   - SQL: job_load_spotify_silver.sql
   - Writes to: spotify_lake.silver.spotify_tracks_silver

3. *load_spotify_gold*
   - Depends on: load_spotify_silver
   - SQL: job_load_spotify_gold.sql
   - Writes to:
     - spotify_lake.gold.spotify_tracks_gold
     - spotify_lake.gold.spotify_popularity_summary
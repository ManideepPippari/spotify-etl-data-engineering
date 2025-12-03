-- 01_setup_objects.sql
-- Create warehouse, database, schema, file format, and external stage.

-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS SPOTIFY_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Database and schema
CREATE DATABASE IF NOT EXISTS SPOTIFY_ETL_DB;
USE DATABASE SPOTIFY_ETL_DB;

CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- File format for CSV
CREATE OR REPLACE FILE FORMAT SPOTIFY_CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL');

-- External stage pointing to S3 processed folder
CREATE OR REPLACE STAGE SPOTIFY_S3_STAGE
  URL = 's3://mani-spotify-etl-data/spotify/processed/'
  FILE_FORMAT = SPOTIFY_CSV_FORMAT;
  -- In a real project, you would also specify STORAGE_INTEGRATION here.
USE ROLE ACCOUNTADMIN;

create or replace database SPOTIFY_ETL_DB;

create or replace schema PUBLIC;

USE DATABASE SPOTIFY_ETL_DB;
USE SCHEMA PUBLIC;

create or replace TABLE SPOTIFY_TRACKS (
	ARTIST VARCHAR(16777216),
	ALBUM_NAME VARCHAR(16777216),
	TRACK_NAME VARCHAR(16777216),
	TRACK_ID VARCHAR(16777216),
	DURATION_MS NUMBER(38,0),
	EXPLICIT VARCHAR(16777216),
	ALBUM_RELEASE_DATE VARCHAR(16777216),
	TRACK_POPULARITY NUMBER(38,0),
	ALBUM_ID VARCHAR(16777216),
	DURATION_MINUTES NUMBER(10,2),
	LENGTH_CATEGORY VARCHAR(16777216),
	ALBUM_TRACK_COUNT NUMBER(38,0),
	ALBUM_POPULARITY_RANK NUMBER(38,0)
);
create or replace TABLE SPOTIFY_TRACKS_BRONZE (
	ARTIST VARCHAR(16777216),
	ALBUM_NAME VARCHAR(16777216),
	TRACK_NAME VARCHAR(16777216),
	TRACK_ID VARCHAR(16777216),
	DURATION_MS NUMBER(38,0),
	EXPLICIT BOOLEAN,
	ALBUM_RELEASE_DATE VARCHAR(16777216),
	TRACK_POPULARITY NUMBER(38,0),
	ALBUM_ID VARCHAR(16777216),
	ALBUM_TYPE VARCHAR(16777216)
);
create or replace TABLE SPOTIFY_TRACKS_GOLD (
	ARTIST VARCHAR(16777216),
	ALBUM_NAME VARCHAR(16777216),
	TRACK_COUNT NUMBER(18,0),
	AVG_TRACK_POPULARITY NUMBER(38,6),
	MAX_TRACK_POPULARITY NUMBER(38,0),
	AVG_DURATION_MINUTES NUMBER(38,2),
	SHORT_TRACKS NUMBER(13,0),
	MEDIUM_TRACKS NUMBER(13,0),
	LONG_TRACKS NUMBER(13,0),
	FIRST_RELEASE_DATE DATE,
	LAST_RELEASE_DATE DATE,
	LAST_LOADED_AT TIMESTAMP_LTZ(9)
);
create or replace TABLE SPOTIFY_TRACKS_HISTORY (
	ARTIST VARCHAR(16777216),
	ALBUM_NAME VARCHAR(16777216),
	TRACK_NAME VARCHAR(16777216),
	TRACK_ID VARCHAR(16777216),
	DURATION_MS NUMBER(38,0),
	EXPLICIT BOOLEAN,
	DURATION_MINUTES NUMBER(10,2),
	LENGTH_CATEGORY VARCHAR(16777216),
	ALBUM_TRACK_COUNT NUMBER(38,0),
	ALBUM_POPULARITY_RANK NUMBER(38,0)
);
create or replace TABLE SPOTIFY_TRACKS_PROCESSED (
	ARTIST VARCHAR(16777216),
	ALBUM_NAME VARCHAR(16777216),
	TRACK_NAME VARCHAR(16777216),
	TRACK_ID VARCHAR(16777216),
	DURATION_MS NUMBER(38,0),
	EXPLICIT VARCHAR(16777216),
	ALBUM_RELEASE_DATE VARCHAR(16777216),
	TRACK_POPULARITY NUMBER(38,0),
	ALBUM_ID VARCHAR(16777216),
	DURATION_MINUTES NUMBER(10,2),
	LENGTH_CATEGORY VARCHAR(16777216),
	ALBUM_TRACK_COUNT NUMBER(38,0),
	ALBUM_POPULARITY_RANK NUMBER(38,0)
);
create or replace TABLE SPOTIFY_TRACKS_RAW (
	ARTIST VARCHAR(16777216),
	ALBUM_NAME VARCHAR(16777216),
	TRACK_NAME VARCHAR(16777216),
	TRACK_ID VARCHAR(16777216),
	DURATION_MS NUMBER(38,0),
	EXPLICIT VARCHAR(16777216),
	ALBUM_RELEASE_DATE VARCHAR(16777216),
	TRACK_POPULARITY NUMBER(38,0),
	ALBUM_ID VARCHAR(16777216),
	DURATION_MINUTES NUMBER(10,2),
	LENGTH_CATEGORY VARCHAR(16777216),
	ALBUM_TRACK_COUNT NUMBER(38,0),
	ALBUM_POPULARITY_RANK NUMBER(38,0)
);
create or replace TABLE SPOTIFY_TRACKS_SILVER (
	TRACK_ID VARCHAR(16777216),
	TRACK_NAME VARCHAR(16777216),
	ARTIST VARCHAR(16777216),
	ALBUM_NAME VARCHAR(16777216),
	DURATION_MS NUMBER(38,0),
	DURATION_MINUTES NUMBER(38,2),
	LENGTH_CATEGORY VARCHAR(16),
	EXPLICIT BOOLEAN,
	ALBUM_RELEASE_DATE DATE,
	TRACK_POPULARITY NUMBER(38,0),
	ALBUM_ID VARCHAR(16777216),
	ALBUM_TYPE VARCHAR(16777216),
	LOAD_TIMESTAMP_UTC TIMESTAMP_LTZ(9)
);
create or replace view VW_SPOTIFY_DQ(
	TOTAL_RECORDS,
	NULL_TRACK_NAMES,
	LONG_SONGS,
	CHECK_TIME
) as
SELECT 
    COUNT(*) AS total_records,
    COUNT_IF(track_name IS NULL) AS null_track_names,
    COUNT_IF(duration_minutes > 15) AS long_songs,
    CURRENT_TIMESTAMP() AS check_time
FROM VW_SPOTIFY_TRACKS_GOLD;
create or replace view VW_SPOTIFY_TRACKS_10(
	ARTIST,
	ALBUM_NAME,
	TRACK_NAME,
	TRACK_ID,
	DURATION_MS,
	EXPLICIT,
	DURATION_MINUTES,
	LENGTH_CATEGORY,
	ALBUM_TRACK_COUNT,
	ALBUM_POPULARITY_RANK
) as
SELECT
    artist,
    album_name,
    track_name,
    track_id,
    duration_ms,
    explicit,
    duration_minutes,
    length_category,
    album_track_count,
    album_popularity_rank
FROM SPOTIFY_TRACKS;
create or replace view VW_SPOTIFY_TRACKS_GOLD(
	ARTIST,
	ALBUM_NAME,
	TRACK_NAME,
	TRACK_ID,
	DURATION_MINUTES,
	LENGTH_CATEGORY,
	ALBUM_TRACK_COUNT,
	ALBUM_POPULARITY_RANK,
	ALBUM_ID,
	LOAD_TIMESTAMP,
	RECORD_SOURCE
) as
SELECT
    artist,
    album_name,
    track_name,
    track_id,
    duration_minutes,
    length_category,
    album_track_count,
    album_popularity_rank,
    album_id,
    CURRENT_TIMESTAMP() AS load_timestamp,
    'DAILY_LOAD' AS record_source
FROM SPOTIFY_TRACKS

UNION ALL

SELECT
    artist,
    album_name,
    track_name,
    track_id,
    duration_minutes,
    length_category,
    NULL AS album_track_count,
    NULL AS album_popularity_rank,
    NULL AS album_id,
    CURRENT_TIMESTAMP() AS load_timestamp,
    'HISTORICAL_LOAD' AS record_source
FROM SPOTIFY_TRACKS_HISTORY;
CREATE OR REPLACE FILE FORMAT SPOTIFY_CSV_FORMAT
	SKIP_HEADER = 1
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
;
CREATE OR REPLACE FILE FORMAT SPOTIFY_FORMAT
	SKIP_HEADER = 1
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('', 'NULL')
;
CREATE OR REPLACE PROCEDURE "SPOTIFY_REFRESH_BRONZE"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
  -- Optional: snapshot style (clear table before reload)
  TRUNCATE TABLE SPOTIFY_TRACKS_BRONZE;

  INSERT INTO SPOTIFY_TRACKS_BRONZE (
      artist,
      album_name,
      track_name,
      track_id,
      duration_ms,
      explicit,
      album_release_date,
      track_popularity,
      album_id,
      album_type
  )
  SELECT
      $1::STRING        AS artist,
      $2::STRING        AS album_name,
      $3::STRING        AS track_name,
      $4::STRING        AS track_id,
      $5::NUMBER        AS duration_ms,
      $6::BOOLEAN       AS explicit,
      $7::DATE          AS album_release_date,
      $8::NUMBER        AS track_popularity,
      $9::STRING        AS album_id,
      $10::STRING       AS album_type
  FROM @SPOTIFY_SCHEMA.SPOTIFY_STAGE_TRANSFORMED (FILE_FORMAT => SPOTIFY_FORMAT);

  RETURN ''Bronze refreshed from S3 stage'';
END;
';
CREATE OR REPLACE PROCEDURE "SPOTIFY_REFRESH_GOLD"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    -- Rebuild GOLD table from SILVER
    CREATE OR REPLACE TABLE SPOTIFY_TRACKS_GOLD AS
    SELECT
        artist,
        album_name,
        COUNT(*) AS track_count,
        AVG(track_popularity) AS avg_track_popularity,
        MAX(track_popularity) AS max_track_popularity,
        ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes,
        SUM(CASE WHEN length_category = ''Short (<3 min)'' THEN 1 ELSE 0 END)  AS short_tracks,
        SUM(CASE WHEN length_category = ''Medium (3-5 min)'' THEN 1 ELSE 0 END) AS medium_tracks,
        SUM(CASE WHEN length_category = ''Long (>5 min)'' THEN 1 ELSE 0 END)   AS long_tracks,
        MIN(album_release_date) AS first_release_date,
        MAX(album_release_date) AS last_release_date,
        MAX(load_timestamp_utc) AS last_loaded_at
    FROM SPOTIFY_TRACKS_SILVER
    GROUP BY
        artist,
        album_name;

    RETURN ''GOLD refresh done'';
END;
';
CREATE OR REPLACE PROCEDURE "SPOTIFY_REFRESH_SILVER"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
  TRUNCATE TABLE SPOTIFY_TRACKS_SILVER;

  INSERT INTO SPOTIFY_TRACKS_SILVER (
      artist,
      album_name,
      track_name,
      track_id,
      duration_ms,
      duration_minutes,
      length_category,
      explicit,
      album_release_date,
      track_popularity,
      album_id,
      album_type,
      load_timestamp_utc
  )
  SELECT
      artist,
      album_name,
      track_name,
      track_id,
      duration_ms,
      duration_ms / 60000.0 AS duration_minutes,
      CASE
        WHEN duration_ms / 60000.0 < 3 THEN ''Short (<3 min)''
        WHEN duration_ms / 60000.0 BETWEEN 3 AND 5 THEN ''Medium (3-5 min)''
        ELSE ''Long (>5 min)''
      END AS length_category,
      explicit,
      album_release_date,
      track_popularity,
      album_id,
      album_type,
      CURRENT_TIMESTAMP() AS load_timestamp_utc
  FROM SPOTIFY_TRACKS_BRONZE;

  RETURN ''Silver refreshed from Bronze'';
END;
';
create or replace task TASK_LOAD_SPOTIFY_FROM_S3
	warehouse=SPOTIFY_WH
	schedule='USING CRON 0 * * * * America/Los_Angeles'
	as COPY INTO spotify_tracks_raw
FROM @spotify_s3_stage
PATTERN = '.*\.csv'
ON_ERROR = 'CONTINUE';
create or replace task TASK_LOAD_SPOTIFY_TRACKS
	warehouse=SPOTIFY_WH
	schedule='5 MINUTE'
	as COPY INTO SPOTIFY_TRACKS
  FROM @SPOTIFY_S3_STAGE_PROCESSED
  FILE_FORMAT = (FORMAT_NAME = 'SPOTIFY_CSV_FORMAT')
  ON_ERROR   = 'CONTINUE';
create or replace task TASK_SPOTIFY_RAW_TO_SILVER
	warehouse=SPOTIFY_WH
	schedule='15 MINUTE'
	as CREATE OR REPLACE TABLE SPOTIFY_TRACKS_SILVER AS
  SELECT DISTINCT
      track_id,
      TRIM(artist)                           AS artist,
      TRIM(track_name)                       AS track_name,
      TRIM(album_name)                       AS album_name,
      duration_ms,
      ROUND(duration_ms / 60000.0, 2)        AS duration_minutes,
      popularity,
      release_date
  FROM SPOTIFY_TRACKS_RAW
  WHERE track_id IS NOT NULL;
create or replace task TASK_SPOTIFY_SILVER_TO_GOLD
	warehouse=SPOTIFY_WH
	schedule='60 MINUTE'
	COMMENT='Refresh SPOTIFY_TRACKS_GOLD from SPOTIFY_TRACKS_SILVER every hour'
	as CREATE OR REPLACE TABLE SPOTIFY_TRACKS_GOLD AS
  SELECT 
      TRACK_NAME,
      TRIM(ALBUM_NAME) AS ALBUM_NAME,
      TRIM(ARTIST)     AS ARTIST,
      DURATION_MS,
      ROUND(DURATION_MS / 60000.0, 2) AS DURATION_MINUTES,
      TRACK_POPULARITY,
      RELEASE_DATE,
      ALBUM_ID
  FROM SPOTIFY_TRACKS_SILVER;
create or replace schema SPOTIFY_SCHEMA;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
	SKIP_HEADER = 1
;
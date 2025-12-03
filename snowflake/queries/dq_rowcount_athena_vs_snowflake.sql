-- Snowflake GOLD rowcount for DQ comparison with Athena
USE DATABASE SPOTIFY_ETL_DB;
USE SCHEMA PUBLIC;

SELECT
    COUNT(*) AS snowflake_gold_tracks
FROM SPOTIFY_TRACKS_GOLD;
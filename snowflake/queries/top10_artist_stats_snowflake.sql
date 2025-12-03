-- Top 10 artists by total tracks in Snowflake GOLD layer
USE DATABASE SPOTIFY_ETL_DB;
USE SCHEMA PUBLIC;

WITH artist_stats AS (
    SELECT
        ARTIST,
        COUNT(DISTINCT ALBUM_NAME)                    AS total_albums,
        SUM(TRACK_COUNT)                              AS total_tracks,
        ROUND(AVG(AVG_DURATION_MINUTES), 2)           AS avg_track_length_min,
        ROUND(AVG(AVG_TRACK_POPULARITY), 2)           AS avg_popularity,
        SUM(SHORT_TRACKS)                             AS short_tracks,
        SUM(MEDIUM_TRACKS)                            AS medium_tracks,
        SUM(LONG_TRACKS)                              AS long_tracks,
        MIN(FIRST_RELEASE_DATE)                       AS first_release_date,
        MAX(LAST_RELEASE_DATE)                        AS last_release_date
    FROM SPOTIFY_TRACKS_GOLD
    GROUP BY ARTIST
)

SELECT
    ARTIST,
    total_albums,
    total_tracks,
    avg_track_length_min,
    avg_popularity,
    short_tracks,
    medium_tracks,
    long_tracks,
    first_release_date,
    last_release_date
FROM artist_stats
QUALIFY ROW_NUMBER() OVER (ORDER BY total_tracks DESC) <= 10
ORDER BY total_tracks DESC;
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
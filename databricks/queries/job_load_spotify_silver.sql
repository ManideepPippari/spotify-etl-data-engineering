USE CATALOG spotify_lake;
USE SCHEMA silver;

CREATE TABLE IF NOT EXISTS spotify_tracks_silver AS
SELECT
    artist,
    artist_id,
    album_name,
    album_id,
    track_name,
    track_id,

    -- Raw duration from Lambda (string)
    duration_ms,

    -- Duration converted to minutes (FLOAT)
    CAST(duration_ms AS DOUBLE) / 60000 AS duration_minutes,

    explicit
FROM spotify_lake.bronze.spotify_tracks_bronze
WHERE track_id IS NOT NULL;
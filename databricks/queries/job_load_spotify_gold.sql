USE CATALOG spotify_lake;
USE SCHEMA gold;

CREATE TABLE IF NOT EXISTS spotify_tracks_gold AS
SELECT
    artist,
    artist_id,
    album_name,
    album_id,
    track_name,
    track_id,
    duration_ms,
    duration_minutes,
    explicit
FROM spotify_lake.silver.spotify_tracks_silver
WHERE track_id IS NOT NULL;
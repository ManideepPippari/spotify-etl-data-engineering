WITH joined AS (
    SELECT
        p.artist,
        p.artist_id,
        p.album_name,
        p.album_id,
        p.track_name,
        p.track_id,
        p.duration_ms,
        t.duration_min,
        p.explicit,
        p.album_release_date,
        t.load_timestamp_utc
    FROM processed p
    JOIN transformed t
      ON p.track_id = t.track_id
),

artist_stats AS (
    SELECT
        artist,
        COUNT(DISTINCT album_id)                         AS total_albums,
        COUNT(DISTINCT track_id)                         AS total_tracks,
        ROUND(AVG(duration_min), 2)                      AS avg_track_length_min,
        SUM(CASE WHEN duration_min < 2 THEN 1 ELSE 0 END)             AS short_tracks,
        SUM(CASE WHEN duration_min BETWEEN 2 AND 4 THEN 1 ELSE 0 END) AS medium_tracks,
        SUM(CASE WHEN duration_min > 4 THEN 1 ELSE 0 END)             AS long_tracks,
        MIN(album_release_date)                          AS first_release_date,
        MAX(album_release_date)                          AS last_release_date,
        ROUND(
            (SUM(CASE WHEN explicit = TRUE THEN 1 ELSE 0 END) * 100.0)
            / NULLIF(COUNT(track_id), 0), 2
        )                                                AS explicit_percentage
    FROM joined
    GROUP BY artist
)

SELECT
    artist,
    total_albums,
    total_tracks,
    avg_track_length_min,
    short_tracks,
    medium_tracks,
    long_tracks,
    first_release_date,
    last_release_date,
    explicit_percentage
FROM artist_stats
ORDER BY total_tracks DESC
LIMIT 10;
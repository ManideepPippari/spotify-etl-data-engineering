SELECT
    COUNT(*)                           AS athena_rows,
    COUNT(DISTINCT album_name)         AS athena_albums
FROM processed;
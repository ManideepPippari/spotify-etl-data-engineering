USE CATALOG spotify_lake;
USE SCHEMA bronze;

-- Table will be created automatically on first COPY
CREATE TABLE IF NOT EXISTS spotify_tracks_bronze
USING DELTA;

COPY INTO spotify_tracks_bronze
FROM 's3://mani-spotify-etl-data/spotify/processed/*.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS (
  'mergeSchema' = 'true',
  'force'       = 'true'
);
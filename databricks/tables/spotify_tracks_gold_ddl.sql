CREATE TABLE spotify_lake.gold.spotify_tracks_gold (
  artist STRING,
  artist_id STRING,
  album_name STRING,
  album_id STRING,
  track_name STRING,
  track_id STRING,
  duration_ms STRING,
  duration_minutes DOUBLE,
  explicit STRING)
USING delta
COLLATION 'UTF8_BINARY'
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
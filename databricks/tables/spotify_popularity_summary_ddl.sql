CREATE TABLE spotify_lake.gold.spotify_popularity_summary (
  artist STRING,
  total_tracks BIGINT,
  avg_popularity DOUBLE,
  avg_duration_minutes DECIMAL(18,6))
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
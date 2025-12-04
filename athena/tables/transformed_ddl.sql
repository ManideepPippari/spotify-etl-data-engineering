CREATE EXTERNAL TABLE `transformed`(
  `artist` string, 
  `artist_id` string, 
  `album_name` string, 
  `album_id` string, 
  `track_name` string, 
  `track_id` string, 
  `duration_ms` bigint, 
  `explicit` boolean, 
  `duration_min` double, 
  `load_timestamp_utc` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://mani-spotify-etl-data/spotify/transformed/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='ID', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='spotify_transformed-crawler', 
  'areColumnsQuoted'='false', 
  'averageRecordSize'='143', 
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none', 
  'delimiter'=',', 
  'objectCount'='1', 
  'recordCount'='1713', 
  'sizeKey'='245043', 
  'skip.header.line.count'='1', 
  'typeOfData'='file')
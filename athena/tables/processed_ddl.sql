CREATE EXTERNAL TABLE `processed`(
  `artist` string, 
  `artist_id` string, 
  `album_name` string, 
  `album_id` string, 
  `track_name` string, 
  `track_id` string, 
  `duration_ms` bigint, 
  `explicit` boolean, 
  `album_release_date` string, 
  `track_popularity` bigint, 
  `duration_minutes` double, 
  `length_category` string, 
  `album_track_count` bigint, 
  `album_popularity_rank` bigint)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://mani-spotify-etl-data/spotify/processed/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='3fc8863f-68d6-41d3-9d23-12a68ac85258', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='spotify-etl-crawler', 
  'areColumnsQuoted'='false', 
  'averageRecordSize'='113', 
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none', 
  'delimiter'=',', 
  'objectCount'='3', 
  'recordCount'='1854', 
  'sizeKey'='212727', 
  'skip.header.line.count'='1', 
  'typeOfData'='file')
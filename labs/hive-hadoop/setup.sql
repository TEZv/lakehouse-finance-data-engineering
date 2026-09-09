CREATE DATABASE IF NOT EXISTS portfolio;
USE portfolio;
CREATE EXTERNAL TABLE IF NOT EXISTS event_snapshot (
    event_id STRING, version BIGINT, quantity BIGINT
)
PARTITIONED BY (batch_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/portfolio/events';
ALTER TABLE event_snapshot ADD IF NOT EXISTS
PARTITION (batch_date='2026-01-02') LOCATION 'hdfs://localhost:9000/portfolio/events/batch_date=2026-01-02';

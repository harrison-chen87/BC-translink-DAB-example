-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze: GTFS Trips
-- MAGIC
-- MAGIC Trip definitions linking routes to shapes and service calendars.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_gtfs_trips
COMMENT 'Raw GTFS trip definitions across weekly snapshots'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  route_id,
  service_id,
  trip_id,
  trip_headsign,
  CAST(direction_id AS INT) AS direction_id,
  block_id,
  shape_id,
  CAST(wheelchair_accessible AS INT) AS wheelchair_accessible,
  CAST(bikes_allowed AS INT) AS bikes_allowed,
  regexp_extract(_metadata.file_path, '(\d{4}-\d{2}-\d{2})', 1) AS gtfs_snapshot_date,
  _metadata.file_name AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/${volume}/gtfs/extracted/*/trips.txt',
  format => 'csv',
  header => true
);

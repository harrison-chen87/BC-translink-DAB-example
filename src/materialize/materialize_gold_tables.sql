-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Materialize Gold MVs as Physical Delta Tables
-- MAGIC
-- MAGIC SDP materialized views are logical — they exist within the pipeline's
-- MAGIC managed catalog but are recomputed on refresh. This notebook reads each
-- MAGIC gold MV and writes it as a standalone physical Delta table so that
-- MAGIC downstream consumers (dashboards, BI tools, Genie spaces) can query
-- MAGIC them via SQL warehouse without depending on pipeline state.
-- MAGIC
-- MAGIC **Runs on the SQL warehouse** created by the DAB.

-- COMMAND ----------

-- Create the physical schema
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}_physical;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Materialize gold_schedule_vs_actual

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${schema}_physical.gold_schedule_vs_actual
COMMENT 'Physical Delta snapshot of MV gold_schedule_vs_actual'
TBLPROPERTIES ('quality' = 'gold', 'source_mv' = '${catalog}.${schema}.gold_schedule_vs_actual')
AS SELECT * FROM ${catalog}.${schema}.gold_schedule_vs_actual;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Materialize gold_route_delay_patterns

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${schema}_physical.gold_route_delay_patterns
COMMENT 'Physical Delta snapshot of MV gold_route_delay_patterns'
TBLPROPERTIES ('quality' = 'gold', 'source_mv' = '${catalog}.${schema}.gold_route_delay_patterns')
AS SELECT * FROM ${catalog}.${schema}.gold_route_delay_patterns;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Materialize gold_corridor_daily_summary

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${schema}_physical.gold_corridor_daily_summary
COMMENT 'Physical Delta snapshot of MV gold_corridor_daily_summary'
TBLPROPERTIES ('quality' = 'gold', 'source_mv' = '${catalog}.${schema}.gold_corridor_daily_summary')
AS SELECT * FROM ${catalog}.${schema}.gold_corridor_daily_summary;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Materialize gold_congestion_patterns

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${schema}_physical.gold_congestion_patterns
COMMENT 'Physical Delta snapshot of MV gold_congestion_patterns'
TBLPROPERTIES ('quality' = 'gold', 'source_mv' = '${catalog}.${schema}.gold_congestion_patterns')
AS SELECT * FROM ${catalog}.${schema}.gold_congestion_patterns;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify all physical tables exist

-- COMMAND ----------

SHOW TABLES IN ${catalog}.${schema}_physical;

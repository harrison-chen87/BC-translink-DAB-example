# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Corridor Daily Summary
# MAGIC
# MAGIC Daily corridor-level summary combining traffic congestion
# MAGIC data with GTFS route metadata. Primary analytics table for dashboards.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

@dlt.table(
    name="gold_corridor_daily_summary",
    comment="Daily corridor performance: congestion data enriched with GTFS route metadata",
    table_properties={"quality": "gold"},
)
def gold_corridor_daily_summary():
    silver_traffic = dlt.read("silver_traffic_readings")

    api_daily = (
        silver_traffic
        .groupBy("corridor_id", "corridor_name", "poll_date", "day_type")
        .agg(
            F.count("*").alias("num_readings"),
            F.round(F.avg("congestion_ratio"), 3).alias("avg_congestion_ratio"),
            F.round(F.max("congestion_ratio"), 3).alias("peak_congestion_ratio"),
            F.round(F.avg("duration_seconds"), 0).cast("int").alias("avg_duration_seconds"),
            F.round(F.avg("static_duration_seconds"), 0).cast("int").alias("avg_static_duration_seconds"),
            F.round(F.avg("distance_meters"), 0).cast("int").alias("avg_distance_meters"),
        )
    )

    return (
        api_daily
        .select(
            F.col("corridor_id"),
            F.col("corridor_name"),
            F.col("poll_date").alias("report_date"),
            F.col("day_type"),
            F.col("num_readings"),
            F.col("avg_congestion_ratio"),
            F.col("peak_congestion_ratio"),
            F.col("avg_duration_seconds"),
            F.col("avg_static_duration_seconds"),
            F.col("avg_distance_meters"),
        )
    )

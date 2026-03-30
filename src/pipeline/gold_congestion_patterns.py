# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Congestion Patterns
# MAGIC
# MAGIC Hourly congestion patterns by corridor, day of week, and time period.
# MAGIC Used for identifying peak congestion windows and comparing
# MAGIC weekday vs weekend patterns across corridors.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

@dlt.table(
    name="gold_congestion_patterns",
    comment="Hourly congestion patterns by corridor, day of week, and time period",
    table_properties={"quality": "gold"},
)
def gold_congestion_patterns():
    silver_traffic = dlt.read("silver_traffic_readings")

    return (
        silver_traffic
        .groupBy(
            "corridor_id",
            "corridor_name",
            "day_of_week",
            "day_type",
            "hour_of_day",
            "time_period",
        )
        .agg(
            F.count("*").alias("num_observations"),
            F.round(F.avg("congestion_ratio"), 3).alias("avg_congestion_ratio"),
            F.round(F.percentile_approx("congestion_ratio", 0.50), 3).alias("median_congestion_ratio"),
            F.round(F.percentile_approx("congestion_ratio", 0.95), 3).alias("p95_congestion_ratio"),
            F.round(F.avg("duration_seconds"), 0).cast("int").alias("avg_duration_seconds"),
            F.round(F.avg("distance_meters"), 0).cast("int").alias("avg_distance_meters"),
            # Severity distribution
            F.sum(F.when(F.col("congestion_severity") == "free_flow", 1).otherwise(0)).alias("free_flow_count"),
            F.sum(F.when(F.col("congestion_severity") == "light", 1).otherwise(0)).alias("light_count"),
            F.sum(F.when(F.col("congestion_severity") == "moderate", 1).otherwise(0)).alias("moderate_count"),
            F.sum(F.when(F.col("congestion_severity") == "heavy", 1).otherwise(0)).alias("heavy_count"),
            F.sum(F.when(F.col("congestion_severity") == "severe", 1).otherwise(0)).alias("severe_count"),
        )
    )

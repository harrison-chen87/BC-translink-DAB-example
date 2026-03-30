# TransLink Databricks Automation Bundle Example

An example Databricks Automation Bundle (DAB) that showcases bundle structure, resource definitions, and source file layout using a Vancouver transit traffic analysis scenario.

This project demonstrates how to define and deploy a complete data engineering workflow — from synthetic data generation through a medallion-architecture pipeline to physical Delta table materialization — entirely as code using DABs.

## Bundle Structure

```
translink_DAB_example/
├── databricks.yml                              # Bundle configuration and targets
├── resources/
│   ├── traffic_pipeline.yml                    # SDP pipeline resource (13 notebooks)
│   ├── sql_warehouse.yml                       # SQL warehouse resource
│   └── traffic_workflow.yml                    # Workflow job with 3 tasks
└── src/
    ├── data_gen/
    │   ├── corridors.json                      # Vancouver corridor definitions
    │   └── generate_traffic_data.py            # Synthetic data generator
    ├── pipeline/
    │   ├── bronze_gtfs_routes.sql              # Auto Loader ingestion
    │   ├── bronze_gtfs_stops.sql               # Auto Loader ingestion
    │   ├── bronze_gtfs_trips.sql               # Auto Loader ingestion
    │   ├── bronze_gtfs_stop_times.sql          # Auto Loader ingestion
    │   ├── bronze_traffic_api.sql              # Auto Loader ingestion (JSON)
    │   ├── silver_gtfs_routes.sql              # Auto CDC — SCD Type 1
    │   ├── silver_gtfs_stops.sql               # Auto CDC — SCD Type 2
    │   ├── silver_gtfs_scheduled_times.sql     # Materialized view
    │   ├── silver_traffic_readings.sql         # Materialized view
    │   ├── gold_schedule_vs_actual.sql         # Materialized view (SQL)
    │   ├── gold_route_delay_patterns.sql       # Materialized view (SQL)
    │   ├── gold_corridor_daily_summary.py      # Materialized view (Python @dlt.table)
    │   └── gold_congestion_patterns.py         # Materialized view (Python @dlt.table)
    └── materialize/
        └── materialize_gold_tables.sql         # CTAS to physical Delta tables
```

## Key Concepts Demonstrated

### Bundle Configuration (`databricks.yml`)

- **Variables**: Parameterized `catalog`, `schema`, and `volume` for multi-environment deployment
- **Targets**: `dev` (development mode) and `prod` (production mode)
- **Resource includes**: Modular YAML files under `resources/`

### Resource Definitions (`resources/`)

| Resource | File | What It Defines |
|----------|------|-----------------|
| **SDP Pipeline** | `traffic_pipeline.yml` | A serverless Spark Declarative Pipeline with 13 notebooks across bronze, silver, and gold layers |
| **SQL Warehouse** | `sql_warehouse.yml` | A 2X-Large Pro SQL warehouse used for materializing gold tables |
| **Workflow Job** | `traffic_workflow.yml` | A 3-task job that chains data generation, pipeline execution, and table materialization |

### Source Files (`src/`)

**Data Generation** — A Python notebook generates synthetic GTFS transit data (routes, stops, trips, stop_times) and traffic API readings, writing them as CSV/JSON to a Unity Catalog Volume for Auto Loader ingestion.

**SDP Pipeline** — A mix of SQL and Python notebooks organized in a medallion architecture:

- **Bronze**: Streaming tables via Auto Loader ingest raw CSV and JSON files
- **Silver**: Auto CDC applies SCD Type 1 (routes) and Type 2 (stops) change tracking; materialized views handle deduplication and congestion classification
- **Gold**: Materialized views join GTFS schedules with traffic data to produce delay analysis, congestion patterns, and corridor summaries

**Materialization** — A SQL notebook runs on the bundle-managed SQL warehouse, executing `CREATE OR REPLACE TABLE` statements to snapshot each gold materialized view as a physical Delta table in a separate schema.

### Workflow Orchestration

```
generate_synthetic_data  →  run_pipeline  →  materialize_gold_tables
     (serverless)           (SDP pipeline)       (SQL warehouse)
```

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- An existing catalog with `CREATE SCHEMA` permission
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) v0.230+ with a configured profile

### Deploy

```bash
# Validate the bundle
databricks bundle validate -t dev --var="catalog=<your_catalog>"

# Deploy resources to the workspace
databricks bundle deploy -t dev --var="catalog=<your_catalog>"

# Run the full workflow
databricks bundle run traffic_workflow -t dev --var="catalog=<your_catalog>"
```

### Clean Up

```bash
databricks bundle destroy -t dev --auto-approve
```

## Reference

For more on Databricks Automation Bundles, see:

- [Databricks Asset Bundles documentation](https://docs.databricks.com/dev-tools/bundles/)
- [Supported resource types](https://docs.databricks.com/dev-tools/bundles/resources)
- [Default bundle templates](https://docs.databricks.com/dev-tools/bundles/templates)

# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Synthetic Vancouver Traffic Data
# MAGIC
# MAGIC Creates realistic synthetic GTFS and traffic API data for the TransLink
# MAGIC pipeline demo. Writes CSV/JSON files to a UC Volume for Auto Loader ingestion.
# MAGIC
# MAGIC **No external API keys required** — all data is synthesized locally.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")
dbutils.widgets.text("volume", "traffic_data", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
print(f"Writing synthetic data to: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create catalog, schema, and volume if needed

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume}")

# COMMAND ----------

import json
import csv
import io
import random
import math
from datetime import datetime, timedelta

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Synthetic GTFS Data
# MAGIC
# MAGIC Creates 3 weekly snapshots of GTFS-like CSV files:
# MAGIC - **routes.txt** — 30 transit routes (bus, SkyTrain, SeaBus)
# MAGIC - **stops.txt** — 200 transit stops across Metro Vancouver
# MAGIC - **trips.txt** — ~1,500 trips per snapshot
# MAGIC - **stop_times.txt** — ~15,000 scheduled arrival/departure records per snapshot

# COMMAND ----------

# --- Vancouver transit route definitions ---
ROUTES = [
    ("R1", "R1", "King George Blvd RapidBus", 3),
    ("R2", "R2", "Marine-Main RapidBus", 3),
    ("R3", "R3", "Lougheed Hwy RapidBus", 3),
    ("R4", "R4", "41st Ave RapidBus", 3),
    ("R5", "R5", "Hastings St RapidBus", 3),
    ("099", "099", "Commercial-Broadway/UBC (B-Line)", 3),
    ("009", "9", "Boundary/Commercial-Broadway/Alma", 3),
    ("014", "14", "Hastings/UBC", 3),
    ("020", "20", "Victoria/Downtown", 3),
    ("025", "25", "Brentwood/UBC", 3),
    ("033", "33", "29th Avenue Station/UBC", 3),
    ("041", "41", "Joyce Station/UBC", 3),
    ("049", "49", "Metrotown/UBC", 3),
    ("050", "50", "Waterfront Station/False Creek South", 3),
    ("084", "84", "VCC-Clark Station/UBC", 3),
    ("096", "96", "Guildford/Langley", 3),
    ("100", "100", "22nd St Stn/Marpole Loop", 3),
    ("106", "106", "New Westminster Stn/Metrotown Stn", 3),
    ("130", "130", "Hastings/Kootenay/Metrotown", 3),
    ("145", "145", "Production Way/SFU", 3),
    ("SKY_E", "EXPO", "Expo Line (King George to Waterfront)", 1),
    ("SKY_M", "MILL", "Millennium Line (VCC-Clark to Lafarge)", 1),
    ("SKY_C", "CAND", "Canada Line (Waterfront to Richmond/YVR)", 1),
    ("WCE", "WCE", "West Coast Express (Mission to Waterfront)", 2),
    ("SB1", "SB", "SeaBus (Waterfront to Lonsdale Quay)", 4),
    ("N10", "N10", "NightBus Downtown/Richmond", 3),
    ("N15", "N15", "NightBus Cambie/Langara", 3),
    ("N19", "N19", "NightBus Metrotown/Stanley Park", 3),
    ("N22", "N22", "NightBus McDonald/Dunbar", 3),
    ("N24", "N24", "NightBus East Vancouver", 3),
]

# --- Vancouver stop locations (realistic lat/lng) ---
STOP_AREAS = {
    "downtown": {"lat_range": (49.2800, 49.2870), "lng_range": (-123.1250, -123.1050), "count": 25},
    "broadway": {"lat_range": (49.2600, 49.2700), "lng_range": (-123.1600, -123.0650), "count": 20},
    "commercial": {"lat_range": (49.2500, 49.2700), "lng_range": (-123.0700, -123.0600), "count": 15},
    "kitsilano": {"lat_range": (49.2650, 49.2750), "lng_range": (-123.1700, -123.1400), "count": 12},
    "ubc": {"lat_range": (49.2550, 49.2700), "lng_range": (-123.2500, -123.2200), "count": 8},
    "burnaby": {"lat_range": (49.2200, 49.2650), "lng_range": (-123.0200, -122.9600), "count": 20},
    "metrotown": {"lat_range": (49.2200, 49.2350), "lng_range": (-123.0100, -122.9900), "count": 10},
    "surrey": {"lat_range": (49.1700, 49.2100), "lng_range": (-122.8600, -122.7800), "count": 20},
    "richmond": {"lat_range": (49.1600, 49.1900), "lng_range": (-123.1400, -123.0800), "count": 15},
    "north_van": {"lat_range": (49.3100, 49.3350), "lng_range": (-123.1000, -123.0600), "count": 12},
    "new_west": {"lat_range": (49.2000, 49.2200), "lng_range": (-122.9200, -122.8900), "count": 10},
    "coquitlam": {"lat_range": (49.2700, 49.2900), "lng_range": (-122.8200, -122.7800), "count": 10},
    "east_van": {"lat_range": (49.2350, 49.2600), "lng_range": (-123.0650, -123.0200), "count": 15},
    "main_st": {"lat_range": (49.2200, 49.2700), "lng_range": (-123.1050, -123.0950), "count": 8},
}


def generate_stops():
    """Generate synthetic transit stops across Metro Vancouver."""
    stops = []
    stop_id = 50000
    for area_name, area in STOP_AREAS.items():
        for i in range(area["count"]):
            lat = round(random.uniform(*area["lat_range"]), 6)
            lng = round(random.uniform(*area["lng_range"]), 6)
            stops.append({
                "stop_id": str(stop_id),
                "stop_code": str(stop_id),
                "stop_name": f"{area_name.replace('_', ' ').title()} Stop {i+1}",
                "stop_desc": "",
                "stop_lat": lat,
                "stop_lon": lng,
                "zone_id": area_name,
                "stop_url": "",
                "location_type": 0,
                "parent_station": "",
                "wheelchair_boarding": random.choice([0, 1, 1, 1]),
            })
            stop_id += 1
    return stops


def generate_trips_and_stop_times(routes, stops, snapshot_date):
    """Generate trips and stop_times for a given snapshot date."""
    trips = []
    stop_times = []
    trip_counter = 1

    for route in routes:
        route_id = route[0]
        route_type = route[3]

        if route_type == 1:     # SkyTrain: frequent
            trips_per_direction = 25
            stops_per_trip = 12
        elif route_type == 2:   # WCE: few trips
            trips_per_direction = 5
            stops_per_trip = 8
        elif route_type == 4:   # SeaBus: moderate
            trips_per_direction = 15
            stops_per_trip = 2
        else:                   # Bus
            trips_per_direction = random.randint(10, 20)
            stops_per_trip = random.randint(8, 18)

        for direction in [0, 1]:
            route_stops = random.sample(stops, min(stops_per_trip, len(stops)))
            for trip_num in range(trips_per_direction):
                trip_id = f"trip_{route_id}_{direction}_{trip_num:03d}"
                start_hour = random.randint(5, 23)
                start_min = random.randint(0, 59)

                trips.append({
                    "route_id": route_id,
                    "service_id": "weekday" if random.random() > 0.3 else "weekend",
                    "trip_id": trip_id,
                    "trip_headsign": route[2].split("/")[0] if "/" in route[2] else route[2],
                    "direction_id": direction,
                    "block_id": "",
                    "shape_id": f"shape_{route_id}_{direction}",
                    "wheelchair_accessible": 1,
                    "bikes_allowed": 1,
                })

                current_seconds = start_hour * 3600 + start_min * 60
                for seq, stop in enumerate(route_stops):
                    arr_h = current_seconds // 3600
                    arr_m = (current_seconds % 3600) // 60
                    arr_s = current_seconds % 60
                    arr_time = f"{arr_h:02d}:{arr_m:02d}:{arr_s:02d}"

                    dwell = random.randint(15, 45)
                    dep_seconds = current_seconds + dwell
                    dep_h = dep_seconds // 3600
                    dep_m = (dep_seconds % 3600) // 60
                    dep_s = dep_seconds % 60
                    dep_time = f"{dep_h:02d}:{dep_m:02d}:{dep_s:02d}"

                    stop_times.append({
                        "trip_id": trip_id,
                        "arrival_time": arr_time,
                        "departure_time": dep_time,
                        "stop_id": stop["stop_id"],
                        "stop_sequence": seq + 1,
                        "stop_headsign": "",
                        "pickup_type": 0,
                        "drop_off_type": 0,
                        "shape_dist_traveled": round(seq * random.uniform(0.3, 1.2), 3),
                    })

                    # Travel time to next stop: 60-300 seconds
                    travel = random.randint(60, 300)
                    current_seconds += dwell + travel

                trip_counter += 1

    return trips, stop_times

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write GTFS snapshots to volume (3 weekly snapshots)

# COMMAND ----------

snapshot_dates = ["2026-03-08", "2026-03-15", "2026-03-22"]
stops = generate_stops()

for snapshot_date in snapshot_dates:
    base_path = f"{volume_path}/gtfs/extracted/{snapshot_date}"
    dbutils.fs.mkdirs(base_path)

    # --- routes.txt ---
    routes_csv = io.StringIO()
    writer = csv.DictWriter(routes_csv, fieldnames=[
        "route_id", "agency_id", "route_short_name", "route_long_name",
        "route_desc", "route_type", "route_url", "route_color", "route_text_color"
    ])
    writer.writeheader()
    for r in ROUTES:
        writer.writerow({
            "route_id": r[0], "agency_id": "TransLink",
            "route_short_name": r[1], "route_long_name": r[2],
            "route_desc": "", "route_type": r[3], "route_url": "",
            "route_color": "0000FF", "route_text_color": "FFFFFF"
        })
    dbutils.fs.put(f"{base_path}/routes.txt", routes_csv.getvalue(), overwrite=True)

    # --- stops.txt ---
    # Slightly perturb a few stops between snapshots to demonstrate SCD Type 2
    snapshot_stops = []
    for s in stops:
        s_copy = dict(s)
        if snapshot_date != snapshot_dates[0] and random.random() < 0.02:
            s_copy["stop_name"] = s_copy["stop_name"] + " (Relocated)"
            s_copy["stop_lat"] = round(s_copy["stop_lat"] + random.uniform(-0.001, 0.001), 6)
            s_copy["stop_lon"] = round(s_copy["stop_lon"] + random.uniform(-0.001, 0.001), 6)
        snapshot_stops.append(s_copy)

    stops_csv = io.StringIO()
    writer = csv.DictWriter(stops_csv, fieldnames=[
        "stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon",
        "zone_id", "stop_url", "location_type", "parent_station", "wheelchair_boarding"
    ])
    writer.writeheader()
    for s in snapshot_stops:
        writer.writerow(s)
    dbutils.fs.put(f"{base_path}/stops.txt", stops_csv.getvalue(), overwrite=True)

    # --- trips.txt and stop_times.txt ---
    trips, stop_times = generate_trips_and_stop_times(ROUTES, snapshot_stops, snapshot_date)

    trips_csv = io.StringIO()
    writer = csv.DictWriter(trips_csv, fieldnames=[
        "route_id", "service_id", "trip_id", "trip_headsign",
        "direction_id", "block_id", "shape_id", "wheelchair_accessible", "bikes_allowed"
    ])
    writer.writeheader()
    for t in trips:
        writer.writerow(t)
    dbutils.fs.put(f"{base_path}/trips.txt", trips_csv.getvalue(), overwrite=True)

    stop_times_csv = io.StringIO()
    writer = csv.DictWriter(stop_times_csv, fieldnames=[
        "trip_id", "arrival_time", "departure_time", "stop_id",
        "stop_sequence", "stop_headsign", "pickup_type", "drop_off_type",
        "shape_dist_traveled"
    ])
    writer.writeheader()
    for st in stop_times:
        writer.writerow(st)
    dbutils.fs.put(f"{base_path}/stop_times.txt", stop_times_csv.getvalue(), overwrite=True)

    print(f"Snapshot {snapshot_date}: {len(ROUTES)} routes, {len(snapshot_stops)} stops, "
          f"{len(trips)} trips, {len(stop_times)} stop_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Synthetic Traffic API Data
# MAGIC
# MAGIC Simulates Google Routes API responses with realistic congestion patterns:
# MAGIC - Morning rush (7-9am): higher congestion on inbound corridors
# MAGIC - Evening rush (4-6pm): higher congestion on outbound corridors
# MAGIC - Bridges: more congestion variability than arterials
# MAGIC - Weekends: lower overall congestion

# COMMAND ----------

import os

# Load corridors.json relative to this notebook's location
_notebook_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
_corridors_path = f"/Workspace{_notebook_dir}/corridors.json"
with open(_corridors_path) as f:
    corridors = json.load(f)

print(f"Loaded {len(corridors)} corridor definitions")

# COMMAND ----------

def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in kilometers."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def congestion_multiplier(hour: int, day_of_week: int, corridor_type: str) -> float:
    """
    Returns a congestion ratio multiplier based on time of day and corridor type.
    Weekday rush hours have higher congestion; bridges are more volatile.
    """
    is_weekend = day_of_week in (5, 6)  # Saturday=5, Sunday=6

    # Base congestion by time of day (weekday)
    if is_weekend:
        if 10 <= hour <= 16:
            base = random.uniform(1.0, 1.3)
        else:
            base = random.uniform(1.0, 1.15)
    else:
        if 7 <= hour <= 9:       # Morning rush
            base = random.uniform(1.2, 2.2)
        elif 16 <= hour <= 18:   # Evening rush
            base = random.uniform(1.3, 2.5)
        elif 11 <= hour <= 14:   # Midday
            base = random.uniform(1.0, 1.4)
        elif 19 <= hour <= 21:   # Evening
            base = random.uniform(1.0, 1.3)
        else:                    # Overnight
            base = random.uniform(1.0, 1.1)

    # Corridor type modifier
    if corridor_type == "bridge":
        base *= random.uniform(1.0, 1.3)
    elif corridor_type == "highway":
        base *= random.uniform(0.95, 1.15)

    return round(base, 3)


# Generate 7 days of 15-minute interval readings
start_date = datetime(2026, 3, 22, 5, 0, 0)  # Start at 5 AM
num_days = 7
poll_interval_minutes = 15

traffic_records = []
current_time = start_date

for day in range(num_days):
    day_start = start_date + timedelta(days=day)
    day_of_week = day_start.weekday()

    for hour in range(5, 24):  # 5 AM to 11 PM
        for minute in [0, 15, 30, 45]:
            polled_at = day_start.replace(hour=hour, minute=minute, second=0)

            for corridor in corridors:
                distance_km = haversine_km(
                    corridor["origin"]["lat"], corridor["origin"]["lng"],
                    corridor["destination"]["lat"], corridor["destination"]["lng"]
                )
                distance_meters = int(distance_km * 1000)

                # Static duration (free-flow) based on speed limit
                speed_limit = corridor.get("speed_limit_kmh", 50)
                static_seconds = int((distance_km / speed_limit) * 3600)
                static_seconds = max(static_seconds, 60)  # minimum 1 minute

                # Apply congestion
                ratio = congestion_multiplier(hour, day_of_week, corridor["corridor_type"])
                actual_seconds = int(static_seconds * ratio)

                traffic_records.append({
                    "corridor_id": corridor["corridor_id"],
                    "corridor_name": corridor["corridor_name"],
                    "origin_lat": corridor["origin"]["lat"],
                    "origin_lng": corridor["origin"]["lng"],
                    "dest_lat": corridor["destination"]["lat"],
                    "dest_lng": corridor["destination"]["lng"],
                    "duration_seconds": actual_seconds,
                    "static_duration_seconds": static_seconds,
                    "distance_meters": distance_meters,
                    "congestion_ratio": ratio,
                    "api_response_json": json.dumps({"status": "OK", "corridor": corridor["corridor_id"]}),
                    "polled_at": polled_at.strftime("%Y-%m-%dT%H:%M:%S"),
                })

print(f"Generated {len(traffic_records)} traffic API records "
      f"({num_days} days x {len(corridors)} corridors x {19*4} polls/day)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write traffic API JSON files to volume (one file per day)

# COMMAND ----------

from collections import defaultdict

records_by_day = defaultdict(list)
for rec in traffic_records:
    day_str = rec["polled_at"][:10]
    records_by_day[day_str].append(rec)

api_path = f"{volume_path}/traffic_api"
dbutils.fs.mkdirs(api_path)

for day_str, day_records in sorted(records_by_day.items()):
    # Write as newline-delimited JSON (one JSON object per line)
    lines = [json.dumps(r) for r in day_records]
    content = "\n".join(lines)
    file_path = f"{api_path}/{day_str}_traffic.json"
    dbutils.fs.put(file_path, content, overwrite=True)
    print(f"Wrote {len(day_records)} records to {file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Synthetic data written to volume:
# MAGIC - `gtfs/extracted/{date}/routes.txt` — 30 routes x 3 snapshots
# MAGIC - `gtfs/extracted/{date}/stops.txt` — 200 stops x 3 snapshots (with SCD changes)
# MAGIC - `gtfs/extracted/{date}/trips.txt` — ~1,500 trips x 3 snapshots
# MAGIC - `gtfs/extracted/{date}/stop_times.txt` — ~15,000 records x 3 snapshots
# MAGIC - `traffic_api/{date}_traffic.json` — ~1,200 records/day x 7 days
# MAGIC
# MAGIC Ready for Auto Loader ingestion by the SDP pipeline.

# COMMAND ----------

print("Synthetic data generation complete!")
print(f"Volume path: {volume_path}")
dbutils.fs.ls(volume_path)

import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from datetime import datetime

# --- KAFKA CONSUMER ---
consumer = KafkaConsumer(
    "weather_processed",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# --- CASSANDRA CONNECTION ---
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

# --- CREATE KEYSPACE ---
session.execute("""
CREATE KEYSPACE IF NOT EXISTS weather
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace("weather")

# --- CREATE TABLE (UPDATED SCHEMA) ---
session.execute("""
CREATE TABLE IF NOT EXISTS weather_data (
    location_name text,
    timestamp timestamp,
    temperature_celsius float,
    humidity float,
    wind_kph float,
    precip_mm float,
    cloud float,
    pressure_mb float,
    uv_index float,
    visibility_km float,
    classification text,
    PRIMARY KEY (location_name, timestamp)
)
""")

print("Consumer running...")

# --- HELPER: PARSE TIMESTAMP ---
def parse_timestamp(ts):
    formats = [
        "%d/%m/%Y %H:%M",  # 16/05/2024 10:45
        "%Y-%m-%d %H:%M"   # 2024-05-16 02:45
    ]

    for fmt in formats:
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue

    print("Timestamp parse error: unknown format ->", ts)
    return datetime.now()  # fallback

# --- CONSUME LOOP ---
for msg in consumer:
    data = msg.value

    try:
        ts = parse_timestamp(data.get("last_updated", ""))

        session.execute("""
        INSERT INTO weather_data (
            location_name,
            timestamp,
            temperature_celsius,
            humidity,
            wind_kph,
            precip_mm,
            cloud,
            pressure_mb,
            uv_index,
            visibility_km,
            classification
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data.get("location_name", "unknown"),
            ts,
            float(data.get("temperature_celsius", 0)),
            float(data.get("humidity", 0)),
            float(data.get("wind_kph", 0)),
            float(data.get("precip_mm", 0)),
            float(data.get("cloud", 0)),
            float(data.get("pressure_mb", 0)),
            float(data.get("uv_index", 0)),
            float(data.get("visibility_km", 0)),
            data.get("classification", "unknown")
        ))

        print("Inserted:", data)

    except Exception as e:
        print("Cassandra error:", e)
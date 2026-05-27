import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "weather_raw"

def stream_csv(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]
        print("Detected:", reader.fieldnames)

        for row in reader:
            row = {k.strip().lower(): v for k, v in row.items()}

            record = {
                "location_name": row.get("location_name", ""),
                "last_updated": row.get("last_updated", ""),

                "temperature_celsius": float(row.get("temperature_celsius", 0) or 0),
                "humidity": float(row.get("humidity", 0) or 0),
                "wind_kph": float(row.get("wind_kph", 0) or 0),
                "precip_mm": float(row.get("precip_mm", 0) or 0),
                "cloud": float(row.get("cloud", 0) or 0),
                "pressure_mb": float(row.get("pressure_mb", 0) or 0),
                "uv_index": float(row.get("uv_index", 0) or 0),
                "visibility_km": float(row.get("visibility_km", 0) or 0)
            }

            producer.send(TOPIC, record)
            producer.flush()

            print("Sent:", record)
            time.sleep(1)

if __name__ == "__main__":
    stream_csv("GlobalWeatherRepository.csv")
import json
import joblib
from datetime import datetime
import pandas as pd
import traceback

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types

KAFKA_BROKER = "kafka:29092"
INPUT_TOPIC = "weather_raw"
OUTPUT_TOPIC = "weather_processed"

model = joblib.load("/opt/flink/model.pkl")
model.verbose = False

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_topics("weather_raw") \
    .set_group_id("flink-group") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

def get_season_from_timestamp(ts):
    formats = [
        "%d/%m/%Y %H:%M",  # old format
        "%Y-%m-%d %H:%M"   # new format
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(ts, fmt)
            month = dt.month

            # Southern Hemisphere (correct for your location)
            if month in [12, 1, 2]:
                return "Summer"
            elif month in [3, 4, 5]:
                return "Autumn"
            elif month in [6, 7, 8]:
                return "Winter"
            else:
                return "Spring"

        except ValueError:
            continue

    print("Season parse error: unknown format ->", ts)
    return "Summer"


def process(record):

    # Ensure string
    if isinstance(record, bytes):
        record = record.decode("utf-8")

    data = json.loads(record)

    try:
        season = get_season_from_timestamp(data.get("last_updated", ""))
        season = season.strip().title()
        if season not in ["Winter", "Summer", "Autumn", "Spring"]:
            season = "Summer"

        precip_percent = min(float(data.get("precip_mm", 0)) * 10, 100)


        RAW_COLUMNS = [
            "Temperature",
            "Humidity",
            "Wind Speed",
            "Precipitation (%)",
            "Cloud Cover",
            "Atmospheric Pressure",
            "UV Index",
            "Season",
            "Visibility (km)",
            "Location"
        ]

        features = pd.DataFrame([{
            "Temperature": float(data.get("temperature_celsius", 0)),
            "Humidity": float(data.get("humidity", 0)),
            "Wind Speed": float(data.get("wind_kph", 0)),
            "Precipitation (%)": min(float(data.get("precip_mm", 0)) * 10, 100),
            "Cloud Cover": float(data.get("cloud", 0)),
            "Atmospheric Pressure": float(data.get("pressure_mb", 0)),
            "UV Index": float(data.get("uv_index", 0)),
            "Season": str(season),
            "Visibility (km)": float(data.get("visibility_km", 0)),
            "Location": str(data.get("location_name", "unknown"))
        }])

        features = features.reindex(columns=RAW_COLUMNS)
        
        for col in ["Season", "Location"]:
            features[col] = features[col].astype(str).fillna("unknown")
    
        try:
            prediction = model.predict(features.astype(object))[0]
        except Exception as e:
            print("MODEL PREDICTION FAILED:", e)
            prediction = "error"
        data["classification"] = str(prediction)

    except Exception as e:
        data["classification"] = "error"
        print("FULL ERROR TRACE:")
        traceback.print_exc()

    return json.dumps(data)

processed = stream.map(process, output_type=Types.STRING())

sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic("weather_processed")
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    ) \
    .build()

processed.sink_to(sink)

processed.print()

env.execute("Weather Pipeline")
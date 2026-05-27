CREATE KEYSPACE IF NOT EXISTS weather
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE weather;

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
);
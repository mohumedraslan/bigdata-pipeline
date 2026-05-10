import os
import json
import time
from kafka import KafkaConsumer
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "parking.sensors.raw")
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "super-secret-token-change-me")
INFLUX_ORG = os.getenv("INFLUX_ORG", "smart_parking")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "sensor_data")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "smart_parking")

def build_consumer(retries: int = 15, delay: int = 4) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                TOPIC_RAW,
                bootstrap_servers=KAFKA_SERVERS,
                group_id="stream-processor-group",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready (attempt {attempt}/{retries}) — retrying in {delay}s")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")

def main() -> None:
    consumer = build_consumer()
    mongo_client = MongoClient(MONGO_URI)
    mongo_col = mongo_client[MONGO_DB]["sensor_events"]
    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    influx_write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    print("Stream processor listening...")

    for msg in consumer:
        data = msg.value

        # Insert to MongoDB
        mongo_col.insert_one(data)

        # Insert to InfluxDB
        p = (
            Point("sensor_reading")
            .tag("unit_id", data["unit_id"])
            .tag("spot_id", data["spot_id"])
            .tag("phase", data["phase"])
            .tag("session_id", data["session_id"])
            .field("front_cm", float(data["front_cm"]))
            .field("rear_cm", float(data["rear_cm"]))
            .field("left_cm", float(data["left_cm"]))
            .field("right_cm", float(data["right_cm"]))
            .field("servo_angle", int(data["servo_angle"]))
            .field("speed_ms", float(data["speed_ms"]))
            .field("min_clearance", min(data["front_cm"], data["rear_cm"], data["left_cm"], data["right_cm"]))
            .time(data["ts"], WritePrecision.NS)
        )
        influx_write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)

        print(f"Processed: {data['unit_id']} {data['phase']}")

if __name__ == "__main__":
    main()

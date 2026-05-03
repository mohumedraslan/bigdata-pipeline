"""
consumers/src/alert_consumer.py
────────────────────────────────
Dedicated consumer for the parking.alerts topic.

In production this is where you would:
  • Send push notifications to operators
  • Write to a PagerDuty / Slack webhook
  • Trigger a physical alarm / emergency stop relay on the truck

For the university demo it prints a structured alert log and persists
all alerts to MongoDB for post-incident analysis.
"""

import os
import json
import logging
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("alert-consumer")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_ALERTS  = os.getenv("KAFKA_TOPIC_ALERTS", "parking.alerts")
MONGO_URI     = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB      = os.getenv("MONGO_DB", "smart_parking")


def build_consumer(retries: int = 15, delay: int = 4) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                TOPIC_ALERTS,
                bootstrap_servers=KAFKA_SERVERS,
                group_id="alert-consumer-group",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %d/%d) — retrying in %ds", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")


def main() -> None:
    consumer = build_consumer()
    mongo    = MongoClient(MONGO_URI)
    col      = mongo[MONGO_DB]["alerts"]

    log.info("🚨 Alert consumer listening on topic: %s", TOPIC_ALERTS)

    for msg in consumer:
        alert = msg.value

        severity = alert.get("severity", "UNKNOWN")
        unit_id  = alert.get("unit_id",  "?")
        message  = alert.get("message",  "")

        if severity == "CRITICAL":
            log.critical("🔴 [%s] %s", unit_id, message)
        else:
            log.warning("🟡 [%s] %s", unit_id, message)

        # Persist for post-incident review
        col.insert_one(alert)


if __name__ == "__main__":
    main()

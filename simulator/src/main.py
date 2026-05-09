import os
import time
import logging
import random

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from schemas import SensorReading, ParkingDecision, Alert
from parking_state_machine import TruckUnit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("simulator")

BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_RAW          = os.getenv("KAFKA_TOPIC_RAW", "parking.sensors.raw")
TOPIC_DECISIONS    = os.getenv("KAFKA_TOPIC_DECISIONS", "parking.decisions")
TOPIC_ALERTS       = os.getenv("KAFKA_TOPIC_ALERTS", "parking.alerts")
EMIT_INTERVAL      = float(os.getenv("EMIT_INTERVAL_SECONDS", "1"))
NUM_UNITS          = int(os.getenv("UNITS", "3"))

SPOT_IDS = [f"SPOT_{row}{num}" for row in "ABCD" for num in range(1, 6)]


def build_producer(retries: int = 15, delay: int = 4) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: v.encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=10,
                compression_type="gzip",
            )
            log.info("✅ Connected to Kafka at %s", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning("⏳ Kafka not ready (attempt %d/%d) — retrying in %ds", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries")


def build_units(n: int) -> list[TruckUnit]:
    units = []
    for i in range(1, n + 1):
        unit = TruckUnit(
            unit_id = f"TRUCK_{i:03d}",
            spot_id = random.choice(SPOT_IDS),
        )
        units.append(unit)
        log.info("🚛 Initialised %s → %s", unit.unit_id, unit.spot_id)
    return units


def main() -> None:
    producer = build_producer()
    units    = build_units(NUM_UNITS)

    log.info("🚦 Simulator running — emitting every %.1fs", EMIT_INTERVAL)

    while True:
        for unit in units:
            reading, decision = unit.step()

            producer.send(
                TOPIC_RAW,
                key=unit.unit_id.encode(),
                value=reading.to_json(),
            )

            producer.send(
                TOPIC_DECISIONS,
                key=unit.unit_id.encode(),
                value=decision.to_json(),
            )

            if reading.is_critical:
                alert = Alert(
                    unit_id    = unit.unit_id,
                    session_id = unit.session_id,
                    severity   = "CRITICAL",
                    message    = (
                        f"Obstacle within critical range: "
                        f"F={reading.front_cm}cm R={reading.rear_cm}cm "
                        f"L={reading.left_cm}cm Ri={reading.right_cm}cm"
                    ),
                    front_cm   = reading.front_cm,
                    rear_cm    = reading.rear_cm,
                    left_cm    = reading.left_cm,
                    right_cm   = reading.right_cm,
                )
                producer.send(TOPIC_ALERTS, key=unit.unit_id.encode(), value=alert.to_json())
                log.warning("🚨 ALERT: %s", alert.message)

            log.info(
                "📡 %s | Phase=%-12s | F=%5.1f R=%5.1f L=%5.1f Ri=%5.1f | %-16s (%.2f)",
                unit.unit_id, unit.phase,
                reading.front_cm, reading.rear_cm, reading.left_cm, reading.right_cm,
                decision.decision, decision.confidence,
            )

            if unit.is_done():
                log.info("🅿️  %s successfully parked at %s — resetting", unit.unit_id, unit.spot_id)
                unit.spot_id = random.choice(SPOT_IDS)
                unit.reset()

        producer.flush()
        time.sleep(EMIT_INTERVAL)


if __name__ == "__main__":
    main()
            )

            if reading.is_critical:
                alert = Alert(
                    unit_id    = unit.unit_id,
                    session_id = unit.session_id,
                    severity   = "CRITICAL",
                    message    = (
                        f"Obstacle within critical range: "
                        f"F={reading.front_cm}cm R={reading.rear_cm}cm "
                        f"L={reading.left_cm}cm Ri={reading.right_cm}cm"
                    ),
                    front_cm   = reading.front_cm,
                    rear_cm    = reading.rear_cm,
                    left_cm    = reading.left_cm,
                    right_cm   = reading.right_cm,
                )
                producer.send(TOPIC_ALERTS, key=unit.unit_id.encode(), value=alert.to_json())
                log.warning("🚨 ALERT: %s", alert.message)

            log.info(
                "📡 %s | Phase=%-12s | F=%5.1f R=%5.1f L=%5.1f Ri=%5.1f | %-16s (%.2f)",
                unit.unit_id, unit.phase,
                reading.front_cm, reading.rear_cm, reading.left_cm, reading.right_cm,
                decision.decision, decision.confidence,
            )

            if unit.is_done():
                log.info("🅿️  %s successfully parked at %s — resetting", unit.unit_id, unit.spot_id)
                unit.spot_id = random.choice(SPOT_IDS)
                unit.reset()

        producer.flush()
        time.sleep(EMIT_INTERVAL)


if __name__ == "__main__":
    main()

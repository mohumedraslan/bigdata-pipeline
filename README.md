# Smart Parking – Big Data Pipeline

> **Real-time IoT streaming system for autonomous truck parking**
> University project · Faculty of AI · Egypt

---

## Overview

This system ingests high-frequency ultrasonic sensor readings from trucks performing automated parking manoeuvres and runs them through a full big data pipeline — from raw sensor events all the way to live Grafana dashboards and batch analytics reports.

The sensor data currently comes from a **simulator** that models realistic truck-parking physics (approach → align → reverse → confirm → parked). Before the final presentation, the simulator will be swapped out for live **Firebase Realtime Database** reads with zero changes to the downstream pipeline.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  Firebase Realtime DB  ←→  Simulator (current)                  │
└────────────────────────────┬────────────────────────────────────┘
                             │  JSON events (1 Hz per truck)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                          KAFKA                                  │
│   parking.sensors.raw   parking.decisions   parking.alerts      │
│   (3 partitions)        (3 partitions)      (1 partition)       │
└───────┬─────────────────────┬──────────────────────────────────┘
        │                     │
        ▼                     ▼
┌───────────────┐    ┌──────────────────────────────────────────┐
│ Alert Consumer│    │   PySpark Structured Streaming           │
│ (real-time    │    │   - Parse + validate schema              │
│  critical     │    │   - Enrich (min_clearance, alert_flag)   │
│  events)      │    │   - 30s tumbling window aggregations     │
└───────┬───────┘    └────────────┬──────────────┬─────────────┘
        │                         │              │
        ▼                         ▼              ▼
┌──────────────┐       ┌──────────────┐  ┌──────────────┐
│   MongoDB    │       │   InfluxDB   │  │   MongoDB    │
│  alerts col  │       │  time-series │  │ sensor_events│
└──────────────┘       └──────┬───────┘  └──────┬───────┘
                              │                  │
                              ▼                  ▼
                       ┌──────────────┐  ┌──────────────┐
                       │   Grafana    │  │  Spark Batch │
                       │  Live Dash   │  │  Analytics   │
                       └──────────────┘  └──────────────┘
                                                 │
                                         ┌───────▼──────┐
                                         │  FastAPI REST │
                                         │  Query Layer  │
                                         └──────────────┘
```

---

## Tech Stack

| Layer          | Technology          | Purpose                              |
|---------------|---------------------|--------------------------------------|
| Message Bus   | Apache Kafka 7.6    | Durable, partitioned event streaming |
| Stream Engine | PySpark 3.5         | Structured Streaming with watermarks |
| Time-series   | InfluxDB 2.7        | High-resolution sensor metrics       |
| Document Store| MongoDB 7.0         | Event history & session storage      |
| Dashboard     | Grafana 10.4        | Live visualisation (auto-provisioned)|
| REST API      | FastAPI             | Query interface over stored data     |
| Containers    | Docker Compose      | Single-command deployment            |

---

## Kafka Topics

| Topic                  | Partitions | Producers  | Consumers              |
|------------------------|------------|------------|------------------------|
| `parking.sensors.raw`  | 3          | Simulator  | PySpark Structured Streaming |
| `parking.decisions`    | 3          | Simulator  | (future: control system)     |
| `parking.alerts`       | 1          | Simulator  | Alert Consumer               |

---

## Sensor Payload Schema

```json
{
  "unit_id":     "TRUCK_001",
  "spot_id":     "SPOT_B4",
  "session_id":  "550e8400-e29b-41d4-a716-446655440000",
  "phase":       "REVERSING",
  "front_cm":    182.4,
  "rear_cm":     34.1,
  "left_cm":     45.8,
  "right_cm":    48.2,
  "servo_angle": 90,
  "speed_ms":    -0.2,
  "ts":          "2024-11-01T14:32:05.123456+00:00"
}
```

### Parking Phases

| Phase        | Description                                        |
|--------------|----------------------------------------------------|
| `APPROACHING`| Truck moving toward spot from ~500 cm              |
| `ALIGNING`   | Correcting lateral offset before reversing         |
| `REVERSING`  | Backing into the spot                              |
| `CONFIRMING` | Final all-round clearance check                    |
| `PARKED`     | Successfully parked — session complete             |

---

## Project Structure

```
smart-parking/
├── .env                      # All config (single source of truth)
├── docker-compose.yml
│
├── simulator/                # Fake Firebase sensor emitter
│   ├── src/
│   │   ├── main.py           # Kafka producer entrypoint
│   │   ├── schemas.py        # Shared data models
│   │   └── parking_state_machine.py   # Physics + state machine
│   ├── Dockerfile
│   └── requirements.txt
│
├── stream_processor/         # PySpark Structured Streaming
│   ├── src/main.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── consumers/                # Alert consumer
│   ├── src/alert_consumer.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── spark/                    # Scheduled batch analytics
│   ├── jobs/batch_analytics.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── api/                      # FastAPI query layer
│   ├── src/main.py
│   ├── Dockerfile
│   └── requirements.txt
│
└── grafana/
    └── provisioning/
        ├── datasources/influxdb.yml
        └── dashboards/
            ├── dashboard.yml
            └── parking.json  # Auto-loaded dashboard
```

---

## Running the Project

### Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- 4 GB RAM minimum available to Docker

### Start everything

```bash
git clone https://github.com/your-org/smart-parking.git
cd smart-parking
docker compose up --build
```

First boot takes ~2 minutes while Kafka initialises and Spark downloads its connectors. Wait for:

```
simulator     | ✅ Connected to Kafka at kafka:9092
simulator     | 🚦 Simulator running — emitting every 1.0s
stream-processor | ✅ All streaming sinks started — awaiting data …
```

### Access the services

| Service         | URL                          | Credentials     |
|-----------------|------------------------------|-----------------|
| Grafana         | http://localhost:3000        | admin / admin   |
| FastAPI Docs    | http://localhost:8000/docs   | —               |
| InfluxDB UI     | http://localhost:8086        | admin / admin123456 |
| Kafka           | localhost:9092               | —               |

### Run batch analytics manually

```bash
docker compose run --rm spark
```

---

## Connecting Firebase (Before Presentation)

The simulator produces JSON identical to what Firebase would emit.  
To switch to live data, replace `simulator/src/main.py` with a Firebase listener:

```python
import firebase_admin
from firebase_admin import credentials, db

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred, {"databaseURL": "https://your-project.firebaseio.com"})

ref = db.reference("/sensors")

def on_update(event):
    reading = SensorReading(**event.data)
    producer.send(TOPIC_RAW, key=reading.unit_id.encode(), value=reading.to_json())

ref.listen(on_update)
```

Everything downstream (Spark, InfluxDB, MongoDB, Grafana) remains unchanged.

---

## API Endpoints

```
GET /health                        — liveness check
GET /units                         — all active truck IDs
GET /units/{unit_id}/latest        — latest reading for a truck
GET /sessions                      — list recent parking sessions
GET /sessions/{session_id}         — full event trace for one session
GET /stats/clearance?window_minutes=5  — rolling clearance stats
GET /alerts                        — recent critical alerts
```

Interactive docs at **http://localhost:8000/docs**

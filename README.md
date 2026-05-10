# Smart Parking Big Data Pipeline

University project demonstrating a real-time IoT data pipeline for autonomous truck parking.

## Overview

This project simulates truck parking sensors and processes the data through a big data pipeline:

- **Simulator**: Generates sensor data for trucks parking.
- **Kafka**: Message broker for streaming data.
- **Spark Streaming**: Processes real-time data, stores in MongoDB and InfluxDB.
- **Grafana**: Visualizes data from InfluxDB.
- **Batch Analytics**: Uses Spark to analyze historical data from MongoDB and load into DWH (MongoDB collection).
- **Airflow**: Schedules batch jobs.
- **API**: FastAPI for querying data.

## Architecture

```
Simulator → Kafka → Spark Streaming → MongoDB / InfluxDB → Grafana
                                      ↓
                                   Batch Spark → DWH (MongoDB)
                                      ↓
                                   Airflow (scheduling)
```

## Tech Stack

- Kafka: Message streaming
- Spark: Streaming and batch processing
- MongoDB: Document store for events and DWH
- InfluxDB: Time-series database
- Grafana: Dashboard
- Airflow: Workflow scheduling
- Docker: Containerization

## How to Run

1. Clone the repo.
2. Run `docker-compose up` to start all services.
3. Access:
   - Grafana: http://localhost:3000 (admin/admin)
   - API: http://localhost:8000
   - Airflow: http://localhost:8080

## Pipeline Explanation

- **Simulator**: Creates fake sensor readings (front, rear, left, right distances) for trucks in phases: APPROACHING, ALIGNING, REVERSING, CONFIRMING, PARKED.
- **Kafka Topics**:
  - parking.sensors.raw: Sensor data
  - parking.decisions: Parking decisions
  - parking.alerts: Critical alerts
- **Spark Streaming**: Reads from Kafka, parses JSON, writes to MongoDB (events) and InfluxDB (time-series).
- **Alert Consumer**: Listens to alerts topic, logs and stores in MongoDB.
- **Batch Analytics**: Reads from MongoDB, computes KPIs, stores in DWH collection.
- **Airflow**: Runs batch job daily.
- **API**: Endpoints to query units, sessions, stats, alerts.

## Data Flow

1. Simulator sends sensor readings to Kafka.
2. Spark Streaming consumes, enriches, stores in DBs.
3. Grafana shows real-time dashboards.
4. Batch job analyzes historical data, loads to DWH.
5. API allows querying the data.

## For University Presentation

- Show the simulator running and generating data.
- Demonstrate data in Kafka, MongoDB, InfluxDB.
- Show Grafana dashboards with live data.
- Run batch analytics and show results.
- Explain each component and how they fit the pipeline.

## Files Structure

- simulator/: Sensor simulator
- stream_processor/: Spark streaming job
- consumers/: Alert consumer
- spark/jobs/: Batch analytics
- api/: REST API
- grafana/: Dashboard configs
- docker-compose.yml: Services setup

## Key Points to Study

- **Kafka**: Pub-sub messaging. Topics: parking.sensors.raw, parking.decisions, parking.alerts.
- **Spark Streaming**: Real-time processing using Structured Streaming. Reads from Kafka, writes to MongoDB and InfluxDB.
- **Batch Processing**: Uses Spark to read from MongoDB, compute aggregations, store in DWH collection.
- **Airflow**: Workflow scheduler for batch jobs.
- **Grafana**: Visualization tool connected to InfluxDB.
- **API**: REST endpoints for data access.

## Commands

- Start: `docker-compose up -d`
- Stop: `docker-compose down`
- View logs: `docker-compose logs -f [service]`
- Run batch manually: `docker-compose exec airflow bash -c "cd /opt/airflow/jobs && python batch_analytics.py"`


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

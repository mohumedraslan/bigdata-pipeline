
This repo is a complete smart-parking data pipeline with:
- a **simulator** that emits fake truck sensor data,
- **Kafka** as the message bus,
- a **PySpark stream processor** that enriches and stores data,
- an **alert consumer** for critical events,
- a **FastAPI** REST query service,
- **InfluxDB** for time-series metrics,
- **MongoDB** for event history,
- and **Grafana** for dashboards.

It is designed so the simulator can later be swapped with a real Firebase data source without changing downstream processing.

---

## Architecture

The data flow is:

1. simulator emits:
   - `parking.sensors.raw`
   - `parking.decisions`
   - `parking.alerts`

2. Kafka holds the events.

3. stream_processor reads `parking.sensors.raw`, enriches the data, and writes:
   - InfluxDB — real-time time-series
   - MongoDB — event history

4. `alert_consumer` reads `parking.alerts` and saves alerts to MongoDB.

5. api exposes REST endpoints for:
   - latest truck status
   - sessions
   - alerts
   - InfluxDB clearance stats

6. grafana visualizes metrics from InfluxDB.

7. batch_analytics.py can run separately to compute offline KPIs from MongoDB.

---

## File-by-file explanation

### README.md
- Explains the project purpose and architecture.
- Lists technologies used.
- Shows Kafka topics and how services connect.
- Describes the payload schema and parking phases.
- Documents how to run the system.

### docker-compose.yml
Defines the runtime environment:
- `networks` and `volumes`
- `zookeeper` and `kafka`: Kafka cluster bootstrap
- `kafka-init`: creates required Kafka topics
- `influxdb`: time-series storage
- `mongo`: document database
- simulator: produces Kafka events
- `stream-processor`: consumes raw events and stores results
- `alert-consumer`: consumes alerts
- api: REST API service
- grafana: dashboard service

Key points:
- `KAFKA_NUM_PARTITIONS: 3` sets default partitions for topics.
- `kafka-init` uses `kafka-topics --create` to ensure topics exist.
- `InfluxDB` and `MongoDB` are exposed for persistence.
- Services use .env to get connection strings and topic names.

---

## Simulator

### schemas.py
This file defines shared data models.

- `Phase`: valid parking phases like `APPROACHING`, `ALIGNING`, `REVERSING`, `CONFIRMING`, `PARKED`, `EXITING`.
- `Decision`: valid actions like `MOVE_FORWARD`, `MOVE_BACKWARD`, `TURN_LEFT`, `STOP`, `EMERGENCY_STOP`.

Dataclasses:
- `SensorReading`
  - fields: `unit_id`, `spot_id`, `session_id`, `phase`, `front_cm`, `rear_cm`, `left_cm`, `right_cm`, `servo_angle`, `speed_ms`, `ts`
  - `to_json()` serializes it to JSON.
  - `from_json()` deserializes from JSON.
  - `min_clearance` computes smallest sensor value.
  - `is_critical` is true if any reading is below 15 cm.

- `ParkingDecision`
  - same basic structure as a decision event.
  - also serializes/deserializes to JSON.

- `Alert`
  - adds `alert_id`, `severity`, `message`, and timestamp.
  - serializes to JSON.

### parking_state_machine.py
Simulates truck parking physics and decisions.

- Defines thresholds:
  - `APPROACH_DONE = 150`
  - `ALIGN_DONE = 80`
  - `REVERSE_DONE = 25`
  - `CONFIRM_DONE = 20`
  - `EMERGENCY_STOP = 12`

- `TruckUnit`
  - holds a single truck’s state.
  - internal state: front/rear/left/right distances, servo, speed, lateral error.
  - `step()` does one tick:
    - decide action
    - apply action
    - emit `SensorReading`
    - emit `ParkingDecision`

- `_decide()`
  - emergency stop if too close.
  - `APPROACHING`: move forward until front < 150 cm.
  - `ALIGNING`: nudge left/right until lateral error is small.
  - `REVERSING`: move backward until rear < 25 cm.
  - `CONFIRMING`: finalize if sides are clear.
  - `PARKED`: stop.

- `_apply()`
  - moves the simulated truck state based on the decision.
  - updates distances and speed, adding randomized motion.

- `_make_reading()` and `_make_decision()`
  - create the actual payload objects sent to Kafka.

- `is_done()` and `reset()`
  - detect finished sessions and restart a new session.

### main.py
Simulator entrypoint.

- imports environment variables and logger.
- `build_producer()`
  - creates a `KafkaProducer`
  - retries until Kafka is available
  - uses gzip compression and `acks="all"` for reliability

- `build_units(n)`
  - creates `n` `TruckUnit` objects
  - assigns each truck a random `spot_id`

- `main()`
  - connects to Kafka
  - loops forever
  - for each truck:
    - calls `unit.step()`
    - sends raw reading to `parking.sensors.raw`
    - sends decision to `parking.decisions`
    - if reading is critical, sends an `Alert` to `parking.alerts`
    - logs the state
    - if parked, resets the truck for a new session
  - flushes Kafka producer and sleeps for the configured interval

---

## Stream processor

### main.py
PySpark Structured Streaming pipeline.

- Configures environment variables for Kafka, InfluxDB, MongoDB.

- Defines `SENSOR_SCHEMA`
  - exact schema used to parse Kafka JSON payloads.

- `write_to_influx(df, epoch_id)`
  - converts each Spark row into InfluxDB points
  - writes `sensor_reading` measurement
  - includes `min_clearance`

- `write_to_mongo(df, epoch_id)`
  - writes each batch into MongoDB collection `sensor_events`

- `main()`
  - creates Spark session
  - reads Kafka topic `parking.sensors.raw`
  - parses JSON into typed columns
  - converts `ts` string into a timestamp
  - computes:
    - `min_clearance`
    - `alert_flag` if clearance < 15 cm
  - writes two sinks:
    - InfluxDB via `foreachBatch`
    - MongoDB via `foreachBatch`
  - also computes a 30-second tumbling window aggregation:
    - count of readings
    - average front/rear distances
    - minimum clearance
    - alert count
  - prints aggregation to console

Important detail:
- `checkpointLocation` is used so Spark can recover state after restart.

---

## Alert consumer

### alert_consumer.py
Dedicated consumer for critical alert events.

- connects to Kafka topic `parking.alerts`
- listens in an infinite loop
- for every alert message:
  - logs it
  - stores it into MongoDB `alerts` collection

This is the safety/notification path.

---

## REST API

### main.py
FastAPI query layer.

- Configures MongoDB and InfluxDB connections.
- Creates endpoints:

1. `/health`
   - returns status and current timestamp

2. `/units`
   - returns all distinct `unit_id` values from MongoDB

3. `/units/{unit_id}/latest`
   - returns the most recent sensor reading for a truck

4. `/sessions`
   - returns summary of recent parking sessions
   - uses MongoDB aggregation to group by `session_id`

5. `/sessions/{session_id}`
   - returns all readings for one session ordered by time

6. `/stats/clearance`
   - queries InfluxDB for average clearance over the last `N` minutes

7. `/alerts`
   - returns recent alerts from MongoDB

This service is the query/reporting interface for end users or dashboards.

---

## Batch analytics

### batch_analytics.py
Offline analytics job.

- reads full `sensor_events` history from MongoDB
- computes:
  - session KPIs: start/end, duration, total readings, min clearance
  - alert frequency by spot
  - clearance stats by phase
  - phase distribution per truck

This is for scheduled analysis, not real-time streaming.

---

## Core concepts to understand for your exam

1. **Kafka** is the streaming bus.
   - Producers: simulator
   - Consumers: stream processor, alert consumer, future services
   - Topics separate raw sensor data, decisions, and alerts

2. **Simulator** generates realistic test data.
   - state machine phases
   - sensor noise
   - decision events and alerts

3. **PySpark Structured Streaming**
   - reads Kafka as an unbounded `DataFrame`
   - uses schema validation
   - writes to external databases in micro-batches
   - uses time windows for aggregation

4. **InfluxDB**
   - optimized for metrics and time-series
   - stores `min_clearance`, sensor distances, phase tags

5. **MongoDB**
   - stores full event history and alerts
   - used by API and batch analytics

6. **FastAPI**
   - exposes business queries
   - combines event store and time-series store results

7. **Grafana**
   - displays live metrics from InfluxDB
   - can show clearance, alert counts, and phase statistics

---

## Exam answer style

If asked “what does main.py do?”
- answer: “it creates Kafka producer, generates truck sessions, emits raw sensor and decision JSON, sends alerts for critical proximity, and resets trucks after parking.”

If asked “why use MongoDB and InfluxDB?”
- answer: “InfluxDB for time-series metrics and live monitoring, MongoDB for event history, session details, and alert records.”

If asked about the Kafka topic `parking.sensors.raw`:
- answer: “it carries raw sensor readings from trucks; the stream processor consumes it, enriches it, and stores results.”

If asked about the stream processor’s `foreachBatch`:
- answer: “it writes each micro-batch into external stores using Python functions rather than built-in Spark connectors directly.”

---

## Quick “what to remember”

- docker-compose.yml boots the full pipeline.
- simulator is the source of events.
- stream_processor is the real-time ETL layer.
- consumers handles alerts.
- api serves queries.
- jobs runs batch analytics.
- README.md documents architecture and commands.

If you want, I can also turn this into a one-page study sheet with bullet points per file and a simplified diagram.
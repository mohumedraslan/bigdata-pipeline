"""
api/src/main.py
────────────────
FastAPI service exposing query endpoints over the stored data.

Endpoints
─────────
GET  /health                     — liveness probe
GET  /units                      — list all known unit_ids
GET  /units/{unit_id}/latest     — latest sensor reading for a truck
GET  /sessions                   — list all parking sessions
GET  /sessions/{session_id}      — full event history for one session
GET  /stats/clearance            — global clearance stats from InfluxDB
GET  /alerts                     — recent alerts
"""

import os
from datetime import datetime, timezone, timedelta
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, DESCENDING
from influxdb_client import InfluxDBClient

# ── Config ────────────────────────────────────────────────────────────────────
MONGO_URI    = os.getenv("MONGO_URI",    "mongodb://mongo:27017/")
MONGO_DB     = os.getenv("MONGO_DB",    "smart_parking")
INFLUX_URL   = os.getenv("INFLUX_URL",  "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN","super-secret-token-change-me")
INFLUX_ORG   = os.getenv("INFLUX_ORG",  "smart_parking")
INFLUX_BUCKET= os.getenv("INFLUX_BUCKET","sensor_data")

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Smart Parking – Pipeline API",
    description="Query interface over InfluxDB time-series and MongoDB event store",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {
        "message": "Smart Parking API",
        "endpoints": [
            "/health",
            "/units",
            "/units/{unit_id}/latest",
            "/sessions",
            "/sessions/{session_id}",
            "/stats/clearance",
            "/alerts",
        ],
    }

# ── DB clients (module-level singletons) ──────────────────────────────────────
_mongo   = MongoClient(MONGO_URI)
_influx  = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)


def _events() :  return _mongo[MONGO_DB]["sensor_events"]
def _alerts():   return _mongo[MONGO_DB]["alerts"]
def _strip_id(doc: dict) -> dict:
    doc.pop("_id", None)
    return doc


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/units")
def list_units():
    """Return all distinct unit_ids seen in the event store."""
    ids = _events().distinct("unit_id")
    return {"units": sorted(ids)}


@app.get("/units/{unit_id}/latest")
def latest_reading(unit_id: str):
    """Most recent sensor reading for a given truck."""
    doc = _events().find_one(
        {"unit_id": unit_id},
        sort=[("ts", DESCENDING)],
    )
    if not doc:
        raise HTTPException(404, f"No data for unit '{unit_id}'")
    return _strip_id(doc)


@app.get("/sessions")
def list_sessions(limit: int = Query(default=20, le=100)):
    """Summary list of the most recent parking sessions."""
    pipeline = [
        {"$group": {
            "_id":         "$session_id",
            "unit_id":     {"$first": "$unit_id"},
            "spot_id":     {"$first": "$spot_id"},
            "started_at":  {"$min":   "$ts"},
            "ended_at":    {"$max":   "$ts"},
            "readings":    {"$sum":   1},
        }},
        {"$sort": {"started_at": -1}},
        {"$limit": limit},
    ]
    sessions = list(_events().aggregate(pipeline))
    for s in sessions:
        s["session_id"] = s.pop("_id")
    return {"sessions": sessions}


@app.get("/sessions/{session_id}")
def session_detail(session_id: str):
    """All sensor events for a single parking session, ordered by time."""
    docs = list(
        _events()
        .find({"session_id": session_id}, sort=[("ts", 1)])
    )
    if not docs:
        raise HTTPException(404, f"Session '{session_id}' not found")
    return {"session_id": session_id, "events": [_strip_id(d) for d in docs]}


@app.get("/stats/clearance")
def clearance_stats(window_minutes: int = Query(default=5, le=60)):
    """
    Rolling clearance statistics from InfluxDB for the last N minutes.
    Returns avg / min clearance per unit.
    """
    query = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{window_minutes}m)
      |> filter(fn: (r) => r._measurement == "sensor_reading")
      |> filter(fn: (r) => r._field == "min_clearance")
      |> group(columns: ["unit_id"])
      |> mean()
    """
    tables = _influx.query_api().query(query)
    stats  = []
    for table in tables:
        for rec in table.records:
            stats.append({
                "unit_id":           rec.values.get("unit_id"),
                "avg_min_clearance": round(rec.get_value(), 2),
                "window_minutes":    window_minutes,
            })
    return {"clearance_stats": stats}


@app.get("/alerts")
def recent_alerts(limit: int = Query(default=50, le=200)):
    """Most recent critical alerts."""
    docs = list(
        _alerts()
        .find({}, sort=[("ts", DESCENDING)], limit=limit)
    )
    return {"alerts": [_strip_id(d) for d in docs], "count": len(docs)}

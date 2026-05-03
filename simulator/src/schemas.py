"""
shared/schemas.py
─────────────────
Single source of truth for all data models used across the pipeline.
Any service that produces or consumes Kafka messages imports from here.
"""

from __future__ import annotations
from dataclasses import dataclass, asdict, field
from typing import Literal
from datetime import datetime, timezone
import uuid
import json


# ── Parking session phases ────────────────────────────────────────────────────
Phase = Literal[
    "APPROACHING",   # Truck moving toward the spot from a distance
    "ALIGNING",      # Fine-tuning lateral alignment
    "REVERSING",     # Backing into the spot
    "CONFIRMING",    # Final check – sensors verifying clearance
    "PARKED",        # Successfully parked
    "EXITING",       # Leaving the spot
]

# ── Actuation decisions issued by the decision engine ─────────────────────────
Decision = Literal[
    "MOVE_FORWARD",
    "MOVE_BACKWARD",
    "TURN_LEFT",
    "TURN_RIGHT",
    "NUDGE_LEFT",    # Small lateral correction
    "NUDGE_RIGHT",
    "STOP",
    "EMERGENCY_STOP",  # Obstacle within critical range on any sensor
]


@dataclass
class SensorReading:
    """
    Raw payload from a single sensor unit attached to a truck.
    Mirrors what the physical ultrasonic sensors emit.

    Fields
    ------
    unit_id      : Unique identifier of the truck/unit
    spot_id      : Target parking spot being attempted
    session_id   : UUID grouping all readings for one parking manoeuvre
    phase        : Current parking phase
    front_cm     : Distance from front bumper to nearest obstacle (cm)
    rear_cm      : Distance from rear bumper to nearest obstacle (cm)
    left_cm      : Distance from left side to nearest obstacle (cm)
    right_cm     : Distance from right side to nearest obstacle (cm)
    servo_angle  : Current steering servo angle (0–180°, 90 = straight)
    speed_ms     : Current speed in m/s (positive = forward)
    ts           : ISO-8601 UTC timestamp
    """
    unit_id: str
    spot_id: str
    session_id: str
    phase: Phase
    front_cm: float
    rear_cm: float
    left_cm: float
    right_cm: float
    servo_angle: int        # degrees
    speed_ms: float
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str | bytes) -> "SensorReading":
        data = json.loads(raw)
        return cls(**data)

    @property
    def min_clearance(self) -> float:
        """Minimum clearance across all four sensors."""
        return min(self.front_cm, self.rear_cm, self.left_cm, self.right_cm)

    @property
    def is_critical(self) -> bool:
        """True if any sensor is within the hard-stop threshold."""
        return self.min_clearance < 15.0


@dataclass
class ParkingDecision:
    """Enriched record produced by the decision engine and written to Kafka."""
    unit_id: str
    session_id: str
    phase: Phase
    decision: Decision
    confidence: float       # 0.0 – 1.0
    front_cm: float
    rear_cm: float
    left_cm: float
    right_cm: float
    servo_angle: int
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str | bytes) -> "ParkingDecision":
        data = json.loads(raw)
        return cls(**data)


@dataclass
class Alert:
    """Critical event pushed to the alerts topic."""
    alert_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    unit_id: str = ""
    session_id: str = ""
    severity: Literal["WARNING", "CRITICAL"] = "WARNING"
    message: str = ""
    front_cm: float = 0.0
    rear_cm: float = 0.0
    left_cm: float = 0.0
    right_cm: float = 0.0
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_json(self) -> str:
        return json.dumps(asdict(self))

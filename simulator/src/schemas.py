from __future__ import annotations
from dataclasses import dataclass, asdict, field
from typing import Literal
from datetime import datetime, timezone
import uuid
import json

Phase = Literal[
    "APPROACHING",
    "ALIGNING",
    "REVERSING",
    "CONFIRMING",
    "PARKED",
    "EXITING",
]

Decision = Literal[
    "MOVE_FORWARD",
    "MOVE_BACKWARD",
    "TURN_LEFT",
    "TURN_RIGHT",
    "NUDGE_LEFT",
    "NUDGE_RIGHT",
    "STOP",
    "EMERGENCY_STOP",
]


@dataclass
class SensorReading:
    unit_id: str
    spot_id: str
    session_id: str
    phase: Phase
    front_cm: float
    rear_cm: float
    left_cm: float
    right_cm: float
    servo_angle: int
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
        return min(self.front_cm, self.rear_cm, self.left_cm, self.right_cm)

    @property
    def is_critical(self) -> bool:
        return self.min_clearance < 15.0


@dataclass
class ParkingDecision:
    unit_id: str
    session_id: str
    phase: Phase
    decision: Decision
    confidence: float
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

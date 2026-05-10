from __future__ import annotations
import math
import random
import uuid
from dataclasses import dataclass, field
from typing import Tuple

from schemas import Phase, Decision, SensorReading, ParkingDecision

APPROACH_DONE   = 150
ALIGN_DONE      = 80
REVERSE_DONE    = 25
CONFIRM_DONE    = 20
EMERGENCY_STOP  = 12

def _noise(sigma: float = 2.0) -> float:
    return random.gauss(0, sigma)


@dataclass
class TruckUnit:
    unit_id: str
    spot_id: str
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    phase: Phase = "APPROACHING"

    _front: float  = field(default=500.0, repr=False)
    _rear:  float  = field(default=600.0, repr=False)
    _left:  float  = field(default=200.0, repr=False)
    _right: float  = field(default=200.0, repr=False)
    _servo: int    = field(default=90,    repr=False)
    _speed: float  = field(default=0.0,  repr=False)
    _lateral_err: float = field(default_factory=lambda: random.uniform(-40, 40), repr=False)

    def step(self) -> Tuple[SensorReading, ParkingDecision]:
        decision, confidence = self._decide()
        self._apply(decision)
        reading  = self._make_reading()
        dec_rec  = self._make_decision(decision, confidence)
        return reading, dec_rec

    def _decide(self) -> Tuple[Decision, float]:
        front = self._front
        rear  = self._rear

        if min(front, rear, self._left, self._right) < EMERGENCY_STOP:
            return "EMERGENCY_STOP", 1.0

        if self.phase == "APPROACHING":
            if front > APPROACH_DONE:
                return "MOVE_FORWARD", 0.95
            else:
                self.phase = "ALIGNING"
                return "STOP", 0.9

        if self.phase == "ALIGNING":
            err = self._lateral_err
            if abs(err) < 5:
                self.phase = "REVERSING"
                return "STOP", 0.9
            elif err > 0:
                return "NUDGE_RIGHT", round(random.uniform(0.80, 0.95), 2)
            else:
                return "NUDGE_LEFT", round(random.uniform(0.80, 0.95), 2)

        if self.phase == "REVERSING":
            if rear < REVERSE_DONE:
                self.phase = "CONFIRMING"
                return "STOP", 0.92
            return "MOVE_BACKWARD", round(random.uniform(0.85, 0.98), 2)

        if self.phase == "CONFIRMING":
            sides_ok = self._left > 20 and self._right > 20
            if rear > CONFIRM_DONE and sides_ok:
                self.phase = "PARKED"
                return "STOP", 1.0
            return "NUDGE_LEFT" if self._left < self._right else "NUDGE_RIGHT", 0.88

        if self.phase == "PARKED":
            return "STOP", 1.0

        return "STOP", 0.5

    def _apply(self, decision: Decision) -> None:
        if decision == "MOVE_FORWARD":
            self._front = max(0, self._front - random.uniform(5, 15))
            self._rear  = min(700, self._rear + random.uniform(5, 12))
            self._speed = 0.4
            self._servo = 90

        elif decision == "MOVE_BACKWARD":
            self._rear  = max(0, self._rear - random.uniform(3, 8))
            self._front = min(700, self._front + random.uniform(3, 8))
            self._speed = -0.2
            self._servo = 90

        elif decision in ("NUDGE_LEFT", "TURN_LEFT"):
            self._lateral_err = max(-5, self._lateral_err - random.uniform(2, 8))
            self._left  = max(10, self._left - random.uniform(1, 5))
            self._right = min(300, self._right + random.uniform(1, 5))
            self._servo = random.randint(50, 75)
            self._speed = 0.1

        elif decision in ("NUDGE_RIGHT", "TURN_RIGHT"):
            self._lateral_err = min(5, self._lateral_err + random.uniform(2, 8))
            self._right = max(10, self._right - random.uniform(1, 5))
            self._left  = min(300, self._left + random.uniform(1, 5))
            self._servo = random.randint(105, 130)
            self._speed = 0.1

        elif decision in ("STOP", "EMERGENCY_STOP"):
            self._speed = 0.0
            self._servo = 90

    def _make_reading(self) -> SensorReading:
        return SensorReading(
            unit_id      = self.unit_id,
            spot_id      = self.spot_id,
            session_id   = self.session_id,
            phase        = self.phase,
            front_cm     = round(max(0, self._front + _noise()), 1),
            rear_cm      = round(max(0, self._rear  + _noise()), 1),
            left_cm      = round(max(0, self._left  + _noise()), 1),
            right_cm     = round(max(0, self._right + _noise()), 1),
            servo_angle  = self._servo,
            speed_ms     = round(self._speed, 3),
        )

    def _make_decision(self, decision: Decision, confidence: float) -> ParkingDecision:
        return ParkingDecision(
            unit_id      = self.unit_id,
            session_id   = self.session_id,
            phase        = self.phase,
            decision     = decision,
            confidence   = confidence,
            front_cm     = round(self._front, 1),
            rear_cm      = round(self._rear, 1),
            left_cm      = round(self._left, 1),
            right_cm     = round(self._right, 1),
            servo_angle  = self._servo,
        )

    def is_done(self) -> bool:
        return self.phase == "PARKED"

    def reset(self) -> None:
        self.session_id   = str(uuid.uuid4())
        self.phase        = "APPROACHING"
        self._front       = random.uniform(450, 550)
        self._rear        = random.uniform(550, 650)
        self._left        = random.uniform(150, 250)
        self._right       = random.uniform(150, 250)
        self._servo       = 90
        self._speed       = 0.0
        self._lateral_err = random.uniform(-40, 40)
        return "MOVE_BACKWARD", round(random.uniform(0.85, 0.98), 2)

        if self.phase == "CONFIRMING":
            sides_ok = self._left > 20 and self._right > 20
            if rear > CONFIRM_DONE and sides_ok:
                self.phase = "PARKED"
                return "STOP", 1.0
            return "NUDGE_LEFT" if self._left < self._right else "NUDGE_RIGHT", 0.88

        if self.phase == "PARKED":
            return "STOP", 1.0

        return "STOP", 0.5   # fallback

    def _apply(self, decision: Decision) -> None:
        """Update internal continuous state based on decision."""
        if decision == "MOVE_FORWARD":
            self._front = max(0, self._front - random.uniform(5, 15))
            self._rear  = min(700, self._rear + random.uniform(5, 12))
            self._speed = 0.4
            self._servo = 90

        elif decision == "MOVE_BACKWARD":
            self._rear  = max(0, self._rear - random.uniform(3, 8))
            self._front = min(700, self._front + random.uniform(3, 8))
            self._speed = -0.2
            self._servo = 90

        elif decision in ("NUDGE_LEFT", "TURN_LEFT"):
            self._lateral_err = max(-5, self._lateral_err - random.uniform(2, 8))
            self._left  = max(10, self._left - random.uniform(1, 5))
            self._right = min(300, self._right + random.uniform(1, 5))
            self._servo = random.randint(50, 75)
            self._speed = 0.1

        elif decision in ("NUDGE_RIGHT", "TURN_RIGHT"):
            self._lateral_err = min(5, self._lateral_err + random.uniform(2, 8))
            self._right = max(10, self._right - random.uniform(1, 5))
            self._left  = min(300, self._left + random.uniform(1, 5))
            self._servo = random.randint(105, 130)
            self._speed = 0.1

        elif decision in ("STOP", "EMERGENCY_STOP"):
            self._speed = 0.0
            self._servo = 90

    def _make_reading(self) -> SensorReading:
        return SensorReading(
            unit_id      = self.unit_id,
            spot_id      = self.spot_id,
            session_id   = self.session_id,
            phase        = self.phase,
            front_cm     = round(max(0, self._front + _noise()), 1),
            rear_cm      = round(max(0, self._rear  + _noise()), 1),
            left_cm      = round(max(0, self._left  + _noise()), 1),
            right_cm     = round(max(0, self._right + _noise()), 1),
            servo_angle  = self._servo,
            speed_ms     = round(self._speed, 3),
        )

    def _make_decision(self, decision: Decision, confidence: float) -> ParkingDecision:
        return ParkingDecision(
            unit_id      = self.unit_id,
            session_id   = self.session_id,
            phase        = self.phase,
            decision     = decision,
            confidence   = confidence,
            front_cm     = round(self._front, 1),
            rear_cm      = round(self._rear, 1),
            left_cm      = round(self._left, 1),
            right_cm     = round(self._right, 1),
            servo_angle  = self._servo,
        )

    def is_done(self) -> bool:
        return self.phase == "PARKED"

    def reset(self) -> None:
        """Start a new session after parking is complete."""
        self.session_id   = str(uuid.uuid4())
        self.phase        = "APPROACHING"
        self._front       = random.uniform(450, 550)
        self._rear        = random.uniform(550, 650)
        self._left        = random.uniform(150, 250)
        self._right       = random.uniform(150, 250)
        self._servo       = 90
        self._speed       = 0.0
        self._lateral_err = random.uniform(-40, 40)

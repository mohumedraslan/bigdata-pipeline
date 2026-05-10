from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from typing import Tuple

from schemas import Phase, Decision, SensorReading, ParkingDecision


APPROACH_DONE = 180
ALIGN_TOLERANCE = 8
REVERSE_DONE = 35
PARKED_REAR = 20
CRITICAL_DISTANCE = 6


def noise(sigma: float = 1.5) -> float:
    return random.gauss(0, sigma)


@dataclass
class TruckUnit:
    unit_id: str
    spot_id: str

    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    phase: Phase = "APPROACHING"

    _front: float = field(default=520.0, repr=False)
    _rear: float = field(default=40.0, repr=False)

    _left: float = field(default=120.0, repr=False)
    _right: float = field(default=120.0, repr=False)

    _speed: float = field(default=0.0, repr=False)
    _servo: int = field(default=90, repr=False)

    _lateral_error: float = field(
        default_factory=lambda: random.uniform(-35, 35),
        repr=False,
    )

    def step(self) -> Tuple[SensorReading, ParkingDecision]:
        decision, confidence = self._decide()

        self._apply(decision)

        reading = self._make_reading()
        decision_record = self._make_decision(decision, confidence)

        return reading, decision_record

    def _decide(self) -> Tuple[Decision, float]:

        nearest = min(
            self._front,
            self._rear,
            self._left,
            self._right,
        )

        if nearest < CRITICAL_DISTANCE:
            return "EMERGENCY_STOP", 1.0

        if self.phase == "APPROACHING":

            if self._front > APPROACH_DONE:
                return "MOVE_FORWARD", 0.96

            self.phase = "ALIGNING"
            return "STOP", 0.93

        if self.phase == "ALIGNING":

            err = self._lateral_error

            if abs(err) <= ALIGN_TOLERANCE:
                self.phase = "REVERSING"
                return "STOP", 0.95

            if err > 0:
                return "NUDGE_RIGHT", random.uniform(0.82, 0.94)

            return "NUDGE_LEFT", random.uniform(0.82, 0.94)

        if self.phase == "REVERSING":

            if self._rear >= REVERSE_DONE:
                return "MOVE_BACKWARD", random.uniform(0.88, 0.98)

            self.phase = "CONFIRMING"
            return "STOP", 0.97

        if self.phase == "CONFIRMING":

            centered = abs(self._left - self._right) < 10

            if self._rear <= PARKED_REAR and centered:
                self.phase = "PARKED"
                return "STOP", 1.0

            if self._left > self._right:
                return "NUDGE_LEFT", 0.90

            return "NUDGE_RIGHT", 0.90

        return "STOP", 1.0

    def _apply(self, decision: Decision) -> None:

        if decision == "MOVE_FORWARD":

            move = random.uniform(8, 16)

            self._front -= move
            self._rear += move * 0.25

            self._speed = 1.4
            self._servo = 90

        elif decision == "MOVE_BACKWARD":

            move = random.uniform(4, 10)

            self._rear -= move
            self._front += move * 0.15

            self._speed = -0.8
            self._servo = 90

        elif decision == "NUDGE_LEFT":

            correction = random.uniform(3, 7)

            self._lateral_error -= correction

            self._left += correction * 0.5
            self._right -= correction * 0.5

            self._speed = 0.3
            self._servo = random.randint(65, 80)

        elif decision == "NUDGE_RIGHT":

            correction = random.uniform(3, 7)

            self._lateral_error += correction

            self._right += correction * 0.5
            self._left -= correction * 0.5

            self._speed = 0.3
            self._servo = random.randint(100, 115)

        elif decision in ("STOP", "EMERGENCY_STOP"):

            self._speed = 0.0
            self._servo = 90

        self._front = max(0, self._front)
        self._rear = max(0, self._rear)

        self._left = max(15, self._left)
        self._right = max(15, self._right)

    def _make_reading(self) -> SensorReading:

        return SensorReading(
            unit_id=self.unit_id,
            spot_id=self.spot_id,
            session_id=self.session_id,
            phase=self.phase,

            front_cm=round(self._front + noise(), 1),
            rear_cm=round(self._rear + noise(), 1),
            left_cm=round(self._left + noise(), 1),
            right_cm=round(self._right + noise(), 1),

            servo_angle=self._servo,
            speed_ms=round(self._speed, 2),
        )

    def _make_decision(
        self,
        decision: Decision,
        confidence: float,
    ) -> ParkingDecision:

        return ParkingDecision(
            unit_id=self.unit_id,
            session_id=self.session_id,
            phase=self.phase,

            decision=decision,
            confidence=round(confidence, 2),

            front_cm=round(self._front, 1),
            rear_cm=round(self._rear, 1),
            left_cm=round(self._left, 1),
            right_cm=round(self._right, 1),

            servo_angle=self._servo,
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


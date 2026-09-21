from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from industrial_process_engine.config import TrackingConfig
from industrial_process_engine.domain import SignalUpdate, SignalValue, TrackingStatus
from industrial_process_engine.units import MeasurementUnit, speed_to_m_s


@dataclass(frozen=True, slots=True)
class TransportMovement:
    start_m: float
    end_m: float
    start_ts: int
    end_ts: int
    status: TrackingStatus
    estimated: bool = False


class GlobalTransportTracker:
    """Monotonic line transport coordinate with direct-counter/speed fallback."""

    def __init__(
        self, config: TrackingConfig, speed_unit: MeasurementUnit | None = None,
    ) -> None:
        self.config = config
        self.speed_unit = speed_unit
        self.position_m = 0.0
        self.last_ts: int | None = None
        self.raw_position: float | None = None
        self.last_direct_ts: int | None = None
        self.speed_m_s = 0.0
        self.last_speed_ts: int | None = None
        self.speed_valid = False
        self.fallback_active = False
        self.status = TrackingStatus.STOPPED

    def process_group(
        self, timestamp_ms: int, updates: Iterable[SignalUpdate],
        state: Mapping[str, SignalValue], *, force_fallback: bool = False,
    ) -> TransportMovement | None:
        updates = tuple(updates)
        direct = next((u for u in updates if u.name == self.config.signal), None)
        speed = next((u for u in updates if u.name == self.config.speed_signal), None)
        end_speed_m_s = (
            speed_to_m_s(float(speed.value), self.speed_unit)
            if speed is not None and speed.quality and self.speed_unit is not None else None
        )
        movement: TransportMovement | None = None

        if self.last_ts is None:
            self.last_ts = timestamp_ms
        elif timestamp_ms < self.last_ts:
            return None

        if self.config.source == "speed":
            if (
                self.last_speed_ts is not None
                and timestamp_ms - self.last_speed_ts > self.config.stale_after_ms
            ):
                movement = self._integrate_to(
                    self.last_speed_ts + self.config.stale_after_ms, estimated=False,
                )
                self.last_ts = timestamp_ms
                self.speed_valid = False
                self.status = TrackingStatus.LOST
            else:
                movement = self._integrate_to(
                    timestamp_ms, estimated=False, end_speed_m_s=end_speed_m_s,
                )
        elif direct is not None and direct.quality:
            raw = float(direct.value)
            stale_resume = (
                self.last_direct_ts is not None
                and timestamp_ms - self.last_direct_ts > self.config.stale_after_ms
            )
            if self.fallback_active or stale_resume:
                if self.config.fallback_to_speed:
                    movement = self._integrate_to(
                        timestamp_ms, estimated=True, end_speed_m_s=end_speed_m_s,
                    )
                else:
                    self.last_ts = timestamp_ms
                    self.status = TrackingStatus.LOST
                self.raw_position = raw
                self.last_direct_ts = timestamp_ms
                self.last_ts = timestamp_ms
                self.fallback_active = False
            elif self.raw_position is None:
                # A reading after fallback is a new baseline. Its raw delta overlaps
                # movement already integrated from speed and must not be counted.
                self.raw_position = raw
                self.last_direct_ts = timestamp_ms
                self.last_ts = timestamp_ms
                self.fallback_active = False
            else:
                delta = raw - self.raw_position
                reset = delta < -self.config.stopped_epsilon
                oversized = delta > self.config.max_forward_jump_m
                if reset or oversized:
                    if self.config.fallback_to_speed:
                        movement = self._integrate_to(
                            timestamp_ms, estimated=True, end_speed_m_s=end_speed_m_s,
                        )
                    else:
                        self.last_ts = timestamp_ms
                        self.status = TrackingStatus.LOST
                    self.raw_position = raw
                    self.fallback_active = movement is not None
                elif delta > self.config.stopped_epsilon:
                    start_ts = self.last_direct_ts if self.last_direct_ts is not None else self.last_ts
                    movement = self._movement(delta, start_ts or timestamp_ms, timestamp_ms, False)
                    self.raw_position = raw
                    self.last_ts = timestamp_ms
                    self.status = TrackingStatus.OK
                else:
                    if force_fallback and self.config.fallback_to_speed:
                        movement = self._integrate_to(
                            timestamp_ms, estimated=True, end_speed_m_s=end_speed_m_s,
                        )
                        self.fallback_active = movement is not None
                    else:
                        self.last_ts = timestamp_ms
                        self.status = TrackingStatus.STOPPED
                    self.raw_position = raw
                self.last_direct_ts = timestamp_ms
        else:
            direct_bad = direct is not None and not direct.quality
            direct_stale = (
                self.last_direct_ts is None
                or timestamp_ms - self.last_direct_ts > self.config.stale_after_ms
            )
            if direct_bad and not self.config.fallback_to_speed:
                self.last_ts = timestamp_ms
                self.fallback_active = True
                self.status = TrackingStatus.LOST
            elif self.config.fallback_to_speed and (direct_bad or direct_stale or force_fallback):
                movement = self._integrate_to(
                    timestamp_ms, estimated=True, end_speed_m_s=end_speed_m_s,
                )
                # A forced drain check while stopped covered no distance, so the
                # next good counter delta remains authoritative and needs no rebase.
                if movement is not None or direct_bad or direct_stale:
                    self.fallback_active = True

        # The endpoint speed participates in trapezoidal integration above and
        # becomes the starting speed for the following interval.
        if speed is not None and self.speed_unit is not None:
            if speed.quality:
                assert end_speed_m_s is not None
                self.speed_m_s = end_speed_m_s
                self.last_speed_ts = timestamp_ms
                self.speed_valid = True
            else:
                self.speed_valid = False
        elif self.speed_unit is not None and self.config.speed_signal in state:
            current = state[self.config.speed_signal]
            if current.quality and not self.speed_valid:
                self.speed_m_s = speed_to_m_s(float(current.value), self.speed_unit)
                self.speed_valid = True
        return movement

    def advance_to(self, timestamp_ms: int, *, force_fallback: bool = False) -> TransportMovement | None:
        if self.config.source == "speed":
            return self.process_group(timestamp_ms, (), {})
        if not self.config.fallback_to_speed and self.config.source != "speed":
            return None
        if self.config.source == "direct" and not force_fallback:
            stale = self.last_direct_ts is None or timestamp_ms - self.last_direct_ts > self.config.stale_after_ms
            if not stale:
                return None
        movement = self._integrate_to(timestamp_ms, estimated=self.config.source == "direct")
        if movement and self.config.source == "direct":
            self.fallback_active = True
        return movement

    def _integrate_to(
        self, timestamp_ms: int, estimated: bool, end_speed_m_s: float | None = None,
    ) -> TransportMovement | None:
        if self.last_ts is None:
            self.last_ts = timestamp_ms
            return None
        start_ts = self.last_ts
        self.last_ts = max(self.last_ts, timestamp_ms)
        if timestamp_ms <= start_ts or not self.speed_valid:
            self.status = TrackingStatus.LOST if not self.speed_valid else TrackingStatus.STOPPED
            return None
        final_speed_m_s = self.speed_m_s if end_speed_m_s is None else end_speed_m_s
        average_speed_m_s = (self.speed_m_s + final_speed_m_s) / 2.0
        delta = max(0.0, average_speed_m_s) * (timestamp_ms - start_ts) / 1000.0
        if delta <= self.config.stopped_epsilon:
            self.status = TrackingStatus.STOPPED
            return None
        return self._movement(delta, start_ts, timestamp_ms, estimated)

    def _movement(self, delta: float, start_ts: int, end_ts: int, estimated: bool) -> TransportMovement:
        start = self.position_m
        self.position_m += delta
        self.status = TrackingStatus.OK
        return TransportMovement(start, self.position_m, start_ts, end_ts, self.status, estimated)

    def snapshot(self) -> dict[str, Any]:
        return {
            "position_m": self.position_m, "last_ts": self.last_ts,
            "raw_position": self.raw_position, "last_direct_ts": self.last_direct_ts,
            "speed_m_s": self.speed_m_s, "last_speed_ts": self.last_speed_ts,
            "speed_valid": self.speed_valid,
            "fallback_active": self.fallback_active, "status": str(self.status),
        }

    def restore(self, value: Mapping[str, Any]) -> None:
        self.position_m = float(value.get("position_m", 0.0))
        self.last_ts = value.get("last_ts")
        self.raw_position = value.get("raw_position")
        self.last_direct_ts = value.get("last_direct_ts")
        self.speed_m_s = float(value.get("speed_m_s", 0.0))
        self.last_speed_ts = value.get("last_speed_ts")
        self.speed_valid = bool(value.get("speed_valid", False))
        self.fallback_active = bool(value.get("fallback_active", False))
        self.status = TrackingStatus(value.get("status", TrackingStatus.STOPPED))
        if self.config.source == "speed":
            self.last_ts = None
            self.last_speed_ts = None
            self.speed_valid = False

from __future__ import annotations

from dataclasses import dataclass

from industrial_process_engine.config import TrackingConfig
from industrial_process_engine.domain import TrackingStatus
from industrial_process_engine.units import MeasurementUnit, speed_to_m_s


@dataclass(frozen=True, slots=True)
class Movement:
    start_m: float
    end_m: float
    start_ts: int
    end_ts: int
    status: TrackingStatus
    signal_gap: bool = False


class PositionTracker:
    def __init__(
        self, config: TrackingConfig, speed_unit: MeasurementUnit | None = None,
    ) -> None:
        self.config = config
        self.speed_unit = speed_unit
        self.raw_position: float | None = None
        self.position_m = 0.0
        self.last_ts: int | None = None
        self.speed_m_s = 0.0
        self.speed_path_m = 0.0
        self.furthest_position_m = 0.0
        self.status = TrackingStatus.STOPPED
        self.active = False
        self._sticky_lost = False
        self._input_gap = False
        self._speed_valid = False

    def start_product(self, timestamp_ms: int) -> None:
        self.position_m = 0.0
        self.speed_path_m = 0.0
        self.furthest_position_m = 0.0
        self.last_ts = timestamp_ms
        self.active = True
        self.status = TrackingStatus.STOPPED
        self._sticky_lost = False
        self._input_gap = False

    def pause_segment(self) -> None:
        self.active = False

    def start_segment(self, timestamp_ms: int, raw_position: float | None = None) -> None:
        """Reset product-relative distance for a new direct-position segment."""
        self.position_m = 0.0
        self.speed_path_m = 0.0
        self.furthest_position_m = 0.0
        self.last_ts = timestamp_ms
        if raw_position is not None:
            self.raw_position = raw_position
        self.active = True
        self.status = TrackingStatus.STOPPED
        self._sticky_lost = False
        self._input_gap = False

    def end_product(self) -> None:
        self.active = False

    @property
    def speed_valid(self) -> bool:
        return self._speed_valid

    def is_direct_reset(self, previous: object, current: object) -> bool:
        """Return whether consecutive raw readings represent a counter reset."""
        if self.config.source != "direct" or self.config.reset_ratio is None:
            return False
        previous_position = float(previous)
        current_position = float(current)
        return (
            previous_position > 0
            and current_position >= 0
            and current_position <= previous_position * self.config.reset_ratio
        )

    def observe(self, name: str, value: object, timestamp_ms: int) -> Movement | None:
        if self.config.source == "direct":
            if name != self.config.signal:
                return None
            return self._observe_direct(float(value), timestamp_ms)
        if name != self.config.speed_signal:
            return None
        assert self.speed_unit is not None
        speed_m_s = speed_to_m_s(float(value), self.speed_unit)
        return self._observe_speed(speed_m_s, timestamp_ms)

    def invalidate(self, name: str) -> None:
        relevant = name == (self.config.signal if self.config.source == "direct" else self.config.speed_signal)
        if not relevant:
            return
        self._input_gap = True
        self._sticky_lost = True
        self.status = TrackingStatus.LOST
        if self.config.source == "speed":
            self._speed_valid = False

    def _observe_direct(self, raw: float, timestamp_ms: int) -> Movement | None:
        previous_raw = self.raw_position
        previous_ts = self.last_ts
        self.last_ts = timestamp_ms
        if previous_raw is None or previous_ts is None or not self.active:
            self.raw_position = raw
            return None
        delta = raw - previous_raw
        stale = timestamp_ms - previous_ts > self.config.stale_after_ms
        status = TrackingStatus.LOST if self._sticky_lost or self._input_gap or stale else TrackingStatus.OK
        if abs(delta) <= self.config.stopped_epsilon:
            self.status = TrackingStatus.LOST if self._sticky_lost else TrackingStatus.STOPPED
            return None
        if delta < 0:
            if self.config.reverse_policy == "ignore":
                self.raw_position = raw
                self.status = TrackingStatus.STOPPED
                return None
            if self.config.reverse_policy == "hold":
                # Keep the previous maximum. Forward movement only resumes once
                # the raw position exceeds it, so traversed material is not counted twice.
                self.status = TrackingStatus.LOST if self._sticky_lost else TrackingStatus.STOPPED
                return None
            if self.config.reverse_policy == "lost":
                self.raw_position = raw
                self._sticky_lost = True
                self.status = TrackingStatus.LOST
                return Movement(self.position_m, self.position_m, previous_ts, timestamp_ms, self.status)
        self.raw_position = raw
        if delta > self.config.max_forward_jump_m:
            status = TrackingStatus.LOST
        self._input_gap = False
        start = self.position_m
        self.position_m = max(0.0, self.position_m + delta)
        self.status = status
        return Movement(
            start, self.position_m, previous_ts, timestamp_ms, status,
            signal_gap=delta > self.config.max_forward_jump_m,
        )

    def _observe_speed(self, new_speed_m_s: float, timestamp_ms: int) -> Movement | None:
        previous_ts = self.last_ts
        previous_speed = self.speed_m_s
        previous_valid = self._speed_valid
        self.last_ts = timestamp_ms
        self.speed_m_s = new_speed_m_s
        self._speed_valid = True
        if previous_ts is None or timestamp_ms <= previous_ts or not self.active or not previous_valid:
            self.status = TrackingStatus.LOST if self._sticky_lost else (
                TrackingStatus.STOPPED if abs(new_speed_m_s) <= self.config.stopped_epsilon else TrackingStatus.OK
            )
            return None
        elapsed_ms = timestamp_ms - previous_ts
        status = TrackingStatus.LOST if self._sticky_lost or self._input_gap or elapsed_ms > self.config.stale_after_ms else TrackingStatus.OK
        elapsed_s = elapsed_ms / 1000.0
        if self.config.reverse_policy == "hold":
            signed_delta = ((previous_speed + new_speed_m_s) / 2.0) * elapsed_s
            previous_furthest_position = self.furthest_position_m
            self.speed_path_m += signed_delta
            self.furthest_position_m = max(self.furthest_position_m, self.speed_path_m)
            forward_delta = self.furthest_position_m - previous_furthest_position
            self._input_gap = False
            if forward_delta <= self.config.stopped_epsilon:
                self.status = TrackingStatus.LOST if self._sticky_lost else TrackingStatus.STOPPED
                return None
            if forward_delta > self.config.max_forward_jump_m:
                self._sticky_lost = True
                status = TrackingStatus.LOST
            start = self.position_m
            self.position_m += forward_delta
            self.status = status
            return Movement(start, self.position_m, previous_ts, timestamp_ms, status)
        if previous_speed < 0 or new_speed_m_s < 0:
            if self.config.reverse_policy == "ignore":
                previous_speed = max(0.0, previous_speed)
                new_speed_m_s = max(0.0, new_speed_m_s)
            else:
                self._sticky_lost = True
                self.status = TrackingStatus.LOST
                return Movement(self.position_m, self.position_m, previous_ts, timestamp_ms, self.status)
        delta = ((previous_speed + new_speed_m_s) / 2.0) * elapsed_s
        if delta > self.config.max_forward_jump_m:
            self._sticky_lost = True
            status = TrackingStatus.LOST
        self._input_gap = False
        if delta <= self.config.stopped_epsilon:
            self.status = TrackingStatus.LOST if self._sticky_lost else TrackingStatus.STOPPED
            return None
        start = self.position_m
        self.position_m = max(0.0, start + delta)
        self.status = status
        return Movement(start, self.position_m, previous_ts, timestamp_ms, status)

    def restore(
        self, position_m: float, raw_position: float | None, last_ts: int | None,
        status: str, speed_m_s: float = 0.0, speed_valid: bool = False,
        speed_path_m: float | None = None, furthest_position_m: float | None = None,
        active: bool = True,
    ) -> None:
        self.position_m = position_m
        self.raw_position = raw_position
        self.last_ts = last_ts
        self.status = TrackingStatus(status)
        self.speed_m_s = speed_m_s
        self.speed_path_m = position_m if speed_path_m is None else speed_path_m
        self.furthest_position_m = position_m if furthest_position_m is None else furthest_position_m
        self._speed_valid = speed_valid
        self._sticky_lost = self.status == TrackingStatus.LOST
        self.active = active

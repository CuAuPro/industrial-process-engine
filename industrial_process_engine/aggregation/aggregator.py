from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any

from industrial_process_engine.config import AggregationVariableConfig
from industrial_process_engine.domain import SignalValue, TrackingStatus, WindowQuality, WindowRecord


@dataclass
class VariableAccumulator:
    weighted_sum: float = 0.0
    coverage_weight: float = 0.0
    minimum: float | None = None
    maximum: float | None = None
    count: int = 0
    first: Any | None = None
    last: Any | None = None

    def add(self, value: Any, weight: float, calculation: str) -> None:
        numeric_value: float | None = None
        if calculation in {"weighted_mean", "min", "max"}:
            numeric_value = float(value)
        if calculation == "weighted_mean":
            assert numeric_value is not None
            self.weighted_sum += numeric_value * weight
        self.coverage_weight += weight
        if numeric_value is not None:
            self.minimum = numeric_value if self.minimum is None else min(self.minimum, numeric_value)
            self.maximum = numeric_value if self.maximum is None else max(self.maximum, numeric_value)
        self.count += 1
        if self.first is None:
            self.first = value
        self.last = value

    def result(self, calculation: str) -> Any | None:
        return {
            "weighted_mean": self.weighted_sum / self.coverage_weight if self.coverage_weight > 0 else None,
            "first": self.first,
            "last": self.last,
            "min": self.minimum,
            "max": self.maximum,
        }[calculation]


class Aggregator:
    def __init__(
        self, process_id: str, variables: list[AggregationVariableConfig],
        mode: str, interval: float, stale_after_ms: int,
    ) -> None:
        self.process_id = process_id
        self.specifications = {variable.name: variable for variable in variables}
        self.variables = list(self.specifications)
        self.mode = mode
        self.interval = interval
        self.stale_after_ms = stale_after_ms
        self.run_id: str | None = None
        self.product_id: str | None = None
        self.process_start_ts = 0
        self.segment_no = 1
        self.segment_active = False
        self.window_no = 0
        self.window_start_axis = 0.0
        self.axis_position = 0.0
        self.ts_start = 0
        self.ts_end = 0
        self.accumulators: dict[str, VariableAccumulator] = {}
        self.has_gap = False
        self.tracking_lost = False
        self.window_started_partial = False

    def start(
        self, run_id: str, product_id: str, timestamp_ms: int, segment_no: int = 1,
        start_axis: float | None = None,
    ) -> None:
        self.run_id = run_id
        self.product_id = product_id
        self.process_start_ts = timestamp_ms
        self.segment_no = segment_no
        self.segment_active = True
        self.axis_position = (
            max(0.0, start_axis or 0.0)
            if self.mode == "distance" else timestamp_ms / 1000.0
        )
        self.window_no = (
            math.floor(self.axis_position / self.interval + 1e-12)
            if self.mode == "distance" else 0
        )
        self.window_start_axis = self.axis_position
        self.window_started_partial = not math.isclose(
            self.axis_position % self.interval, 0.0, abs_tol=1e-9,
        )
        self.ts_start = timestamp_ms
        self.ts_end = timestamp_ms
        self._reset_accumulators()

    def pause_segment(self, timestamp_ms: int) -> WindowRecord | None:
        partial = self.finalize_partial(timestamp_ms)
        self.segment_active = False
        return partial

    def start_segment(self, segment_no: int, timestamp_ms: int) -> None:
        if self.product_id is None or self.mode != "distance":
            raise RuntimeError("distance product must be active before starting a segment")
        self.segment_no = segment_no
        self.segment_active = True
        self.window_no = 0
        self.axis_position = 0.0
        self.window_start_axis = 0.0
        self.ts_start = timestamp_ms
        self.ts_end = timestamp_ms
        self.window_started_partial = False
        self._reset_accumulators()

    def add_span(
        self, start_axis: float, end_axis: float, start_ts: int, end_ts: int,
        signals: dict[str, SignalValue], tracking: TrackingStatus = TrackingStatus.OK,
    ) -> list[WindowRecord]:
        if self.product_id is None or not self.segment_active or end_axis <= start_axis:
            if tracking == TrackingStatus.LOST:
                self.tracking_lost = True
            return []
        start_axis = max(start_axis, self.axis_position)
        if end_axis <= start_axis:
            return []
        records: list[WindowRecord] = []
        cursor = start_axis
        while cursor < end_axis - 1e-12:
            boundary = self._next_boundary(cursor)
            chunk_end = min(end_axis, boundary)
            weight = chunk_end - cursor
            ratio_start = (cursor - start_axis) / (end_axis - start_axis)
            ratio_end = (chunk_end - start_axis) / (end_axis - start_axis)
            chunk_ts_start = round(start_ts + ratio_start * (end_ts - start_ts))
            chunk_ts_end = round(start_ts + ratio_end * (end_ts - start_ts))
            self._add_chunk(weight, chunk_ts_start, chunk_ts_end, signals, tracking)
            self.axis_position = chunk_end
            self.ts_end = chunk_ts_end
            cursor = chunk_end
            if math.isclose(chunk_end, boundary, abs_tol=1e-9):
                records.append(self._record(partial=self.window_started_partial))
                self.window_no += 1
                self.window_start_axis = boundary
                self.ts_start = chunk_ts_end
                self._reset_accumulators()
                self.window_started_partial = False
        return records

    def add_movement(
        self, start_m: float, end_m: float, start_ts: int, end_ts: int,
        signals: dict[str, SignalValue], tracking: TrackingStatus = TrackingStatus.OK,
    ) -> list[WindowRecord]:
        return self.add_span(start_m, end_m, start_ts, end_ts, signals, tracking)

    def add_time(self, timestamp_ms: int, signals: dict[str, SignalValue]) -> list[WindowRecord]:
        if self.mode != "time" or self.product_id is None:
            return []
        return self.add_span(
            self.axis_position, timestamp_ms / 1000.0, self.ts_end, timestamp_ms, signals,
        )

    def next_boundary_ms(self) -> int | None:
        if self.mode != "time" or self.product_id is None:
            return None
        return round(self._next_boundary(self.axis_position) * 1000)

    def finalize_partial(self, timestamp_ms: int) -> WindowRecord | None:
        if (
            self.product_id is None or not self.segment_active
            or self.axis_position <= self.window_start_axis + 1e-12
        ):
            return None
        self.ts_end = timestamp_ms
        return self._record(partial=True)

    def cut(self, timestamp_ms: int) -> WindowRecord | None:
        """Close the current partial window and continue from the same axis position."""
        record = self.finalize_partial(timestamp_ms)
        if record is None:
            self.ts_start = timestamp_ms
            self.ts_end = timestamp_ms
            return None
        self.window_no += 1
        self.window_start_axis = self.axis_position
        self.ts_start = timestamp_ms
        self.ts_end = timestamp_ms
        self.window_started_partial = True
        self._reset_accumulators()
        return record

    def _next_boundary(self, cursor: float) -> float:
        return (math.floor(cursor / self.interval + 1e-12) + 1) * self.interval

    def _add_chunk(
        self, weight: float, start_ts: int, end_ts: int,
        signals: dict[str, SignalValue], tracking: TrackingStatus,
    ) -> None:
        if tracking == TrackingStatus.LOST:
            self.tracking_lost = True
        for name in self.variables:
            specification = self.specifications[name]
            signal = signals.get(specification.source_name or name)
            holds_until_changed = specification.calculation in {"first", "last"}
            if signal is None or not signal.quality:
                self.has_gap = True
                continue
            valid_weight = weight
            if not holds_until_changed:
                expires_at = signal.timestamp_ms + self.stale_after_ms
                if end_ts > start_ts:
                    valid_ms = max(0, min(end_ts, expires_at) - start_ts)
                    valid_weight = weight * valid_ms / (end_ts - start_ts)
                elif start_ts > expires_at:
                    valid_weight = 0.0
                if valid_weight < weight - 1e-12:
                    self.has_gap = True
            if valid_weight <= 0:
                continue
            try:
                self.accumulators[name].add(signal.value, valid_weight, specification.calculation)
            except (TypeError, ValueError):
                self.has_gap = True

    def _record(self, partial: bool) -> WindowRecord:
        assert self.run_id is not None and self.product_id is not None
        quality = WindowQuality.PARTIAL if partial else WindowQuality.GOOD
        covered_weight = self.axis_position - self.window_start_axis
        complete = {
            name: self.accumulators[name].coverage_weight >= covered_weight - 1e-9
            for name in self.variables
        }
        if self.has_gap or not all(complete.values()):
            quality = WindowQuality.DATA_GAP
        if self.tracking_lost:
            quality = WindowQuality.TRACKING_LOST
        values: dict[str, Any | None] = {}
        for name in self.variables:
            if not complete[name]:
                values[name] = None
                continue
            specification = self.specifications[name]
            try:
                values[name] = self.convert_output(
                    self.accumulators[name].result(specification.calculation), specification.output_type,
                )
            except (TypeError, ValueError):
                values[name] = None
                if quality != WindowQuality.TRACKING_LOST:
                    quality = WindowQuality.DATA_GAP
        return WindowRecord(
            process_id=self.process_id, run_id=self.run_id, product_id=self.product_id,
            segment_no=self.segment_no,
            window_no=self.window_no,
            axis=self.mode, ts_start=self.ts_start, ts_end=self.ts_end,
            elapsed_start_s=(self.ts_start - self.process_start_ts) / 1000.0,
            elapsed_end_s=(self.ts_end - self.process_start_ts) / 1000.0,
            position_start_m=self.window_start_axis if self.mode == "distance" else None,
            position_end_m=self.axis_position if self.mode == "distance" else None,
            values=values, quality=quality,
        )

    @staticmethod
    def convert_output(value: Any, output_type: str) -> Any:
        if value is None:
            return None
        if output_type == "double":
            return float(value)
        if output_type in {"int", "long"}:
            integer = int(value)
            minimum, maximum = ((-2_147_483_648, 2_147_483_647) if output_type == "int" else
                                (-9_223_372_036_854_775_808, 9_223_372_036_854_775_807))
            if not minimum <= integer <= maximum:
                raise ValueError(f"{output_type} output is out of range")
            return integer
        if output_type in {"symbol", "varchar"}:
            return str(value)
        if output_type == "char":
            text = str(value)
            if len(text) != 1:
                raise ValueError("char output requires exactly one character")
            return text
        raise ValueError(f"unsupported output type: {output_type}")

    def _reset_accumulators(self) -> None:
        self.accumulators = {name: VariableAccumulator() for name in self.variables}
        self.has_gap = False
        self.tracking_lost = False

    def snapshot(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id, "product_id": self.product_id,
            "process_start_ts": self.process_start_ts,
            "segment_no": self.segment_no, "segment_active": self.segment_active,
            "window_no": self.window_no, "window_start_axis": self.window_start_axis,
            "axis_position": self.axis_position, "ts_start": self.ts_start, "ts_end": self.ts_end,
            "accumulators": {name: asdict(value) for name, value in self.accumulators.items()},
            "has_gap": self.has_gap, "tracking_lost": self.tracking_lost,
            "window_started_partial": self.window_started_partial,
        }

    def restore(self, snapshot: dict[str, Any]) -> None:
        self.run_id = snapshot["run_id"]
        self.product_id = snapshot["product_id"]
        self.process_start_ts = int(snapshot["process_start_ts"])
        self.segment_no = int(snapshot["segment_no"])
        self.segment_active = bool(snapshot["segment_active"])
        self.window_no = int(snapshot["window_no"])
        self.window_start_axis = float(snapshot["window_start_axis"])
        self.axis_position = float(snapshot["axis_position"])
        self.ts_start = int(snapshot["ts_start"])
        self.ts_end = int(snapshot["ts_end"])
        self.accumulators = {
            name: VariableAccumulator(**snapshot["accumulators"].get(name, {})) for name in self.variables
        }
        self.has_gap = bool(snapshot.get("has_gap", False))
        self.tracking_lost = bool(snapshot.get("tracking_lost", False))
        self.window_started_partial = bool(snapshot["window_started_partial"])


class NullProductAggregator:
    """Product-stream contract used when summaries are enabled without product windows."""

    run_id: str | None = None
    product_id: str | None = None
    segment_no = 1
    segment_active = False
    window_no = 0
    axis_position = 0.0

    def start(
        self, run_id: str, product_id: str, timestamp_ms: int, segment_no: int = 1,
        start_axis: float | None = None,
    ) -> None:
        self.run_id, self.product_id, self.segment_no = run_id, product_id, segment_no
        self.segment_active = True

    def start_segment(self, segment_no: int, timestamp_ms: int) -> None:
        self.segment_no, self.segment_active = segment_no, True

    def pause_segment(self, timestamp_ms: int) -> None:
        self.segment_active = False

    def add_movement(self, *args: Any, **kwargs: Any) -> list[WindowRecord]:
        return []

    def add_time(self, *args: Any, **kwargs: Any) -> list[WindowRecord]:
        return []

    def next_boundary_ms(self) -> None:
        return None

    def finalize_partial(self, timestamp_ms: int) -> None:
        return None

    def cut(self, timestamp_ms: int) -> None:
        return None

    def snapshot(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "product_id": self.product_id, "segment_no": self.segment_no}

    def restore(self, snapshot: dict[str, Any]) -> None:
        self.run_id = snapshot.get("run_id")
        self.product_id = snapshot.get("product_id")
        self.segment_no = int(snapshot.get("segment_no", 1))
        self.segment_active = self.product_id is not None

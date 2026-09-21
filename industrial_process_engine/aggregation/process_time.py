from __future__ import annotations

from dataclasses import asdict
from typing import Any

from industrial_process_engine.aggregation.aggregator import VariableAccumulator
from industrial_process_engine.config import AggregationVariableConfig
from industrial_process_engine.domain import ProcessTimeRecord, SignalValue, WindowQuality
from industrial_process_engine.processing.lifecycle_rules import SafeCondition


class ProcessTimeAggregator:
    """Wall-clock aligned aggregation that is independent of product lifecycle."""

    def __init__(
        self, process_id: str, variables: list[AggregationVariableConfig],
        interval_s: float, stale_after_ms: int, when: str | None = None,
    ) -> None:
        self.process_id = process_id
        self.specifications = {variable.name: variable for variable in variables}
        self.interval_s = interval_s
        self.interval_ms = max(1, round(interval_s * 1000))
        self.stale_after_ms = stale_after_ms
        self.condition = SafeCondition(when) if when else None
        self.window_start_ms: int | None = None
        self.cursor_ms: int | None = None
        self.started_partial = False
        self.accumulators: dict[str, VariableAccumulator] = {}
        self.active_weight = 0.0
        self.has_gap = False

    def start(self, timestamp_ms: int) -> None:
        self.window_start_ms = timestamp_ms // self.interval_ms * self.interval_ms
        self.cursor_ms = timestamp_ms
        self.started_partial = timestamp_ms != self.window_start_ms
        self._reset()

    def advance(self, timestamp_ms: int, signals: dict[str, SignalValue]) -> list[ProcessTimeRecord]:
        if self.cursor_ms is None:
            self.start(timestamp_ms)
            return []
        if timestamp_ms <= self.cursor_ms:
            return []
        records: list[ProcessTimeRecord] = []
        while self.cursor_ms < timestamp_ms:
            boundary = ((self.cursor_ms // self.interval_ms) + 1) * self.interval_ms
            chunk_end = min(timestamp_ms, boundary)
            self._add_chunk(self.cursor_ms, chunk_end, signals)
            self.cursor_ms = chunk_end
            if chunk_end == boundary:
                record = self._record(boundary)
                if record is not None:
                    records.append(record)
                self.window_start_ms = boundary
                self.started_partial = False
                self._reset()
        return records

    def next_boundary_ms(self) -> int | None:
        if self.cursor_ms is None:
            return None
        return ((self.cursor_ms // self.interval_ms) + 1) * self.interval_ms

    def snapshot(self) -> dict[str, Any]:
        return {
            "window_start_ms": self.window_start_ms,
            "cursor_ms": self.cursor_ms,
            "started_partial": self.started_partial,
            "has_gap": self.has_gap,
            "active_weight": self.active_weight,
            "accumulators": {
                name: asdict(value) for name, value in self.accumulators.items()
            },
        }

    def restore(self, payload: dict[str, Any], now_ms: int) -> None:
        saved_start = payload.get("window_start_ms")
        current_start = now_ms // self.interval_ms * self.interval_ms
        if saved_start != current_start:
            self.start(now_ms)
            return
        self.window_start_ms = int(saved_start)
        # Resume at the current wall clock. The elapsed outage/pause is unknown
        # coverage and must never be integrated using the last signal value.
        self.cursor_ms = now_ms
        self.started_partial = bool(payload.get("started_partial", False))
        self.has_gap = True  # recovery introduces an unknown interval before reconciliation
        self.accumulators = {
            name: VariableAccumulator(**payload.get("accumulators", {}).get(name, {}))
            for name in self.specifications
        }
        self.active_weight = float(payload.get(
            "active_weight",
            max((value.coverage_weight for value in self.accumulators.values()), default=0.0),
        ))

    def _add_chunk(self, start_ts: int, end_ts: int, signals: dict[str, SignalValue]) -> None:
        if self.condition is not None and not self.condition.evaluate({
            name: signal.value for name, signal in signals.items()
            if signal.quality and end_ts <= signal.timestamp_ms + self.stale_after_ms
        }):
            return
        weight = (end_ts - start_ts) / 1000.0
        self.active_weight += weight
        for name, specification in self.specifications.items():
            signal = signals.get(specification.source_name or name)
            if signal is None or not signal.quality:
                self.has_gap = True
                continue
            if specification.calculation in {"first", "last"}:
                valid_weight = weight
            else:
                expires_at = signal.timestamp_ms + self.stale_after_ms
                valid_ms = max(0, min(end_ts, expires_at) - start_ts)
                valid_weight = valid_ms / 1000.0
                if valid_weight < weight - 1e-12:
                    self.has_gap = True
            if valid_weight <= 0:
                continue
            try:
                self.accumulators[name].add(signal.value, valid_weight, specification.calculation)
            except (TypeError, ValueError):
                self.has_gap = True

    def _record(self, end_ts: int) -> ProcessTimeRecord | None:
        assert self.window_start_ms is not None
        if self.active_weight == 0:
            return None
        expected = self.active_weight
        quality = WindowQuality.PARTIAL if self.started_partial else WindowQuality.GOOD
        values: dict[str, Any | None] = {}
        for name, specification in self.specifications.items():
            accumulator = self.accumulators[name]
            complete = accumulator.coverage_weight >= expected - 1e-9
            values[name] = (
                self._convert(accumulator.result(specification.calculation), specification.output_type)
                if complete else None
            )
            if not complete:
                quality = WindowQuality.DATA_GAP
        if self.has_gap:
            quality = WindowQuality.DATA_GAP
        return ProcessTimeRecord(self.process_id, self.window_start_ms, end_ts, values, quality)

    @staticmethod
    def _convert(value: Any, output_type: str) -> Any:
        if value is None:
            return None
        if output_type == "double":
            return float(value)
        if output_type in {"int", "long"}:
            return int(value)
        return str(value) if output_type in {"symbol", "varchar", "char"} else value

    def _reset(self) -> None:
        self.accumulators = {name: VariableAccumulator() for name in self.specifications}
        self.active_weight = 0.0
        self.has_gap = False

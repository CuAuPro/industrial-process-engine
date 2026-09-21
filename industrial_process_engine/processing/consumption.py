from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from typing import Any, Iterator, Literal, Mapping

from industrial_process_engine.domain import ProductContext, ProductFieldValue, SignalValue


SourceKind = Literal["counter", "rate"]


@dataclass(frozen=True, slots=True)
class ConsumptionMetric:
    name: str
    source_kind: SourceKind
    source_signal: str
    mass_field: str | None
    cumulative_field: str
    rate_field: str
    specific_field: str | None
    quality_field: str
    stale_after_ms: int
    refresh_interval_ms: int | None = None
    summary_quality: bool = False


class ConsumptionMetricRegistry(Mapping[str, ConsumptionMetric]):
    def __init__(self) -> None:
        self._metrics: dict[str, ConsumptionMetric] = {}

    def __getitem__(self, name: str) -> ConsumptionMetric:
        return self._metrics[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._metrics)

    def __len__(self) -> int:
        return len(self._metrics)

    def counter(self, name: str, **kwargs: Any) -> None:
        self._add(name, "counter", **kwargs)

    def rate(self, name: str, **kwargs: Any) -> None:
        self._add(name, "rate", **kwargs)

    def _add(
        self, name: str, source_kind: SourceKind, *, source_signal: str,
        mass_field: str | None, cumulative_field: str, rate_field: str,
        specific_field: str | None = None, stale_after_ms: int,
        refresh_interval_ms: int | None = None,
        quality_field: str | None = None,
        summary_quality: bool = False,
    ) -> None:
        resolved_quality_field = quality_field or f"{name}_consumption_quality"
        values = [name, source_signal, cumulative_field, rate_field, resolved_quality_field]
        if specific_field is not None:
            values.append(specific_field)
        if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) for value in values):
            raise ValueError("consumption metric names and fields must be safe identifiers")
        if name in self._metrics:
            raise ValueError(f"consumption metric already registered: {name}")
        if stale_after_ms <= 0 or (refresh_interval_ms is not None and refresh_interval_ms <= 0):
            raise ValueError("consumption timing values must be positive")
        self._metrics[name] = ConsumptionMetric(
            name, source_kind, source_signal, mass_field, cumulative_field, rate_field,
            specific_field if mass_field is not None else None,
            resolved_quality_field, stale_after_ms, refresh_interval_ms, summary_quality,
        )


@dataclass(slots=True)
class _Segment:
    start_ms: int
    end_ms: int
    members: tuple[tuple[str, float | None], ...]


@dataclass(slots=True)
class _MetricState:
    last_timestamp_ms: int | None = None
    last_value: float | None = None
    last_quality: bool = False
    current_rate: float | None = None
    quality_gap: bool = False
    initialized: bool = False
    source_timestamp_ms: int | None = None
    segments: list[_Segment] | None = None

    def __post_init__(self) -> None:
        if self.segments is None:
            self.segments = []


@dataclass(frozen=True, slots=True)
class _AllocationSlice:
    metric: str
    ts_start: int
    ts_end: int
    product_id: str | None
    allocated_consumption: float
    average_rate: float | None
    allocation_policy: str
    mass_t: float | None
    quality: str
    reason: str


class ConsumptionAllocator:
    """Deterministic interval allocator for cumulative counters and instantaneous rates."""

    def __init__(self, process_id: str, metrics: Mapping[str, ConsumptionMetric] | None = None) -> None:
        self.process_id = process_id
        self.metrics = dict(metrics or {})
        self.states = {name: _MetricState() for name in self.metrics}
        self.totals: dict[str, dict[str, float]] = {name: {} for name in self.metrics}
        self.quality: dict[str, dict[str, str]] = {name: {} for name in self.metrics}
        self.masses: dict[str, dict[str, float | None]] = {name: {} for name in self.metrics}
        self.last_advanced_ms: int | None = None

    def advance(
        self, timestamp_ms: int, run_id: str | None, products: tuple[ProductContext, ...],
        signals: Mapping[str, SignalValue], *, reason: str = "TICK",
    ) -> list[_AllocationSlice]:
        if not self.metrics:
            return []
        if self.last_advanced_ms is not None and timestamp_ms < self.last_advanced_ms:
            return []
        rows: list[_AllocationSlice] = []
        for name, metric in self.metrics.items():
            state = self.states[name]
            signal = signals.get(metric.source_signal)
            members = self.members(products, metric)
            self.masses[name].update(dict(members))
            if metric.source_kind == "rate":
                rows.extend(self._advance_rate(metric, state, timestamp_ms, run_id, members, signal, reason))
            else:
                rows.extend(self._advance_counter(metric, state, timestamp_ms, run_id, members, signal, reason))
        self.last_advanced_ms = timestamp_ms
        self.update_product_fields(products, timestamp_ms)
        return rows

    def _advance_rate(self, metric: ConsumptionMetric, state: _MetricState, timestamp_ms: int,
                      run_id: str | None, members: tuple[tuple[str, float | None], ...],
                      signal: SignalValue | None, reason: str) -> list[_AllocationSlice]:
        rows: list[_AllocationSlice] = []
        if state.last_timestamp_ms is not None and timestamp_ms > state.last_timestamp_ms:
            end = timestamp_ms
            freshness_end = (state.source_timestamp_ms or state.last_timestamp_ms) + metric.stale_after_ms
            measurable_end = min(end, freshness_end)
            if state.last_quality and state.last_value is not None and measurable_end > state.last_timestamp_ms:
                rows += self._allocate(metric, run_id, state.last_timestamp_ms, measurable_end,
                                       state.last_value * (measurable_end-state.last_timestamp_ms)/3_600_000,
                                       members, "GOOD", reason)
            gap_start = measurable_end if state.last_quality else state.last_timestamp_ms
            if gap_start < end:
                rows += self._gap(metric, run_id, gap_start, end, members,
                                  "STALE_RATE" if state.last_quality else "BAD_QUALITY")
                state.quality_gap = True
                state.current_rate = None
        if signal is not None and signal.timestamp_ms == timestamp_ms:
            try:
                value = float(signal.value)
                good = signal.quality and math.isfinite(value) and value >= 0
            except (TypeError, ValueError):
                value, good = 0.0, False
            state.last_value = value if good else None
            state.last_quality = good
            state.current_rate = value if good else None
            state.source_timestamp_ms = timestamp_ms
            state.initialized = True
            if not good:
                state.quality_gap = True
        if (state.last_quality and state.source_timestamp_ms is not None
                and timestamp_ms >= state.source_timestamp_ms + metric.stale_after_ms):
            state.current_rate = None
            state.quality_gap = True
            for product_id, _ in members:
                self.quality[metric.name][product_id] = "DATA_GAP"
        state.last_timestamp_ms = timestamp_ms
        return rows

    def _advance_counter(self, metric: ConsumptionMetric, state: _MetricState, timestamp_ms: int,
                         run_id: str | None, members: tuple[tuple[str, float | None], ...],
                         signal: SignalValue | None, reason: str) -> list[_AllocationSlice]:
        assert state.segments is not None
        if state.last_timestamp_ms is not None and timestamp_ms > state.last_timestamp_ms:
            state.segments.append(_Segment(state.last_timestamp_ms, timestamp_ms, members))
        if signal is None or signal.timestamp_ms != timestamp_ms:
            state.last_timestamp_ms = timestamp_ms
            return []
        try:
            value = float(signal.value)
            good = signal.quality and math.isfinite(value)
        except (TypeError, ValueError):
            value, good = 0.0, False
        rows: list[_AllocationSlice] = []
        baseline_good = state.last_quality and state.last_value is not None
        if not state.initialized:
            if not good:
                state.quality_gap = True
            state.initialized = True
        elif good and baseline_good and value >= state.last_value:
            delta = value - state.last_value
            total_ms = sum(s.end_ms-s.start_ms for s in state.segments)
            state.current_rate = delta * 3_600_000 / total_ms if total_ms > 0 else None
            if delta > 0 and total_ms > 0:
                for segment in state.segments:
                    part = delta * (segment.end_ms-segment.start_ms) / total_ms
                    rows += self._allocate(metric, run_id, segment.start_ms, segment.end_ms,
                                           part, segment.members, "GOOD", reason)
        elif state.last_timestamp_ms is not None:
            start = state.segments[0].start_ms if state.segments else timestamp_ms
            gap_reason = "BAD_QUALITY" if not good else (
                "COUNTER_RESET" if baseline_good else "QUALITY_RESUMED"
            )
            rows += self._gap(metric, run_id, start, timestamp_ms, members, gap_reason)
            state.quality_gap = True
            state.current_rate = None
        state.last_value = value if good else None
        state.last_quality = good
        state.last_timestamp_ms = timestamp_ms
        state.segments.clear()
        return rows

    def _allocate(self, metric: ConsumptionMetric, run_id: str | None, start: int, end: int,
                  amount: float, members: tuple[tuple[str, float | None], ...], quality: str,
                  reason: str) -> list[_AllocationSlice]:
        valid_mass = bool(members) and all(m is not None and m > 0 for _, m in members)
        policy = "MASS" if valid_mass else ("EQUAL" if members else "UNALLOCATED")
        recipients = members or ((None, None),)
        total_mass = sum(m or 0 for _, m in members)
        rows = []
        for product_id, mass in recipients:
            weight = (mass or 0) / total_mass if valid_mass else 1 / len(recipients)
            allocated = amount * weight
            if product_id is not None:
                self.totals[metric.name][product_id] = self.totals[metric.name].get(product_id, 0.0) + allocated
                if quality != "GOOD":
                    self.quality[metric.name][product_id] = "DATA_GAP"
            rows.append(self._row(metric.name, run_id, start, end, product_id, allocated,
                                  allocated * 3_600_000 / (end-start) if end > start else None,
                                  policy, mass, quality, reason))
        return rows

    def _gap(self, metric: ConsumptionMetric, run_id: str | None, start: int, end: int,
             members: tuple[tuple[str, float | None], ...], reason: str) -> list[_AllocationSlice]:
        for product_id, _ in members:
            self.quality[metric.name][product_id] = "DATA_GAP"
        return [self._row(metric.name, run_id, start, end, None, 0.0, None,
                          "UNMEASURED", None, "DATA_GAP", reason)] if end > start else []

    def _row(self, metric: str, run_id: str | None, start: int, end: int, product_id: str | None,
             amount: float, rate: float | None, policy: str, mass: float | None,
             quality: str, reason: str) -> _AllocationSlice:
        return _AllocationSlice(metric, start, end, product_id, amount, rate,
                                policy, mass, quality, reason)

    def members(self, products: tuple[ProductContext, ...], metric: ConsumptionMetric) -> tuple[tuple[str, float | None], ...]:
        result = []
        for product in products:
            raw = product.field(metric.mass_field) if metric.mass_field else None
            try:
                mass = float(raw) if raw is not None and float(raw) > 0 else None
            except (TypeError, ValueError):
                mass = None
            result.append((product.product_id, mass))
        return tuple(result)

    def update_product_fields(self, products: tuple[ProductContext, ...], timestamp_ms: int) -> None:
        for metric in self.metrics.values():
            state = self.states[metric.name]
            mass_map = dict(self.members(products, metric))
            active = self.members(products, metric)
            valid_mass = bool(active) and all(m is not None for _, m in active)
            total_mass = sum(m or 0 for _, m in active)
            for product in products:
                total = self.totals[metric.name].get(product.product_id, 0.0)
                mass = mass_map.get(product.product_id)
                quality = self.quality[metric.name].get(product.product_id, "GOOD")
                rate = state.current_rate
                if rate is not None:
                    rate *= (mass or 0) / total_mass if valid_mass and total_mass else 1 / len(active)
                product.context[metric.cumulative_field] = ProductFieldValue(total, quality == "GOOD", timestamp_ms)
                product.context[metric.rate_field] = ProductFieldValue(rate, rate is not None and quality == "GOOD", timestamp_ms)
                if metric.specific_field is not None:
                    product.context[metric.specific_field] = ProductFieldValue(
                        total / mass if mass else None,
                        mass is not None and quality == "GOOD", timestamp_ms,
                    )
                product.context[metric.quality_field] = ProductFieldValue(quality, True, timestamp_ms)

    def next_boundary_ms(self) -> int | None:
        boundaries: list[int] = []
        for name, metric in self.metrics.items():
            if metric.source_kind != "rate":
                continue
            state = self.states[name]
            if state.last_quality and state.source_timestamp_ms is not None:
                boundaries.append(state.source_timestamp_ms + metric.stale_after_ms)
            if metric.refresh_interval_ms and self.last_advanced_ms is not None:
                boundaries.append(self.last_advanced_ms + metric.refresh_interval_ms)
        return min((value for value in boundaries if self.last_advanced_ms is None or value > self.last_advanced_ms), default=None)

    def snapshot(self) -> dict[str, Any]:
        return {"states": {k: asdict(v) for k, v in self.states.items()}, "totals": self.totals,
                "quality": self.quality, "masses": self.masses,
                "last_advanced_ms": self.last_advanced_ms}

    def restore(self, value: Mapping[str, Any]) -> None:
        for name, raw in value.get("states", {}).items():
            if name in self.states:
                raw = dict(raw)
                raw["segments"] = [_Segment(**item) for item in raw.get("segments", [])]
                self.states[name] = _MetricState(**raw)
        self.totals = {k: dict(v) for k, v in value.get("totals", self.totals).items()}
        self.quality = {k: dict(v) for k, v in value.get("quality", self.quality).items()}
        self.masses = {k: dict(v) for k, v in value.get("masses", self.masses).items()}
        self.last_advanced_ms = value.get("last_advanced_ms")

    def summary_values(self, product_id: str) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for metric in self.metrics.values():
            total = self.totals[metric.name].get(product_id, 0.0)
            mass = self.masses[metric.name].get(product_id)
            values[metric.cumulative_field] = total
            if metric.specific_field is not None:
                values[metric.specific_field] = total / mass if mass else None
            if metric.summary_quality:
                values[metric.quality_field] = self.quality[metric.name].get(product_id, "GOOD")
        return values

    def bind_product_id(self, old_product_id: str, new_product_id: str) -> None:
        for collection in (self.totals, self.quality, self.masses):
            for values in collection.values():
                if old_product_id in values:
                    values[new_product_id] = values.pop(old_product_id)
        for state in self.states.values():
            if state.segments is not None:
                state.segments = [
                    _Segment(
                        segment.start_ms, segment.end_ms,
                        tuple(
                            (new_product_id if product_id == old_product_id else product_id, mass)
                            for product_id, mass in segment.members
                        ),
                    )
                    for segment in state.segments
                ]

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from typing import Any, Mapping

from industrial_process_engine.aggregation.aggregator import Aggregator
from industrial_process_engine.config import AggregationVariableConfig
from industrial_process_engine.domain import SignalValue, TrackingStatus, WindowQuality, WindowRecord
from industrial_process_engine.processing.global_transport import TransportMovement


QUALITY_PRIORITY = {
    WindowQuality.GOOD: 0,
    WindowQuality.PARTIAL: 1,
    WindowQuality.ESTIMATED_POSITION: 2,
    WindowQuality.DATA_GAP: 3,
    WindowQuality.TRACKING_LOST: 4,
}


@dataclass
class SpatialProduct:
    run_id: str
    product_id: str
    start_ts: int
    entry_coordinate_m: float
    aggregators: dict[str, Aggregator]
    exit_coordinate_m: float | None = None
    end_ts: int | None = None
    material_length_m: float | None = None
    origin_times: dict[str, int] = field(default_factory=dict)
    pending: dict[int, dict[str, WindowRecord]] = field(default_factory=dict)
    estimated_windows: set[int] = field(default_factory=set)
    position_m: float = 0.0


class SpatialDistanceAggregator:
    """Aligns each aggregate to the material coordinate at its sensor."""

    def __init__(
        self, process_id: str, variables: list[AggregationVariableConfig],
        interval_m: float, stale_after_ms: int, line_length_m: float,
    ) -> None:
        self.process_id = process_id
        self.specifications = {v.name: v for v in variables}
        self.interval = interval_m
        self.stale_after_ms = stale_after_ms
        self.line_length_m = line_length_m
        self.products: dict[str, SpatialProduct] = {}
        self.max_offset_m = max((v.spatial_offset_m for v in variables), default=0.0)

    def start(
        self, run_id: str, product_id: str, timestamp_ms: int, coordinate_m: float,
        start_position_m: float = 0.0,
    ) -> None:
        aggregators: dict[str, Aggregator] = {}
        for variable in self.specifications.values():
            aggregator = Aggregator(
                self.process_id, [variable], "distance", self.interval, self.stale_after_ms,
            )
            aggregator.start(run_id, product_id, timestamp_ms, start_axis=start_position_m)
            aggregators[variable.name] = aggregator
        self.products[run_id] = SpatialProduct(
            run_id, product_id, timestamp_ms, coordinate_m - start_position_m, aggregators,
            origin_times={self._key(start_position_m): timestamp_ms},
            position_m=start_position_m,
        )

    def origin_exit(self, run_id: str, timestamp_ms: int, coordinate_m: float) -> float:
        product = self.products[run_id]
        product.exit_coordinate_m = coordinate_m
        product.end_ts = timestamp_ms
        product.material_length_m = max(0.0, coordinate_m - product.entry_coordinate_m)
        product.position_m = product.material_length_m
        product.origin_times[self._key(product.material_length_m)] = timestamp_ms
        return product.material_length_m

    def bind_product_id(self, run_id: str, new_product_id: str) -> None:
        product = self.products[run_id]
        product.product_id = new_product_id
        for aggregator in product.aggregators.values():
            aggregator.product_id = new_product_id
        product.pending = {
            window_no: {
                name: replace(record, product_id=new_product_id)
                for name, record in records.items()
            }
            for window_no, records in product.pending.items()
        }

    def add_movement(
        self, movement: TransportMovement, signals: Mapping[str, SignalValue],
        run_id: str | None = None,
    ) -> tuple[list[WindowRecord], list[str]]:
        ready: list[WindowRecord] = []
        drained: list[str] = []
        selected = (
            (self.products[run_id],) if run_id is not None and run_id in self.products else ()
        )
        for product in selected:
            self._record_origin_times(product, movement)
            for name, specification in self.specifications.items():
                local_start = movement.start_m - product.entry_coordinate_m - specification.spatial_offset_m
                local_end = movement.end_m - product.entry_coordinate_m - specification.spatial_offset_m
                limit = product.material_length_m
                start = max(0.0, local_start)
                end = max(0.0, local_end)
                if limit is not None:
                    start, end = min(start, limit), min(end, limit)
                if end <= start + 1e-12:
                    continue
                actual_start = product.entry_coordinate_m + specification.spatial_offset_m + start
                actual_end = product.entry_coordinate_m + specification.spatial_offset_m + end
                start_ts = self._interpolate(movement, actual_start)
                end_ts = self._interpolate(movement, actual_end)
                records = product.aggregators[name].add_movement(
                    start, end, start_ts, end_ts, dict(signals), movement.status,
                )
                if movement.estimated:
                    first = int(math.floor(start / self.interval + 1e-12))
                    last = int(math.floor(max(start, end - 1e-12) / self.interval))
                    product.estimated_windows.update(range(first, last + 1))
                for record in records:
                    product.pending.setdefault(record.window_no, {})[name] = record
            ready.extend(self._collect_ready(product))
            product.position_m = max(
                product.position_m, movement.end_m - product.entry_coordinate_m,
            )
            if self.is_drained(product.run_id):
                ready.extend(self.finalize(product.run_id))
                drained.append(product.run_id)
        return ready, drained

    def advance_draining(
        self, delta_m: float, start_ts: int, end_ts: int,
        signals: Mapping[str, SignalValue],
    ) -> tuple[list[WindowRecord], list[str]]:
        ready: list[WindowRecord] = []
        drained: list[str] = []
        if delta_m <= 0:
            return ready, drained
        for product in tuple(self.products.values()):
            if product.exit_coordinate_m is None:
                continue
            start = product.position_m
            end = min(self._drain_target(product), start + delta_m)
            movement = TransportMovement(
                product.entry_coordinate_m + start,
                product.entry_coordinate_m + end,
                start_ts, end_ts, TrackingStatus.OK, estimated=True,
            )
            rows, completed = self.add_movement(movement, signals, product.run_id)
            ready.extend(rows)
            drained.extend(completed)
        return ready, drained

    def is_drained(self, run_id: str, coordinate_m: float | None = None) -> bool:
        product = self.products[run_id]
        return (
            product.exit_coordinate_m is not None
            and product.position_m >= self._drain_target(product) - 1e-9
        )

    def drain_target_m(self, run_id: str) -> float:
        return self._drain_target(self.products[run_id])

    def _drain_target(self, product: SpatialProduct) -> float:
        return (product.material_length_m or 0.0) + self.line_length_m

    def finalize(self, run_id: str) -> list[WindowRecord]:
        product = self.products[run_id]
        for name, aggregator in product.aggregators.items():
            partial = aggregator.finalize_partial(product.end_ts or product.start_ts)
            if partial is not None:
                product.pending.setdefault(partial.window_no, {})[name] = partial
        return self._collect_ready(product, force=True)

    def remove(self, run_id: str) -> SpatialProduct:
        return self.products.pop(run_id)

    def _collect_ready(self, product: SpatialProduct, force: bool = False) -> list[WindowRecord]:
        ready: list[WindowRecord] = []
        for window_no in sorted(tuple(product.pending)):
            pieces = product.pending[window_no]
            if not force and len(pieces) != len(self.specifications):
                continue
            template = next(iter(pieces.values()))
            values = {
                name: pieces[name].values.get(name) if name in pieces else None
                for name in self.specifications
            }
            qualities = [piece.quality for piece in pieces.values()]
            if len(pieces) != len(self.specifications):
                qualities.append(WindowQuality.DATA_GAP)
            quality = max(qualities, key=QUALITY_PRIORITY.get)
            if window_no in product.estimated_windows and QUALITY_PRIORITY[quality] < QUALITY_PRIORITY[WindowQuality.DATA_GAP]:
                quality = WindowQuality.ESTIMATED_POSITION
            position_start = template.position_start_m or 0.0
            position_end = template.position_end_m or position_start
            ts_start = self._origin_time(product, position_start)
            ts_end = self._origin_time(product, position_end)
            ready.append(replace(
                template, values=values, quality=quality, ts_start=ts_start, ts_end=ts_end,
                elapsed_start_s=(ts_start - product.start_ts) / 1000.0,
                elapsed_end_s=(ts_end - product.start_ts) / 1000.0,
            ))
            del product.pending[window_no]
        return ready

    def _record_origin_times(self, product: SpatialProduct, movement: TransportMovement) -> None:
        local_start = movement.start_m - product.entry_coordinate_m
        local_end = movement.end_m - product.entry_coordinate_m
        if local_end <= 0:
            return
        first_boundary = max(1, math.floor(max(0.0, local_start) / self.interval) + 1)
        last_boundary = math.floor(local_end / self.interval + 1e-12)
        for index in range(first_boundary, last_boundary + 1):
            position = index * self.interval
            product.origin_times[self._key(position)] = self._interpolate(
                movement, product.entry_coordinate_m + position,
            )

    def _origin_time(self, product: SpatialProduct, position: float) -> int:
        key = self._key(position)
        if key in product.origin_times:
            return product.origin_times[key]
        if product.material_length_m is not None and math.isclose(position, product.material_length_m, abs_tol=1e-8):
            return product.end_ts or product.start_ts
        # This only occurs after recovery from an older/incomplete checkpoint.
        return product.end_ts or product.start_ts

    @staticmethod
    def _interpolate(movement: TransportMovement, coordinate: float) -> int:
        span = movement.end_m - movement.start_m
        if span <= 0:
            return movement.end_ts
        ratio = min(1.0, max(0.0, (coordinate - movement.start_m) / span))
        return round(movement.start_ts + ratio * (movement.end_ts - movement.start_ts))

    @staticmethod
    def _key(value: float) -> str:
        return f"{value:.9f}"

    def snapshot(self) -> dict[str, Any]:
        return {
            "products": {
                run_id: {
                    "run_id": p.run_id, "product_id": p.product_id, "start_ts": p.start_ts,
                    "entry_coordinate_m": p.entry_coordinate_m,
                    "exit_coordinate_m": p.exit_coordinate_m, "end_ts": p.end_ts,
                    "material_length_m": p.material_length_m, "origin_times": p.origin_times,
                    "position_m": p.position_m,
                    "pending": {
                        str(no): {name: self._record_dict(row) for name, row in parts.items()}
                        for no, parts in p.pending.items()
                    },
                    "estimated_windows": sorted(p.estimated_windows),
                    "aggregators": {name: agg.snapshot() for name, agg in p.aggregators.items()},
                } for run_id, p in self.products.items()
            }
        }

    def restore(self, payload: Mapping[str, Any]) -> None:
        self.products.clear()
        for run_id, value in payload.get("products", {}).items():
            aggregators: dict[str, Aggregator] = {}
            for name, specification in self.specifications.items():
                aggregator = Aggregator(
                    self.process_id, [specification], "distance", self.interval, self.stale_after_ms,
                )
                aggregator.restore(value["aggregators"][name])
                aggregators[name] = aggregator
            product = SpatialProduct(
                run_id=value["run_id"], product_id=value["product_id"], start_ts=int(value["start_ts"]),
                entry_coordinate_m=float(value["entry_coordinate_m"]), aggregators=aggregators,
                exit_coordinate_m=value.get("exit_coordinate_m"), end_ts=value.get("end_ts"),
                material_length_m=value.get("material_length_m"),
                origin_times={str(k): int(v) for k, v in value.get("origin_times", {}).items()},
                estimated_windows={int(v) for v in value.get("estimated_windows", [])},
                position_m=float(value.get("position_m", value.get("material_length_m") or 0.0)),
            )
            product.pending = {
                int(no): {name: self._record_from_dict(row) for name, row in parts.items()}
                for no, parts in value.get("pending", {}).items()
            }
            self.products[run_id] = product

    @staticmethod
    def _record_dict(record: WindowRecord) -> dict[str, Any]:
        from dataclasses import asdict
        value = asdict(record)
        value["quality"] = str(record.quality)
        return value

    @staticmethod
    def _record_from_dict(value: Mapping[str, Any]) -> WindowRecord:
        data = dict(value)
        data["quality"] = WindowQuality(data["quality"])
        return WindowRecord(**data)

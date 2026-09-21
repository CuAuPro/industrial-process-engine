from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Literal, Mapping

from industrial_process_engine.aggregation.aggregator import Aggregator
from industrial_process_engine.config import OutputType
from industrial_process_engine.domain import ProductContext, ProductFieldValue, SignalValue


PRODUCT_SUMMARY_COLUMNS = {
    "process_id", "run_id", "product_id", "start_ts", "end_ts", "processing_time_s",
    "start_mode", "state", "sync_state", "error_message",
}
OUTPUT_TYPES = {"double", "int", "long", "symbol", "varchar", "char"}
CapturePolicy = Literal["start", "latest"]
SummaryTarget = bool | str


@dataclass(frozen=True, slots=True)
class ProductFieldResult:
    value: Any
    quality: bool = True


@dataclass(frozen=True, slots=True)
class ProductCalculationContext:
    product: ProductContext
    products: tuple[ProductContext, ...]
    signals: Mapping[str, SignalValue]
    timestamp_ms: int
    position_m: float | None = None

    @property
    def elapsed_s(self) -> float:
        return (self.timestamp_ms - self.product.start_ts) / 1000.0

    def field(self, name: str) -> Any | None:
        return self.product.field(name)

    def signal(self, name: str) -> Any | None:
        value = self.signals.get(name)
        return value.value if value is not None and value.quality else None


@dataclass(frozen=True, slots=True)
class ProductSummaryContext:
    product: ProductContext
    end_ts: int
    windows: tuple[Mapping[str, Any], ...]
    products: tuple[ProductContext, ...] = ()

    @property
    def processing_time_s(self) -> float:
        return (self.end_ts - self.product.start_ts) / 1000.0


ProductCalculatedCalculator = Callable[[ProductCalculationContext], ProductFieldResult | Any]
ProductSummaryCalculator = Callable[[ProductSummaryContext], Any]


@dataclass(frozen=True, slots=True)
class ProductInputField:
    name: str
    output_type: OutputType
    from_signal: str | None
    capture: CapturePolicy
    checkpoint: bool
    summary: SummaryTarget


@dataclass(frozen=True, slots=True)
class ProductCalculatedField:
    name: str
    output_type: OutputType
    calculator: ProductCalculatedCalculator
    field_dependencies: frozenset[str]
    signal_dependencies: frozenset[str]
    refresh_interval_ms: int | None
    checkpoint: bool
    summary: SummaryTarget


@dataclass(frozen=True, slots=True)
class ProductSummaryField:
    name: str
    output_type: OutputType
    calculator: ProductSummaryCalculator


@dataclass(frozen=True, slots=True)
class ProductManagedField:
    """A live field owned by an engine subsystem rather than application assignment."""
    name: str
    output_type: OutputType
    checkpoint: bool
    summary: SummaryTarget
    preserve_invalid_summary: bool = False


ProductField = ProductInputField | ProductCalculatedField | ProductSummaryField | ProductManagedField


class ProductFieldRegistry(Mapping[str, ProductField]):
    """Application-owned product inputs, live calculations, and final summaries."""

    def __init__(self) -> None:
        self._fields: dict[str, ProductField] = {}

    def __getitem__(self, name: str) -> ProductField:
        return self._fields[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._fields)

    def __len__(self) -> int:
        return len(self._fields)

    def input(
        self, name: str, *, output_type: OutputType, from_signal: str | None = None,
        capture: CapturePolicy = "latest", checkpoint: bool = True,
        summary: SummaryTarget = True,
    ) -> None:
        if capture not in {"start", "latest"}:
            raise ValueError(f"unsupported product field capture policy: {capture}")
        self._add(ProductInputField(
            name, output_type, from_signal, capture, checkpoint, summary,
        ))

    def calculated(
        self, name: str, *, output_type: OutputType,
        field_dependencies: set[str] | frozenset[str] = frozenset(),
        signal_dependencies: set[str] | frozenset[str] = frozenset(),
        refresh_interval_ms: int | None = None, checkpoint: bool = False,
        summary: SummaryTarget = False,
    ) -> Callable[[ProductCalculatedCalculator], ProductCalculatedCalculator]:
        if refresh_interval_ms is not None and refresh_interval_ms <= 0:
            raise ValueError("product field refresh_interval_ms must be positive")

        def decorator(calculator: ProductCalculatedCalculator) -> ProductCalculatedCalculator:
            self._add(ProductCalculatedField(
                name, output_type, calculator, frozenset(field_dependencies),
                frozenset(signal_dependencies), refresh_interval_ms, checkpoint, summary,
            ))
            return calculator

        return decorator

    def summary(
        self, name: str, *, output_type: OutputType,
    ) -> Callable[[ProductSummaryCalculator], ProductSummaryCalculator]:
        def decorator(calculator: ProductSummaryCalculator) -> ProductSummaryCalculator:
            self._add(ProductSummaryField(name, output_type, calculator))
            return calculator

        return decorator

    def _add(self, field: ProductField) -> None:
        self._validate_name(field.name)
        if field.output_type not in OUTPUT_TYPES:
            raise ValueError(f"unsupported product field output type: {field.output_type}")
        if field.name in self._fields:
            raise ValueError(f"product field already registered: {field.name}")
        self._fields[field.name] = field

    @staticmethod
    def _validate_name(name: str) -> None:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"invalid product field name: {name!r}")


class ProductFieldEngine:
    def __init__(
        self, fields: Mapping[str, ProductField] | None = None,
        signal_names: set[str] | None = None,
    ) -> None:
        self.fields = dict(fields or {})
        self.inputs = {
            name: field for name, field in self.fields.items()
            if isinstance(field, ProductInputField)
        }
        self.calculated = {
            name: field for name, field in self.fields.items()
            if isinstance(field, ProductCalculatedField)
        }
        self.summaries = {
            name: field for name, field in self.fields.items()
            if isinstance(field, ProductSummaryField)
        }
        self.managed = {
            name: field for name, field in self.fields.items()
            if isinstance(field, ProductManagedField)
        }
        self.live_names = set(self.inputs) | set(self.calculated) | set(self.managed)
        self._due: dict[tuple[str, str, str], int] = {}
        self._failed: set[tuple[str, str, str]] = set()
        self._validate(signal_names)
        self.calculation_order = self._topological_order()

    def _validate(self, signal_names: set[str] | None) -> None:
        for name, field in self.fields.items():
            if name != field.name:
                raise ValueError(f"product field registry key does not match field name: {name}")
            ProductFieldRegistry._validate_name(name)
            if field.output_type not in OUTPUT_TYPES:
                raise ValueError(f"unsupported product field output type: {field.output_type}")
        if signal_names is not None:
            referenced = {
                field.from_signal for field in self.inputs.values() if field.from_signal
            } | {
                dependency for field in self.calculated.values()
                for dependency in field.signal_dependencies
            }
            missing = sorted(referenced - signal_names)
            if missing:
                raise ValueError(f"product fields reference unknown signals: {missing}")
        missing_fields = sorted({
            dependency for field in self.calculated.values()
            for dependency in field.field_dependencies if dependency not in self.live_names
        })
        if missing_fields:
            raise ValueError(f"product fields reference unknown fields: {missing_fields}")
        destinations: list[str] = []
        for name, field in {**self.inputs, **self.calculated, **self.managed}.items():
            target = self._summary_target(name, field.summary)
            if target:
                destinations.append(target)
        destinations.extend(self.summaries)
        if len(destinations) != len(set(destinations)):
            raise ValueError("duplicate product summary field")
        reserved = sorted(set(destinations) & PRODUCT_SUMMARY_COLUMNS)
        if reserved:
            raise ValueError(f"product fields collide with fixed product_summary columns: {reserved}")
        for destination in destinations:
            ProductFieldRegistry._validate_name(destination)

    def _topological_order(self) -> tuple[str, ...]:
        pending = dict(self.calculated)
        available = set(self.inputs) | set(self.managed)
        order: list[str] = []
        while pending:
            ready = [
                name for name, field in pending.items()
                if field.field_dependencies <= available
            ]
            if not ready:
                raise ValueError(f"cyclic product field dependencies: {sorted(pending)}")
            for name in ready:
                order.append(name)
                available.add(name)
                del pending[name]
        return tuple(order)

    @property
    def storage_schema(self) -> dict[str, str]:
        schema: dict[str, str] = {}
        for name, field in {**self.inputs, **self.calculated, **self.managed}.items():
            target = self._summary_target(name, field.summary)
            if target:
                schema[target] = field.output_type
        schema.update({name: field.output_type for name, field in self.summaries.items()})
        return schema

    @property
    def durable_signal_names(self) -> set[str]:
        return {
            field.from_signal for field in self.inputs.values()
            if field.from_signal and field.checkpoint
        }

    def initialize(
        self, product: ProductContext, raw_values: Mapping[str, Any],
        signals: Mapping[str, SignalValue], timestamp_ms: int,
    ) -> set[str]:
        product.context = {
            name: ProductFieldValue(None, False, timestamp_ms) for name in self.live_names
        }
        changed = self.project_inputs(
            (product,), signals, set(signals), timestamp_ms, include_start=True,
        )
        changed |= self.assign(product, raw_values, timestamp_ms)
        return changed

    def assign(
        self, product: ProductContext, values: Mapping[str, Any], timestamp_ms: int,
    ) -> set[str]:
        self.validate_assignments(values)
        changed: set[str] = set()
        for name, raw in values.items():
            field = self.inputs[name]
            value = self._convert(raw, field.output_type)
            product.context[name] = ProductFieldValue(value, value is not None, timestamp_ms)
            changed.add(name)
        return changed

    def validate_assignments(self, values: Mapping[str, Any]) -> None:
        unknown = sorted(set(values) - set(self.inputs))
        if unknown:
            raise ValueError(f"unknown or read-only product fields: {unknown}")
        for name, raw in values.items():
            self._convert(raw, self.inputs[name].output_type)

    def project_inputs(
        self, products: tuple[ProductContext, ...], signals: Mapping[str, SignalValue],
        changed_signals: set[str], timestamp_ms: int, *, include_start: bool = False,
    ) -> set[str]:
        changed: set[str] = set()
        for name, field in self.inputs.items():
            if not field.from_signal or field.from_signal not in changed_signals:
                continue
            if field.capture == "start" and not include_start:
                continue
            signal = signals.get(field.from_signal)
            for product in products:
                if signal is not None and signal.quality:
                    try:
                        value = self._convert(signal.value, field.output_type)
                        product.context[name] = ProductFieldValue(value, value is not None, timestamp_ms)
                    except (TypeError, ValueError, ArithmeticError):
                        product.context[name] = ProductFieldValue(None, False, timestamp_ms)
                else:
                    product.context[name] = ProductFieldValue(None, False, timestamp_ms)
            changed.add(name)
        return changed

    def evaluate(
        self, products: tuple[ProductContext, ...], signals: Mapping[str, SignalValue],
        timestamp_ms: int, position_m: float | None, *,
        changed_fields: set[str] | None = None, changed_signals: set[str] | None = None,
        force: bool = False,
    ) -> list[tuple[str, str]]:
        changed_fields = set(changed_fields or ())
        changed_signals = set(changed_signals or ())
        errors: list[tuple[str, str]] = []
        for name in self.calculation_order:
            field = self.calculated[name]
            interval_due = any(
                self._due.get((product.run_id, product.product_id, name), timestamp_ms) <= timestamp_ms
                for product in products
            ) if field.refresh_interval_ms is not None else False
            dependency_changed = bool(
                field.field_dependencies & changed_fields
                or field.signal_dependencies & changed_signals
            )
            if not force and not interval_due and not dependency_changed:
                continue
            for product in products:
                key = (product.run_id, product.product_id, name)
                try:
                    raw = field.calculator(ProductCalculationContext(
                        product, products, signals, timestamp_ms, position_m,
                    ))
                    result = raw if isinstance(raw, ProductFieldResult) else ProductFieldResult(raw)
                    quality = bool(result.quality and result.value is not None)
                    value = self._convert(result.value, field.output_type) if quality else None
                    product.context[name] = ProductFieldValue(value, quality, timestamp_ms)
                    self._failed.discard(key)
                except Exception as error:
                    product.context[name] = ProductFieldValue(None, False, timestamp_ms)
                    if key not in self._failed:
                        errors.append((product.product_id, f"{name}: {error}"))
                        self._failed.add(key)
                if field.refresh_interval_ms is not None:
                    self._due[key] = timestamp_ms + field.refresh_interval_ms
            changed_fields.add(name)
        return errors

    def next_due_ms(self) -> int | None:
        return min(self._due.values(), default=None)

    def clear_run(self, run_id: str) -> None:
        self._due = {key: value for key, value in self._due.items() if key[0] != run_id}
        self._failed = {key for key in self._failed if key[0] != run_id}

    def bind_product_id(self, run_id: str, old_product_id: str, new_product_id: str) -> None:
        self._due = {
            (key[0], new_product_id if key[0] == run_id and key[1] == old_product_id else key[1], key[2]): value
            for key, value in self._due.items()
        }
        self._failed = {
            (key[0], new_product_id if key[0] == run_id and key[1] == old_product_id else key[1], key[2])
            for key in self._failed
        }

    def checkpoint_context(self, product: ProductContext) -> dict[str, dict[str, Any]]:
        checkpointed = {
            name for name, field in {**self.inputs, **self.calculated, **self.managed}.items()
            if field.checkpoint
        }
        return {
            name: {
                "value": value.value, "quality": value.quality,
                "timestamp_ms": value.timestamp_ms,
            }
            for name, value in product.context.items() if name in checkpointed
        }

    def restore_context(
        self, raw: Mapping[str, Mapping[str, Any]], timestamp_ms: int,
    ) -> dict[str, ProductFieldValue]:
        return {
            name: ProductFieldValue(**raw[name]) if name in raw
            else ProductFieldValue(None, False, timestamp_ms)
            for name in self.live_names
        }

    def calculate_summary(
        self, context: ProductSummaryContext,
    ) -> tuple[dict[str, Any | None], list[str]]:
        values: dict[str, Any | None] = {}
        errors: list[str] = []
        for name, field in {**self.inputs, **self.calculated, **self.managed}.items():
            target = self._summary_target(name, field.summary)
            if target:
                live = context.product.context[name]
                values[target] = live.value if live.quality or (
                    isinstance(field, ProductManagedField) and field.preserve_invalid_summary
                ) else None
        for name, field in self.summaries.items():
            try:
                values[name] = self._convert(field.calculator(context), field.output_type)
            except Exception as error:
                values[name] = None
                errors.append(f"{name}: {error}")
        return values, errors

    @staticmethod
    def _summary_target(name: str, summary: SummaryTarget) -> str | None:
        if summary is True:
            return name
        if summary is False:
            return None
        return summary

    @staticmethod
    def _convert(value: Any, output_type: OutputType) -> Any | None:
        return Aggregator.convert_output(value, output_type)

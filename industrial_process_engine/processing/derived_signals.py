from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Mapping

from industrial_process_engine.config import (
    AggregationVariableConfig, MappingOutputsConfig, PROCESS_DATA_TIME_COLUMNS,
    PRODUCT_DATA_COLUMNS, SignalType, validate_aggregate,
)
from industrial_process_engine.domain import SignalUpdate, SignalValue
from industrial_process_engine.input.conversion import coerce_signal_value

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DerivedSignalResult:
    """Value returned by an internal signal derivation."""

    value: Any
    quality: bool = True


DerivedSignalCalculator = Callable[[Mapping[str, SignalValue], int], DerivedSignalResult | Any]


@dataclass(frozen=True, slots=True)
class DerivedSignal:
    name: str
    value_type: SignalType
    derivation: DerivedSignalCalculator
    outputs: MappingOutputsConfig
    station: str | None = None
    spatial_offset_m: float | None = None


class DerivedSignalRegistry(Mapping[str, DerivedSignal]):
    """Application-owned derived signal registry."""

    def __init__(self) -> None:
        self._derived_signals: dict[str, DerivedSignal] = {}

    def __getitem__(self, name: str) -> DerivedSignal:
        return self._derived_signals[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._derived_signals)

    def __len__(self) -> int:
        return len(self._derived_signals)

    def register(
        self, name: str, *, value_type: SignalType,
        outputs: MappingOutputsConfig | Mapping[str, Any] | None = None,
        station: str | None = None,
        spatial_offset_m: float | None = None,
    ) -> Callable[[DerivedSignalCalculator], DerivedSignalCalculator]:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"invalid derived signal name: {name!r}")
        if value_type not in {"float", "int", "bool", "string"}:
            raise ValueError(f"unsupported derived signal value type: {value_type}")
        if station is not None and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", station):
            raise ValueError(f"invalid station name: {station!r}")
        if spatial_offset_m is not None and (
            not math.isfinite(spatial_offset_m) or spatial_offset_m < 0
        ):
            raise ValueError("spatial_offset_m must be finite and non-negative")
        output_config = (
            outputs if isinstance(outputs, MappingOutputsConfig)
            else MappingOutputsConfig.model_validate(outputs or {})
        )
        for output in output_config.product_data:
            output_name = output.name or name
            if output_name in PRODUCT_DATA_COLUMNS:
                raise ValueError(f"derived output collides with fixed data column: {output_name}")
            validate_aggregate(
                name, value_type, output.calculation, output_config.output_type_for(output),
            )
        for output in output_config.process_data_time:
            output_name = output.name or name
            if output_name in PROCESS_DATA_TIME_COLUMNS:
                raise ValueError(f"derived output collides with fixed time-data column: {output_name}")
            validate_aggregate(
                name, value_type, output.calculation, output_config.output_type_for(output),
            )

        def decorator(derivation: DerivedSignalCalculator) -> DerivedSignalCalculator:
            if name in self._derived_signals:
                raise ValueError(f"derived signal already registered: {name}")
            self._derived_signals[name] = DerivedSignal(
                name, value_type, derivation, output_config, station, spatial_offset_m,
            )
            return derivation

        return decorator


class DerivedSignalEngine:
    def __init__(
        self,
        derived_signals: Mapping[str, DerivedSignal] | None = None,
        mapped_signal_names: set[str] | None = None,
    ) -> None:
        self.derived_signals = dict(derived_signals or {})
        for name, signal in self.derived_signals.items():
            if name != signal.name:
                raise ValueError(f"derived signal registry key does not match name: {name}")
        overlap = sorted(set(self.derived_signals) & set(mapped_signal_names or ()))
        if overlap:
            raise ValueError(f"derived signal names collide with mappings: {overlap}")

    @property
    def aggregation_variables(self) -> list[AggregationVariableConfig]:
        return [
            AggregationVariableConfig(
                name=output.name or signal.name,
                source_name=signal.name,
                spatial_offset_m=signal.spatial_offset_m or 0.0,
                calculation=output.calculation,
                output_type=signal.outputs.output_type_for(output),
            )
            for signal in self.derived_signals.values()
            for output in signal.outputs.product_data
        ]

    @property
    def process_time_variables(self) -> list[AggregationVariableConfig]:
        return [
            AggregationVariableConfig(
                name=output.name or signal.name, source_name=signal.name,
                calculation=output.calculation,
                output_type=signal.outputs.output_type_for(output),
            )
            for signal in self.derived_signals.values()
            for output in signal.outputs.process_data_time
        ]

    def evaluate(
        self, signal_state: Mapping[str, SignalValue], timestamp_ms: int,
    ) -> list[SignalUpdate]:
        # Declaration order permits a later derived signal to use an earlier one.
        state = dict(signal_state)
        updates: list[SignalUpdate] = []
        for signal in self.derived_signals.values():
            try:
                raw_result = signal.derivation(state, timestamp_ms)
                result = (
                    raw_result if isinstance(raw_result, DerivedSignalResult)
                    else DerivedSignalResult(raw_result)
                )
                quality = result.quality and result.value is not None
                value = coerce_signal_value(result.value, signal.value_type) if quality else None
            except (KeyError, TypeError, ValueError, ArithmeticError) as error:
                log.warning("Derived signal %s is invalid: %s", signal.name, error)
                value = None
                quality = False
            update = SignalUpdate(
                name=signal.name,
                value=value,
                quality=quality,
                timestamp_ms=timestamp_ms,
                source_id=f"internal:{signal.name}",
                topic="internal",
            )
            updates.append(update)
            state[signal.name] = SignalValue(value, quality, timestamp_ms)
        return updates

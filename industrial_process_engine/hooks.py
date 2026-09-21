from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

from industrial_process_engine.domain import (
    LifecycleSnapshot, ProductContext, ProcessContext, ProcessProduct, ProcessEvent, SignalBatch,
)


class OpcUaReader(Protocol):
    def read_many(self, names: list[str] | tuple[str, ...], reason: str) -> SignalBatch:
        ...


@dataclass(frozen=True, slots=True)
class HookServices:
    opcua: OpcUaReader | None = None


@dataclass(frozen=True, slots=True)
class ProcessStartPreparation:
    snapshot: SignalBatch | None = None
    context: Mapping[str, Any] = field(default_factory=dict)
    products: tuple[ProcessProduct, ...] = ()


@dataclass(frozen=True, slots=True)
class ProcessEndPreparation:
    snapshot: SignalBatch | None = None
    context: Mapping[str, Any] = field(default_factory=dict)


class ProcessHooks:
    """Typed application extension points; default implementations do nothing."""

    def on_startup(self, services: HookServices) -> LifecycleSnapshot | None:
        return None

    def before_process_start(
        self, event: ProcessEvent, product_id: str | None, services: HookServices,
    ) -> ProcessStartPreparation:
        return ProcessStartPreparation()

    def before_product_enter(
        self, event: ProcessEvent, product_id: str, services: HookServices,
    ) -> ProcessStartPreparation:
        return self.before_process_start(event, product_id, services)

    def after_product_enter(self, product: ProductContext) -> None:
        pass

    def before_product_update(
        self, event: ProcessEvent, product: ProductContext, services: HookServices,
    ) -> Mapping[str, Any]:
        return {}

    def before_product_exit(
        self, event: ProcessEvent, product: ProductContext, services: HookServices,
    ) -> ProcessEndPreparation:
        return ProcessEndPreparation()

    def after_product_exit(self, product: ProductContext, end_ts: int, reason: str) -> None:
        pass

    def after_product_update(self, product: ProductContext, changed_fields: frozenset[str]) -> None:
        pass

    def after_process_start(self, process: ProcessContext) -> None:
        pass

    def before_process_end(
        self, event: ProcessEvent, process: ProcessContext, services: HookServices,
    ) -> ProcessEndPreparation:
        return ProcessEndPreparation()

    def after_process_end(self, process: ProcessContext, end_ts: int, reason: str) -> None:
        pass

    def after_segment_start(self, process: ProcessContext, segment_no: int, timestamp_ms: int) -> None:
        pass

    def after_segment_end(self, process: ProcessContext, segment_no: int, timestamp_ms: int) -> None:
        pass

    def after_processing_error(
        self, error: Exception, process: ProcessContext | None,
    ) -> None:
        pass

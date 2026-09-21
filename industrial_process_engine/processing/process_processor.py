from __future__ import annotations

import logging
import math
import time
from dataclasses import asdict, replace
from typing import Any, Callable, Mapping

from industrial_process_engine.aggregation.aggregator import Aggregator, NullProductAggregator
from industrial_process_engine.aggregation.process_time import ProcessTimeAggregator
from industrial_process_engine.aggregation.spatial import SpatialDistanceAggregator
from industrial_process_engine.config import AppConfig
from industrial_process_engine.domain import (
    CommandType, ControlCommand, EventType, LifecycleSnapshot, ProcessContext, ProcessProduct,
    ProductContext, ProductFieldValue, ProductState, ProcessEvent, ServiceState, SignalBatch, SignalUpdate,
    SignalValue, TimeTick, TrackingStatus, WindowQuality, WindowRecord,
)
from industrial_process_engine.storage.sqlite import SQLiteStore
from industrial_process_engine.input.opcua_client import OpcUaClient
from industrial_process_engine.hooks import HookServices, ProcessHooks, ProcessStartPreparation
from .derived_signals import DerivedSignal, DerivedSignalEngine
from .lifecycle_rules import LifecycleRuleEvaluator
from .position_tracker import PositionTracker
from .global_transport import GlobalTransportTracker, TransportMovement
from .product_fields import ProductField, ProductFieldEngine, ProductSummaryContext
from .consumption import ConsumptionAllocator, ConsumptionMetric
from .run_id import generate_run_id
from .models import create_model_controller
from .tracking_coordinator import TrackingCoordinator
from .lifecycle_coordinator import LifecycleCoordinator
from .state_manager import ProcessorStateManager
from .signal_coordinator import SignalAggregationCoordinator

log = logging.getLogger(__name__)


class ProcessProcessor(
    TrackingCoordinator,
    LifecycleCoordinator,
    ProcessorStateManager,
    SignalAggregationCoordinator,
):
    """Single-writer sequential product processor."""

    CHECKPOINT_INTERVAL_MS = 5_000
    MAX_GAP_WINDOWS_PER_UPDATE = 10_000

    def __init__(
        self, config: AppConfig, store: SQLiteStore,
        derived_signals: Mapping[str, DerivedSignal] | None = None,
        product_fields: Mapping[str, ProductField] | None = None,
        run_id_factory: Callable[[int], str] = generate_run_id,
        opcua_client: OpcUaClient | None = None,
        hooks: ProcessHooks | None = None,
        consumption_metrics: Mapping[str, ConsumptionMetric] | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.model_controller = create_model_controller(config.process)
        self.service_state = ServiceState.STARTING
        self.signal_state: dict[str, SignalValue] = {}
        self.current_product: ProductContext | None = None
        self.current_products: tuple[ProductContext, ...] = ()
        self.position = (
            PositionTracker(config.position, config.speed_unit)
            if config.position is not None else None
        )
        self.derived_signals = DerivedSignalEngine(derived_signals, config.signal_names)
        derived_variables = self._spatial_derived_variables()
        aggregation_variables = [*config.aggregation_variables, *derived_variables]
        self._validate_output_variables(
            "product_data", aggregation_variables, config.aggregation.enabled,
        )
        self.aggregator = (
            Aggregator(
                config.process_id, aggregation_variables, config.aggregation.mode,
                config.aggregation.interval, config.aggregation.stale_after_ms,
            ) if config.aggregation.enabled else NullProductAggregator()
        )
        time_variables = [*config.process_time_variables, *self.derived_signals.process_time_variables]
        time_stream = config.streams.process_data_time
        self._validate_output_variables(
            "process_data_time", time_variables, time_stream.enabled,
        )
        self.process_time_aggregator = (
            ProcessTimeAggregator(
                config.process_id, time_variables, time_stream.interval_s or 1.0,
                time_stream.stale_after_ms, time_stream.when,
            ) if time_stream.enabled else None
        )
        self.spatial_enabled = bool(
            config.aggregation.enabled and config.aggregation.mode == "distance" and config.spatial is not None
        )
        self.transport = (
            GlobalTransportTracker(config.position, config.speed_unit)
            if self.spatial_enabled and config.position is not None else None
        )
        self.spatial_aggregator = (
            SpatialDistanceAggregator(
                config.process_id, aggregation_variables, config.aggregation.interval,
                config.aggregation.stale_after_ms,
                config.spatial.line_length_m,
            ) if self.spatial_enabled else None
        )
        self.spatial_contexts: dict[str, tuple[ProductContext, ...]] = {}
        self.spatial_drain_last_ts: int | None = None
        self.product_fields = ProductFieldEngine(
            product_fields, config.signal_names | set(self.derived_signals.derived_signals),
        )
        self.consumption = ConsumptionAllocator(config.process_id, consumption_metrics)
        self.participant_ids: set[str] = set()
        self.last_lifecycle_timestamp_ms: int | None = None
        self.run_id_factory = run_id_factory
        self.opcua_client = opcua_client
        self.hooks = hooks or ProcessHooks()
        self.hook_services = HookServices(opcua=opcua_client)
        self.rules = LifecycleRuleEvaluator(config.lifecycle.rules)
        self.durable_signal_names = set(self.product_fields.durable_signal_names)
        self.durable_signal_names.update(
            metric.source_signal for metric in self.consumption.metrics.values()
        )
        if config.lifecycle.product_id is not None:
            self.durable_signal_names.add(config.lifecycle.product_id.signal)
        if config.position is not None and config.position.segment_signal is not None:
            self.durable_signal_names.add(config.position.segment_signal)
        self.last_checkpoint_ms = 0
        self.restart_requested = False
        self.awaiting_reconciliation = False
        self.pending_product_id: str | None = None
        self.pending_product_timestamp_ms: int | None = None
        self.provisional_run_id: str | None = None
        self.provisional_product_id: str | None = None
        self.late_binding_timeout_reported = False
        self.position_reset_timestamp_ms: int | None = None
        self.pending_segment_signal_timestamp_ms: int | None = None
        self.pending_segment_signal_value: Any | None = None
        self.segment_signal_initialized = False
        self.last_segment_signal_value: Any | None = None
        self.pending_segment_reset_timestamp_ms: int | None = None
        self.pending_segment_reset_value: float | None = None
        self.segment_rebase_pending = False
        self.product_rebase_pending = False
        self.last_direct_reset_timestamp_ms: int | None = None
        self.used_segment_numbers: set[int] = set()

    @staticmethod
    def _validate_output_variables(name: str, variables: list[Any], enabled: bool) -> None:
        output_names = [variable.name for variable in variables]
        duplicates = sorted({item for item in output_names if output_names.count(item) > 1})
        if duplicates:
            raise ValueError(f"{name} output names must be unique: {duplicates}")
        if enabled and not output_names:
            raise ValueError(f"enabled {name} requires at least one mapped or derived output")

    def _spatial_derived_variables(self) -> list[Any]:
        variables = self.derived_signals.aggregation_variables
        station_names = set(self.config.spatial.stations) if self.config.spatial else set()
        origin = self.config.spatial.origin if self.config.spatial else None
        for signal in self.derived_signals.derived_signals.values():
            if self.config.aggregation.mode != "distance" and (
                signal.station is not None or signal.spatial_offset_m is not None
            ):
                raise ValueError("derived signal spatial location is only valid for distance aggregation")
            if self.config.spatial is None and (
                signal.station is not None or signal.spatial_offset_m is not None
            ):
                raise ValueError(
                    f"derived signal {signal.name} spatial location requires spatial configuration"
                )
            if signal.station is not None and signal.station != origin and signal.station not in station_names:
                raise ValueError(f"unknown station for derived signal {signal.name}: {signal.station}")
            offset = signal.spatial_offset_m
            if offset is None and signal.station is not None and self.config.spatial is not None:
                offset = 0.0 if signal.station == origin else self.config.spatial.stations[signal.station].offset_m
            for variable in variables:
                if variable.source_name == signal.name:
                    variable.spatial_offset_m = offset or 0.0
        return variables

    def start(self) -> None:
        durable_signals = {
            name: SignalValue(**value)
            for name, value in self.store.load_signal_state(self.config.process_id).items()
        }
        checkpoint = self.store.load_checkpoint(self.config.process_id)
        if checkpoint:
            self._restore(checkpoint)
            for name, value in durable_signals.items():
                current = self.signal_state.get(name)
                if current is None or value.timestamp_ms >= current.timestamp_ms:
                    self.signal_state[name] = value
            if self.current_product:
                changed = self.product_fields.project_inputs(
                    self.current_products, self.signal_state, set(self.signal_state), self._now(),
                )
                self._evaluate_product_fields(
                    self._now(), changed_fields=changed,
                    changed_signals=set(self.signal_state), force=True,
                )
                self.awaiting_reconciliation = True
                self.store.log_event(
                    self._now(), self.config.process_id, "RECOVERY", "Restored active product checkpoint",
                    self.current_product.product_id, self.current_product.run_id,
                )
            else:
                self.store.log_event(
                    self._now(), self.config.process_id, "RECOVERY",
                    "Restored pending product lifecycle state", self.pending_product_id,
                )
        else:
            self.signal_state.update(durable_signals)
        if self.process_time_aggregator is not None:
            now = self._now()
            saved_time = self.store.load_process_time_checkpoint(self.config.process_id)
            if saved_time:
                self.process_time_aggregator.restore(saved_time, now)
            else:
                self.process_time_aggregator.start(now)
        self.service_state = ServiceState.RUNNING
        self.store.log_event(self._now(), self.config.process_id, "SERVICE_START", "Service processing started")

    def handle(
        self,
        item: LifecycleSnapshot | SignalBatch | SignalUpdate | ProcessEvent | ControlCommand | TimeTick,
    ) -> None:
        if isinstance(item, ControlCommand):
            self._control(item)
            return
        if self.service_state != ServiceState.RUNNING:
            return
        if isinstance(item, LifecycleSnapshot):
            self._event(item.event, item.signals)
        elif isinstance(item, SignalBatch):
            self._signal_batch(item)
        elif isinstance(item, SignalUpdate):
            self._signal_batch(SignalBatch((item,), item.timestamp_ms, item.topic))
        elif isinstance(item, TimeTick):
            self._advance_process_time(item.timestamp_ms)
            consumption_boundary = self.consumption.next_boundary_ms()
            if consumption_boundary is not None and consumption_boundary <= item.timestamp_ms:
                self._advance_consumption(item.timestamp_ms, "TICK")
            self._advance_spatial_drains(item.timestamp_ms)
            self._advance_spatial_to(item.timestamp_ms)
            self._advance_time(item.timestamp_ms)
            self._check_late_binding_timeout(item.timestamp_ms)
        else:
            self._event(item)

    def next_time_boundary_ms(self) -> int | None:
        if self.service_state != ServiceState.RUNNING or self.awaiting_reconciliation:
            return None
        boundaries = [
            value for value in (
                self.aggregator.next_boundary_ms(), self.product_fields.next_due_ms(),
                self.consumption.next_boundary_ms(),
                self.process_time_aggregator.next_boundary_ms() if self.process_time_aggregator else None,
            ) if value is not None
        ]
        late_binding = self._late_binding_config()
        if (
            late_binding is not None and late_binding.timeout_s is not None
            and self.provisional_run_id is not None and self.current_product is not None
            and not self.late_binding_timeout_reported
        ):
            boundaries.append(
                self.current_product.start_ts + int(late_binding.timeout_s * 1000)
            )
        if (
            self.transport is not None and self.spatial_aggregator is not None
            and self.spatial_aggregator.products and self.transport.speed_valid
            and self.config.position is not None
            and (self.config.position.source == "speed" or self.config.position.fallback_to_speed)
            and self.transport.last_ts is not None
        ):
            boundaries.append(self.transport.last_ts + 1_000)
        return min(boundaries, default=None)

    def _event(self, event: ProcessEvent, snapshot: SignalBatch | None = None) -> None:
        if event.event_type in {
            EventType.PRODUCT_ENTER, EventType.PRODUCT_UPDATE,
            EventType.PRODUCT_EXIT, EventType.PRODUCT_ABORT,
        }:
            self._product_event(event)
            return
        if self.model_controller.handle_run_event(self, event):
            return
        if event.source == "explicit" and self.config.lifecycle.source == "derived":
            return
        if event.source == "derived" and self.config.lifecycle.source == "explicit":
            return
        if (
            self.transport is not None and self.transport.last_ts is not None
            and event.timestamp_ms < self.transport.last_ts
        ):
            self.store.log_event(
                event.timestamp_ms, self.config.process_id, f"{event.event_type}_REJECTED",
                "Lifecycle event predates the global transport cursor", event.product_id,
            )
            return
        self._advance_consumption(event.timestamp_ms, str(event.event_type))
        self._advance_spatial_drains(event.timestamp_ms)
        self._advance_spatial_to(event.timestamp_ms)
        if self.config.aggregation.mode == "time" and not self.awaiting_reconciliation:
            self._advance_time(event.timestamp_ms)
        if self.awaiting_reconciliation and self.current_product:
            if event.event_type == EventType.PROCESS_END:
                product_id = self.current_product.product_id
                run_id = self.current_product.run_id
                reason = "Live process reported no active product after restart"
                self.store.fail_product(self.config.process_id, run_id, ProductState.ERROR, event.timestamp_ms, reason)
                self.store.log_event(
                    event.timestamp_ms, self.config.process_id, "PROCESS_ERROR", reason, product_id, run_id,
                )
                log.warning(
                    "Recovery ended unmatched run run_id=%s product_id=%s: %s",
                    run_id, product_id, reason,
                )
                self._clear_product()
                self.awaiting_reconciliation = False
                return
            if event.event_type == EventType.PROCESS_START:
                live_product_id = event.product_id or self._product_id()
                active_ids = {product.product_id for product in self.current_products}
                if live_product_id in active_ids:
                    self.awaiting_reconciliation = False
                    self._reconcile(event.timestamp_ms)
                    self.store.log_event(
                        event.timestamp_ms, self.config.process_id, "RECOVERY",
                        "Live product matches checkpoint", live_product_id,
                        self.current_product.run_id,
                    )
                    log.info(
                        "Recovered active run run_id=%s product_id=%s",
                        self.current_product.run_id, live_product_id,
                    )
                    return
                if live_product_id is None and self.config.lifecycle.product_id is None:
                    try:
                        preparation = self.hooks.before_process_start(
                            event, None, self.hook_services,
                        )
                    except Exception as error:
                        self.store.log_event(
                            event.timestamp_ms, self.config.process_id,
                            "PROCESS_START_HOOK_FAILED",
                            f"Process recovery snapshot failed: {error}"[:500],
                            None, self.current_product.run_id,
                        )
                        log.warning("Could not reconcile active process: %s", error)
                        return
                    prepared_ids = {product.product_id for product in preparation.products}
                    if prepared_ids and prepared_ids == active_ids:
                        if preparation.snapshot is not None:
                            self._apply_snapshot(preparation.snapshot)
                        for product in self.current_products:
                            self.product_fields.assign(
                                product, preparation.context, event.timestamp_ms,
                            )
                        self.awaiting_reconciliation = False
                        self._reconcile(event.timestamp_ms)
                        self.store.log_event(
                            event.timestamp_ms, self.config.process_id, "RECOVERY",
                            "Live process products match checkpoint", None,
                            self.current_product.run_id,
                        )
                        log.info(
                            "Recovered active run run_id=%s products=%s",
                            self.current_product.run_id, sorted(active_ids),
                        )
                        return
                    self.awaiting_reconciliation = False
                    self._start_product(event, snapshot, preparation)
                    return
                self.awaiting_reconciliation = False
                if self.config.aggregation.mode == "time":
                    self._advance_time(event.timestamp_ms, {})
        if event.event_type == EventType.PROCESS_START:
            self._start_product(event, snapshot)
        elif event.event_type == EventType.PROCESS_END:
            if snapshot is not None:
                self._apply_snapshot(snapshot)
            self._end_product(event)
        elif event.event_type == EventType.PROCESS_ABORT:
            self._abort_product(event)
        else:
            self.store.log_event(
                event.timestamp_ms, self.config.process_id, str(event.event_type),
                f"{event.source} lifecycle event",
                self.current_product.product_id if self.current_product else None,
                self.current_product.run_id if self.current_product else None,
            )

    def _control(self, command: ControlCommand) -> None:
        now = self._now()
        if command.command == CommandType.PAUSE and self.service_state == ServiceState.RUNNING:
            self._advance_process_time(now)
            self.checkpoint(now)
            self.service_state = ServiceState.PAUSED
        elif command.command == CommandType.RESUME and self.service_state == ServiceState.PAUSED:
            if self.process_time_aggregator is not None:
                saved = self.store.load_process_time_checkpoint(self.config.process_id)
                if saved:
                    self.process_time_aggregator.restore(saved, now)
                else:
                    self.process_time_aggregator.start(now)
                self.store.save_process_time_checkpoint(
                    self.config.process_id, self.process_time_aggregator.snapshot(), now,
                )
            self.service_state = ServiceState.RUNNING
        elif command.command in {CommandType.STOP, CommandType.RESTART}:
            self._advance_process_time(now)
            self._advance_consumption(now, "SHUTDOWN_CHECKPOINT")
            self.checkpoint(now)
            self.service_state = ServiceState.STOPPED
            self.restart_requested = command.command == CommandType.RESTART
        self.store.log_event(
            now, self.config.process_id, f"SERVICE_{command.command}", f"Control command {command.command}",
            self.current_product.product_id if self.current_product else None,
            self.current_product.run_id if self.current_product else None,
        )

    def _product_id(self) -> str | None:
        product_id = self.config.lifecycle.product_id
        if product_id is None:
            return None
        return self._normalized_product_id(self.signal_state.get(product_id.signal))

    @staticmethod
    def _normalized_product_id(signal: SignalValue | None) -> str | None:
        if not signal or not signal.quality:
            return None
        value = str(signal.value).strip()
        return value if value and value != "0" else None

    @staticmethod
    def _merge_snapshots(first: SignalBatch | None, second: SignalBatch) -> SignalBatch:
        if first is None:
            return second
        return SignalBatch(
            updates=(*first.updates, *second.updates),
            timestamp_ms=max(first.timestamp_ms, second.timestamp_ms),
            source="process_start",
            reason="process_start",
            errors=(*first.errors, *second.errors),
        )

    def _notify_hook(self, name: str, *args: Any) -> None:
        try:
            getattr(self.hooks, name)(*args)
        except Exception as error:
            product = self.current_product
            self.store.log_event(
                self._now(), self.config.process_id, "HOOK_ERROR",
                f"{name}: {error}"[:500],
                product.product_id if product else None,
                product.run_id if product else None,
            )
            log.exception("Application hook %s failed", name)

    def _hook_process(self) -> ProcessContext:
        if not self.current_product:
            raise RuntimeError("no active process")
        return ProcessContext(
            run_id=self.current_product.run_id,
            start_ts=self.current_product.start_ts,
            products=tuple(
                ProductContext(
                    run_id=product.run_id, product_id=product.product_id,
                    start_ts=product.start_ts,
                    context={
                        name: ProductFieldValue(value.value, value.quality, value.timestamp_ms)
                        for name, value in product.context.items()
                    },
                )
                for product in self.current_products
            ),
        )

    @staticmethod
    def _now() -> int:
        return time.time_ns() // 1_000_000

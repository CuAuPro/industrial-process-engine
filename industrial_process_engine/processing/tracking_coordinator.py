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
from industrial_process_engine.units import speed_to_m_s
from industrial_process_engine.hooks import HookServices, ProcessHooks, ProcessStartPreparation
from .derived_signals import DerivedSignal, DerivedSignalEngine
from .lifecycle_rules import LifecycleRuleEvaluator
from .position_tracker import PositionTracker
from .global_transport import GlobalTransportTracker, TransportMovement
from .product_fields import ProductField, ProductFieldEngine, ProductSummaryContext
from .consumption import ConsumptionAllocator, ConsumptionMetric
from .run_id import generate_run_id


log = logging.getLogger("industrial_process_engine.processing.process_processor")


class TrackingCoordinator:
    """Coordinates position, spatial transport, resets, segments and late binding."""

    def _advance_spatial_to(self, timestamp_ms: int) -> None:
        if not self.spatial_enabled or self.transport is None:
            return
        draining = bool(self.current_product is None and self.spatial_aggregator and any(
            product.exit_coordinate_m is not None
            for product in self.spatial_aggregator.products.values()
        ))
        movement = self.transport.advance_to(timestamp_ms, force_fallback=draining)
        if movement:
            self._apply_spatial_movement(movement)

    def _apply_spatial_movement(self, movement: TransportMovement) -> None:
        assert self.spatial_aggregator is not None
        active_run_id = self.current_product.run_id if self.current_product else None
        records, drained = self.spatial_aggregator.add_movement(
            movement, self.signal_state, active_run_id,
        )
        for record in records:
            self.store.persist_window(record)
        for run_id in drained:
            self._finish_spatial_run(run_id, movement.end_ts)
        if records or drained:
            self.checkpoint(movement.end_ts)

    def _advance_spatial_drains(
        self, timestamp_ms: int, updates: list[SignalUpdate] | None = None,
    ) -> None:
        if not self.spatial_enabled or self.spatial_aggregator is None:
            return
        previous_ts = self.spatial_drain_last_ts
        self.spatial_drain_last_ts = timestamp_ms
        if previous_ts is None or timestamp_ms <= previous_ts:
            return
        draining = any(
            product.exit_coordinate_m is not None
            for product in self.spatial_aggregator.products.values()
        )
        if (
            not draining or self.config.position is None
            or self.config.speed_unit is None
        ):
            return
        speed = self.signal_state.get(self.config.position.speed_signal)
        if (
            speed is None or not speed.quality
            or timestamp_ms - speed.timestamp_ms > self.config.position.stale_after_ms
        ):
            return
        try:
            start_speed_m_s = speed_to_m_s(
                float(speed.value), self.config.speed_unit,
            )
        except (TypeError, ValueError):
            return
        speed_update = next((
            update for update in (updates or ())
            if update.name == self.config.position.speed_signal
        ), None)
        end_speed_m_s = start_speed_m_s
        if speed_update is not None and speed_update.quality:
            try:
                end_speed_m_s = speed_to_m_s(
                    float(speed_update.value), self.config.speed_unit,
                )
            except (TypeError, ValueError):
                return
        average_speed_m_s = (start_speed_m_s + end_speed_m_s) / 2.0
        delta_m = max(0.0, average_speed_m_s) * (timestamp_ms - previous_ts) / 1000.0
        records, completed = self.spatial_aggregator.advance_draining(
            delta_m, previous_ts, timestamp_ms, self.signal_state,
        )
        for record in records:
            self.store.persist_window(record)
        for run_id in completed:
            self._finish_spatial_run(run_id, timestamp_ms)

    def _uses_direct_reset(self) -> bool:
        return (
            self.position is not None
            and self.config.position is not None
            and self.config.position.source == "direct"
            and self.config.position.reset_ratio is not None
        )

    def _uses_product_position_reset_start(self) -> bool:
        return self._uses_direct_reset() and self.config.position.reset_scope == "product"

    def _late_binding_config(self) -> Any | None:
        product_id = self.config.lifecycle.product_id
        if product_id is None or not product_id.late_binding.enabled:
            return None
        return product_id.late_binding

    def _uses_late_binding(self) -> bool:
        return self._late_binding_config() is not None

    def _is_current_provisional(self) -> bool:
        return bool(
            self.current_product is not None
            and self.provisional_run_id == self.current_product.run_id
            and self.provisional_product_id == self.current_product.product_id
        )

    def _uses_segment_reset(self) -> bool:
        return bool(
            self.position is not None
            and self.config.position is not None
            and self.config.position.reset_scope == "segment"
            and self.config.position.segment_signal
        )

    def _reject_oversized_forward_jump(
        self, accepted: list[SignalUpdate], prior_values: dict[str, SignalValue | None],
    ) -> list[SignalUpdate]:
        if (
            self.position is None
            or self.config.position is None
            or self.config.position.source != "direct"
            or self.current_product is None
            or self.awaiting_reconciliation
            or not self.aggregator.segment_active
            or self.segment_rebase_pending
            or self.product_rebase_pending
        ):
            return accepted
        filtered: list[SignalUpdate] = []
        for update in accepted:
            prior = prior_values[update.name]
            if (
                update.name != self.config.position.signal
                or not update.quality
                or prior is None
                or not prior.quality
            ):
                filtered.append(update)
                continue
            delta = float(update.value) - float(prior.value)
            gap_windows = math.ceil(max(0.0, delta) / self.config.aggregation.interval)
            if (
                delta <= self.config.position.max_forward_jump_m
                or gap_windows <= self.MAX_GAP_WINDOWS_PER_UPDATE
            ):
                filtered.append(update)
                continue
            self.position.invalidate(update.name)
            message = (
                f"Rejected direct position jump of {delta:.3f} m; "
                f"it would create {gap_windows} empty windows"
            )
            self.store.log_event(
                update.timestamp_ms, self.config.process_id, "POSITION_JUMP_REJECTED", message,
                self.current_product.product_id, self.current_product.run_id,
            )
            log.error(
                "%s run_id=%s product_id=%s",
                message, self.current_product.run_id, self.current_product.product_id,
            )
        return filtered

    def _direct_reset_update(
        self, accepted: list[SignalUpdate], prior_values: dict[str, SignalValue | None],
    ) -> tuple[SignalUpdate, bool] | None:
        if not self._uses_direct_reset() or self.position is None or self.config.position is None:
            return None
        update = next(
            (
                item for item in accepted
                if item.name == self.config.position.signal and item.quality
            ),
            None,
        )
        if update is None:
            return None
        previous = prior_values[update.name]
        if previous is None:
            # The first trustworthy direct reading establishes the position
            # baseline. This also permits a clean service start in either MQTT
            # retained-message order (ID first or position first).
            return (update, True) if self.current_product is None else None
        if not previous.quality:
            return None
        if self.position.is_direct_reset(previous.value, update.value):
            return update, False
        return None

    def _register_segment_reset(self, update: SignalUpdate, initial_baseline: bool) -> None:
        if self.position is None or self.config.position is None:
            return
        if initial_baseline:
            return
        self.last_direct_reset_timestamp_ms = update.timestamp_ms
        if self.product_rebase_pending:
            self.position.start_segment(update.timestamp_ms, float(update.value))
            self.product_rebase_pending = False
            self.checkpoint(update.timestamp_ms)
            return
        if self.segment_rebase_pending:
            self.position.start_segment(update.timestamp_ms, float(update.value))
            self.segment_rebase_pending = False
            self.checkpoint(update.timestamp_ms)
            return
        if not self.current_product:
            return
        self.pending_segment_reset_timestamp_ms = update.timestamp_ms
        self.pending_segment_reset_value = float(update.value)
        self._pause_current_segment(update.timestamp_ms, "position reset")
        if self.config.position.segment_start == "position_reset":
            if self._configured_segment_no() != self.aggregator.segment_no:
                self._start_next_segment(update.timestamp_ms, self.pending_segment_reset_value)
            else:
                self._log_segment_waiting(update.timestamp_ms)
        elif (
            self.config.position.segment_start in {
                "segment_change_and_position_reset", "segment_change",
            }
            and self.pending_segment_signal_timestamp_ms is not None
        ):
            self._start_next_segment(
                max(update.timestamp_ms, self.pending_segment_signal_timestamp_ms),
                self.pending_segment_reset_value,
            )
        else:
            self._log_segment_waiting(update.timestamp_ms)

    def _observe_segment_signal(self, update: SignalUpdate) -> None:
        if not self.segment_signal_initialized:
            self.segment_signal_initialized = True
            self.last_segment_signal_value = update.value
            self.checkpoint(update.timestamp_ms)
            return
        if update.value == self.last_segment_signal_value:
            return
        self.last_segment_signal_value = update.value
        self._register_segment_signal(update)

    def _register_segment_signal(self, update: SignalUpdate) -> None:
        if not self.current_product or self.product_rebase_pending:
            return
        assert self.config.position is not None
        self.pending_segment_signal_timestamp_ms = update.timestamp_ms
        self.pending_segment_signal_value = update.value
        if self.config.position.segment_start == "position_reset":
            if self.pending_segment_reset_timestamp_ms is not None:
                self._start_next_segment(
                    max(update.timestamp_ms, self.pending_segment_reset_timestamp_ms),
                    self.pending_segment_reset_value,
                )
            return
        self._pause_current_segment(update.timestamp_ms, f"signal {update.name} changed")
        if self.config.position.segment_start == "segment_change":
            self._start_next_segment(
                update.timestamp_ms,
                self.pending_segment_reset_value,
                wait_for_rebase=(
                    self.config.position.source == "direct"
                    and self.pending_segment_reset_timestamp_ms is None
                ),
            )
        elif (
            self.config.position.segment_start == "segment_change_and_position_reset"
            and self.pending_segment_reset_timestamp_ms is not None
        ):
            self._start_next_segment(
                max(update.timestamp_ms, self.pending_segment_reset_timestamp_ms),
                self.pending_segment_reset_value,
            )
        else:
            self._log_segment_waiting(update.timestamp_ms)

    def _pause_current_segment(self, timestamp_ms: int, reason: str) -> None:
        if not self.current_product or not self.aggregator.segment_active:
            return
        partial = self.aggregator.pause_segment(timestamp_ms)
        if partial is not None:
            self.store.persist_window(partial)
        if self.position is not None:
            self.position.pause_segment()
        self.store.log_event(
            timestamp_ms, self.config.process_id, "SEGMENT_END",
            f"Segment {self.aggregator.segment_no} ended: {reason}",
            self.current_product.product_id, self.current_product.run_id,
        )
        log.info(
            "Ended segment segment_no=%s run_id=%s product_id=%s reason=%s",
            self.aggregator.segment_no, self.current_product.run_id,
            self.current_product.product_id, reason,
        )
        self._notify_hook(
            "after_segment_end", self._hook_process(),
            self.aggregator.segment_no, timestamp_ms,
        )

    def _start_next_segment(
        self, timestamp_ms: int, raw_position: float | None,
        *, wait_for_rebase: bool = False,
    ) -> None:
        if not self.current_product:
            return
        segment_no = self._configured_segment_no()
        if segment_no is None or segment_no == self.aggregator.segment_no:
            self._log_segment_waiting(timestamp_ms)
            return
        if segment_no in self.used_segment_numbers:
            assert self.current_product is not None
            message = f"Segment number {segment_no} was already used in this run"
            self.store.log_event(
                timestamp_ms, self.config.process_id, "SEGMENT_START_REJECTED", message,
                self.current_product.product_id, self.current_product.run_id,
            )
            log.warning(
                "%s run_id=%s product_id=%s", message,
                self.current_product.run_id, self.current_product.product_id,
            )
            self.checkpoint(timestamp_ms)
            return
        self.aggregator.start_segment(segment_no, timestamp_ms)
        self.used_segment_numbers.add(segment_no)
        if self.position is not None:
            if wait_for_rebase:
                self.position.pause_segment()
            else:
                self.position.start_segment(timestamp_ms, raw_position)
        self.segment_rebase_pending = wait_for_rebase
        self.pending_segment_signal_timestamp_ms = None
        self.pending_segment_signal_value = None
        self.pending_segment_reset_timestamp_ms = None
        self.pending_segment_reset_value = None
        self.store.log_event(
            timestamp_ms, self.config.process_id, "SEGMENT_START",
            f"Segment {segment_no} started",
            self.current_product.product_id, self.current_product.run_id,
        )
        log.info(
            "Started segment segment_no=%s run_id=%s product_id=%s",
            segment_no, self.current_product.run_id, self.current_product.product_id,
        )
        self.checkpoint(timestamp_ms)
        self._notify_hook(
            "after_segment_start", self._hook_process(), segment_no, timestamp_ms,
        )

    def _configured_segment_no(self) -> int | None:
        if not self._uses_segment_reset():
            return 1
        value = self.pending_segment_signal_value
        if value is None:
            assert self.config.position is not None
            signal = self.signal_state.get(self.config.position.segment_signal or "")
            value = signal.value if signal and signal.quality else self.last_segment_signal_value
        try:
            segment_no = int(value)
        except (TypeError, ValueError):
            return None
        return segment_no if segment_no > 0 else None

    def _log_segment_waiting(self, timestamp_ms: int) -> None:
        if not self.current_product:
            return
        waiting_for = "position reset" if self.pending_segment_reset_timestamp_ms is None else "segment signal"
        self.store.log_event(
            timestamp_ms, self.config.process_id, "SEGMENT_WAITING",
            f"Segment transition waiting for {waiting_for}",
            self.current_product.product_id, self.current_product.run_id,
        )
        self.checkpoint(timestamp_ms)

    def _register_position_reset(self, timestamp_ms: int, initial_baseline: bool) -> None:
        if self.position_reset_timestamp_ms is not None and self.current_product is None:
            return
        self.position_reset_timestamp_ms = timestamp_ms
        if initial_baseline:
            self.checkpoint(timestamp_ms)
            return
        product_id = self.current_product.product_id if self.current_product else None
        run_id = self.current_product.run_id if self.current_product else None
        if self.current_product:
            self._complete_current(timestamp_ms, "POSITION_RESET")
        self.store.log_event(
            timestamp_ms, self.config.process_id, "POSITION_RESET",
            "Direct position counter reset detected", product_id, run_id,
        )
        log.info(
            "Detected direct position reset%s",
            f"; completed run_id={run_id} product_id={product_id}" if run_id else "",
        )
        late_binding = self._late_binding_config()
        if late_binding is not None and late_binding.start_on_position_reset:
            if self.pending_product_id and self.pending_product_timestamp_ms is not None:
                self._start_pending_after_reset()
            elif self.current_product is None:
                self._start_product(ProcessEvent(
                    EventType.PROCESS_START, timestamp_ms, source="position_reset",
                ))
        self.checkpoint(timestamp_ms)

    def _start_pending_after_reset(self) -> None:
        if (
            self.current_product is not None
            or not self.pending_product_id
            or self.pending_product_timestamp_ms is None
            or self.position_reset_timestamp_ms is None
        ):
            return
        product_id = self.pending_product_id
        start_timestamp_ms = max(
            self.pending_product_timestamp_ms, self.position_reset_timestamp_ms,
        )
        self._start_product(ProcessEvent(
            EventType.PROCESS_START, start_timestamp_ms, product_id, source="position_reset",
        ))

    def _bind_current_product(self, product_id: str, timestamp_ms: int) -> None:
        if not self._is_current_provisional() or self.current_product is None:
            return
        run_id = self.current_product.run_id
        old_product_id = self.current_product.product_id
        self.store.bind_product_id(
            self.config.process_id, run_id, old_product_id, product_id,
        )
        self.product_fields.bind_product_id(run_id, old_product_id, product_id)
        self.consumption.bind_product_id(old_product_id, product_id)
        for product in self.current_products:
            if product.product_id == old_product_id:
                product.product_id = product_id
        self.current_product = self.current_products[0]
        self.participant_ids.discard(old_product_id)
        self.participant_ids.add(product_id)
        if self.aggregator.run_id == run_id:
            self.aggregator.product_id = product_id
        if self.spatial_aggregator is not None and run_id in self.spatial_aggregator.products:
            self.spatial_aggregator.bind_product_id(run_id, product_id)
        self.provisional_run_id = None
        self.provisional_product_id = None
        self.late_binding_timeout_reported = False
        self.awaiting_reconciliation = False
        self.pending_product_id = None
        self.pending_product_timestamp_ms = None
        self.store.log_event(
            timestamp_ms, self.config.process_id, "PRODUCT_ID_BOUND",
            f"Bound provisional product {old_product_id} to {product_id}", product_id, run_id,
        )
        self.checkpoint(timestamp_ms)
        log.info(
            "Bound provisional product run_id=%s old_product_id=%s product_id=%s",
            run_id, old_product_id, product_id,
        )

    def _bind_unresolved_run(self, product_id: str, timestamp_ms: int) -> bool:
        if not self._uses_late_binding():
            return False
        unresolved = self.store.unresolved_runs(self.config.process_id)
        if not unresolved:
            return False
        if len(unresolved) != 1:
            self.store.log_event(
                timestamp_ms, self.config.process_id, "PRODUCT_ID_BIND_REJECTED",
                f"Cannot choose among {len(unresolved)} unresolved runs", product_id,
            )
            return True
        run_id = unresolved[0]["run_id"]
        products = self.store.products_for_run(self.config.process_id, run_id)
        if len(products) != 1:
            self.store.log_event(
                timestamp_ms, self.config.process_id, "PRODUCT_ID_BIND_REJECTED",
                "Late binding requires exactly one provisional product", product_id, run_id,
            )
            return True
        old_product_id = products[0]["product_id"]
        self.store.bind_product_id(
            self.config.process_id, run_id, old_product_id, product_id,
        )
        self.store.log_event(
            timestamp_ms, self.config.process_id, "PRODUCT_ID_BOUND",
            f"Bound completed provisional product {old_product_id} to {product_id}",
            product_id, run_id,
        )
        log.info(
            "Bound completed provisional product run_id=%s old_product_id=%s product_id=%s",
            run_id, old_product_id, product_id,
        )
        return True

    def _check_late_binding_timeout(self, timestamp_ms: int) -> None:
        late_binding = self._late_binding_config()
        if (
            late_binding is None or late_binding.timeout_s is None
            or self.current_product is None or not self._is_current_provisional()
            or self.late_binding_timeout_reported
            or timestamp_ms < self.current_product.start_ts + int(late_binding.timeout_s * 1000)
        ):
            return
        self.late_binding_timeout_reported = True
        self.store.log_event(
            timestamp_ms, self.config.process_id, "PRODUCT_ID_BIND_TIMEOUT",
            "Provisional run is still waiting for a product ID",
            self.current_product.product_id, self.current_product.run_id,
        )
        self.checkpoint(timestamp_ms)

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


log = logging.getLogger("industrial_process_engine.processing.process_processor")


class SignalAggregationCoordinator:
    """Coordinates input batches, derived values and both aggregation streams."""

    def _signal_batch(self, batch: SignalBatch) -> None:
        timestamps = sorted({update.timestamp_ms for update in batch.updates})
        if len(timestamps) > 1:
            for timestamp in timestamps:
                updates = tuple(update for update in batch.updates if update.timestamp_ms == timestamp)
                self._signal_batch(SignalBatch(updates, timestamp, batch.source, batch.reason, batch.errors))
            return
        run_id_before_batch = self.current_product.run_id if self.current_product else None
        accepted: list[SignalUpdate] = []
        prior_values: dict[str, SignalValue | None] = {}
        for update in batch.updates:
            prior = self.signal_state.get(update.name)
            if prior and update.timestamp_ms < prior.timestamp_ms:
                log.warning("Ignoring out-of-order signal %s at %s", update.name, update.timestamp_ms)
                continue
            accepted.append(update)
            prior_values[update.name] = prior
        if not accepted:
            return
        self._advance_process_time(batch.timestamp_ms)
        accepted = self._reject_oversized_forward_jump(accepted, prior_values)
        if not accepted:
            return
        self._advance_consumption(batch.timestamp_ms, "SOURCE_UPDATE")
        self._advance_spatial_drains(batch.timestamp_ms, accepted)
        reset_update: SignalUpdate | None = None
        reset_candidate = self._direct_reset_update(accepted, prior_values)
        if reset_candidate is not None and not self.awaiting_reconciliation:
            reset_update, initial_baseline = reset_candidate
        if self.spatial_enabled:
            assert self.transport is not None
            movement = self.transport.process_group(
                batch.timestamp_ms, accepted, self.signal_state,
                force_fallback=bool(self.current_product is None and self.spatial_aggregator and any(
                    product.exit_coordinate_m is not None
                    for product in self.spatial_aggregator.products.values()
                )),
            )
            if movement:
                self._apply_spatial_movement(movement)
            if reset_candidate is not None and not self.awaiting_reconciliation:
                if self._uses_segment_reset():
                    self._register_segment_reset(reset_update, initial_baseline)
                else:
                    self._register_position_reset(reset_update.timestamp_ms, initial_baseline)
        elif self.config.aggregation.mode == "time" and not self.awaiting_reconciliation:
            self._advance_time(batch.timestamp_ms)
        elif self.position is not None:
            if reset_candidate is not None and not self.awaiting_reconciliation:
                if self._uses_segment_reset():
                    self._register_segment_reset(reset_update, initial_baseline)
                else:
                    self._register_position_reset(reset_update.timestamp_ms, initial_baseline)
            for update in accepted:
                if not update.quality:
                    self.position.invalidate(update.name)
                if update.quality and not self.awaiting_reconciliation:
                    if reset_update is update and self._is_current_provisional():
                        self.position.start_segment(update.timestamp_ms, float(update.value))
                        continue
                    if (
                        self.config.position is not None
                        and update.name == self.config.position.signal
                        and (self.segment_rebase_pending or self.product_rebase_pending)
                    ):
                        if reset_update is update:
                            self.position.start_segment(update.timestamp_ms, float(update.value))
                            self.segment_rebase_pending = False
                            self.product_rebase_pending = False
                            self.checkpoint(update.timestamp_ms)
                        continue
                    movement = self.position.observe(update.name, update.value, update.timestamp_ms)
                    if movement and self.current_product:
                        records = self.aggregator.add_movement(
                            movement.start_m, movement.end_m, movement.start_ts, movement.end_ts,
                            {} if movement.signal_gap else self.signal_state, movement.status,
                        )
                        self._persist_windows(records)
                        if records:
                            self.checkpoint(update.timestamp_ms)
                        if movement.status == TrackingStatus.LOST:
                            self.store.log_event(
                                update.timestamp_ms, self.config.process_id, "TRACKING_LOST",
                                "Material position became unreliable", self.current_product.product_id,
                                self.current_product.run_id,
                            )

        self._apply_signal_values(accepted)
        derived = self.derived_signals.evaluate(self.signal_state, batch.timestamp_ms)
        self._apply_signal_values(derived)
        self._advance_consumption(batch.timestamp_ms, "SOURCE_UPDATE")
        changed_signals = {update.name for update in (*accepted, *derived)}
        changed_fields = self.product_fields.project_inputs(
            self.current_products, self.signal_state, changed_signals, batch.timestamp_ms,
        )
        self._evaluate_product_fields(
            batch.timestamp_ms, changed_fields=changed_fields,
            changed_signals=changed_signals,
        )
        for error in batch.errors:
            self.store.log_event(
                batch.timestamp_ms, self.config.process_id, "INPUT_BATCH_WARNING", error[:500],
                self.current_product.product_id if self.current_product else None,
                self.current_product.run_id if self.current_product else None,
            )

        mapped_events: list[ProcessEvent] = []
        for update in accepted:
            mapping = self.config.mapping_by_name.get(update.name)
            if not mapping or not update.quality or not (mapping.true_event or mapping.false_event):
                continue
            prior = prior_values[update.name]
            level = bool(update.value)
            changed = prior is None or not prior.quality or bool(prior.value) != level
            if changed or self.awaiting_reconciliation:
                event_type = mapping.true_event if level else mapping.false_event
                if event_type:
                    mapped_events.append(ProcessEvent(event_type, update.timestamp_ms, source="explicit"))
        product_config = self.config.lifecycle.product_id
        product_update = next(
            (
                update for update in accepted
                if product_config is not None and update.name == product_config.signal
            ),
            None,
        )
        product_id = self.config.lifecycle.product_id
        if product_update is not None and product_id is not None:
            previous_id = self._normalized_product_id(prior_values[product_update.name])
            current_id = self._normalized_product_id(self.signal_state[product_update.name])
            changed = current_id and current_id != previous_id
            if changed and current_id and self._is_current_provisional():
                self._bind_current_product(current_id, product_update.timestamp_ms)
            elif previous_id and not current_id and self._is_current_provisional():
                self.store.log_event(
                    product_update.timestamp_ms, self.config.process_id,
                    "PRODUCT_ID_CLEAR_IGNORED",
                    "Ignored stale product-ID clear while waiting for late binding",
                    self.current_product.product_id, self.current_product.run_id,
                )
                self.checkpoint(product_update.timestamp_ms)
            elif changed and current_id and self.current_product is None and self._bind_unresolved_run(
                current_id, product_update.timestamp_ms,
            ):
                pass
            elif changed and current_id and self._uses_late_binding() and self.current_product is None:
                self.pending_product_id = current_id
                self.pending_product_timestamp_ms = product_update.timestamp_ms
                if product_id.on_change is not None and not self._uses_product_position_reset_start():
                    event_product_id = (
                        previous_id if product_id.on_change == EventType.PROCESS_END else current_id
                    )
                    self._event(ProcessEvent(
                        product_id.on_change, product_update.timestamp_ms,
                        event_product_id, source="explicit",
                    ))
                if self._uses_product_position_reset_start():
                    self._start_pending_after_reset()
                self.checkpoint(product_update.timestamp_ms)
            elif (
                self.awaiting_reconciliation and self.current_product and current_id
                and current_id == self.current_product.product_id
            ):
                self._event(ProcessEvent(
                    EventType.PROCESS_START, product_update.timestamp_ms, current_id, source="explicit",
                ))
            elif changed and self._uses_product_position_reset_start():
                self.pending_product_id = current_id
                self.pending_product_timestamp_ms = product_update.timestamp_ms
                if self.current_product:
                    self._event(ProcessEvent(
                        EventType.PROCESS_END, product_update.timestamp_ms,
                        previous_id or self.current_product.product_id, source="explicit",
                    ))
                self._start_pending_after_reset()
                self.checkpoint(product_update.timestamp_ms)
            elif previous_id and not current_id and self._uses_product_position_reset_start():
                self.pending_product_id = None
                self.pending_product_timestamp_ms = None
                if self.current_product and product_id.on_clear is not None:
                    self._event(ProcessEvent(
                        product_id.on_clear, product_update.timestamp_ms, previous_id, source="explicit",
                    ))
                self.checkpoint(product_update.timestamp_ms)
            else:
                reconcile_start = (
                    current_id and self.awaiting_reconciliation
                    and product_id.on_change == EventType.PROCESS_START
                )
                if (changed or reconcile_start) and product_id.on_change is not None:
                    event_product_id = previous_id if product_id.on_change == EventType.PROCESS_END else current_id
                    self._event(ProcessEvent(
                        product_id.on_change, product_update.timestamp_ms, event_product_id, source="explicit",
                    ))
                elif previous_id and not current_id and product_id.on_clear is not None:
                    self._event(ProcessEvent(
                        product_id.on_clear, product_update.timestamp_ms, previous_id, source="explicit",
                    ))
        if reset_update is not None and self._uses_product_position_reset_start():
            self._start_pending_after_reset()
            self.checkpoint(reset_update.timestamp_ms)
        # ID transitions run first, allowing PROCESS_END-on-change followed by a
        # start edge in the same atomic input batch.
        for event in mapped_events:
            self._event(event)
        if self._uses_segment_reset() and not self.awaiting_reconciliation:
            segment_update = next(
                (
                    update for update in accepted
                    if update.name == self.config.position.segment_signal and update.quality
                ),
                None,
            )
            if (
                segment_update is not None
                and self.current_product is not None
                and self.current_product.run_id == run_id_before_batch
            ):
                self._observe_segment_signal(segment_update)
        if self.config.lifecycle.source in {"derived", "both"}:
            for event in self.rules.update(self.signal_state, batch.timestamp_ms):
                self._event(event)
        if self.current_product and batch.timestamp_ms - self.last_checkpoint_ms >= self.CHECKPOINT_INTERVAL_MS:
            self.checkpoint(batch.timestamp_ms)

    def _apply_signal_values(self, updates: list[SignalUpdate] | tuple[SignalUpdate, ...]) -> None:
        for update in updates:
            self.signal_state[update.name] = SignalValue(update.value, update.quality, update.timestamp_ms)
            if update.name in self.durable_signal_names:
                self.store.save_signal_state(
                    self.config.process_id, update.name, update.value, update.quality, update.timestamp_ms,
                )

    def _advance_time(self, timestamp_ms: int, signals: dict[str, SignalValue] | None = None) -> None:
        if not self.current_product or self.awaiting_reconciliation:
            return
        records = []
        if self.config.aggregation.mode == "time":
            records = self.aggregator.add_time(
                timestamp_ms, self.signal_state if signals is None else signals,
            )
            self._persist_windows(records)
        self._evaluate_product_fields(timestamp_ms)
        if records:
            self.checkpoint(timestamp_ms)

    def _advance_process_time(self, timestamp_ms: int) -> None:
        if self.process_time_aggregator is None:
            return
        records = self.process_time_aggregator.advance(timestamp_ms, self.signal_state)
        for record in records:
            self.store.persist_process_time(record)
        self.store.save_process_time_checkpoint(
            self.config.process_id, self.process_time_aggregator.snapshot(), timestamp_ms,
        )

    def _persist_windows(self, records: list[WindowRecord]) -> None:
        for record in records:
            for product_record in self._windows_for_products(record):
                self.store.persist_window(product_record)

    def _windows_for_products(self, record: WindowRecord) -> tuple[WindowRecord, ...]:
        return tuple(
            replace(record, product_id=product.product_id)
            for product in self.current_products
        )

    def _evaluate_product_fields(
        self, timestamp_ms: int, *, changed_fields: set[str] | None = None,
        changed_signals: set[str] | None = None, force: bool = False,
    ) -> None:
        if not self.current_product:
            return
        position_m = self.position.position_m if self.position is not None else None
        if self.transport is not None and self.spatial_aggregator is not None:
            spatial_product = self.spatial_aggregator.products.get(self.current_product.run_id)
            if spatial_product is not None:
                position_m = max(
                    0.0, self.transport.position_m - spatial_product.entry_coordinate_m,
                )
        errors = self.product_fields.evaluate(
            self.current_products, self.signal_state, timestamp_ms,
            position_m,
            changed_fields=changed_fields, changed_signals=changed_signals, force=force,
        )
        for product_id, error in errors:
            self.store.log_event(
                timestamp_ms, self.config.process_id, "PRODUCT_FIELD_CALCULATION_ERROR",
                error[:500], product_id, self.current_product.run_id,
            )

    @staticmethod
    def _window_mapping(record: WindowRecord) -> dict[str, Any]:
        result = asdict(record)
        values = result.pop("values")
        result.update(values)
        result["quality"] = str(record.quality)
        return result

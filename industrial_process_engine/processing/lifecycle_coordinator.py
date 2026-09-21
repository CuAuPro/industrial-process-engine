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


log = logging.getLogger("industrial_process_engine.processing.process_processor")


class LifecycleCoordinator:
    """Coordinates product membership, summaries, run completion and spatial draining."""

    def _product_event(self, event: ProcessEvent) -> None:
        product_id = str(event.product_id or "").strip()
        if not product_id or product_id == "0":
            self._reject_product_event(event, "A valid product_id is required")
            return
        if self.last_lifecycle_timestamp_ms is not None and event.timestamp_ms < self.last_lifecycle_timestamp_ms:
            self._reject_product_event(event, "Out-of-order lifecycle event")
            return
        if (
            self.transport is not None and self.transport.last_ts is not None
            and event.timestamp_ms < self.transport.last_ts
        ):
            self._reject_product_event(event, "Lifecycle event predates the global transport cursor")
            return
        self._advance_consumption(event.timestamp_ms, str(event.event_type))
        self._advance_spatial_drains(event.timestamp_ms)
        self._advance_spatial_to(event.timestamp_ms)
        if self.config.aggregation.mode == "time" and self.current_product:
            self._advance_time(event.timestamp_ms)
        if (
            self._is_current_provisional()
            and event.event_type in {EventType.PRODUCT_ENTER, EventType.PRODUCT_UPDATE}
        ):
            self._bind_current_product(product_id, event.timestamp_ms)
            if event.context:
                self._update_product(event, product_id)
            return
        if event.event_type == EventType.PRODUCT_ENTER:
            self._enter_product(event, product_id)
        elif event.event_type == EventType.PRODUCT_UPDATE:
            self._update_product(event, product_id)
        else:
            self._exit_product(event, product_id, event.event_type == EventType.PRODUCT_ABORT)

    def _enter_product(self, event: ProcessEvent, product_id: str) -> None:
        owned_run_id = self.model_controller.active_run_id
        try:
            parents = self.model_controller.validate_enter(
                product_id, event.parent_product_ids, len(self.current_products),
                lambda parent: bool(
                    owned_run_id is not None
                    and self.store.product_exists(
                        self.config.process_id, owned_run_id, parent,
                    )
                ),
            )
        except ValueError as error:
            self._reject_product_event(event, str(error))
            return
        if product_id in self.participant_ids:
            self._reject_product_event(event, "Product is already active or already participated in this run")
            return
        if not self.current_product and owned_run_id is not None:
            product = ProductContext(
                owned_run_id, product_id, event.timestamp_ms,
            )
            try:
                preparation = self.hooks.before_product_enter(
                    event, product_id, self.hook_services,
                )
                values = {**event.context, **dict(preparation.context)}
                self.product_fields.validate_assignments(values)
                self.product_fields.initialize(
                    product, values, self.signal_state, event.timestamp_ms,
                )
                self.model_controller.persist_product(
                    self.store, self.config.process_id, product, parents, event.timestamp_ms,
                )
            except Exception as error:
                self._reject_product_event(event, f"Invalid product: {error}")
                return
            self.current_product = product
            self.current_products = (product,)
            self.participant_ids.add(product_id)
            self.last_lifecycle_timestamp_ms = event.timestamp_ms
            self.aggregator.start(product.run_id, product_id, event.timestamp_ms)
            self.store.log_event(
                event.timestamp_ms, self.config.process_id, "PRODUCT_ENTER",
                "Product entered processing", product_id, product.run_id,
            )
            log.info("Product entered run_id=%s product_id=%s", product.run_id, product_id)
            self._notify_hook("after_product_enter", product)
            self.checkpoint(event.timestamp_ms)
            return
        if not self.current_product:
            try:
                preparation = self.hooks.before_product_enter(event, product_id, self.hook_services)
            except Exception as error:
                self._reject_product_event(event, f"Product enter preparation failed: {error}")
                return
            self._start_product(event, preparation=preparation)
            product = next((p for p in self.current_products if p.product_id == product_id), None)
            if product is not None:
                log.info("Product entered run_id=%s product_id=%s", product.run_id, product_id)
                self._notify_hook("after_product_enter", product)
            return
        if self.spatial_enabled:
            self._reject_product_event(
                event, "A spatial origin already contains an active product; exit it before the next entry",
            )
            return
        cut = self.aggregator.cut(event.timestamp_ms)
        if cut:
            self._persist_windows([cut])
        try:
            preparation = self.hooks.before_product_enter(event, product_id, self.hook_services)
            values = {**event.context, **dict(preparation.context)}
            self.product_fields.validate_assignments(values)
            product = ProductContext(self.current_product.run_id, product_id, event.timestamp_ms)
            self.product_fields.initialize(product, values, self.signal_state, event.timestamp_ms)
            self.model_controller.persist_product(
                self.store, self.config.process_id, product, parents, event.timestamp_ms,
            )
        except Exception as error:
            self._reject_product_event(event, f"Invalid product fields: {error}")
            return
        self.current_products = (*self.current_products, product)
        self.participant_ids.add(product_id)
        self.last_lifecycle_timestamp_ms = event.timestamp_ms
        self._advance_consumption(event.timestamp_ms, "PRODUCT_ENTER")
        self.checkpoint(event.timestamp_ms)
        self.store.log_event(event.timestamp_ms, self.config.process_id, "PRODUCT_ENTER",
                             "Product entered processing", product_id, product.run_id)
        log.info("Product entered run_id=%s product_id=%s", product.run_id, product_id)
        self._notify_hook("after_product_enter", product)

    def _update_product(self, event: ProcessEvent, product_id: str) -> None:
        product = next((p for p in self.current_products if p.product_id == product_id), None)
        if product is None:
            self._reject_product_event(event, "Unknown or inactive product")
            return
        try:
            prepared = self.hooks.before_product_update(event, product, self.hook_services)
            changed = self.product_fields.assign(
                product, {**event.context, **dict(prepared)}, event.timestamp_ms,
            )
        except Exception as error:
            self._reject_product_event(event, f"Invalid product fields: {error}")
            return
        self.last_lifecycle_timestamp_ms = event.timestamp_ms
        self._evaluate_product_fields(event.timestamp_ms, changed_fields=changed)
        self._advance_consumption(event.timestamp_ms, "PRODUCT_UPDATE")
        self.checkpoint(event.timestamp_ms)
        self.store.log_event(event.timestamp_ms, self.config.process_id, "PRODUCT_UPDATE",
                             f"Updated fields: {sorted(changed)}", product_id, product.run_id)
        log.info(
            "Product updated run_id=%s product_id=%s fields=%s",
            product.run_id, product_id, sorted(changed),
        )
        self._notify_hook("after_product_update", product, frozenset(changed))

    def _exit_product(self, event: ProcessEvent, product_id: str, aborted: bool) -> None:
        product = next((p for p in self.current_products if p.product_id == product_id), None)
        if product is None:
            if aborted and self.spatial_aggregator is not None:
                draining = next((
                    item for item in self.spatial_aggregator.products.values()
                    if item.product_id == product_id and item.exit_coordinate_m is not None
                ), None)
                if draining is not None:
                    for record in self.spatial_aggregator.finalize(draining.run_id):
                        self.store.persist_window(record)
                    self.store.abort_draining_process(
                        self.config.process_id, draining.run_id, event.timestamp_ms,
                        "Lifecycle abort during downstream drain",
                    )
                    self.product_fields.clear_run(draining.run_id)
                    self.spatial_aggregator.remove(draining.run_id)
                    self.spatial_contexts.pop(draining.run_id, None)
                    self.store.log_event(
                        event.timestamp_ms, self.config.process_id, "PRODUCT_ABORT",
                        "Draining product aborted", product_id, draining.run_id,
                    )
                    self.checkpoint(event.timestamp_ms)
                    return
            self._reject_product_event(event, "Unknown or inactive product")
            return
        if self.spatial_enabled:
            if aborted:
                self._abort_product(ProcessEvent(
                    EventType.PROCESS_ABORT, event.timestamp_ms, product_id,
                    event.context, event.source,
                ))
                return
            try:
                preparation = self.hooks.before_product_exit(event, product, self.hook_services)
                self.product_fields.assign(product, preparation.context, event.timestamp_ms)
            except Exception as error:
                self.store.log_event(
                    event.timestamp_ms, self.config.process_id, "PRODUCT_EXIT_HOOK_FAILED",
                    str(error)[:500], product_id, product.run_id,
                )
            self._begin_spatial_drain(event.timestamp_ms, "PRODUCT_EXIT")
            log.info("Product exited origin run_id=%s product_id=%s", product.run_id, product_id)
            self._notify_hook("after_product_exit", product, event.timestamp_ms, str(event.event_type))
            return
        cut = self.aggregator.cut(event.timestamp_ms)
        if cut:
            self._persist_windows([cut])
        try:
            preparation = self.hooks.before_product_exit(event, product, self.hook_services)
            self.product_fields.assign(product, preparation.context, event.timestamp_ms)
        except Exception as error:
            self.store.log_event(event.timestamp_ms, self.config.process_id, "PRODUCT_EXIT_HOOK_FAILED",
                                 str(error)[:500], product_id, product.run_id)
        self._advance_consumption(event.timestamp_ms, "PRODUCT_ABORT" if aborted else "PRODUCT_EXIT")
        windows = tuple(self.store.windows_for_product(self.config.process_id, product.run_id, product_id))
        summary, errors = self.product_fields.calculate_summary(ProductSummaryContext(
            product=product, end_ts=event.timestamp_ms, windows=windows, products=self.current_products,
        ))
        state = ProductState.ABORTED if aborted else ProductState.COMPLETE
        self.store.finalize_product(self.config.process_id, product.run_id, product_id, event.timestamp_ms,
                                    summary, state, "Lifecycle abort" if aborted else None)
        for error in errors:
            self.store.log_event(event.timestamp_ms, self.config.process_id, "PRODUCT_SUMMARY_ERROR",
                                 error[:500], product_id, product.run_id)
        remaining = tuple(p for p in self.current_products if p.product_id != product_id)
        self.current_products = remaining
        self.current_product = remaining[0] if remaining else None
        self.last_lifecycle_timestamp_ms = event.timestamp_ms
        self.store.log_event(event.timestamp_ms, self.config.process_id, str(event.event_type),
                             "Product left processing", product_id, product.run_id)
        log.info(
            "Product %s run_id=%s product_id=%s",
            "aborted" if aborted else "exited", product.run_id, product_id,
        )
        self._notify_hook("after_product_exit", product, event.timestamp_ms, str(event.event_type))
        if remaining:
            self._advance_consumption(event.timestamp_ms, "MEMBERSHIP_CHANGED")
            self.checkpoint(event.timestamp_ms)
            return
        if not self.model_controller.close_on_last_product_exit:
            self.current_product = None
            self.current_products = ()
            self.aggregator.run_id = None
            self.aggregator.product_id = None
            self.aggregator.segment_active = False
            self.checkpoint(event.timestamp_ms)
            return
        for participant_id in self.participant_ids:
            self.store.update_product_summary_values(
                self.config.process_id, product.run_id, participant_id,
                self.consumption.summary_values(participant_id),
            )
        self.store.complete_run(self.config.process_id, product.run_id, event.timestamp_ms)
        self.product_fields.clear_run(product.run_id)
        self._clear_product()
        self.checkpoint(event.timestamp_ms)

    def _reject_product_event(self, event: ProcessEvent, message: str) -> None:
        self.store.log_event(event.timestamp_ms, self.config.process_id,
                             f"{event.event_type}_REJECTED", message[:500], event.product_id,
                             self.current_product.run_id if self.current_product else None)
        log.warning("Rejected %s for %s: %s", event.event_type, event.product_id, message)

    def _advance_consumption(self, timestamp_ms: int, reason: str) -> None:
        run_id = self.current_product.run_id if self.current_product else None
        self.consumption.advance(
            timestamp_ms, run_id, self.current_products, self.signal_state, reason=reason,
        )

    def _start_product(
        self, event: ProcessEvent, snapshot: SignalBatch | None = None,
        preparation: ProcessStartPreparation | None = None,
    ) -> None:
        late_binding = self._late_binding_config()
        proposed_product_id = event.product_id or self.pending_product_id
        provisional_product_id: str | None = None
        if proposed_product_id is None and late_binding is not None:
            provisional_product_id = (
                f"{late_binding.placeholder_prefix.strip()}-{event.timestamp_ms}"
            )
            proposed_product_id = provisional_product_id
        elif proposed_product_id is None:
            proposed_product_id = self._product_id()
        if preparation is None:
            try:
                preparation = self.hooks.before_process_start(
                    event, proposed_product_id, self.hook_services,
                )
            except Exception as error:
                message = f"Process start preparation failed: {error}"
                self.store.log_event(
                    event.timestamp_ms, self.config.process_id, "PROCESS_START_HOOK_FAILED",
                    message[:500], event.product_id,
                )
                log.warning("Rejected process start: %s", message)
                return
        if preparation.snapshot is not None:
            snapshot = self._merge_snapshots(snapshot, preparation.snapshot)
        if snapshot is not None:
            required_failures = [
                update.name for update in snapshot.updates
                if not update.quality
                and self.config.mapping_by_name.get(update.name) is not None
                and self.config.mapping_by_name[update.name].required
            ]
            if required_failures:
                self.store.log_event(
                    event.timestamp_ms, self.config.process_id, "PROCESS_START_SNAPSHOT_FAILED",
                    f"Required OPC UA values unavailable: {required_failures}", event.product_id,
                )
                log.warning(
                    "Rejected process start: required OPC UA values unavailable: %s", required_failures,
                )
                return
        configured_products = preparation.products
        if not configured_products:
            configured_products = (
                (ProcessProduct(proposed_product_id),) if proposed_product_id else ()
            )
        product_ids = [str(product.product_id).strip() for product in configured_products]
        if not product_ids or any(not value or value == "0" for value in product_ids):
            self.store.log_event(
                event.timestamp_ms, self.config.process_id, "PROCESS_START_REJECTED",
                "No valid products were supplied",
            )
            log.warning("Rejected process start: no valid products were supplied")
            return
        if len(product_ids) != len(set(product_ids)):
            self.store.log_event(
                event.timestamp_ms, self.config.process_id, "PROCESS_START_REJECTED",
                "Duplicate product IDs were supplied",
            )
            log.warning("Rejected process start: duplicate product IDs were supplied")
            return
        if self.spatial_enabled and len(product_ids) != 1:
            self.store.log_event(
                event.timestamp_ms, self.config.process_id, "PROCESS_START_REJECTED",
                "Spatial origin tracking requires one product per run", product_ids[0],
            )
            log.warning("Rejected spatial process start with multiple products: %s", product_ids)
            return
        try:
            self.product_fields.validate_assignments(preparation.context)
            for product in configured_products:
                self.product_fields.validate_assignments(product.context)
        except (TypeError, ValueError, ArithmeticError) as error:
            self.store.log_event(
                event.timestamp_ms, self.config.process_id, "PROCESS_START_REJECTED",
                f"Invalid product fields: {error}"[:500], product_ids[0],
            )
            log.warning("Rejected process start: invalid product fields: %s", error)
            return
        initial_segment_no = self._configured_segment_no()
        if self._uses_segment_reset() and initial_segment_no is None:
            self.store.log_event(
                event.timestamp_ms, self.config.process_id, "PROCESS_START_REJECTED",
                "No valid positive segment number is available", product_ids[0],
            )
            log.warning(
                "Rejected process start: no valid positive segment number is available",
            )
            return
        had_current = self.current_product is not None
        boundary_rebased = bool(
            had_current and self._uses_segment_reset()
            and self.last_direct_reset_timestamp_ms is not None
            and (
                self.pending_segment_reset_timestamp_ms is not None
                or self.aggregator.axis_position <= 1e-12
            )
        )
        if self.current_product:
            old_run_id = self.current_product.run_id
            try:
                self._complete_current(event.timestamp_ms, "PROCESS_ROLLOVER")
            except Exception:
                self.service_state = ServiceState.ERROR
                raise
            self.store.log_event(
                event.timestamp_ms, self.config.process_id, "PROCESS_ROLLOVER",
                f"Completed process before starting products {product_ids}", None, old_run_id,
            )
            log.info(
                "Process rollover completed old run_id=%s; next products=%s",
                old_run_id, product_ids,
            )
        if snapshot is not None:
            self._apply_snapshot(snapshot)
        run_id = self.run_id_factory(event.timestamp_ms)
        common_context = dict(event.context)
        common_context.update(preparation.context)
        contexts_list: list[ProductContext] = []
        try:
            for product_id, product in zip(product_ids, configured_products, strict=True):
                context = ProductContext(
                    run_id=run_id, product_id=product_id, start_ts=event.timestamp_ms,
                )
                self.product_fields.initialize(
                    context, {**common_context, **dict(product.context)},
                    self.signal_state, event.timestamp_ms,
                )
                contexts_list.append(context)
        except (TypeError, ValueError, ArithmeticError) as error:
            self.store.log_event(
                event.timestamp_ms, self.config.process_id, "PROCESS_START_REJECTED",
                f"Invalid product fields: {error}"[:500], product_ids[0],
            )
            log.warning("Rejected process start: invalid product fields: %s", error)
            return
        contexts = tuple(contexts_list)
        try:
            self.store.start_products(
                self.config.process_id, contexts,
                sync_blocked=bool(
                    provisional_product_id is not None
                    and len(contexts) == 1
                    and contexts[0].product_id == provisional_product_id
                ),
                start_mode=(
                    "startup_partial" if event.source == "startup" else "normal"
                ),
            )
        except Exception:
            self.service_state = ServiceState.ERROR
            raise
        self.current_products = contexts
        self.current_product = contexts[0]
        if (
            provisional_product_id is not None
            and len(contexts) == 1
            and contexts[0].product_id == provisional_product_id
        ):
            self.provisional_run_id = run_id
            self.provisional_product_id = provisional_product_id
            self.late_binding_timeout_reported = False
        else:
            self.provisional_run_id = None
            self.provisional_product_id = None
            self.late_binding_timeout_reported = False
        if self.spatial_enabled:
            self.spatial_contexts[run_id] = contexts
        self.participant_ids = set(product_ids)
        self.last_lifecycle_timestamp_ms = event.timestamp_ms
        self.pending_product_id = None
        self.pending_product_timestamp_ms = None
        self.position_reset_timestamp_ms = None
        self.pending_segment_signal_timestamp_ms = None
        self.pending_segment_signal_value = None
        self.segment_signal_initialized = False
        self.last_segment_signal_value = None
        if self._uses_segment_reset() and self.config.position.segment_signal:
            live_segment = self.signal_state.get(self.config.position.segment_signal)
            if live_segment and live_segment.quality:
                self.segment_signal_initialized = True
                self.last_segment_signal_value = live_segment.value
        self.pending_segment_reset_timestamp_ms = None
        self.pending_segment_reset_value = None
        self.segment_rebase_pending = False
        self.product_rebase_pending = False
        self.used_segment_numbers = {initial_segment_no or 1}
        start_position_m = 0.0
        if event.source == "startup" and self.config.position is not None:
            live_position = self.signal_state.get(self.config.position.signal)
            if live_position and live_position.quality:
                start_position_m = max(0.0, float(live_position.value))
        if self.position is not None and not self.spatial_enabled:
            assert self.config.position is not None
            if self.config.position.source == "direct":
                live_position = self.signal_state.get(self.config.position.signal)
                if live_position and live_position.quality:
                    self.position.raw_position = float(live_position.value)
            self.position.start_product(event.timestamp_ms)
            self.position.position_m = start_position_m
            self.position.speed_path_m = start_position_m
            self.position.furthest_position_m = start_position_m
            if had_current and self._uses_segment_reset() and not boundary_rebased:
                self.position.pause_segment()
                self.product_rebase_pending = True
        if self.spatial_enabled:
            assert self.spatial_aggregator is not None and self.transport is not None
            self.spatial_aggregator.start(
                run_id, product_ids[0], event.timestamp_ms, self.transport.position_m,
                start_position_m,
            )
        else:
            self.aggregator.start(
                run_id, product_ids[0], event.timestamp_ms, initial_segment_no or 1,
                start_position_m,
            )
        self._evaluate_product_fields(event.timestamp_ms, force=True)
        self._advance_consumption(event.timestamp_ms, "PRODUCT_ENTER")
        self.store.log_event(
            event.timestamp_ms, self.config.process_id, "PROCESS_START",
            f"Process started from {event.source} with {len(contexts)} product(s)", None, run_id,
        )
        self.checkpoint(event.timestamp_ms)
        log.info(
            "Started processing run run_id=%s products=%s source=%s",
            run_id, product_ids, event.source,
        )
        self._notify_hook("after_process_start", self._hook_process())

    def _apply_snapshot(self, snapshot: SignalBatch) -> None:
        if self.spatial_enabled and self.transport is not None:
            movement = self.transport.process_group(
                snapshot.timestamp_ms, snapshot.updates, self.signal_state,
                force_fallback=bool(self.current_product is None and self.spatial_aggregator and any(
                    product.exit_coordinate_m is not None
                    for product in self.spatial_aggregator.products.values()
                )),
            )
            if movement:
                self._apply_spatial_movement(movement)
        self._apply_signal_values(snapshot.updates)
        derived = self.derived_signals.evaluate(self.signal_state, snapshot.timestamp_ms)
        self._apply_signal_values(derived)
        changed_signals = {update.name for update in (*snapshot.updates, *derived)}
        changed_fields = self.product_fields.project_inputs(
            self.current_products, self.signal_state, changed_signals, snapshot.timestamp_ms,
        )
        self._evaluate_product_fields(
            snapshot.timestamp_ms, changed_fields=changed_fields,
            changed_signals=changed_signals,
        )
        for error in snapshot.errors:
            self.store.log_event(
                snapshot.timestamp_ms, self.config.process_id, "OPCUA_READ_WARNING", error[:500],
                self.current_product.product_id if self.current_product else None,
                self.current_product.run_id if self.current_product else None,
            )

    def _end_product(self, event: ProcessEvent) -> None:
        if not self.current_product:
            self.store.log_event(event.timestamp_ms, self.config.process_id, "PROCESS_END_IGNORED", "No active process")
            log.info("Ignored process end: no processing run is active")
            return
        active_ids = {product.product_id for product in self.current_products}
        if event.product_id and event.product_id not in active_ids:
            self.store.log_event(
                event.timestamp_ms, self.config.process_id, "PROCESS_END_REJECTED",
                f"Product mismatch: {event.product_id}", None, self.current_product.run_id,
            )
            log.warning(
                "Rejected process end for product_id=%s; active run_id=%s products=%s",
                event.product_id, self.current_product.run_id, sorted(active_ids),
            )
            return
        run_id = self.current_product.run_id
        self._complete_current(event.timestamp_ms, "PROCESS_END")
        self.store.log_event(
            event.timestamp_ms, self.config.process_id, "PROCESS_END", "Process completed", None, run_id,
        )

    def _complete_current(self, timestamp_ms: int, reason: str) -> None:
        assert self.current_product is not None
        if self.spatial_enabled:
            self._begin_spatial_drain(timestamp_ms, reason)
            return
        products = self.current_products
        run_id = self.current_product.run_id
        start_ts = self.current_product.start_ts
        self._prepare_process_end(timestamp_ms, reason)
        self._evaluate_product_fields(timestamp_ms, force=True)
        partial = self.aggregator.finalize_partial(timestamp_ms)
        partials = self._windows_for_products(partial) if partial else ()
        summaries: dict[str, dict[str, Any | None]] = {}
        summary_errors: list[tuple[str, str]] = []
        for product in products:
            existing = self.store.windows_for_product(
                self.config.process_id, run_id, product.product_id,
            )
            product_partial = next(
                (record for record in partials if record.product_id == product.product_id), None,
            )
            windows = tuple([*existing, *([self._window_mapping(product_partial)] if product_partial else [])])
            values, errors = self.product_fields.calculate_summary(ProductSummaryContext(
                product=product, end_ts=timestamp_ms, windows=windows, products=products,
            ))
            summaries[product.product_id] = values
            summary_errors.extend((product.product_id, error) for error in errors)
        self.store.complete_process(
            self.config.process_id, run_id, timestamp_ms, partials, summaries,
        )
        for product_id, error in summary_errors:
            self.store.log_event(
                timestamp_ms, self.config.process_id, "PRODUCT_SUMMARY_ERROR", error[:500], product_id, run_id,
            )
        log.info(
            "Completed processing run run_id=%s products=%s reason=%s duration_s=%.3f",
            run_id, [product.product_id for product in products], reason,
            (timestamp_ms - start_ts) / 1000.0,
        )
        self._notify_hook(
            "after_process_end", self._hook_process(), timestamp_ms, reason,
        )
        self._clear_product()

    def _begin_spatial_drain(self, timestamp_ms: int, reason: str) -> None:
        assert self.current_product is not None
        assert self.spatial_aggregator is not None and self.transport is not None
        run_id = self.current_product.run_id
        self._prepare_process_end(timestamp_ms, reason)
        self._evaluate_product_fields(timestamp_ms, force=True)
        material_length = self.spatial_aggregator.origin_exit(
            run_id, timestamp_ms, self.transport.position_m,
        )
        self.store.mark_process_draining(
            self.config.process_id, run_id, timestamp_ms, material_length,
        )
        self.store.log_event(
            timestamp_ms, self.config.process_id, "PROCESS_DRAINING",
            f"Position handoff recorded; {self.spatial_aggregator.line_length_m:.3f} m of line travel remaining",
            None, run_id,
        )
        log.info("Process draining run_id=%s reason=%s", run_id, reason)
        self.current_product = None
        self.current_products = ()
        self.participant_ids.clear()
        self.last_lifecycle_timestamp_ms = None
        if self.spatial_aggregator.is_drained(run_id, self.transport.position_m):
            for record in self.spatial_aggregator.finalize(run_id):
                self.store.persist_window(record)
            self._finish_spatial_run(run_id, timestamp_ms)
        self.checkpoint(timestamp_ms)

    def _finish_spatial_run(self, run_id: str, drained_ts: int) -> None:
        assert self.spatial_aggregator is not None
        product_state = self.spatial_aggregator.products.get(run_id)
        if product_state is None:
            return
        products = self.spatial_contexts.get(run_id, ())
        summaries: dict[str, dict[str, Any | None]] = {}
        for product in products:
            windows = tuple(self.store.windows_for_product(
                self.config.process_id, run_id, product.product_id,
            ))
            values, errors = self.product_fields.calculate_summary(ProductSummaryContext(
                product=product, end_ts=product_state.end_ts or drained_ts,
                windows=windows, products=products,
            ))
            summaries[product.product_id] = values
            for error in errors:
                self.store.log_event(
                    drained_ts, self.config.process_id, "PRODUCT_SUMMARY_ERROR", error[:500],
                    product.product_id, run_id,
                )
        self.store.complete_draining_process(self.config.process_id, run_id, drained_ts, summaries)
        self.store.log_event(
            drained_ts, self.config.process_id, "PROCESS_DRAINED",
            "The product reached the configured tracked-line end", None, run_id,
        )
        log.info("Completed draining process run_id=%s", run_id)
        self.product_fields.clear_run(run_id)
        self.spatial_aggregator.remove(run_id)
        self.spatial_contexts.pop(run_id, None)

    def _prepare_process_end(self, timestamp_ms: int, reason: str) -> None:
        assert self.current_product is not None
        event = ProcessEvent(
            EventType.PROCESS_END, timestamp_ms, source=reason.lower(),
        )
        try:
            preparation = self.hooks.before_process_end(
                event, self._hook_process(), self.hook_services,
            )
            if preparation.snapshot is not None:
                self._apply_snapshot(preparation.snapshot)
            for product in self.current_products:
                self.product_fields.assign(product, preparation.context, timestamp_ms)
        except Exception as error:
            message = f"Process end preparation failed: {error}"
            self.store.log_event(
                timestamp_ms, self.config.process_id, "PROCESS_END_HOOK_FAILED",
                message[:500], self.current_product.product_id,
                self.current_product.run_id,
            )
            log.warning(
                "Continuing product completion run_id=%s product_id=%s: %s",
                self.current_product.run_id, self.current_product.product_id, message,
            )

    def _abort_product(self, event: ProcessEvent) -> None:
        if not self.current_product:
            return
        run_id = self.current_product.run_id
        if self.spatial_enabled and self.spatial_aggregator is not None:
            product = self.spatial_aggregator.products.get(run_id)
            if product is not None and product.end_ts is None:
                product.end_ts = event.timestamp_ms
                product.material_length_m = max(
                    0.0, (self.transport.position_m if self.transport else 0.0)
                    - product.entry_coordinate_m,
                )
            for record in self.spatial_aggregator.finalize(run_id):
                self.store.persist_window(record)
        self.store.fail_process(
            self.config.process_id, run_id, ProductState.ABORTED, event.timestamp_ms, "Lifecycle abort",
        )
        self.store.log_event(
            event.timestamp_ms, self.config.process_id, "PROCESS_ABORT", "Process aborted", None, run_id,
        )
        log.warning("Aborted processing run run_id=%s", run_id)
        if self.spatial_enabled and self.spatial_aggregator is not None:
            if run_id in self.spatial_aggregator.products:
                self.spatial_aggregator.remove(run_id)
            self.spatial_contexts.pop(run_id, None)
        self._clear_product()
        self.checkpoint(event.timestamp_ms)

    def _clear_product(self) -> None:
        if self.current_product is not None:
            self.product_fields.clear_run(self.current_product.run_id)
        self.current_product = None
        self.current_products = ()
        self.provisional_run_id = None
        self.provisional_product_id = None
        self.late_binding_timeout_reported = False
        self.participant_ids.clear()
        self.last_lifecycle_timestamp_ms = None
        if self.position is not None:
            self.position.end_product()
        self.aggregator.run_id = None
        self.aggregator.product_id = None
        self.aggregator.segment_active = False
        self.pending_segment_signal_timestamp_ms = None
        self.pending_segment_signal_value = None
        self.segment_signal_initialized = False
        self.last_segment_signal_value = None
        self.pending_segment_reset_timestamp_ms = None
        self.pending_segment_reset_value = None
        self.segment_rebase_pending = False
        self.product_rebase_pending = False
        self.used_segment_numbers.clear()

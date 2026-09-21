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



class ProcessorStateManager:
    """Owns processor reset, durable checkpoints, recovery and status projection."""

    def reset_after_storage_reset(self) -> None:
        """Clear non-durable processor state after an authorized local reset."""
        if (
            self.service_state != ServiceState.PAUSED or self.current_product is not None
            or self.model_controller.active_run_id is not None
            or bool(self.spatial_aggregator and self.spatial_aggregator.products)
        ):
            raise RuntimeError("processor must be paused and idle before reset")
        self.signal_state.clear()
        self.model_controller = create_model_controller(self.config.process)
        self.position = (
            PositionTracker(self.config.position, self.config.speed_unit)
            if self.config.position is not None else None
        )
        variables = [
            *self.config.aggregation_variables, *self._spatial_derived_variables(),
        ]
        self.aggregator = (
            Aggregator(
                self.config.process_id, variables, self.config.aggregation.mode,
                self.config.aggregation.interval, self.config.aggregation.stale_after_ms,
            ) if self.config.aggregation.enabled else NullProductAggregator()
        )
        time_stream = self.config.streams.process_data_time
        time_variables = [
            *self.config.process_time_variables,
            *self.derived_signals.process_time_variables,
        ]
        self.process_time_aggregator = (
            ProcessTimeAggregator(
                self.config.process_id, time_variables, time_stream.interval_s or 1.0,
                time_stream.stale_after_ms, time_stream.when,
            ) if time_stream.enabled else None
        )
        if self.spatial_enabled:
            assert self.config.position is not None
            variables = [*self.config.aggregation_variables, *self._spatial_derived_variables()]
            self.transport = GlobalTransportTracker(
                self.config.position, self.config.speed_unit,
            )
            self.spatial_aggregator = SpatialDistanceAggregator(
                self.config.process_id, variables, self.config.aggregation.interval,
                self.config.aggregation.stale_after_ms,
                self.config.spatial.line_length_m,
            )
            self.spatial_contexts.clear()
            self.spatial_drain_last_ts = None
        self.rules = LifecycleRuleEvaluator(self.config.lifecycle.rules)
        self.consumption = ConsumptionAllocator(self.config.process_id, self.consumption.metrics)
        self.last_checkpoint_ms = 0
        self.awaiting_reconciliation = False
        self.pending_product_id = None
        self.pending_product_timestamp_ms = None
        self.provisional_run_id = None
        self.provisional_product_id = None
        self.late_binding_timeout_reported = False
        self.position_reset_timestamp_ms = None
        self.pending_segment_signal_timestamp_ms = None
        self.pending_segment_signal_value = None
        self.segment_signal_initialized = False
        self.last_segment_signal_value = None
        self.pending_segment_reset_timestamp_ms = None
        self.pending_segment_reset_value = None
        self.segment_rebase_pending = False
        self.product_rebase_pending = False
        self.last_direct_reset_timestamp_ms = None
        self.used_segment_numbers.clear()

    def checkpoint(self, timestamp_ms: int | None = None) -> None:
        if (not self.current_product and not self.pending_product_id
                and self.model_controller.active_run_id is None
                and not (self.spatial_aggregator and self.spatial_aggregator.products)
                and self.position_reset_timestamp_ms is None and not self.consumption.metrics):
            self.store.delete_checkpoint(self.config.process_id)
            return
        timestamp_ms = timestamp_ms or self._now()
        payload: dict[str, Any] = {
            "lifecycle": {
                "participant_ids": sorted(self.participant_ids),
                "last_lifecycle_timestamp_ms": self.last_lifecycle_timestamp_ms,
                "pending_product_id": self.pending_product_id,
                "pending_product_timestamp_ms": self.pending_product_timestamp_ms,
                "provisional_run_id": self.provisional_run_id,
                "provisional_product_id": self.provisional_product_id,
                "late_binding_timeout_reported": self.late_binding_timeout_reported,
                "position_reset_timestamp_ms": self.position_reset_timestamp_ms,
                "pending_segment_signal_timestamp_ms": self.pending_segment_signal_timestamp_ms,
                "pending_segment_signal_value": self.pending_segment_signal_value,
                "segment_signal_initialized": self.segment_signal_initialized,
                "last_segment_signal_value": self.last_segment_signal_value,
                "pending_segment_reset_timestamp_ms": self.pending_segment_reset_timestamp_ms,
                "pending_segment_reset_value": self.pending_segment_reset_value,
                "segment_rebase_pending": self.segment_rebase_pending,
                "product_rebase_pending": self.product_rebase_pending,
                "last_direct_reset_timestamp_ms": self.last_direct_reset_timestamp_ms,
                "used_segment_numbers": sorted(self.used_segment_numbers),
            },
            "model": self.model_controller.snapshot(),
            "signals": {name: asdict(value) for name, value in self.signal_state.items()},
            "consumption": self.consumption.snapshot(),
        }
        if self.position is not None:
            payload["position"] = {
                "position_m": self.position.position_m, "raw_position": self.position.raw_position,
                "last_ts": self.position.last_ts, "status": str(self.position.status),
                "speed_m_s": self.position.speed_m_s, "speed_valid": self.position.speed_valid,
                "speed_path_m": self.position.speed_path_m,
                "furthest_position_m": self.position.furthest_position_m,
            }
        if self.transport is not None and self.spatial_aggregator is not None:
            payload["transport"] = self.transport.snapshot()
            payload["spatial_aggregator"] = self.spatial_aggregator.snapshot()
            payload["spatial_drain_last_ts"] = self.spatial_drain_last_ts
            payload["spatial_contexts"] = {
                run_id: [
                    {
                        "run_id": product.run_id, "product_id": product.product_id,
                        "start_ts": product.start_ts,
                        "context": self.product_fields.checkpoint_context(product),
                    } for product in products
                ] for run_id, products in self.spatial_contexts.items()
            }
        if self.current_product:
            payload.update({
                "products": [
                    {
                        "run_id": product.run_id,
                        "product_id": product.product_id,
                        "start_ts": product.start_ts,
                        "context": self.product_fields.checkpoint_context(product),
                    }
                    for product in self.current_products
                ],
                "aggregator": self.aggregator.snapshot(),
            })
        self.store.save_checkpoint(
            self.config.process_id,
            self.current_product.run_id if self.current_product else None,
            self.current_product.product_id if self.current_product else self.pending_product_id,
            payload, timestamp_ms,
        )
        self.last_checkpoint_ms = timestamp_ms

    def _restore(self, payload: dict[str, Any]) -> None:
        lifecycle = payload.get("lifecycle", {})
        self.participant_ids = set(lifecycle.get("participant_ids", []))
        self.last_lifecycle_timestamp_ms = lifecycle.get("last_lifecycle_timestamp_ms")
        self.pending_product_id = lifecycle.get("pending_product_id")
        self.pending_product_timestamp_ms = lifecycle.get("pending_product_timestamp_ms")
        self.provisional_run_id = lifecycle.get("provisional_run_id")
        self.provisional_product_id = lifecycle.get("provisional_product_id")
        self.late_binding_timeout_reported = bool(
            lifecycle.get("late_binding_timeout_reported", False)
        )
        self.position_reset_timestamp_ms = lifecycle.get("position_reset_timestamp_ms")
        self.pending_segment_signal_timestamp_ms = lifecycle.get("pending_segment_signal_timestamp_ms")
        self.pending_segment_signal_value = lifecycle.get("pending_segment_signal_value")
        self.segment_signal_initialized = bool(lifecycle.get("segment_signal_initialized", False))
        self.last_segment_signal_value = lifecycle.get("last_segment_signal_value")
        self.pending_segment_reset_timestamp_ms = lifecycle.get("pending_segment_reset_timestamp_ms")
        self.pending_segment_reset_value = lifecycle.get("pending_segment_reset_value")
        self.segment_rebase_pending = bool(lifecycle.get("segment_rebase_pending", False))
        self.product_rebase_pending = bool(lifecycle.get("product_rebase_pending", False))
        self.last_direct_reset_timestamp_ms = lifecycle.get("last_direct_reset_timestamp_ms")
        self.used_segment_numbers = {
            int(value) for value in lifecycle.get("used_segment_numbers", [])
        }
        self.model_controller.restore(payload.get("model", {}))
        self.last_checkpoint_ms = int(payload["updated_at"])
        has_active_product = "products" in payload
        if self.position is not None:
            position = payload["position"]
            self.position.restore(
                position["position_m"], position.get("raw_position"), position.get("last_ts"), position["status"],
                position.get("speed_m_s", 0.0), position.get("speed_valid", False),
                position.get("speed_path_m"), position.get("furthest_position_m"), has_active_product,
            )
        self.signal_state = {
            name: SignalValue(**value) for name, value in payload.get("signals", {}).items()
        }
        self.consumption.restore(payload.get("consumption", {}))
        if self.transport is not None and self.spatial_aggregator is not None:
            self.transport.restore(payload.get("transport", {}))
            self.spatial_aggregator.restore(payload.get("spatial_aggregator", {}))
            self.spatial_drain_last_ts = payload.get("spatial_drain_last_ts")
            self.spatial_contexts = {
                run_id: tuple(
                    ProductContext(
                        run_id=item["run_id"], product_id=item["product_id"],
                        start_ts=int(item["start_ts"]),
                        context=self.product_fields.restore_context(
                            item.get("context", {}), int(payload["updated_at"]),
                        ),
                    ) for item in items
                ) for run_id, items in payload.get("spatial_contexts", {}).items()
            }
        if not has_active_product:
            return
        self.current_products = tuple(
            ProductContext(
                run_id=value["run_id"], product_id=value["product_id"],
                start_ts=value["start_ts"],
                context=self.product_fields.restore_context(
                    value.get("context", {}), int(payload["updated_at"]),
                ),
            )
            for value in payload["products"]
        )
        self.current_product = self.current_products[0]
        if self.spatial_enabled:
            self.spatial_contexts[self.current_product.run_id] = self.current_products
        self.aggregator.restore(payload["aggregator"])
        if self._uses_segment_reset() and (
            not self.aggregator.segment_active
            or self.segment_rebase_pending
            or self.product_rebase_pending
        ):
            assert self.position is not None
            self.position.pause_segment()

    def _reconcile(self, timestamp_ms: int) -> None:
        if not self.current_product:
            return
        if self.config.aggregation.mode == "time":
            records = self.aggregator.add_time(timestamp_ms, {})
            self._persist_windows(records)
            self.checkpoint(timestamp_ms)
            return
        if self.position is None or self.config.position is None or self.config.position.source != "direct":
            return
        live = self.signal_state.get(self.config.position.signal)
        if not live or not live.quality:
            return
        movement = self.position.observe(self.config.position.signal, live.value, live.timestamp_ms)
        if not movement:
            return
        records = self.aggregator.add_movement(
            movement.start_m, movement.end_m, movement.start_ts, movement.end_ts, {},
            movement.status if movement.status == TrackingStatus.LOST else TrackingStatus.OK,
        )
        self._persist_windows(records)
        self.checkpoint(timestamp_ms)

    def status(
        self, mqtt_connected: bool, questdb_ok: bool, *, mqtt_enabled: bool = True,
        opcua_enabled: bool = False, opcua_connected: bool = False,
    ) -> dict[str, Any]:
        process = None
        if self.current_product:
            now_ms = self._now()
            spatial_product = (
                self.spatial_aggregator.products.get(self.current_product.run_id)
                if self.spatial_aggregator is not None else None
            )
            process = {
                "run_id": self.current_product.run_id,
                "start_ts": min(product.start_ts for product in self.current_products),
                "products": [
                    {
                        "product_id": product.product_id,
                        "start_ts": product.start_ts,
                        "processing_duration_s": (now_ms - product.start_ts) / 1000.0,
                        "context": {
                            name: asdict(value) for name, value in product.context.items()
                        },
                    }
                    for product in self.current_products
                ],
                "state": "ACTIVE",
                "window": (
                    math.floor(spatial_product.position_m / self.config.aggregation.interval + 1e-12)
                    if spatial_product is not None else self.aggregator.window_no
                ),
                "segment_no": None if spatial_product is not None else self.aggregator.segment_no,
                "position_m": (
                    spatial_product.position_m if spatial_product is not None
                    else self.position.position_m if self.position is not None else None
                ),
            }
        elif self.model_controller.active_run_id is not None:
            process = {
                "run_id": self.model_controller.active_run_id,
                "start_ts": self.model_controller.active_start_ts,
                "products": [],
                "state": "ACTIVE",
                "window": None,
                "segment_no": None,
                "position_m": None,
            }
        spatial_status = None
        if self.transport is not None and self.spatial_aggregator is not None:
            products = []
            for item in self.spatial_aggregator.products.values():
                head = item.position_m
                tail = None if item.material_length_m is None else head - item.material_length_m
                remaining = None
                if item.exit_coordinate_m is not None:
                    remaining = max(
                        0.0,
                        self.spatial_aggregator.drain_target_m(item.run_id) - item.position_m,
                    )
                products.append({
                    "run_id": item.run_id, "product_id": item.product_id,
                    "state": "DRAINING" if item.exit_coordinate_m is not None else "ACTIVE",
                    "start_ts": item.start_ts, "end_ts": item.end_ts,
                    "processing_duration_s": (
                        ((item.end_ts or self._now()) - item.start_ts) / 1000.0
                    ),
                    "material_length_m": item.material_length_m,
                    "head_position_m": head, "tail_position_m": tail,
                    "remaining_drain_m": remaining,
                })
            spatial_status = {
                "transport_position_m": self.transport.position_m,
                "transport_quality": (
                    str(WindowQuality.ESTIMATED_POSITION)
                    if (
                        self.transport.fallback_active and self.transport.speed_valid
                        and self.config.position is not None
                        and self.config.position.fallback_to_speed
                    )
                    else str(self.transport.status)
                ),
                "products": products,
            }
        return {
            "process_id": self.config.process_id, "service_state": str(self.service_state),
            "model": self.config.process.model,
            "model_details": self.model_controller.status(),
            "axis": self.config.aggregation.mode if self.config.aggregation.enabled else None,
            "tracking_status": (
                str(self.transport.status) if self.transport is not None
                else str(self.position.status) if self.position is not None else None
            ),
            "spatial": spatial_status,
            "current_run": process,
            "pending_product_id": self.pending_product_id,
            "late_binding": {
                "provisional_run_id": self.provisional_run_id,
                "provisional_product_id": self.provisional_product_id,
                "timeout_reported": self.late_binding_timeout_reported,
                "unresolved_run_count": len(self.store.unresolved_runs(self.config.process_id)),
            } if self._uses_late_binding() else None,
            "position_reset_received": self.position_reset_timestamp_ms is not None,
            "segment_transition": {
                "signal_received": self.pending_segment_signal_timestamp_ms is not None,
                "reset_received": self.pending_segment_reset_timestamp_ms is not None,
                "waiting_for_rebase": self.segment_rebase_pending or self.product_rebase_pending,
            } if self._uses_segment_reset() else None,
            "mqtt_enabled": mqtt_enabled, "mqtt_connected": mqtt_connected,
            "opcua_enabled": opcua_enabled, "opcua_connected": opcua_connected,
            "sqlite_ok": self.store.healthcheck(), "questdb_ok": questdb_ok,
            "pending_sync": self.store.pending_count(self.config.process_id),
        }

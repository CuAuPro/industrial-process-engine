from __future__ import annotations

import queue
import threading
import logging
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Mapping

from industrial_process_engine.config import AppConfig
from industrial_process_engine.domain import (
    CommandType, ControlCommand, OpcUaReadRequest, ProcessContext, ProductContext,
    ProductFieldValue, ServiceState, SyncState, TimeTick, ProcessEvent, EventType,
    LifecycleSnapshot, SignalBatch, SignalUpdate,
)
from industrial_process_engine.input.mqtt_json_adapter import MqttJsonAdapter
from industrial_process_engine.input.mqtt_client import MqttInput
from industrial_process_engine.input.opcua_client import OpcUaClient
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.storage.questdb import QuestDBSink
from industrial_process_engine.storage.sqlite import SQLiteStore
from industrial_process_engine.sync.worker import SyncWorker
from industrial_process_engine.processing.product_fields import ProductFieldEngine
from industrial_process_engine.hooks import ProcessHooks
from industrial_process_engine.processing.derived_signals import DerivedSignal, DerivedSignalEngine
from industrial_process_engine.processing.product_fields import ProductField, ProductInputField
from industrial_process_engine.processing.product_fields import ProductManagedField
from industrial_process_engine.processing.consumption import ConsumptionMetric

log = logging.getLogger(__name__)


class StorageResetRejected(RuntimeError):
    pass


@dataclass(slots=True)
class _StorageResetRequest:
    completed: threading.Event = field(default_factory=threading.Event)
    error: Exception | None = None


class ProcessEngine:
    DEFAULT_SQLITE_PATH = "data/l2.db"
    RETENTION_INTERVAL_S = 3_600

    def __init__(
        self, config: AppConfig, *, sqlite_path: str | None = None,
        opcua_client_factory: Callable[..., Any] | None = None,
        hooks: ProcessHooks | None = None,
        derived_signals: Mapping[str, DerivedSignal] | None = None,
        product_fields: Mapping[str, ProductField] | None = None,
        consumption_metrics: Mapping[str, ConsumptionMetric] | None = None,
    ) -> None:
        self.config = config
        self.hooks = hooks or ProcessHooks()
        self._service_started_ts = time.time_ns() // 1_000_000
        self._service_started_monotonic = time.monotonic()
        database_path = sqlite_path or self.DEFAULT_SQLITE_PATH
        derived_engine = DerivedSignalEngine(derived_signals, config.signal_names)
        signal_names = config.signal_names | set(derived_engine.derived_signals)
        product_variables = [
            *config.aggregation_variables, *derived_engine.aggregation_variables,
        ]
        process_time_variables = [
            *config.process_time_variables, *derived_engine.process_time_variables,
        ]
        product_schema = {
            variable.name: variable.output_type for variable in product_variables
        }
        process_time_schema = {
            variable.name: variable.output_type for variable in process_time_variables
        }
        merged_fields = dict(product_fields or {})
        for metric in (consumption_metrics or {}).values():
            generated = [
                ProductManagedField(metric.cumulative_field, "double", True, True, True),
                ProductManagedField(metric.rate_field, "double", True, False),
                ProductManagedField(
                    metric.quality_field, "symbol", True, metric.summary_quality,
                ),
            ]
            if metric.specific_field is not None:
                generated.append(ProductManagedField(metric.specific_field, "double", True, True))
            for generated_field in generated:
                if generated_field.name in merged_fields:
                    raise ValueError(
                        f"consumption field collides with product field: {generated_field.name}"
                    )
                merged_fields[generated_field.name] = generated_field
            if metric.source_signal not in signal_names:
                raise ValueError(f"consumption metric references unknown signal: {metric.source_signal}")
            if metric.mass_field is not None:
                mass = merged_fields.get(metric.mass_field)
                if not isinstance(mass, ProductInputField) or mass.output_type not in {"double", "int", "long"}:
                    raise ValueError(f"consumption mass_field must be a numeric product input: {metric.mass_field}")
        product_engine = ProductFieldEngine(merged_fields, signal_names)
        summary_schema = product_engine.storage_schema
        self.store = SQLiteStore(
            database_path, product_schema, summary_schema, process_time_schema,
        )
        self.queue: queue.Queue[Any] = queue.Queue(maxsize=10_000)
        self.adapter = MqttJsonAdapter(
            config.mqtt_mappings, config.mqtt, config.lifecycle.product_topics,
        )
        self.mqtt = MqttInput(config, self.adapter, self.queue) if config.mqtt.enabled else None
        self.opcua = OpcUaClient(
            config.opcua, config.opcua_mappings, self.queue, opcua_client_factory,
            application_uri=config.opcua_application_uri,
            application_name=config.service.title,
        ) if config.opcua.enabled else None
        self.processor = ProcessProcessor(
            config, self.store, derived_signals=derived_signals, product_fields=merged_fields,
            consumption_metrics=consumption_metrics,
            opcua_client=self.opcua, hooks=self.hooks,
        )
        self.sink = QuestDBSink(
            config.questdb, product_schema, summary_schema, process_time_schema,
            product_data_enabled=config.aggregation.enabled,
            process_data_time_enabled=config.streams.process_data_time.enabled,
            product_relations_enabled=config.process.model == "transformation",
        )
        self.sync_worker = SyncWorker(config, self.store, self.sink)
        self._worker: threading.Thread | None = None
        self._worker_stop = threading.Event()
        self._inputs_initialized = threading.Event()
        self._startup_hook_called = False
        self._pending_startup: LifecycleSnapshot | None = None
        self._next_retention_at = time.monotonic()

    @property
    def mqtt_connected(self) -> bool:
        return bool(self.mqtt and self.mqtt.connected)

    @property
    def opcua_connected(self) -> bool:
        return bool(self.opcua and self.opcua.connected)

    @property
    def inputs_ready(self) -> bool:
        mqtt_ready = not self.config.mqtt.enabled or self.mqtt_connected
        opcua_ready = not self.config.opcua.enabled or self.opcua_connected
        return mqtt_ready and opcua_ready

    @property
    def waiting_for_initial_inputs(self) -> bool:
        return not self._inputs_initialized.is_set()

    def datasource_status(self) -> dict[str, dict[str, Any]]:
        return {
            "mqtt": {
                "enabled": self.config.mqtt.enabled,
                "status": (
                    self.mqtt.connection_status if self.mqtt else "DISABLED"
                ),
                "connected": self.mqtt_connected,
                "last_error": self.mqtt.last_error if self.mqtt else None,
            },
            "opcua": {
                "enabled": self.config.opcua.enabled,
                "status": (
                    self.opcua.connection_status if self.opcua else "DISABLED"
                ),
                "connected": self.opcua_connected,
                "last_error": self.opcua.last_error if self.opcua else None,
            },
        }

    def status(self) -> dict[str, Any]:
        observed_ts = time.time_ns() // 1_000_000
        value = self.processor.status(
            self.mqtt_connected, self.sink.healthy,
            mqtt_enabled=self.config.mqtt.enabled,
            opcua_enabled=self.config.opcua.enabled,
            opcua_connected=self.opcua_connected,
        )
        value["waiting_for_initial_inputs"] = self.waiting_for_initial_inputs
        value["datasources"] = self.datasource_status()
        products = {
            (product.run_id, product.product_id)
            for product in self.processor.current_products
        }
        if self.processor.spatial_aggregator is not None:
            products.update(
                (product.run_id, product.product_id)
                for product in self.processor.spatial_aggregator.products.values()
            )
        processing = bool(
            products or self.processor.model_controller.active_run_id is not None
        )
        metrics: dict[str, dict[str, Any]] = {}
        for name, signal_name in self.config.status.metrics.items():
            signal = self.processor.signal_state.get(signal_name)
            metric: dict[str, Any] = {
                "value": signal.value if signal is not None and signal.quality else None,
            }
            unit = self.config.mapping_by_name[signal_name].unit
            if unit is not None:
                metric["unit"] = str(unit)
            metrics[name] = metric
        value.update({
            "observed_ts": observed_ts,
            "service_started_ts": self._service_started_ts,
            "uptime_s": max(0.0, time.monotonic() - self._service_started_monotonic),
            "production_state": "PROCESSING" if processing else "IDLE",
            "in_process_product_count": len(products),
            "metrics": metrics,
        })
        return value

    def start(self) -> None:
        self.store.initialize()
        self._apply_retention()
        self._next_retention_at = time.monotonic() + self.RETENTION_INTERVAL_S
        self.processor.start()
        if self.inputs_ready:
            self._inputs_initialized.set()
        self._worker = threading.Thread(target=self._run, name="process-engine", daemon=True)
        self._worker.start()
        self.sync_worker.start()
        if self.mqtt:
            self.mqtt.start()
        if self.opcua:
            self.opcua.start()
        log.info(
            "Engine started process_id=%s model=%s sqlite=%s",
            self.config.process_id, self.config.process.model, self.store.path,
        )

    def enqueue(self, item: Any) -> None:
        self.queue.put_nowait(item)

    def command(self, command: CommandType) -> None:
        self.enqueue(ControlCommand(command))

    def product_enter(
        self, product_id: str, context: Mapping[str, Any] | None = None,
        timestamp_ms: int | None = None,
        parent_product_ids: list[str] | tuple[str, ...] = (),
    ) -> None:
        self.enqueue(ProcessEvent(
            EventType.PRODUCT_ENTER, timestamp_ms or time.time_ns() // 1_000_000,
            product_id, dict(context or {}), "api", tuple(parent_product_ids),
        ))

    def product_update(self, product_id: str, context: Mapping[str, Any],
                       timestamp_ms: int | None = None) -> None:
        self.enqueue(ProcessEvent(EventType.PRODUCT_UPDATE, timestamp_ms or time.time_ns() // 1_000_000,
                                  product_id, dict(context)))

    def product_exit(self, product_id: str, timestamp_ms: int | None = None) -> None:
        self.enqueue(ProcessEvent(EventType.PRODUCT_EXIT, timestamp_ms or time.time_ns() // 1_000_000,
                                  product_id))

    def product_abort(self, product_id: str, timestamp_ms: int | None = None) -> None:
        self.enqueue(ProcessEvent(EventType.PRODUCT_ABORT, timestamp_ms or time.time_ns() // 1_000_000,
                                  product_id))

    def request_opcua_read(self, names: list[str] | tuple[str, ...], reason: str) -> None:
        self.enqueue(OpcUaReadRequest(tuple(names), reason, time.time_ns() // 1_000_000))

    def graceful_shutdown(self) -> None:
        log.info("Engine shutdown started process_id=%s", self.config.process_id)
        if self.mqtt:
            self.mqtt.stop()
        if self.opcua:
            self.opcua.stop()
        self._inputs_initialized.set()
        if self.processor.service_state not in {"STOPPED", "ERROR"}:
            self.command(CommandType.STOP)
            self.queue.join()
        self.sync_worker.stop()
        self._worker_stop.set()
        self.queue.put(None)
        if self._worker:
            self._worker.join(timeout=10)
        log.info("Engine stopped process_id=%s", self.config.process_id)

    def retry_run(self, run_id: str) -> bool:
        process = self.store.get_process(self.config.process_id, run_id, include_windows=False)
        if not process or process["state"] != "COMPLETE":
            return False
        self.store.set_sync_state(self.config.process_id, run_id, SyncState.PENDING)
        self.sync_worker.wake()
        return True

    def reset_local_storage(self, timeout_s: float = 10.0) -> None:
        request = _StorageResetRequest()
        self.enqueue(request)
        if not request.completed.wait(timeout_s):
            raise TimeoutError("Timed out waiting for local storage reset")
        if request.error is not None:
            raise request.error

    def _run(self) -> None:
        while not self._worker_stop.is_set():
            if not self._inputs_initialized.is_set():
                if self.inputs_ready:
                    self._inputs_initialized.set()
                else:
                    self._worker_stop.wait(0.1)
                    continue
            if not self._startup_hook_called:
                self._startup_hook_called = True
                try:
                    snapshot = self.hooks.on_startup(self.processor.hook_services)
                    if snapshot is not None:
                        snapshot = replace(
                            snapshot,
                            event=replace(snapshot.event, source="startup"),
                        )
                        if self._wait_for_startup_position(snapshot):
                            self._pending_startup = snapshot
                        else:
                            self.processor.handle(snapshot)
                except Exception as error:
                    log.exception("Application startup hook failed")
                    self.store.log_event(
                        time.time_ns() // 1_000_000, self.config.process_id,
                        "STARTUP_HOOK_FAILED", str(error)[:500],
                    )
            boundary_ms = self.processor.next_time_boundary_ms()
            now_ms = time.time_ns() // 1_000_000
            now_monotonic = time.monotonic()
            if now_monotonic >= self._next_retention_at:
                self._apply_retention(now_ms)
                self._next_retention_at = now_monotonic + self.RETENTION_INTERVAL_S
                continue
            if boundary_ms is not None and boundary_ms <= now_ms:
                try:
                    self.processor.handle(TimeTick(now_ms))
                except Exception as error:
                    self._processing_error(error)
                continue
            retention_timeout = max(0.0, self._next_retention_at - now_monotonic)
            timeout = retention_timeout if boundary_ms is None else min(
                retention_timeout, max(0.0, (boundary_ms - now_ms) / 1000.0),
            )
            try:
                item = self.queue.get(timeout=timeout)
            except queue.Empty:
                continue
            try:
                if item is None:
                    return
                try:
                    if isinstance(item, OpcUaReadRequest):
                        self._handle_opcua_read(item)
                    elif isinstance(item, _StorageResetRequest):
                        self._handle_storage_reset(item)
                    else:
                        startup = self._complete_pending_startup(item)
                        if startup is not None:
                            self.processor.handle(startup)
                        self.processor.handle(item)
                except Exception as error:
                    self._processing_error(error)
            finally:
                self.queue.task_done()

    def _wait_for_startup_position(self, snapshot: LifecycleSnapshot) -> bool:
        position = self.config.position
        return bool(
            self.config.aggregation.mode == "distance"
            and position is not None
            and position.source == "direct"
            and self._position_update(snapshot.signals) is None
        )

    def _complete_pending_startup(self, item: Any) -> LifecycleSnapshot | None:
        if self._pending_startup is None:
            return None
        position = self._position_update(item)
        if position is None:
            return None
        pending, self._pending_startup = self._pending_startup, None
        timestamp_ms = position.timestamp_ms
        updates = tuple(
            replace(update, timestamp_ms=timestamp_ms)
            for update in pending.signals.updates
        ) + (position,)
        return LifecycleSnapshot(
            replace(pending.event, timestamp_ms=timestamp_ms),
            SignalBatch(
                updates, timestamp_ms, pending.signals.source,
                pending.signals.reason, pending.signals.errors,
            ),
        )

    def _position_update(self, item: Any) -> SignalUpdate | None:
        position = self.config.position
        if position is None:
            return None
        updates = (
            item.updates if isinstance(item, SignalBatch)
            else (item,) if isinstance(item, SignalUpdate)
            else ()
        )
        return next(
            (
                update for update in updates
                if update.name == position.signal and update.quality
            ),
            None,
        )

    def _handle_opcua_read(self, request: OpcUaReadRequest) -> None:
        if self.opcua is None:
            self.store.log_event(
                request.timestamp_ms, self.config.process_id, "OPCUA_READ_FAILED",
                "OPC UA is disabled",
                self.processor.current_product.product_id if self.processor.current_product else None,
                self.processor.current_product.run_id if self.processor.current_product else None,
            )
            return
        try:
            self.processor.handle(self.opcua.read_many(request.names, request.reason))
        except ValueError as error:
            self.store.log_event(
                request.timestamp_ms, self.config.process_id, "OPCUA_READ_FAILED", str(error)[:500],
                self.processor.current_product.product_id if self.processor.current_product else None,
                self.processor.current_product.run_id if self.processor.current_product else None,
            )

    def _handle_storage_reset(self, request: _StorageResetRequest) -> None:
        try:
            if self.processor.service_state != ServiceState.PAUSED:
                raise StorageResetRejected("Service must be PAUSED before resetting local storage")
            if self.processor.current_product is not None:
                raise StorageResetRejected("Cannot reset local storage while a processing run is active")
            if self.processor.model_controller.active_run_id is not None:
                raise StorageResetRejected(
                    "Cannot reset local storage while a process run is active"
                )
            if self.processor.spatial_aggregator and self.processor.spatial_aggregator.products:
                raise StorageResetRejected("Cannot reset local storage while a spatial run is draining")
            self.store.reset()
            self.processor.reset_after_storage_reset()
            logging.getLogger(__name__).warning("Local SQLite storage was dropped and recreated")
        except Exception as error:
            request.error = error
        finally:
            request.completed.set()

    def _processing_error(self, error: Exception) -> None:
        logger = logging.getLogger(__name__)
        logger.exception("Line processor failed")
        self.processor.service_state = ServiceState.ERROR
        try:
            self.store.log_event(
                time.time_ns() // 1_000_000, self.config.process_id,
                "PROCESSING_ERROR", "Line processor failed",
                self.processor.current_product.product_id if self.processor.current_product else None,
                self.processor.current_product.run_id if self.processor.current_product else None,
            )
        except Exception:
            logger.exception("Could not persist processing error")
        product = self.processor.current_product
        process_snapshot = None if product is None else ProcessContext(
            run_id=product.run_id,
            start_ts=product.start_ts,
            products=tuple(
                ProductContext(
                    run_id=value.run_id, product_id=value.product_id,
                    start_ts=value.start_ts,
                    context={
                        name: ProductFieldValue(field.value, field.quality, field.timestamp_ms)
                        for name, field in value.context.items()
                    },
                )
                for value in self.processor.current_products
            ),
        )
        try:
            self.hooks.after_processing_error(error, process_snapshot)
        except Exception:
            logger.exception("Application hook after_processing_error failed")

    def _apply_retention(self, now_ms: int | None = None) -> None:
        now_ms = now_ms or time.time_ns() // 1_000_000
        day_ms = 24 * 60 * 60 * 1_000
        try:
            products, windows, events = self.store.apply_retention(
                self.config.process_id,
                now_ms - self.config.local_retention.synced_days * day_ms,
                now_ms - self.config.local_retention.event_log_days * day_ms,
            )
            if products or windows or events:
                message = (
                    f"Removed {products} synced products, {windows} process windows, "
                    f"and {events} event-log rows"
                )
                logging.getLogger(__name__).info("Local retention: %s", message)
                self.store.log_event(
                    now_ms, self.config.process_id, "LOCAL_RETENTION", message,
                )
        except Exception:
            logging.getLogger(__name__).exception("Local SQLite retention failed")

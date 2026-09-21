import time

from industrial_process_engine.domain import (
    EventType, LifecycleSnapshot, ProcessEvent, SignalBatch, SignalUpdate,
)
from industrial_process_engine.hooks import (
    ProcessHooks, ProcessEndPreparation, ProcessStartPreparation,
)
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.processing.product_fields import ProductFieldRegistry
from industrial_process_engine.engine import ProcessEngine
from industrial_process_engine.storage.sqlite import SQLiteStore


def update(name, value, timestamp_ms):
    return SignalUpdate(name, value, True, timestamp_ms, name, "test")


def test_startup_hook_can_resume_live_product_from_first_position(config_factory):
    class Hooks(ProcessHooks):
        def on_startup(self, services):
            lot = update("product_id", "LIVE", 100)
            return LifecycleSnapshot(
                ProcessEvent(EventType.PROCESS_START, 100, "LIVE"),
                SignalBatch((lot,), 100, "startup"),
            )

    runtime = ProcessEngine(
        config_factory(), sqlite_path=config_factory.sqlite_path, hooks=Hooks(),
    )
    runtime.start()
    try:
        runtime.enqueue(SignalBatch((
            update("position", 200.0, 2), update("temperature", 100.0, 2),
        ), 2, "test"))
        deadline = time.monotonic() + 2
        while runtime.processor.current_product is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert runtime.processor.current_product
        assert runtime.processor.current_product.product_id == "LIVE"
        assert runtime.processor.current_product.start_ts == 2
        assert runtime.store.get_latest_product("TEST_LINE", "LIVE", False)[
            "start_mode"
        ] == "startup_partial"
        assert runtime.processor.signal_state["product_id"].value == "LIVE"
        runtime.enqueue(SignalBatch((
            update("position", 201.0, 3), update("temperature", 100.0, 3),
        ), 3, "test"))
        runtime.queue.join()
        row = runtime.store.windows_for_run(
            runtime.config.process_id, runtime.processor.current_product.run_id,
        )[0]
        assert (row["position_start_m"], row["position_end_m"]) == (200.0, 201.0)
    finally:
        runtime.graceful_shutdown()


def test_start_hook_adds_durable_product_context(config_factory):
    class Hooks(ProcessHooks):
        def before_process_start(self, event, product_id, services):
            return ProcessStartPreparation(context={"recipe": "R-17"})

    config = config_factory()
    fields = ProductFieldRegistry()
    fields.input("recipe", output_type="symbol")
    store = SQLiteStore(
        config_factory.sqlite_path, config.aggregation_storage_schema,
        {"recipe": "symbol"},
    )
    store.initialize()
    processor = ProcessProcessor(config, store, hooks=Hooks(), product_fields=fields)
    processor.start()
    processor.handle(update("position", 0, 0))
    processor.handle(update("product_id", "COIL", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))

    assert processor.current_product.field("recipe") == "R-17"
    assert store.get_latest_product("TEST_LINE", "COIL", False)["start_mode"] == "normal"
    checkpoint = store.load_checkpoint("TEST_LINE")
    assert checkpoint["products"][0]["context"]["recipe"] == {
        "value": "R-17", "quality": True, "timestamp_ms": 2,
    }


def test_failed_start_hook_preserves_active_product(config_factory):
    class Hooks(ProcessHooks):
        def before_process_start(self, event, product_id, services):
            if product_id == "NEW":
                raise RuntimeError("recipe unavailable")
            return ProcessStartPreparation()

    config = config_factory()
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store, hooks=Hooks())
    processor.start()
    processor.handle(update("position", 0, 0))
    processor.handle(update("product_id", "OLD", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    old_run_id = processor.current_product.run_id

    processor.handle(ProcessEvent(EventType.PROCESS_START, 3, product_id="NEW"))

    assert processor.current_product.run_id == old_run_id
    assert store.get_product("TEST_LINE", old_run_id, False)["state"] == "ACTIVE"
    assert any(
        event["event_type"] == "PROCESS_START_HOOK_FAILED"
        for event in store.list_events()
    )


def test_end_hook_adds_context_before_summary_calculation(config_factory):
    class Hooks(ProcessHooks):
        def before_process_end(self, event, product, services):
            return ProcessEndPreparation(context={"result_code": "OK"})

    fields = ProductFieldRegistry()
    fields.input("result_code", output_type="symbol")
    config = config_factory()
    store = SQLiteStore(
        config_factory.sqlite_path, config.aggregation_storage_schema,
        {"result_code": "symbol"},
    )
    store.initialize()
    processor = ProcessProcessor(config, store, hooks=Hooks(), product_fields=fields)
    processor.start()
    processor.handle(update("position", 0, 0))
    processor.handle(update("product_id", "COIL", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    processor.handle(ProcessEvent(EventType.PROCESS_END, 3))

    summary = store.get_latest_product("TEST_LINE", "COIL", False)
    assert summary["result_code"] == "OK"


def test_failed_end_hook_logs_error_and_still_completes(config_factory):
    class Hooks(ProcessHooks):
        def before_process_end(self, event, product, services):
            raise RuntimeError("final PLC read failed")

    config = config_factory()
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store, hooks=Hooks())
    processor.start()
    processor.handle(update("position", 0, 0))
    processor.handle(update("product_id", "COIL", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    processor.handle(ProcessEvent(EventType.PROCESS_END, 3))

    assert store.get_latest_product("TEST_LINE", "COIL", False)["state"] == "COMPLETE"
    assert any(
        event["event_type"] == "PROCESS_END_HOOK_FAILED"
        for event in store.list_events()
    )


def test_processing_error_hook_receives_failure(config_factory):
    failures = []

    class Hooks(ProcessHooks):
        def after_processing_error(self, error, product):
            failures.append((error, product))

    runtime = ProcessEngine(
        config_factory(), sqlite_path=config_factory.sqlite_path, hooks=Hooks(),
    )
    runtime.start()
    try:
        runtime.enqueue(object())
        runtime.queue.join()
        assert runtime.processor.service_state == "ERROR"
        assert len(failures) == 1
        assert isinstance(failures[0][0], Exception)
        assert failures[0][1] is None
    finally:
        runtime.graceful_shutdown()

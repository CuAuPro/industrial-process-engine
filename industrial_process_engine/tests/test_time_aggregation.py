from __future__ import annotations

import time

from industrial_process_engine.domain import EventType, ProcessEvent, SignalUpdate, TimeTick
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.engine import ProcessEngine
from industrial_process_engine.storage.sqlite import SQLiteStore


def update(name, value, timestamp_ms, quality=True):
    return SignalUpdate(name, value, quality, timestamp_ms, name, "test")


def time_config(config_factory, *, interval_s=10, stale_after_ms=60_000, calculation="weighted_mean"):
    raw = config_factory().model_dump()
    raw["tracking"] = None
    raw["streams"]["product_data"] = {
        "enabled": True, "axis": "time", "interval_s": interval_s,
        "stale_after_ms": stale_after_ms,
    }
    temperature = next(mapping for mapping in raw["mappings"] if mapping["name"] == "temperature")
    temperature["outputs"]["product_data"] = [{
        "name": "temperature", "calculation": calculation, "output_type": "double",
    }]
    return type(config_factory()).model_validate(raw)


def build(config, path):
    store = SQLiteStore(path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store)
    processor.start()
    return processor, store


def test_absolute_time_windows_include_partial_entry_and_exit(config_factory):
    processor, store = build(time_config(config_factory), config_factory.sqlite_path)
    processor.handle(update("temperature", 100, 2_999))
    processor.handle(update("product_id", "TIME-1", 3_000))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 3_000))
    processor.handle(TimeTick(10_000))
    processor.handle(TimeTick(20_000))
    processor.handle(ProcessEvent(EventType.PROCESS_END, 25_000))

    windows = store.get_latest_product("TEST_LINE", "TIME-1")["windows"]
    assert [(row["ts_start"], row["ts_end"], row["quality"]) for row in windows] == [
        (3_000, 10_000, "PARTIAL"),
        (10_000, 20_000, "GOOD"),
        (20_000, 25_000, "PARTIAL"),
    ]
    assert [row["temperature"] for row in windows] == [100, 100, 100]
    assert windows[0]["elapsed_start_s"] == 0
    assert windows[-1]["elapsed_end_s"] == 22
    assert all(row["position_start_m"] is None for row in windows)


def test_time_window_uses_null_when_measurement_becomes_stale(config_factory):
    processor, store = build(
        time_config(config_factory, stale_after_ms=1_000), config_factory.sqlite_path,
    )
    processor.handle(update("temperature", 100, 3_000))
    processor.handle(update("product_id", "STALE", 3_000))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 3_000))
    processor.handle(TimeTick(10_000))
    window = store.get_latest_product("TEST_LINE", "STALE")["windows"][0]
    assert window["temperature"] is None
    assert window["quality"] == "DATA_GAP"


def test_last_setpoint_is_held_across_time_windows(config_factory):
    processor, store = build(
        time_config(config_factory, stale_after_ms=1, calculation="last"), config_factory.sqlite_path,
    )
    processor.handle(update("temperature", 750, 3_000))
    processor.handle(update("product_id", "SETPOINT", 3_000))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 3_000))
    processor.handle(TimeTick(20_000))
    assert [row["temperature"] for row in store.get_latest_product("TEST_LINE", "SETPOINT")["windows"]] == [
        750, 750,
    ]


def test_runtime_scheduler_closes_time_window_without_mqtt(config_factory):
    config = time_config(config_factory, interval_s=0.05, stale_after_ms=60_000)
    runtime = ProcessEngine(config, sqlite_path=config_factory.sqlite_path)
    runtime.start()
    try:
        now = time.time_ns() // 1_000_000
        runtime.enqueue(update("temperature", 100, now))
        runtime.enqueue(update("product_id", "TIMER", now))
        runtime.enqueue(ProcessEvent(EventType.PROCESS_START, now))
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            product = runtime.store.get_latest_product("TEST_LINE", "TIMER")
            if product and product["windows"]:
                break
            time.sleep(0.01)
        assert product and product["windows"]
    finally:
        runtime.graceful_shutdown()

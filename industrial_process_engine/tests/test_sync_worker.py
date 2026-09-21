from __future__ import annotations

import time

from industrial_process_engine.domain import (
    EventType, ProcessEvent, ProcessTimeRecord, SignalUpdate, WindowQuality,
)
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.storage.sqlite import SQLiteStore
from industrial_process_engine.sync.worker import SyncWorker


class FailOnceSink:
    def __init__(self):
        self.calls = []

    def upload(self, windows, summary, relations=None):
        self.calls.append((windows, summary, relations or []))
        if len(self.calls) == 1:
            raise ConnectionError("temporary QuestDB failure after process data")


class TimeFailOnceSink:
    def __init__(self):
        self.calls = 0

    def upload_process_time(self, rows):
        self.calls += 1
        if self.calls == 1:
            raise ConnectionError("temporary process-time failure")

    def upload(self, windows, summaries, relations=None):
        pass


def test_sync_retries_both_tables_and_marks_product_synced(config_factory):
    table_names = {
        "product_data_table": "product_data", "product_summary_table": "product_summary",
    }
    raw = config_factory().model_dump()
    raw["questdb"] = {
        "enabled": True, "retry_interval_s": 0.01, "max_backoff_s": 0.02,
    }
    raw["questdb"].update(table_names)
    config = type(config_factory()).model_validate(raw)
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store)
    processor.start()
    processor.handle(SignalUpdate("position", 0, True, 0, "position", "test"))
    processor.handle(SignalUpdate("product_id", "SYNC", True, 1, "product_id", "test"))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    processor.handle(ProcessEvent(EventType.PROCESS_END, 3))

    sink = FailOnceSink()
    worker = SyncWorker(config, store, sink)
    worker.start()
    try:
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            summary = store.get_latest_product("TEST_LINE", "SYNC", include_windows=False)
            if summary["sync_state"] == "SYNCED":
                break
            time.sleep(0.01)
        assert summary["sync_state"] == "SYNCED"
        assert len(sink.calls) >= 2
        assert all(call[1][0]["product_id"] == "SYNC" for call in sink.calls)
        assert all(call[1][0]["run_id"] == summary["run_id"] for call in sink.calls)
        assert all(
            window["run_id"] == summary["run_id"]
            for call in sink.calls for window in call[0]
        )
    finally:
        worker.stop()


def test_sync_retries_process_time_independently(config_factory):
    table_names = {
        "product_data_table": "product_data",
        "process_data_time_table": "process_data_time",
        "product_summary_table": "product_summary",
    }
    raw = config_factory().model_dump()
    temperature = next(item for item in raw["mappings"] if item["name"] == "temperature")
    temperature["outputs"]["process_data_time"] = [{
        "name": "temperature_max", "calculation": "max", "output_type": "double",
    }]
    raw["streams"]["process_data_time"] = {
        "enabled": True, "interval_s": 5.0, "stale_after_ms": 1000,
    }
    raw["questdb"] = {
        "enabled": True, "retry_interval_s": 0.01, "max_backoff_s": 0.02,
    }
    raw["questdb"].update(table_names)
    config = type(config_factory()).model_validate(raw)
    store = SQLiteStore(
        config_factory.sqlite_path, config.aggregation_storage_schema, {},
        config.process_time_storage_schema,
    )
    store.initialize()
    store.persist_process_time(ProcessTimeRecord(
        config.process_id, 0, 5_000, {"temperature_max": 100.0}, WindowQuality.GOOD,
    ))
    sink = TimeFailOnceSink()
    worker = SyncWorker(config, store, sink)
    worker.start()
    try:
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            row = store.list_process_time(config.process_id, 0, 10_000)[0]
            if row["sync_state"] == "SYNCED":
                break
            time.sleep(0.01)
        assert row["sync_state"] == "SYNCED"
        assert sink.calls >= 2
    finally:
        worker.stop()

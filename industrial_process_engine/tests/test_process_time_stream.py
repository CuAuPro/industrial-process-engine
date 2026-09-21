from __future__ import annotations

import json

from industrial_process_engine.config import MqttConfig, MqttMappingConfig
from industrial_process_engine.domain import SignalUpdate, TimeTick
from industrial_process_engine.input.mqtt_json_adapter import MqttJsonAdapter
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.storage.sqlite import SQLiteStore


def process_time_config(config_factory):
    raw = config_factory().model_dump()
    temperature = next(item for item in raw["mappings"] if item["name"] == "temperature")
    temperature["outputs"]["process_data_time"] = [
        {"name": "temperature_avg", "calculation": "weighted_mean", "output_type": "double"},
        {"name": "temperature_max", "calculation": "max", "output_type": "double"},
    ]
    raw["streams"]["process_data_time"] = {
        "enabled": True, "interval_s": 10.0, "stale_after_ms": 60_000,
    }
    return type(config_factory()).model_validate(raw)


def test_process_time_runs_without_products_and_supports_multiple_outputs(
    config_factory, monkeypatch,
):
    config = process_time_config(config_factory)
    store = SQLiteStore(
        config_factory.sqlite_path, config.aggregation_storage_schema, {},
        config.process_time_storage_schema,
    )
    store.initialize()
    monkeypatch.setattr(ProcessProcessor, "_now", staticmethod(lambda: 0))
    processor = ProcessProcessor(config, store)
    processor.start()

    processor.handle(SignalUpdate("temperature", 100.0, True, 0, "temp", "test"))
    processor.handle(SignalUpdate("temperature", 200.0, True, 5_000, "temp", "test"))
    processor.handle(TimeTick(10_000))

    rows = store.list_process_time(config.process_id, 0, 20_000)
    assert len(rows) == 1
    assert rows[0]["temperature_avg"] == 150.0
    assert rows[0]["temperature_max"] == 200.0
    assert rows[0]["quality"] == "GOOD"
    assert "product_id" not in rows[0]
    assert "run_id" not in rows[0]


def test_empty_process_time_bucket_is_persisted_as_data_gap(config_factory, monkeypatch):
    config = process_time_config(config_factory)
    store = SQLiteStore(
        config_factory.sqlite_path, config.aggregation_storage_schema, {},
        config.process_time_storage_schema,
    )
    store.initialize()
    monkeypatch.setattr(ProcessProcessor, "_now", staticmethod(lambda: 0))
    processor = ProcessProcessor(config, store)
    processor.start()
    processor.handle(TimeTick(10_000))

    row = store.list_process_time(config.process_id, 0, 20_000)[0]
    assert row["quality"] == "DATA_GAP"
    assert row["temperature_avg"] is None
    assert row["temperature_max"] is None


def test_repeated_good_mqtt_values_remain_fresh(config_factory, monkeypatch):
    raw = process_time_config(config_factory).model_dump()
    raw["streams"]["process_data_time"].update(interval_s=2.0, stale_after_ms=600)
    config = type(config_factory()).model_validate(raw)
    store = SQLiteStore(
        config_factory.sqlite_path, config.aggregation_storage_schema, {},
        config.process_time_storage_schema,
    )
    store.initialize()
    monkeypatch.setattr(ProcessProcessor, "_now", staticmethod(lambda: 0))
    processor = ProcessProcessor(config, store)
    processor.start()
    mqtt_adapter = MqttJsonAdapter([
        MqttMappingConfig(
            source="mqtt", topic="test", id="temp", name="temperature", type="float",
        ),
    ], MqttConfig(client_id="", payload_format="value_array"))

    for observed_at in range(0, 2_001, 500):
        batch = mqtt_adapter.parse("test", json.dumps({
            "timestamp": observed_at,
            "values": [{"id": "temp", "v": 100, "q": True, "t": 0}],
        }))
        assert batch is not None
        processor.handle(batch)

    row = store.list_process_time(config.process_id, 0, 2_000)[0]
    assert row["temperature_avg"] == 100.0
    assert row["temperature_max"] == 100.0
    assert row["quality"] == "GOOD"


def test_process_time_condition_skips_inactive_buckets(config_factory, monkeypatch):
    raw = process_time_config(config_factory).model_dump()
    raw["streams"]["process_data_time"].update(interval_s=5.0, when="temperature > 0")
    config = type(config_factory()).model_validate(raw)
    store = SQLiteStore(
        config_factory.sqlite_path, config.aggregation_storage_schema, {},
        config.process_time_storage_schema,
    )
    store.initialize()
    monkeypatch.setattr(ProcessProcessor, "_now", staticmethod(lambda: 0))
    processor = ProcessProcessor(config, store)
    processor.start()

    processor.handle(SignalUpdate("temperature", 0.0, True, 0, "temp", "test"))
    processor.handle(TimeTick(5_000))
    processor.handle(SignalUpdate("temperature", 100.0, True, 5_000, "temp", "test"))
    processor.handle(TimeTick(10_000))

    rows = store.list_process_time(config.process_id, 0, 15_000)
    assert [(row["ts_start"], row["temperature_max"]) for row in rows] == [(5_000, 100.0)]


def test_restart_does_not_backfill_missing_wall_clock_buckets(config_factory, monkeypatch):
    config = process_time_config(config_factory)
    store = SQLiteStore(
        config_factory.sqlite_path, config.aggregation_storage_schema, {},
        config.process_time_storage_schema,
    )
    store.initialize()
    monkeypatch.setattr(ProcessProcessor, "_now", staticmethod(lambda: 0))
    first = ProcessProcessor(config, store)
    first.start()
    first.handle(TimeTick(10_000))

    monkeypatch.setattr(ProcessProcessor, "_now", staticmethod(lambda: 35_000))
    recovered = ProcessProcessor(config, store)
    recovered.start()
    recovered.handle(TimeTick(40_000))

    rows = store.list_process_time(config.process_id, 0, 50_000)
    assert [row["ts_start"] for row in rows] == [0, 30_000]
    assert rows[-1]["quality"] == "DATA_GAP"

from __future__ import annotations

import pytest

from industrial_process_engine.domain import EventType, ProcessEvent, SignalUpdate, TimeTick
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.processing.product_fields import (
    ProductFieldRegistry, ProductFieldResult,
)
from industrial_process_engine.storage.sqlite import SQLiteStore


def update(name, value, timestamp_ms, quality=True):
    return SignalUpdate(name, value, quality, timestamp_ms, name, "test")


def configured(config_factory, extra_mappings=()):
    raw = config_factory().model_dump()
    raw["mappings"].extend(extra_mappings)
    return type(config_factory()).model_validate(raw)


def test_input_and_summary_fields_generate_schema_and_values(config_factory):
    fields = ProductFieldRegistry()
    fields.input("grade", from_signal="grade", output_type="symbol", summary="grade_code")

    @fields.summary("maximum_temperature", output_type="double")
    def maximum_temperature(context):
        return max(window["temperature"] for window in context.windows)

    config = configured(config_factory, ({
        "source": "mqtt", "topic": "events", "id": "grade",
        "name": "grade", "type": "string",
    },))
    store = SQLiteStore(
        config_factory.sqlite_path, config.aggregation_storage_schema,
        {"grade_code": "symbol", "maximum_temperature": "double"},
    )
    store.initialize()
    processor = ProcessProcessor(config, store, product_fields=fields)
    processor.start()
    processor.handle(update("position", 0, 0))
    processor.handle(update("grade", "S355", 1))
    processor.handle(update("product_id", "COIL-1", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    processor.handle(update("temperature", 800, 3))
    processor.handle(update("position", 1, 1_000))
    processor.handle(ProcessEvent(EventType.PROCESS_END, 2_002))

    summary = store.get_latest_product("TEST_LINE", "COIL-1", include_windows=False)
    assert summary["grade_code"] == "S355"
    assert summary["maximum_temperature"] == 800
    assert summary["processing_time_s"] == 2


def test_live_calculated_field_reacts_to_dependencies_and_exposes_quality(config_factory):
    fields = ProductFieldRegistry()
    fields.input("initial_length_m", from_signal="length", output_type="double", capture="start")

    @fields.calculated(
        "estimated_processing_time_s", output_type="double",
        field_dependencies={"initial_length_m"}, signal_dependencies={"speed"},
    )
    def estimate(context):
        speed = context.signal("speed")
        length = context.field("initial_length_m")
        if not speed or length is None:
            return ProductFieldResult(None, False)
        return length / speed

    config = configured(config_factory, (
        {"source": "mqtt", "topic": "process", "id": "length", "name": "length", "type": "float"},
        {"source": "mqtt", "topic": "process", "id": "speed", "name": "speed", "type": "float"},
    ))
    store = SQLiteStore(
        config_factory.sqlite_path, config.aggregation_storage_schema,
        {"initial_length_m": "double"},
    )
    store.initialize()
    processor = ProcessProcessor(config, store, product_fields=fields)
    processor.start()
    processor.handle(update("position", 0, 0))
    processor.handle(update("length", 120, 1))
    processor.handle(update("product_id", "COIL", 2))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 3))

    live = processor.status(False, False)["current_run"]["products"][0]["context"]
    assert live["estimated_processing_time_s"] == {
        "value": None, "quality": False, "timestamp_ms": 3,
    }

    processor.handle(update("speed", 2, 4))
    live = processor.status(False, False)["current_run"]["products"][0]["context"]
    assert live["estimated_processing_time_s"] == {
        "value": 60.0, "quality": True, "timestamp_ms": 4,
    }


def test_interval_calculation_runs_in_distance_mode_and_is_not_checkpointed(config_factory):
    fields = ProductFieldRegistry()
    fields.input("initial_length_m", output_type="double", summary=False)

    @fields.calculated(
        "elapsed_estimate_s", output_type="double",
        field_dependencies={"initial_length_m"}, refresh_interval_ms=1000,
    )
    def elapsed_estimate(context):
        return context.field("initial_length_m") - context.elapsed_s

    config = config_factory()
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store, product_fields=fields)
    processor.start()
    processor.handle(update("position", 0, 0))
    processor.handle(update("product_id", "COIL", 1))
    processor.handle(ProcessEvent(
        EventType.PROCESS_START, 2, context={"initial_length_m": 10},
    ))

    assert processor.next_time_boundary_ms() == 1002
    processor.handle(TimeTick(1002))
    live = processor.status(False, False)["current_run"]["products"][0]["context"]
    assert live["elapsed_estimate_s"] == {
        "value": 9.0, "quality": True, "timestamp_ms": 1002,
    }
    checkpoint = store.load_checkpoint("TEST_LINE")
    assert "initial_length_m" in checkpoint["products"][0]["context"]
    assert "elapsed_estimate_s" not in checkpoint["products"][0]["context"]

    recovered = ProcessProcessor(config, store, product_fields=fields)
    recovered.start()
    assert recovered.current_product.context["elapsed_estimate_s"].quality is True


def test_summary_failure_stores_null_and_logs_error(config_factory):
    fields = ProductFieldRegistry()

    @fields.summary("consumption_kwh", output_type="double")
    def broken(context):
        raise ValueError("missing consumption input")

    config = config_factory()
    store = SQLiteStore(
        config_factory.sqlite_path, config.aggregation_storage_schema,
        {"consumption_kwh": "double"},
    )
    store.initialize()
    processor = ProcessProcessor(config, store, product_fields=fields)
    processor.start()
    processor.handle(update("position", 0, 0))
    processor.handle(update("product_id", "BROKEN", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    processor.handle(ProcessEvent(EventType.PROCESS_END, 10))

    summary = store.get_latest_product("TEST_LINE", "BROKEN", include_windows=False)
    assert summary["consumption_kwh"] is None
    assert any(event["event_type"] == "PRODUCT_SUMMARY_ERROR" for event in store.list_events())


def test_registry_rejects_duplicates_reserved_columns_and_bad_dependencies(config_factory):
    fields = ProductFieldRegistry()
    fields.input("test_unique", output_type="double")
    with pytest.raises(ValueError, match="already registered"):
        fields.input("test_unique", output_type="double")

    reserved = ProductFieldRegistry()
    reserved.input("product_id", output_type="symbol")
    with pytest.raises(ValueError, match="fixed"):
        from industrial_process_engine.processing.product_fields import ProductFieldEngine
        ProductFieldEngine(reserved)

    missing = ProductFieldRegistry()

    @missing.calculated("estimate", output_type="double", field_dependencies={"unknown"})
    def estimate(context):
        return 1

    with pytest.raises(ValueError, match="unknown fields"):
        from industrial_process_engine.processing.product_fields import ProductFieldEngine
        ProductFieldEngine(missing)

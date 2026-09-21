from __future__ import annotations

import pytest

from industrial_process_engine.domain import EventType, ProcessEvent, SignalUpdate, SignalValue
from industrial_process_engine.processing.derived_signals import (
    DerivedSignalEngine, DerivedSignalRegistry, DerivedSignalResult,
)
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.engine import ProcessEngine
from industrial_process_engine.storage.sqlite import SQLiteStore


def update(name, value, timestamp_ms, quality=True):
    return SignalUpdate(name, value, quality, timestamp_ms, name, "test")


def test_registry_requires_complete_aggregation_declaration():
    signals = DerivedSignalRegistry()
    with pytest.raises(ValueError, match="output_type"):
        signals.register("power_kw", value_type="float", outputs={
            "product_data": [{"name": "power_kw", "calculation": "weighted_mean"}],
        })


def test_derived_signal_is_typed_and_bad_result_invalidates_signal():
    valid_signals = DerivedSignalRegistry()

    @valid_signals.register("band", value_type="string")
    def band(state, timestamp_ms):
        return DerivedSignalResult(4)

    result = DerivedSignalEngine(valid_signals).evaluate({}, 100)[0]
    assert result.value == "4"
    assert result.quality is True

    invalid_signals = DerivedSignalRegistry()

    @invalid_signals.register("band", value_type="string")
    def invalid_band(state, timestamp_ms):
        return DerivedSignalResult(None, False)

    invalid = DerivedSignalEngine(invalid_signals).evaluate({}, 101)[0]
    assert invalid.value is None
    assert invalid.quality is False


def test_derived_signal_registry_owns_process_aggregation(config_factory):
    signals = DerivedSignalRegistry()

    @signals.register(
        "temperature_x2", value_type="float",
        outputs={
            "output_type": "double",
            "product_data": [{"calculation": "weighted_mean"}],
        },
    )
    def temperature_x2(
        state: dict[str, SignalValue], timestamp_ms: int,
    ) -> DerivedSignalResult:
        temperature = state.get("temperature")
        if not temperature or not temperature.quality:
            return DerivedSignalResult(None, False)
        return DerivedSignalResult(float(temperature.value) * 2)

    config = config_factory()
    engine = DerivedSignalEngine(signals, config.signal_names)
    product_schema = {
        **config.aggregation_storage_schema,
        **{value.name: value.output_type for value in engine.aggregation_variables},
    }
    assert product_schema == {
        "temperature": "double", "temperature_x2": "double",
    }
    runtime = ProcessEngine(
        config, sqlite_path=config_factory.sqlite_path, derived_signals=signals,
    )
    assert runtime.store.product_schema == product_schema
    assert runtime.sink.product_schema == product_schema
    store = SQLiteStore(config_factory.sqlite_path, product_schema)
    store.initialize()
    processor = ProcessProcessor(config, store, derived_signals=signals)
    processor.start()
    processor.handle(update("position", 10, 0))
    processor.handle(update("product_id", "DERIVED-1", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    processor.handle(update("temperature", 25, 3))
    processor.handle(update("position", 11, 1000))

    window = store.get_latest_product("TEST_LINE", "DERIVED-1")["windows"][0]
    assert window["temperature"] == 25
    assert window["temperature_x2"] == 50


def test_derived_signal_cannot_collide_with_mapping(config_factory):
    signals = DerivedSignalRegistry()

    @signals.register("temperature", value_type="float")
    def temperature(state, timestamp_ms):
        return 1

    with pytest.raises(ValueError, match="collide"):
        DerivedSignalEngine(signals, config_factory().signal_names)


def test_runtime_supports_summary_only_when_product_stream_is_disabled(config_factory):
    raw = config_factory().model_dump()
    for mapping in raw["mappings"]:
        mapping["outputs"]["product_data"] = []
    raw["streams"]["product_data"]["enabled"] = False
    config = type(config_factory()).model_validate(raw)
    store = SQLiteStore(config_factory.sqlite_path, {})
    store.initialize()
    processor = ProcessProcessor(config, store)
    assert type(processor.aggregator).__name__ == "NullProductAggregator"

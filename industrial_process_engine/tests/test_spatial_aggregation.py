import pytest
from pydantic import ValidationError

from industrial_process_engine.config import TrackingConfig
from industrial_process_engine.domain import (
    EventType, LifecycleSnapshot, ProcessEvent, SignalBatch, SignalUpdate,
)
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.processing.global_transport import GlobalTransportTracker
from industrial_process_engine.storage.sqlite import SQLiteStore
from industrial_process_engine.units import MeasurementUnit


def update(name, value, ts, quality=True):
    return SignalUpdate(name, value, quality, ts, name, "test")


def spatial_config(config_factory):
    raw = config_factory().model_dump()
    raw["spatial"] = {
        "origin": "process_entry",
        "line_length_m": 5.0,
        "stations": {"brush": {"offset_m": 2.0}},
    }
    raw["mappings"][1]["station"] = "process_entry"
    raw["mappings"].append({
        "source": "mqtt", "topic": "process", "id": "brush",
        "name": "brush_current", "type": "float", "station": "brush",
        "outputs": {"product_data": [{
            "name": "brush_current", "calculation": "weighted_mean",
            "output_type": "double",
        }]},
    })
    raw["mappings"].append({
        "source": "mqtt", "topic": "process", "id": "speed",
        "name": "speed", "type": "float", "unit": "m/min",
    })
    raw["tracking"].update({
        "speed_signal": "speed", "fallback_to_speed": True,
    })
    return type(config_factory()).model_validate(raw)


def spatial_late_binding_config(config_factory):
    raw = spatial_config(config_factory).model_dump()
    raw["tracking"].update({"reset_ratio": 0.10, "reset_scope": "product"})
    raw["lifecycle"] = {
        "source": "explicit",
        "product_id": {
            "signal": "product_id",
            "on_change": "PROCESS_END",
            "on_clear": "PROCESS_END",
            "late_binding": {
                "enabled": True,
                "start_on_position_reset": True,
                "placeholder_prefix": "UNASSIGNED",
                "timeout_s": 300,
                "block_remote_sync": True,
            },
        },
        "rules": [],
    }
    return type(config_factory()).model_validate(raw)


def test_station_resolution_and_override_validation(config_factory):
    config = spatial_config(config_factory)
    assert config.aggregation_variables[0].spatial_offset_m == 0.0
    assert config.aggregation_variables[1].spatial_offset_m == 2.0

    raw = config.model_dump()
    brush = next(mapping for mapping in raw["mappings"] if mapping["name"] == "brush_current")
    brush["spatial_offset_m"] = 3.25
    overridden = type(config).model_validate(raw)
    assert overridden.mapping_by_name["brush_current"].station == "brush"
    assert overridden.aggregation_variables[1].spatial_offset_m == 3.25

    brush["station"] = "missing"
    with pytest.raises(ValidationError, match="unknown station"):
        type(config).model_validate(raw)


def test_time_mode_rejects_spatial_configuration(config_factory):
    raw = config_factory().model_dump()
    raw["streams"]["product_data"] = {
        "enabled": True, "axis": "time", "interval_s": 1.0,
    }
    raw["tracking"] = None
    raw["spatial"] = {"origin": "process_entry", "line_length_m": 5.0, "stations": {}}
    with pytest.raises(ValidationError, match="only valid for distance"):
        type(config_factory()).model_validate(raw)


def test_direct_counter_reset_and_bad_quality_use_speed_without_double_count(config_factory):
    raw = config_factory().model_dump()
    raw["mappings"].append({
        "source": "mqtt", "topic": "process", "id": "speed",
        "name": "speed", "type": "float", "unit": "m/min",
    })
    raw["tracking"].update({"fallback_to_speed": True, "speed_signal": "speed"})
    config = type(config_factory()).model_validate(raw)
    tracker = GlobalTransportTracker(config.position, config.speed_unit)
    tracker.process_group(0, (update("position", 0, 0), update("speed", 60, 0)), {})
    direct = tracker.process_group(1_000, (update("position", 1, 1_000),), {})
    assert direct and direct.end_m == pytest.approx(1.0) and not direct.estimated
    bad = tracker.process_group(2_000, (update("position", None, 2_000, False),), {})
    assert bad and bad.end_m == pytest.approx(2.0) and bad.estimated
    resumed = tracker.process_group(3_000, (update("position", 2, 3_000),), {})
    assert resumed and resumed.end_m == pytest.approx(3.0) and resumed.estimated
    normal = tracker.process_group(4_000, (update("position", 3, 4_000),), {})
    assert normal and normal.end_m == pytest.approx(4.0) and not normal.estimated
    reset = tracker.process_group(5_000, (update("position", 0, 5_000),), {})
    assert reset and reset.end_m == pytest.approx(5.0) and reset.estimated


def test_global_speed_transport_uses_unit_conversion_and_trapezoidal_integration():
    config = TrackingConfig(source="speed", speed_signal="speed")
    tracker = GlobalTransportTracker(config, MeasurementUnit.CENTIMETRES_PER_SECOND)
    tracker.process_group(0, (update("speed", 100.0, 0),), {})
    movement = tracker.process_group(1_000, (update("speed", 300.0, 1_000),), {})
    assert movement is not None
    assert movement.start_m == pytest.approx(0.0)
    assert movement.end_m == pytest.approx(2.0)

    restored = GlobalTransportTracker(config, MeasurementUnit.CENTIMETRES_PER_SECOND)
    restored.restore(tracker.snapshot())
    assert restored.process_group(100_000, (update("speed", 100.0, 100_000),), {}) is None
    resumed = restored.process_group(101_000, (update("speed", 100.0, 101_000),), {})
    assert resumed and resumed.end_m == pytest.approx(3.0)

    stale = restored.advance_to(107_000)
    assert stale and stale.end_m == pytest.approx(8.0)
    assert restored.advance_to(108_000) is None
    assert restored.status == "LOST"


def test_direct_transport_ignores_an_unused_unconfigured_speed_signal():
    tracker = GlobalTransportTracker(TrackingConfig(source="direct", signal="position"))
    assert tracker.process_group(0, (update("speed", 60.0, 0),), {}) is None
    assert tracker.speed_valid is False


def test_downstream_signal_delays_row_and_sequential_run_drains(config_factory):
    config = spatial_config(config_factory)
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    ids = iter(("RUN-1", "RUN-2"))
    processor = ProcessProcessor(config, store, run_id_factory=lambda _: next(ids))
    processor.start()

    processor.handle(SignalBatch((
        update("position", 0.0, 0), update("temperature", 100.0, 0),
        update("brush_current", 10.0, 0), update("speed", 60.0, 0),
        update("product_id", "P1", 0),
        update("product_active", True, 0),
    ), 0, "mqtt"))
    processor.handle(SignalBatch((update("position", 1.0, 1_000),), 1_000, "mqtt"))
    assert store.windows_for_product("TEST_LINE", "RUN-1", "P1") == []

    processor.handle(SignalBatch((update("position", 2.0, 2_000),), 2_000, "mqtt"))
    processor.handle(SignalBatch((update("position", 3.0, 3_000),), 3_000, "mqtt"))
    first = store.windows_for_product("TEST_LINE", "RUN-1", "P1")[0]
    assert first["position_start_m"] == 0.0
    assert first["position_end_m"] == 1.0
    assert first["ts_start"] == 0
    assert first["ts_end"] == 1_000
    assert first["temperature"] == 100.0
    assert first["brush_current"] == 10.0

    processor.handle(SignalBatch((update("product_active", False, 3_001),), 3_001, "mqtt"))
    assert store.get_process("TEST_LINE", "RUN-1", False)["state"] == "DRAINING"
    assert store.pending_count("TEST_LINE") == 0
    processor.handle(SignalBatch((
        update("position", 0.0, 3_002), update("product_id", "P2", 3_002),
        update("product_active", True, 3_002),
    ), 3_002, "mqtt"))
    assert processor.current_product.product_id == "P2"

    processor.handle(SignalBatch((
        update("position", 1.0, 4_002), update("speed", 60.0, 4_002),
    ), 4_002, "mqtt"))
    assert store.get_process("TEST_LINE", "RUN-1", False)["state"] == "DRAINING"
    processor.handle(SignalBatch((
        update("position", 2.0, 5_002), update("speed", 60.0, 5_002),
    ), 5_002, "mqtt"))
    assert store.get_process("TEST_LINE", "RUN-1", False)["state"] == "DRAINING"
    processor.handle(SignalBatch((
        update("position", 5.0, 8_002), update("speed", 60.0, 8_002),
    ), 8_002, "mqtt"))
    completed = store.get_process("TEST_LINE", "RUN-1", False)
    assert completed["state"] == "COMPLETE"
    assert completed["material_length_m"] == pytest.approx(3.0)
    assert completed["drained_ts"] == 8_002
    assert store.pending_count("TEST_LINE") == 1
    status = processor.status(False, True, mqtt_enabled=False)
    assert {item["product_id"] for item in status["spatial"]["products"]} == {"P2"}
    assert status["current_run"]["position_m"] == pytest.approx(5.0)
    assert status["current_run"]["window"] == 5
    assert status["current_run"]["segment_no"] is None


def test_spatial_startup_resumes_at_current_position(config_factory):
    config = spatial_config(config_factory)
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store, run_id_factory=lambda _: "RUN")
    processor.start()
    snapshot = SignalBatch((
        update("position", 204.0, 1), update("temperature", 100.0, 1),
        update("brush_current", 10.0, 1), update("speed", 60.0, 1),
        update("product_id", "LIVE", 1),
    ), 1, "startup")
    processor.handle(LifecycleSnapshot(
        ProcessEvent(EventType.PROCESS_START, 1, "LIVE", source="startup"), snapshot,
    ))
    processor.handle(update("position", 204.0, 1))
    processor.handle(update("position", 205.0, 2))
    processor.handle(update("position", 206.0, 3))
    processor.handle(update("position", 207.0, 4))

    row = store.windows_for_product("TEST_LINE", "RUN", "LIVE")[0]
    assert (row["position_start_m"], row["position_end_m"]) == (204.0, 205.0)


def test_spatial_lot_change_waits_for_reset_while_old_run_drains(config_factory):
    config = spatial_late_binding_config(config_factory)
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    ids = iter(("OLD-RUN", "NEW-RUN"))
    processor = ProcessProcessor(config, store, run_id_factory=lambda _: next(ids))
    processor.start()
    processor.handle(update("position", 0.0, 0))
    processor.handle(update("speed", 60.0, 0))
    processor.handle(update("product_id", "OLD", 1))
    processor.handle(update("position", 4.0, 4_000))

    processor.handle(update("product_id", "NEW", 4_001))
    assert processor.current_product is None
    assert processor.pending_product_id == "NEW"
    assert store.get_process("TEST_LINE", "OLD-RUN", False)["state"] == "DRAINING"

    processor.handle(update("position", 0.0, 4_002))
    processor.handle(update("speed", 60.0, 4_002))
    assert processor.current_product and processor.current_product.product_id == "NEW"
    assert processor.current_product.run_id == "NEW-RUN"
    assert store.get_process("TEST_LINE", "OLD-RUN", False)["state"] == "DRAINING"

    processor.handle(update("position", 5.0, 9_002))
    assert store.get_process("TEST_LINE", "OLD-RUN", False)["state"] == "COMPLETE"


def test_spatial_reset_starts_provisional_while_old_run_drains(config_factory):
    config = spatial_late_binding_config(config_factory)
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    ids = iter(("OLD-RUN", "NEW-RUN"))
    processor = ProcessProcessor(config, store, run_id_factory=lambda _: next(ids))
    processor.start()
    processor.handle(update("position", 0.0, 0))
    processor.handle(update("speed", 60.0, 0))
    processor.handle(update("product_id", "OLD", 1))
    processor.handle(update("position", 4.0, 4_000))

    processor.handle(update("position", 0.0, 4_001))
    processor.handle(update("speed", 60.0, 4_001))
    assert processor.current_product is not None
    assert processor.current_product.product_id.startswith("UNASSIGNED-")
    assert processor.current_product.run_id == "NEW-RUN"
    assert store.get_process("TEST_LINE", "OLD-RUN", False)["state"] == "DRAINING"

    processor.handle(update("position", 2.0, 6_001))
    processor.handle(update("product_id", "NEW", 6_002))
    assert processor.current_product and processor.current_product.product_id == "NEW"
    assert store.get_process("TEST_LINE", "OLD-RUN", False)["state"] == "DRAINING"

    processor.handle(update("position", 5.0, 9_001))
    assert store.get_process("TEST_LINE", "OLD-RUN", False)["state"] == "COMPLETE"


def test_draining_run_recovers_without_duplicate_windows(config_factory):
    config = spatial_config(config_factory)
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    first = ProcessProcessor(config, store, run_id_factory=lambda _: "RECOVER-RUN")
    first.start()
    first.handle(SignalBatch((
        update("position", 0, 0), update("temperature", 100, 0),
        update("brush_current", 10, 0), update("speed", 60, 0),
        update("product_id", "P1", 0),
        update("product_active", True, 0),
    ), 0, "mqtt"))
    first.handle(SignalBatch((update("position", 2, 2_000),), 2_000, "mqtt"))
    first.handle(SignalBatch((update("product_active", False, 2_001),), 2_001, "mqtt"))
    assert store.get_process("TEST_LINE", "RECOVER-RUN", False)["state"] == "DRAINING"

    recovered = ProcessProcessor(config, store)
    recovered.start()
    recovered.handle(SignalBatch((
        update("position", 0, 2_002), update("speed", 60, 2_002),
    ), 2_002, "mqtt"))
    recovered.handle(SignalBatch((
        update("position", 1, 5_001), update("speed", 60, 5_001),
    ), 5_001, "mqtt"))
    recovered.handle(SignalBatch((
        update("position", 3, 7_001), update("speed", 60, 7_001),
    ), 7_001, "mqtt"))
    result = store.get_process("TEST_LINE", "RECOVER-RUN")
    assert result["state"] == "COMPLETE"
    keys = [(row["segment_no"], row["window_no"]) for row in result["windows"]]
    assert len(keys) == len(set(keys))

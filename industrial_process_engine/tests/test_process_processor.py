import logging

import pytest

from industrial_process_engine.domain import (
    EventType, LifecycleSnapshot, ProductState, ProcessEvent, SignalBatch, SignalUpdate,
)
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.processing.product_fields import ProductFieldEngine, ProductFieldRegistry
from industrial_process_engine.storage.sqlite import SQLiteStore


def update(name, value, ts, quality=True):
    return SignalUpdate(name, value, quality, ts, name, "test")


def build_processor(config, sqlite_path, product_fields=None):
    summary_schema = ProductFieldEngine(product_fields, config.signal_names).storage_schema
    store = SQLiteStore(sqlite_path, config.aggregation_storage_schema, summary_schema)
    store.initialize()
    processor = ProcessProcessor(config, store, product_fields=product_fields)
    processor.start()
    return processor, store


def test_mapping_lifecycle_edges_are_evaluated_from_atomic_batches(config_factory):
    processor, _ = build_processor(config_factory(), config_factory.sqlite_path)
    processor.handle(SignalBatch((
        update("position", 0, 1),
        update("product_id", "BATCH-1", 1),
        update("product_active", True, 1),
    ), 1, "mqtt"))
    assert processor.current_product is not None
    assert processor.current_product.product_id == "BATCH-1"
    processor.handle(SignalBatch((update("product_active", False, 2),), 2, "opcua"))
    assert processor.current_product is None


def test_opcua_only_line_supports_position_measurements_and_lifecycle(config_factory):
    raw = config_factory().model_dump()
    raw["mqtt"]["enabled"] = False
    raw["opcua"]["enabled"] = True
    for mapping in raw["mappings"]:
        mapping["source"] = "opcua"
        mapping["node_id"] = f"ns=2;s={mapping['name']}"
        mapping["subscribe"] = True
        mapping.pop("topic")
        mapping.pop("id")
    config = type(config_factory()).model_validate(raw)
    processor, store = build_processor(config, config_factory.sqlite_path)
    processor.handle(SignalBatch((
        update("position", 0, 1),
        update("product_id", "OPC-ONLY", 1),
        update("product_active", True, 1),
        update("temperature", 700, 1),
    ), 1, "opcua"))
    processor.handle(SignalBatch((update("position", 1, 1_001),), 1_001, "opcua"))
    processor.handle(SignalBatch((update("product_active", False, 1_002),), 1_002, "opcua"))
    product = store.get_latest_product("TEST_LINE", "OPC-ONLY")
    assert product["state"] == "COMPLETE"
    assert product["windows"][0]["temperature"] == 700


def test_process_start_snapshot_is_atomic_and_required_failure_preserves_old_product(config_factory):
    raw = config_factory().model_dump()
    raw["mappings"].append({
        "source": "opcua", "node_id": "ns=2;s=Grade", "name": "grade",
        "type": "string", "subscribe": False, "required": True,
    })
    config = type(config_factory()).model_validate(raw)
    fields = ProductFieldRegistry()
    fields.input("grade", from_signal="grade", output_type="symbol", summary=False)
    processor, store = build_processor(config, config_factory.sqlite_path, fields)
    processor.handle(update("position", 0, 0))
    processor.handle(update("product_id", "OLD", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    processor.handle(update("product_id", "NEW", 3))

    bad = SignalBatch((update("grade", None, 4, quality=False),), 4, "opcua", "process_start")
    processor.handle(LifecycleSnapshot(ProcessEvent(EventType.PROCESS_START, 4), bad))
    assert processor.current_product is not None
    assert processor.current_product.product_id == "OLD"
    assert store.get_latest_product("TEST_LINE", "OLD", include_windows=False)["state"] == "ACTIVE"

    good = SignalBatch((update("grade", "S355", 5),), 5, "opcua", "process_start")
    processor.handle(LifecycleSnapshot(ProcessEvent(EventType.PROCESS_START, 5), good))
    assert processor.current_product is not None
    assert processor.current_product.product_id == "NEW"
    assert processor.current_product.field("grade") == "S355"
    assert store.get_latest_product("TEST_LINE", "OLD", include_windows=False)["state"] == "COMPLETE"


def test_failed_end_snapshot_invalidates_value_but_still_completes(config_factory):
    raw = config_factory().model_dump()
    raw["mappings"].append({
        "source": "opcua", "node_id": "ns=2;s=Total", "name": "final_total",
        "type": "float", "subscribe": False, "required": True,
    })
    config = type(config_factory()).model_validate(raw)
    fields = ProductFieldRegistry()
    fields.input("final_total", from_signal="final_total", output_type="double", summary=False)
    processor, store = build_processor(config, config_factory.sqlite_path, fields)
    processor.handle(update("position", 0, 0))
    processor.handle(update("product_id", "END-SNAPSHOT", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    failed = SignalBatch(
        (update("final_total", None, 10, quality=False),), 10, "opcua", "process_end",
        ("final_total: read failed",),
    )
    processor.handle(LifecycleSnapshot(ProcessEvent(EventType.PROCESS_END, 10), failed))
    assert processor.current_product is None
    assert processor.signal_state["final_total"].quality is False
    assert store.get_latest_product("TEST_LINE", "END-SNAPSHOT", include_windows=False)["state"] == "COMPLETE"


def test_product_is_streamed_completed_and_queryable(config_factory):
    processor, store = build_processor(config_factory(), config_factory.sqlite_path)
    processor.handle(update("position", 100, 0))
    processor.handle(update("product_id", "P-1", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    processor.handle(update("temperature", 800, 3))
    processor.handle(update("position", 101.2, 1000))
    active = store.get_latest_product("TEST_LINE", "P-1")
    assert len(active["windows"]) == 1
    assert active["windows"][0]["temperature"] == 800
    processor.handle(ProcessEvent(EventType.PROCESS_END, 1100))
    complete = store.get_latest_product("TEST_LINE", "P-1")
    assert complete["state"] == "COMPLETE"
    assert complete["sync_state"] == "PENDING"
    assert len(complete["windows"]) == 2
    assert complete["windows"][1]["quality"] == "PARTIAL"
    assert store.load_checkpoint("TEST_LINE") is None


def test_large_forward_jump_writes_null_gap_then_resumes_good_windows(config_factory):
    processor, store = build_processor(config_factory(), config_factory.sqlite_path)
    processor.handle(update("position", 10, 0))
    processor.handle(update("product_id", "JUMP", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    processor.handle(update("temperature", 800, 3))
    processor.handle(update("position", 12, 1_000))
    processor.handle(update("position", 40, 2_000))
    processor.handle(update("position", 41, 3_000))

    windows = store.windows_for_run("TEST_LINE", processor.current_product.run_id)
    gap_windows = [window for window in windows if 2 <= window["position_start_m"] < 30]
    assert len(gap_windows) == 28
    assert all(window["temperature"] is None for window in gap_windows)
    assert all(window["quality"] == "TRACKING_LOST" for window in gap_windows)
    assert windows[-1]["position_start_m"] == 30
    assert windows[-1]["temperature"] == 800
    assert windows[-1]["quality"] == "GOOD"


def test_pathological_forward_jump_is_rejected_without_generating_windows(config_factory):
    processor, store = build_processor(config_factory(), config_factory.sqlite_path)
    processor.handle(update("position", 10, 0))
    processor.handle(update("product_id", "BAD-JUMP", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    processor.handle(update("temperature", 800, 3))
    processor.handle(update("position", 12, 1_000))

    processor.handle(update("position", 20_000, 2_000))

    assert processor.signal_state["position"].value == 12
    assert processor.position.raw_position == 12
    assert len(store.windows_for_run("TEST_LINE", processor.current_product.run_id)) == 2
    assert any(
        event["event_type"] == "POSITION_JUMP_REJECTED"
        for event in store.list_events()
    )


def test_checkpoint_restores_incomplete_accumulator(config_factory):
    config = config_factory()
    first, store = build_processor(config, config_factory.sqlite_path)
    first.handle(update("position", 10, 0))
    first.handle(update("product_id", "RECOVER", 1))
    first.handle(ProcessEvent(EventType.PROCESS_START, 2))
    first.handle(update("temperature", 100, 3))
    first.handle(update("position", 10.4, 400))
    first.checkpoint(400)

    recovered = ProcessProcessor(config, store)
    recovered.start()
    assert recovered.current_product.product_id == "RECOVER"
    assert recovered.aggregator.axis_position == pytest.approx(0.4)
    recovered.handle(update("position", 11.0, 1000))
    recovered.handle(ProcessEvent(EventType.PROCESS_START, 1001, product_id="RECOVER"))
    product = store.get_latest_product("TEST_LINE", "RECOVER")
    assert len(product["windows"]) == 1
    assert product["windows"][0]["quality"] == "DATA_GAP"


def test_product_change_completes_old_product_and_starts_new(config_factory):
    processor, store = build_processor(config_factory(), config_factory.sqlite_path)
    processor.handle(update("position", 0, 0))
    processor.handle(update("product_id", "OLD", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    processor.handle(update("product_id", "NEW", 3))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 4))
    assert store.get_latest_product("TEST_LINE", "OLD", False)["state"] == ProductState.COMPLETE
    assert store.get_latest_product("TEST_LINE", "NEW", False)["state"] == "ACTIVE"


def test_different_signals_may_arrive_with_cross_batch_timestamp_skew(config_factory):
    processor, _ = build_processor(config_factory(), config_factory.sqlite_path)

    processor.handle(update("temperature", 800.0, 2_000))
    processor.handle(update("product_id", "COIL", 1_000))

    assert processor.signal_state["temperature"].timestamp_ms == 2_000
    assert processor.signal_state["product_id"].timestamp_ms == 1_000
    assert processor.consumption.last_advanced_ms is None


def test_new_start_creates_distinct_run_for_same_product_id(config_factory):
    run_ids = iter(("RUN001", "RUN002"))
    config = config_factory()
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store, run_id_factory=lambda _: next(run_ids))
    processor.start()
    processor.handle(update("position", 0, 0))
    processor.handle(update("product_id", "REWORK", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 3))

    runs = store.list_products(product_id="REWORK")
    assert {run["run_id"] for run in runs} == {"RUN001", "RUN002"}
    assert store.get_product("TEST_LINE", "RUN001", False)["state"] == "COMPLETE"
    assert store.get_product("TEST_LINE", "RUN002", False)["state"] == "ACTIVE"
    assert processor.current_product and processor.current_product.run_id == "RUN002"
    start_events = [event for event in store.list_events() if event["event_type"] == "PROCESS_START"]
    assert {event["run_id"] for event in start_events} == {"RUN001", "RUN002"}


def test_lifecycle_info_logs_include_run_and_product_ids(config_factory, caplog):
    config = config_factory()
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store, run_id_factory=lambda _: "RUN-LOG")
    processor.start()
    processor.handle(update("position", 0, 0))
    processor.handle(update("product_id", "COIL-LOG", 1))

    with caplog.at_level(
        logging.INFO, logger="industrial_process_engine.processing.process_processor",
    ):
        processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
        processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 100, "COIL-SECOND"))
        processor.handle(ProcessEvent(
            EventType.PRODUCT_UPDATE, 200, "COIL-SECOND", {},
        ))
        processor.handle(ProcessEvent(EventType.PRODUCT_EXIT, 300, "COIL-SECOND"))
        processor.handle(ProcessEvent(EventType.PROCESS_END, 1_002))

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "Started processing run run_id=RUN-LOG products=['COIL-LOG']" in message
        for message in messages
    )
    assert any(
        "Completed processing run run_id=RUN-LOG products=['COIL-LOG']" in message
        for message in messages
    )
    assert any("Product entered run_id=RUN-LOG product_id=COIL-SECOND" in message for message in messages)
    assert any("Product updated run_id=RUN-LOG product_id=COIL-SECOND" in message for message in messages)
    assert any("Product exited run_id=RUN-LOG product_id=COIL-SECOND" in message for message in messages)


def test_failed_rollover_does_not_start_new_product(config_factory, monkeypatch):
    processor, store = build_processor(config_factory(), config_factory.sqlite_path)
    processor.handle(update("position", 0, 0))
    processor.handle(update("product_id", "OLD", 1))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 2))

    def fail_completion(*args, **kwargs):
        raise RuntimeError("disk failure")

    monkeypatch.setattr(store, "complete_process", fail_completion)
    with pytest.raises(RuntimeError, match="disk failure"):
        processor.handle(ProcessEvent(EventType.PROCESS_START, 3, product_id="NEW"))
    assert processor.current_product.product_id == "OLD"
    assert processor.service_state == "ERROR"


def test_recovery_with_no_live_product_marks_checkpoint_product_error(config_factory, caplog):
    config = config_factory()
    first, store = build_processor(config, config_factory.sqlite_path)
    first.handle(update("position", 10, 0))
    first.handle(update("product_id", "ORPHAN", 1))
    first.handle(ProcessEvent(EventType.PROCESS_START, 2))
    first.checkpoint(10)

    recovered = ProcessProcessor(config, store)
    recovered.start()
    with caplog.at_level(logging.WARNING, logger="processing.process_processor"):
        recovered.handle(ProcessEvent(EventType.PROCESS_END, 20))
    assert recovered.current_product is None
    product = store.get_latest_product("TEST_LINE", "ORPHAN", False)
    assert product["state"] == "ERROR"
    assert "no active product" in product["error_message"]
    record = next(
        record for record in caplog.records
        if "Recovery ended unmatched run" in record.getMessage()
    )
    assert record.levelno == logging.WARNING

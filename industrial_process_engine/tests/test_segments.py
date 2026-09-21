from industrial_process_engine.domain import SignalBatch, SignalUpdate
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.storage.sqlite import SQLiteStore


def segment_config(config_factory, start="segment_change_and_position_reset"):
    raw = config_factory().model_dump()
    raw["mappings"].append({
        "source": "mqtt", "topic": "process", "id": "pass", "name": "pass_no", "type": "int",
    })
    raw["lifecycle"] = {
        "source": "explicit",
        "product_id": {
            "signal": "product_id", "on_change": "PROCESS_START", "on_clear": "PROCESS_END",
        },
        "rules": [],
    }
    raw["tracking"].update({
        "reset_ratio": 0.10,
        "reset_scope": "segment",
        "segment_start": start,
        "segment_signal": "pass_no",
        "max_forward_jump_m": 1000,
    })
    raw["streams"]["product_data"]["interval_m"] = 100
    return type(config_factory()).model_validate(raw)


def processor_for(config, sqlite_path):
    store = SQLiteStore(sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store)
    processor.start()
    return processor, store


def update(name, value, timestamp, quality=True):
    return SignalUpdate(name, value, quality, timestamp, name, "process")


def start_product(processor, product_id="COIL"):
    processor.handle(update("temperature", 800.0, 0))
    processor.handle(update("position", 0.0, 1))
    processor.handle(update("pass_no", 1, 2))
    processor.handle(update("product_id", product_id, 3))


def test_three_passes_share_run_and_reset_segment_local_windows(config_factory):
    config = segment_config(config_factory)
    processor, store = processor_for(config, config_factory.sqlite_path)
    start_product(processor)
    run_id = processor.current_product.run_id

    processor.handle(update("position", 400.0, 4))
    processor.handle(update("pass_no", 2, 5))
    assert processor.current_product.run_id == run_id
    assert processor.aggregator.segment_active is False
    processor.handle(update("position", 0.0, 6))
    assert processor.aggregator.segment_no == 2

    processor.handle(update("position", 400.0, 7))
    processor.handle(update("position", 0.0, 8))
    assert processor.aggregator.segment_active is False
    processor.handle(update("pass_no", 3, 9))
    assert processor.aggregator.segment_no == 3
    processor.handle(update("position", 400.0, 10))

    windows = store.windows_for_run("TEST_LINE", run_id)
    assert [(row["segment_no"], row["window_no"]) for row in windows] == [
        (1, 0), (1, 1), (1, 2), (1, 3),
        (2, 0), (2, 1), (2, 2), (2, 3),
        (3, 0), (3, 1), (3, 2), (3, 3),
    ]
    assert all(row["position_start_m"] == row["window_no"] * 100 for row in windows)


def test_segment_change_starts_segment_and_waits_for_position_rebase(config_factory):
    config = segment_config(config_factory, "segment_change")
    processor, _ = processor_for(config, config_factory.sqlite_path)
    start_product(processor)
    processor.handle(update("position", 400.0, 4))

    processor.handle(update("pass_no", 2, 5))
    assert processor.aggregator.segment_no == 2
    assert processor.segment_rebase_pending is True
    assert processor.position.active is False

    processor.handle(update("position", 0.0, 6))
    assert processor.aggregator.segment_no == 2
    assert processor.segment_rebase_pending is False
    assert processor.position.position_m == 0


def test_position_reset_alone_starts_segment(config_factory):
    config = segment_config(config_factory, "position_reset")
    processor, _ = processor_for(config, config_factory.sqlite_path)
    processor.handle(update("temperature", 800.0, 0))
    processor.handle(update("position", 0.0, 1))
    processor.handle(update("pass_no", 1, 2))
    processor.handle(update("product_id", "COIL", 3))
    processor.handle(update("position", 400.0, 4))

    processor.handle(update("pass_no", 2, 5))
    assert processor.aggregator.segment_no == 1
    processor.handle(update("position", 0.0, 6))
    assert processor.current_product.product_id == "COIL"
    assert processor.aggregator.segment_no == 2
    assert processor.position.position_m == 0


def test_duplicate_and_bad_segment_values_do_not_start_segments(config_factory):
    config = segment_config(config_factory)
    processor, _ = processor_for(config, config_factory.sqlite_path)
    start_product(processor)
    processor.handle(update("position", 400.0, 4))
    processor.handle(update("pass_no", 1, 5))
    processor.handle(update("pass_no", 2, 6, quality=False))
    processor.handle(update("pass_no", 1, 7))
    assert processor.aggregator.segment_no == 1
    assert processor.aggregator.segment_active is True
    processor.handle(update("pass_no", 2, 8))
    assert processor.aggregator.segment_active is False
    assert processor.pending_segment_signal_value == 2


def test_ordinary_reverse_does_not_start_segment(config_factory):
    config = segment_config(config_factory)
    processor, _ = processor_for(config, config_factory.sqlite_path)
    start_product(processor)
    processor.handle(update("position", 400.0, 4))
    processor.handle(update("position", 395.0, 5))
    assert processor.aggregator.segment_no == 1
    assert processor.aggregator.segment_active is True


def test_product_change_rolls_over_immediately_and_reset_rebases_segment_one(config_factory):
    config = segment_config(config_factory)
    processor, store = processor_for(config, config_factory.sqlite_path)
    start_product(processor, "OLD")
    old_run = processor.current_product.run_id
    processor.handle(update("position", 400.0, 4))

    processor.handle(SignalBatch((
        update("product_id", "NEW", 5), update("pass_no", 1, 5),
    ), 5, "mqtt"))
    assert store.get_product("TEST_LINE", old_run, False)["state"] == "COMPLETE"
    assert processor.current_product.product_id == "NEW"
    assert processor.aggregator.segment_no == 1
    assert processor.product_rebase_pending is True

    processor.handle(update("position", 0.0, 6))
    assert processor.aggregator.segment_no == 1
    assert processor.product_rebase_pending is False


def test_reset_before_product_change_is_reused_as_new_product_baseline(config_factory):
    config = segment_config(config_factory)
    processor, _ = processor_for(config, config_factory.sqlite_path)
    start_product(processor, "OLD")
    processor.handle(update("position", 400.0, 4))

    processor.handle(update("position", 0.0, 5))
    assert processor.aggregator.segment_active is False
    processor.handle(update("product_id", "NEW", 6))

    assert processor.current_product.product_id == "NEW"
    assert processor.aggregator.segment_no == 1
    assert processor.product_rebase_pending is False
    assert processor.position.active is True


def test_configured_plc_pass_value_is_stored_as_segment_number(config_factory):
    config = segment_config(config_factory)
    processor, store = processor_for(config, config_factory.sqlite_path)
    processor.handle(update("temperature", 800.0, 0))
    processor.handle(update("position", 0.0, 1))
    processor.handle(update("pass_no", 10, 2))
    processor.handle(update("product_id", "COIL", 3))
    processor.handle(update("position", 400.0, 4))
    processor.handle(update("pass_no", 20, 5))
    processor.handle(update("position", 0.0, 6))
    processor.handle(update("position", 100.0, 7))

    windows = store.windows_for_run("TEST_LINE", processor.current_product.run_id)
    assert [(row["segment_no"], row["window_no"]) for row in windows] == [
        (10, 0), (10, 1), (10, 2), (10, 3), (20, 0),
    ]


def test_pending_segment_transition_survives_restart(config_factory):
    config = segment_config(config_factory)
    first, store = processor_for(config, config_factory.sqlite_path)
    start_product(first)
    first.handle(update("position", 400.0, 4))
    first.handle(update("pass_no", 2, 5))
    assert first.aggregator.segment_active is False

    recovered = ProcessProcessor(config, store)
    recovered.start()
    recovered.handle(update("product_id", "COIL", 6))
    assert recovered.awaiting_reconciliation is False
    recovered.handle(update("position", 0.0, 7))

    assert recovered.aggregator.segment_no == 2
    assert recovered.aggregator.segment_active is True
    assert recovered.position.position_m == 0


def test_segment_status_and_events_expose_transition(config_factory):
    config = segment_config(config_factory)
    processor, store = processor_for(config, config_factory.sqlite_path)
    start_product(processor)
    processor.handle(update("position", 400.0, 4))
    processor.handle(update("pass_no", 2, 5))

    status = processor.status(False, False)
    assert status["current_run"]["segment_no"] == 1
    assert status["segment_transition"] == {
        "signal_received": True, "reset_received": False, "waiting_for_rebase": False,
    }
    event_types = {event["event_type"] for event in store.list_events()}
    assert {"SEGMENT_END", "SEGMENT_WAITING"}.issubset(event_types)

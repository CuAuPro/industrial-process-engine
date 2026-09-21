from industrial_process_engine.domain import EventType, ProcessEvent, SignalBatch, SignalUpdate
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.storage.sqlite import SQLiteStore


def reset_lifecycle_config(config_factory):
    raw = config_factory().model_dump()
    raw["tracking"].update({"reset_ratio": 0.10, "max_forward_jump_m": 1000})
    raw["lifecycle"] = {
        "source": "explicit",
        "product_id": {
            "signal": "product_id", "on_change": "PROCESS_END", "on_clear": "PROCESS_END",
        },
        "rules": [],
    }
    return type(config_factory()).model_validate(raw)


def late_binding_reset_config(config_factory):
    raw = reset_lifecycle_config(config_factory).model_dump()
    raw["lifecycle"]["product_id"]["late_binding"] = {
        "enabled": True,
        "start_on_position_reset": True,
        "placeholder_prefix": "UNASSIGNED",
        "timeout_s": 300,
        "block_remote_sync": True,
    }
    return type(config_factory()).model_validate(raw)


def processor_for(config, sqlite_path):
    store = SQLiteStore(sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store)
    processor.start()
    return processor, store


def test_product_id_can_drive_lifecycle_when_configured(config_factory):
    config = config_factory(lifecycle={
        "source": "explicit", "product_id": {
            "signal": "product_id", "on_change": "PROCESS_START", "on_clear": "PROCESS_END",
        },
    })
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store)
    processor.start()
    processor.handle(SignalUpdate("position", 0.0, True, 0, "pos", "process"))
    processor.handle(SignalUpdate("product_id", "P-ID", True, 1, "pid", "events"))
    assert processor.current_product and processor.current_product.product_id == "P-ID"
    processor.handle(SignalUpdate("product_id", "", True, 2, "pid", "events"))
    assert processor.current_product is None
    assert store.get_latest_product("TEST_LINE", "P-ID", False)["state"] == "COMPLETE"


def test_product_id_change_rolls_over_sequential_product(config_factory):
    config = config_factory(lifecycle={
        "source": "explicit", "product_id": {
            "signal": "product_id", "on_change": "PROCESS_START", "on_clear": "PROCESS_END",
        },
    })
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store)
    processor.start()
    processor.handle(SignalUpdate("position", 0.0, True, 0, "pos", "process"))
    processor.handle(SignalUpdate("product_id", "FIRST", True, 1, "pid", "events"))
    processor.handle(SignalUpdate("product_id", "SECOND", True, 2, "pid", "events"))
    assert store.get_latest_product("TEST_LINE", "FIRST", False)["state"] == "COMPLETE"
    assert processor.current_product.product_id == "SECOND"

    processor.handle(SignalUpdate("product_id", "SECOND", True, 3, "pid", "events"))
    assert processor.current_product.product_id == "SECOND"


def test_same_retained_product_id_reconciles_recovered_product(config_factory):
    config = config_factory(lifecycle={
        "source": "explicit", "product_id": {
            "signal": "product_id", "on_change": "PROCESS_START", "on_clear": "PROCESS_END",
        },
    })
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store)
    processor.start()
    processor.handle(SignalUpdate("position", 0.0, True, 0, "pos", "process"))
    processor.handle(SignalUpdate("product_id", "ACTIVE", True, 1, "pid", "events"))
    processor.checkpoint(2)

    recovered = ProcessProcessor(config, store)
    recovered.start()
    assert recovered.awaiting_reconciliation is True

    recovered.handle(SignalUpdate("product_id", "ACTIVE", True, 3, "pid", "events"))

    assert recovered.awaiting_reconciliation is False
    assert recovered.current_product and recovered.current_product.product_id == "ACTIVE"


def test_product_id_change_can_end_then_trigger_starts_new_product(config_factory):
    raw = config_factory().model_dump()
    raw["lifecycle"] = {
        "source": "explicit",
        "product_id": {
            "signal": "product_id", "on_change": "PROCESS_END", "on_clear": "PROCESS_END",
        },
        "rules": [],
    }
    active_mapping = next(mapping for mapping in raw["mappings"] if mapping["name"] == "product_active")
    active_mapping["false_event"] = None
    config = type(config_factory()).model_validate(raw)
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    processor = ProcessProcessor(config, store)
    processor.start()
    processor.handle(SignalUpdate("position", 0.0, True, 0, "pos", "process"))
    processor.handle(SignalUpdate("product_id", "OLD", True, 1, "pid", "events"))
    processor.handle(SignalUpdate("product_active", True, True, 2, "active", "events"))
    processor.handle(SignalUpdate("product_active", False, True, 3, "active", "events"))

    processor.handle(SignalBatch((
        SignalUpdate("product_id", "NEW", True, 4, "pid", "events"),
        SignalUpdate("product_active", True, True, 4, "active", "events"),
    ), 4, "mqtt"))

    assert store.get_latest_product("TEST_LINE", "OLD", False)["state"] == "COMPLETE"
    assert processor.current_product and processor.current_product.product_id == "NEW"


def test_position_reset_then_product_id_change_starts_new_product(config_factory):
    config = reset_lifecycle_config(config_factory)
    processor, store = processor_for(config, config_factory.sqlite_path)
    processor.handle(SignalUpdate("position", 0.0, True, 0, "pos", "process"))
    processor.handle(SignalUpdate("product_id", "OLD", True, 1, "pid", "events"))
    processor.handle(SignalUpdate("position", 400.0, True, 2, "pos", "process"))

    processor.handle(SignalUpdate("position", 2.0, True, 3, "pos", "process"))
    assert processor.current_product is None
    assert processor.position_reset_timestamp_ms == 3
    assert store.get_latest_product("TEST_LINE", "OLD", False)["state"] == "COMPLETE"

    processor.handle(SignalUpdate("product_id", "NEW", True, 4, "pid", "events"))
    assert processor.current_product and processor.current_product.product_id == "NEW"
    assert processor.current_product.start_ts == 4
    assert processor.position.position_m == 0


def test_product_id_change_then_position_reset_starts_pending_product(config_factory):
    config = reset_lifecycle_config(config_factory)
    processor, store = processor_for(config, config_factory.sqlite_path)
    processor.handle(SignalUpdate("position", 0.0, True, 0, "pos", "process"))
    processor.handle(SignalUpdate("product_id", "OLD", True, 1, "pid", "events"))
    processor.handle(SignalUpdate("position", 400.0, True, 2, "pos", "process"))

    processor.handle(SignalUpdate("product_id", "NEW", True, 3, "pid", "events"))
    assert processor.current_product is None
    assert processor.pending_product_id == "NEW"
    assert store.get_latest_product("TEST_LINE", "OLD", False)["state"] == "COMPLETE"

    processor.handle(SignalUpdate("temperature", 999.0, True, 4, "temp", "process"))
    processor.handle(SignalUpdate("position", 2.0, True, 5, "pos", "process"))
    assert processor.current_product and processor.current_product.product_id == "NEW"
    assert processor.current_product.start_ts == 5
    assert processor.position.position_m == 0
    assert store.windows_for_run("TEST_LINE", processor.current_product.run_id) == []


def test_ordinary_reverse_does_not_complete_product(config_factory):
    config = reset_lifecycle_config(config_factory)
    processor, _ = processor_for(config, config_factory.sqlite_path)
    processor.handle(SignalUpdate("position", 0.0, True, 0, "pos", "process"))
    processor.handle(SignalUpdate("product_id", "ACTIVE", True, 1, "pid", "events"))
    processor.handle(SignalUpdate("position", 400.0, True, 2, "pos", "process"))
    run_id = processor.current_product.run_id

    processor.handle(SignalUpdate("position", 395.0, True, 3, "pos", "process"))

    assert processor.current_product and processor.current_product.run_id == run_id
    assert processor.position_reset_timestamp_ms is None
    assert processor.position.position_m == 400


def test_pending_id_and_last_raw_position_survive_restart(config_factory):
    config = reset_lifecycle_config(config_factory)
    first, store = processor_for(config, config_factory.sqlite_path)
    first.handle(SignalUpdate("position", 0.0, True, 0, "pos", "process"))
    first.handle(SignalUpdate("product_id", "OLD", True, 1, "pid", "events"))
    first.handle(SignalUpdate("position", 400.0, True, 2, "pos", "process"))
    first.handle(SignalUpdate("product_id", "NEW", True, 3, "pid", "events"))

    recovered = ProcessProcessor(config, store)
    recovered.start()
    assert recovered.current_product is None
    assert recovered.pending_product_id == "NEW"
    assert recovered.position.raw_position == 400

    recovered.handle(SignalUpdate("position", 2.0, True, 4, "pos", "process"))
    assert recovered.current_product and recovered.current_product.product_id == "NEW"
    assert recovered.current_product.start_ts == 4


def test_position_reset_starts_provisional_and_binding_rewrites_all_records(config_factory):
    config = late_binding_reset_config(config_factory)
    processor, store = processor_for(config, config_factory.sqlite_path)
    processor.handle(SignalUpdate("position", 0.0, True, 0, "pos", "process"))
    processor.handle(SignalUpdate("product_id", "OLD", True, 1, "pid", "events"))
    processor.handle(SignalUpdate("position", 4.0, True, 2, "pos", "process"))

    processor.handle(SignalUpdate("position", 0.0, True, 3, "pos", "process"))
    assert processor.current_product is not None
    assert processor.current_product.product_id.startswith("UNASSIGNED-")
    run_id = processor.current_product.run_id
    provisional_id = processor.current_product.product_id

    processor.handle(SignalUpdate("position", 2.0, True, 4, "pos", "process"))
    assert {row["product_id"] for row in store.windows_for_run("TEST_LINE", run_id)} == {
        provisional_id,
    }

    processor.handle(SignalUpdate("product_id", "", True, 5, "pid", "events"))
    assert processor.current_product and processor.current_product.product_id == provisional_id
    processor.handle(SignalUpdate("product_id", "NEW", True, 6, "pid", "events"))

    assert processor.current_product and processor.current_product.product_id == "NEW"
    assert processor.current_product.start_ts == 3
    assert processor.position.position_m == 2
    assert {row["product_id"] for row in store.windows_for_run("TEST_LINE", run_id)} == {"NEW"}
    assert [row["product_id"] for row in store.products_for_run("TEST_LINE", run_id)] == ["NEW"]
    assert store.unresolved_runs("TEST_LINE") == []
    bound_events = [
        event for event in store.list_events(100)
        if event["run_id"] == run_id and event["event_type"] == "PRODUCT_ID_BOUND"
    ]
    assert bound_events and bound_events[0]["product_id"] == "NEW"
    assert all(
        event["product_id"] != provisional_id
        for event in store.list_events(100) if event["run_id"] == run_id
    )


def test_completed_provisional_run_is_blocked_until_late_binding(config_factory):
    config = late_binding_reset_config(config_factory)
    processor, store = processor_for(config, config_factory.sqlite_path)
    processor.handle(SignalUpdate("position", 0.0, True, 0, "pos", "process"))
    processor.handle(SignalUpdate("product_id", "OLD", True, 1, "pid", "events"))
    processor.handle(SignalUpdate("position", 4.0, True, 2, "pos", "process"))
    processor.handle(SignalUpdate("position", 0.0, True, 3, "pos", "process"))
    run_id = processor.current_product.run_id
    processor.handle(SignalUpdate("position", 2.0, True, 4, "pos", "process"))
    processor.handle(ProcessEvent(EventType.PROCESS_END, 5))

    assert run_id not in {row["run_id"] for row in store.pending_runs("TEST_LINE")}
    assert [row["run_id"] for row in store.unresolved_runs("TEST_LINE")] == [run_id]

    processor.handle(SignalUpdate("product_id", "NEW", True, 6, "pid", "events"))

    assert store.unresolved_runs("TEST_LINE") == []
    assert run_id in {row["run_id"] for row in store.pending_runs("TEST_LINE")}
    assert {row["product_id"] for row in store.windows_for_run("TEST_LINE", run_id)} == {"NEW"}
    assert store.products_for_run("TEST_LINE", run_id)[0]["product_id"] == "NEW"


def test_boolean_start_can_late_bind_without_position_reset(config_factory):
    config = config_factory(lifecycle={
        "source": "explicit",
        "product_id": {
            "signal": "product_id",
            "late_binding": {"enabled": True},
        },
    })
    processor, store = processor_for(config, config_factory.sqlite_path)
    processor.handle(SignalUpdate("position", 0.0, True, 0, "pos", "process"))
    processor.handle(SignalUpdate("product_id", "OLD", True, 1, "pid", "events"))
    processor.handle(SignalUpdate("product_active", True, True, 2, "active", "events"))
    processor.handle(SignalUpdate("position", 2.0, True, 3, "pos", "process"))
    processor.handle(SignalUpdate("product_active", False, True, 4, "active", "events"))

    processor.handle(SignalUpdate("product_active", True, True, 5, "active", "events"))
    assert processor.current_product is not None
    assert processor.current_product.product_id.startswith("UNASSIGNED-")
    run_id = processor.current_product.run_id
    processor.handle(SignalUpdate("position", 4.0, True, 6, "pos", "process"))
    processor.handle(SignalUpdate("product_id", "NEW", True, 7, "pid", "events"))

    assert processor.current_product and processor.current_product.product_id == "NEW"
    assert {row["product_id"] for row in store.windows_for_run("TEST_LINE", run_id)} == {"NEW"}


def test_active_provisional_binding_survives_restart(config_factory):
    config = late_binding_reset_config(config_factory)
    first, store = processor_for(config, config_factory.sqlite_path)
    first.handle(SignalUpdate("position", 0.0, True, 0, "pos", "process"))
    first.handle(SignalUpdate("product_id", "OLD", True, 1, "pid", "events"))
    first.handle(SignalUpdate("position", 4.0, True, 2, "pos", "process"))
    first.handle(SignalUpdate("position", 0.0, True, 3, "pos", "process"))
    run_id = first.current_product.run_id
    first.handle(SignalUpdate("position", 2.0, True, 4, "pos", "process"))
    first.checkpoint(5)

    recovered = ProcessProcessor(config, store)
    recovered.start()
    assert recovered.awaiting_reconciliation is True
    assert recovered.current_product is not None
    assert recovered.current_product.product_id.startswith("UNASSIGNED-")

    recovered.handle(SignalUpdate("product_id", "NEW", True, 6, "pid", "events"))

    assert recovered.awaiting_reconciliation is False
    assert recovered.current_product and recovered.current_product.product_id == "NEW"
    assert recovered.current_product.run_id == run_id
    assert {row["product_id"] for row in store.windows_for_run("TEST_LINE", run_id)} == {"NEW"}

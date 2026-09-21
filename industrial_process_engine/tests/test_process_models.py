from __future__ import annotations

from industrial_process_engine.config import load_config
from industrial_process_engine.domain import EventType, ProcessEvent
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.storage.sqlite import SQLiteStore


def transformation_processor(tmp_path, monkeypatch):
    config = load_config("industrial_process_engine/simulators/cutting_line.yaml")
    store = SQLiteStore(
        str(tmp_path / "transformation.db"), config.aggregation_storage_schema, {},
        config.process_time_storage_schema,
    )
    store.initialize()
    monkeypatch.setattr(ProcessProcessor, "_now", staticmethod(lambda: 0))
    processor = ProcessProcessor(config, store, run_id_factory=lambda timestamp: f"R-{timestamp}")
    processor.start()
    return config, processor, store


def test_transformation_run_survives_empty_product_membership_and_records_genealogy(
    tmp_path, monkeypatch,
):
    config, processor, store = transformation_processor(tmp_path, monkeypatch)
    processor.handle(ProcessEvent(EventType.PROCESS_START, 1_000))
    assert store.get_process(config.process_id, "R-1000")["products"] == []
    processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 1_100, "PARENT"))
    processor.handle(ProcessEvent(EventType.PRODUCT_EXIT, 1_200, "PARENT"))
    assert processor.status(False, False)["current_run"]["run_id"] == "R-1000"

    processor.handle(ProcessEvent(
        EventType.PRODUCT_ENTER, 1_300, "CHILD-A",
        parent_product_ids=("PARENT",),
    ))
    processor.handle(ProcessEvent(EventType.PRODUCT_EXIT, 1_400, "CHILD-A"))
    processor.handle(ProcessEvent(
        EventType.PRODUCT_ENTER, 1_500, "CHILD-B",
        parent_product_ids=("PARENT",),
    ))
    processor.handle(ProcessEvent(EventType.PRODUCT_EXIT, 1_600, "CHILD-B"))
    processor.handle(ProcessEvent(EventType.PROCESS_END, 1_700))

    run = store.get_process(config.process_id, "R-1000")
    assert run["state"] == "COMPLETE"
    assert {(row["parent_product_id"], row["child_product_id"]) for row in run["relations"]} == {
        ("PARENT", "CHILD-A"), ("PARENT", "CHILD-B"),
    }


def test_transformation_rejects_invalid_and_duplicate_parent_relations(tmp_path, monkeypatch):
    _, processor, store = transformation_processor(tmp_path, monkeypatch)
    processor.handle(ProcessEvent(EventType.PROCESS_START, 1_000))
    processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 1_100, "PARENT"))
    processor.handle(ProcessEvent(EventType.PRODUCT_EXIT, 1_200, "PARENT"))
    processor.handle(ProcessEvent(
        EventType.PRODUCT_ENTER, 1_300, "UNKNOWN-CHILD",
        parent_product_ids=("MISSING",),
    ))
    processor.handle(ProcessEvent(
        EventType.PRODUCT_ENTER, 1_400, "DUP-CHILD",
        parent_product_ids=("PARENT", "PARENT"),
    ))
    processor.handle(ProcessEvent(
        EventType.PRODUCT_ENTER, 1_500, "SELF",
        parent_product_ids=("SELF",),
    ))

    assert {row["product_id"] for row in store.list_products()} == {"PARENT"}
    rejected = [row for row in store.list_events(20) if row["event_type"] == "PRODUCT_ENTER_REJECTED"]
    assert len(rejected) == 3


def test_cycle_defaults_to_summary_only(config_factory):
    raw = config_factory().model_dump()
    raw["process"] = {
        "id": raw["process"]["id"], "model": "cycle",
        "membership": "single", "close_run": "last_product_exit",
    }
    raw["tracking"] = None
    raw["streams"]["product_data"] = None
    config = type(config_factory()).model_validate(raw)
    assert config.streams.product_data.enabled is False


def test_cycle_single_rejects_a_second_active_product(config_factory, tmp_path, monkeypatch):
    raw = config_factory().model_dump()
    raw["process"] = {
        "id": raw["process"]["id"], "model": "cycle",
        "membership": "single", "close_run": "last_product_exit",
    }
    raw["tracking"] = None
    raw["streams"]["product_data"] = {"enabled": False}
    config = type(config_factory()).model_validate(raw)
    store = SQLiteStore(str(tmp_path / "single-cycle.db"), {}, {}, {})
    store.initialize()
    monkeypatch.setattr(ProcessProcessor, "_now", staticmethod(lambda: 0))
    processor = ProcessProcessor(config, store, run_id_factory=lambda timestamp: f"R-{timestamp}")
    processor.start()
    processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 1_000, "A"))
    processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 1_100, "B"))
    assert [product.product_id for product in processor.current_products] == ["A"]


def test_process_end_cycle_owns_run_boundary(config_factory, tmp_path, monkeypatch):
    raw = config_factory().model_dump()
    raw["process"] = {
        "id": raw["process"]["id"], "model": "cycle",
        "membership": "multiple", "close_run": "process_end",
    }
    raw["tracking"] = None
    raw["streams"]["product_data"] = {"enabled": False}
    config = type(config_factory()).model_validate(raw)
    store = SQLiteStore(str(tmp_path / "explicit-cycle.db"), {}, {}, {})
    store.initialize()
    monkeypatch.setattr(ProcessProcessor, "_now", staticmethod(lambda: 0))
    processor = ProcessProcessor(config, store, run_id_factory=lambda timestamp: f"R-{timestamp}")
    processor.start()

    processor.handle(ProcessEvent(EventType.PROCESS_START, 1_000))
    processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 1_100, "A"))
    processor.handle(ProcessEvent(EventType.PRODUCT_EXIT, 1_200, "A"))
    assert processor.status(False, False)["current_run"]["run_id"] == "R-1000"
    processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 1_300, "B"))
    processor.handle(ProcessEvent(EventType.PROCESS_END, 1_400))

    run = store.get_process(config.process_id, "R-1000")
    assert run["state"] == "COMPLETE"
    assert {product["product_id"] for product in run["products"]} == {"A", "B"}


def test_process_end_cycle_restores_an_empty_active_run(config_factory, tmp_path, monkeypatch):
    raw = config_factory().model_dump()
    raw["process"] = {
        "id": raw["process"]["id"], "model": "cycle",
        "membership": "multiple", "close_run": "process_end",
    }
    raw["tracking"] = None
    raw["streams"]["product_data"] = {"enabled": False}
    config = type(config_factory()).model_validate(raw)
    store = SQLiteStore(str(tmp_path / "cycle-restart.db"), {}, {}, {})
    store.initialize()
    monkeypatch.setattr(ProcessProcessor, "_now", staticmethod(lambda: 1_050))
    first = ProcessProcessor(config, store, run_id_factory=lambda timestamp: f"R-{timestamp}")
    first.start()
    first.handle(ProcessEvent(EventType.PROCESS_START, 1_000))

    restored = ProcessProcessor(config, store, run_id_factory=lambda timestamp: f"R-{timestamp}")
    restored.start()
    assert restored.model_controller.active_run_id == "R-1000"
    assert restored.status(False, False)["current_run"]["products"] == []

    restored.handle(ProcessEvent(EventType.PRODUCT_ENTER, 1_100, "AFTER-RESTART"))
    restored.handle(ProcessEvent(EventType.PROCESS_END, 1_200))
    assert store.get_process(config.process_id, "R-1000")["state"] == "COMPLETE"

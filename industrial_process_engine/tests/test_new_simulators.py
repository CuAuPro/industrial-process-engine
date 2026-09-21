from __future__ import annotations

from industrial_process_engine.config import load_config
from industrial_process_engine.engine import ProcessEngine
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.simulators.discrete_cnc import DiscreteCncSimulator, Settings as CncSettings
from industrial_process_engine.simulators.transformation_cutting_line import (
    Settings as CuttingSettings, TransformationCuttingLineSimulator,
)
from industrial_process_engine.simulators.run_engine import (
    DEMO_MODULES, demo_config_path, demo_extensions,
)


def test_demo_runner_resolves_every_packaged_configuration():
    expected_models = {
        "continuous-line": "continuous",
        "rolling-mill": "continuous",
        "furnace": "cycle",
        "cnc": "cycle",
        "cutting-line": "transformation",
    }
    assert set(DEMO_MODULES) == set(expected_models)
    for demo, expected_model in expected_models.items():
        path = demo_config_path(demo)
        assert path.is_file()
        assert load_config(path).process.model == expected_model
        assert set(demo_extensions(demo)) == {
            "derived_signals", "product_fields", "hooks", "consumption_metrics",
        }


def test_discrete_cnc_simulator_creates_one_summary_per_part(tmp_path, monkeypatch):
    config = load_config("industrial_process_engine/simulators/discrete_cnc.yaml")
    monkeypatch.setattr(ProcessProcessor, "_now", staticmethod(lambda: 1_000))
    engine = ProcessEngine(config, sqlite_path=str(tmp_path / "cnc.db"))
    engine.store.initialize()
    engine.processor.start()
    for message in DiscreteCncSimulator(
        CncSettings(parts=2, samples_per_part=2, interval_s=0.1, seed=1), 1_000,
    ).messages():
        item = engine.adapter.parse(message.topic, message.json())
        if item is not None:
            engine.processor.handle(item)

    products = engine.store.list_products()
    assert len(products) == 2
    assert all(product["state"] == "COMPLETE" for product in products)
    assert all(not engine.store.windows_for_run(config.process_id, product["run_id"]) for product in products)


def test_transformation_simulator_creates_parent_child_relations(tmp_path, monkeypatch):
    config = load_config("industrial_process_engine/simulators/cutting_line.yaml")
    monkeypatch.setattr(ProcessProcessor, "_now", staticmethod(lambda: 1_000))
    engine = ProcessEngine(config, sqlite_path=str(tmp_path / "cutting.db"))
    engine.store.initialize()
    engine.processor.start()
    for message in TransformationCuttingLineSimulator(
        CuttingSettings(children=3, interval_s=0.1, seed=1), 1_000,
    ).messages():
        item = engine.adapter.parse(message.topic, message.json())
        if item is not None:
            engine.processor.handle(item)

    runs = engine.store.list_runs(config.process_id)
    assert len(runs) == 1
    assert runs[0]["state"] == "COMPLETE"
    products = engine.store.products_for_run(config.process_id, runs[0]["run_id"])
    assert len(products) == 4
    relations = engine.store.relations_for_run(config.process_id, runs[0]["run_id"])
    assert len(relations) == 3
    assert {row["parent_product_id"] for row in relations} == {"COIL-PARENT-001"}

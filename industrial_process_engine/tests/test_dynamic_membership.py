from __future__ import annotations

from industrial_process_engine.domain import EventType, ProcessEvent, SignalUpdate
from industrial_process_engine.processing.consumption import ConsumptionMetricRegistry
from industrial_process_engine.processing.product_fields import ProductFieldRegistry
from industrial_process_engine.engine import ProcessEngine


def test_staggered_membership_closes_run_only_on_last_exit(config_factory) -> None:
    raw = config_factory().model_dump()
    raw["streams"]["product_data"] = {
        "enabled": True, "axis": "time", "interval_s": 3600,
        "stale_after_ms": 10_000_000,
    }
    raw["tracking"] = None
    raw["mappings"].append({
        "source": "mqtt", "topic": "process", "id": "meter", "name": "energy_meter_kwh", "type": "float",
    })
    config = type(config_factory()).model_validate(raw)
    fields = ProductFieldRegistry()
    fields.input("mass_t", output_type="double", checkpoint=True, summary=True)
    metrics = ConsumptionMetricRegistry()
    metrics.counter(
        "electricity", source_signal="energy_meter_kwh", mass_field="mass_t",
        cumulative_field="consumption_kwh", rate_field="consumption_rate_kw",
        specific_field="specific_consumption_kwh_t", stale_after_ms=5_000,
    )
    runtime = ProcessEngine(
        config, sqlite_path=config_factory.sqlite_path,
        product_fields=fields, consumption_metrics=metrics,
    )
    runtime.store.initialize()
    runtime.processor.start()
    processor = runtime.processor

    processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 0, "P1", {"mass_t": 2}))
    run_id = processor.current_product.run_id
    processor.handle(SignalUpdate("energy_meter_kwh", 100, True, 0, "meter", "test"))
    processor.handle(SignalUpdate("energy_meter_kwh", 130, True, 3_600_000, "meter", "test"))
    processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 3_600_000, "P2", {"mass_t": 1}))
    processor.handle(ProcessEvent(EventType.PRODUCT_EXIT, 5_400_000, "P1"))

    products = {row["product_id"]: row for row in runtime.store.products_for_run("TEST_LINE", run_id)}
    assert products["P1"]["state"] == "COMPLETE"
    assert products["P1"]["processing_time_s"] == 5400
    assert products["P2"]["state"] == "ACTIVE"
    assert runtime.store.pending_runs("TEST_LINE") == []

    processor.handle(SignalUpdate("energy_meter_kwh", 160, True, 7_200_000, "meter", "test"))
    processor.handle(SignalUpdate("energy_meter_kwh", 190, True, 10_800_000, "meter", "test"))
    processor.handle(ProcessEvent(EventType.PRODUCT_EXIT, 10_800_000, "P2"))
    process = runtime.store.get_process("TEST_LINE", run_id)
    assert process["state"] == "COMPLETE"
    assert process["start_ts"] == 0
    assert process["end_ts"] == 10_800_000
    totals = {row["product_id"]: row["consumption_kwh"] for row in process["products"]}
    assert totals == {"P1": 40.0, "P2": 50.0}
    assert sum(totals.values()) == 90


def test_duplicate_unknown_and_same_run_reentry_are_rejected(config_factory) -> None:
    runtime = ProcessEngine(config_factory(), sqlite_path=config_factory.sqlite_path)
    runtime.store.initialize()
    runtime.processor.start()
    processor = runtime.processor
    processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 1, "P1"))
    processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 2, "P2"))
    processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 3, "P1"))
    processor.handle(ProcessEvent(EventType.PRODUCT_UPDATE, 4, "UNKNOWN", {}))
    processor.handle(ProcessEvent(EventType.PRODUCT_EXIT, 5, "P1"))
    processor.handle(ProcessEvent(EventType.PRODUCT_ENTER, 6, "P1"))

    rejected = [row for row in runtime.store.list_events(20) if row["event_type"].endswith("_REJECTED")]
    assert len(rejected) == 3
    assert {product.product_id for product in processor.current_products} == {"P2"}

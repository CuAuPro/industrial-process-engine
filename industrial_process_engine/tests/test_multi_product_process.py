from industrial_process_engine.domain import EventType, ProcessProduct, ProcessEvent, SignalUpdate
from industrial_process_engine.hooks import ProcessHooks, ProcessStartPreparation
from industrial_process_engine.processing.process_processor import ProcessProcessor
from industrial_process_engine.processing.product_fields import ProductFieldRegistry
from industrial_process_engine.storage.sqlite import SQLiteStore


class ThreeProductHooks(ProcessHooks):
    def before_process_start(self, event, product_id, services):
        return ProcessStartPreparation(
            context={"furnace_recipe": "R-17"},
            products=(
                ProcessProduct("P001", {"mass_t": 2.0}),
                ProcessProduct("P002", {"mass_t": 3.0}),
                ProcessProduct("P003", {"mass_t": 4.0}),
            ),
        )


def update(name, value, timestamp_ms):
    return SignalUpdate(name, value, True, timestamp_ms, name, "test")


def fields():
    registry = ProductFieldRegistry()
    registry.input("furnace_recipe", output_type="symbol", summary=False)
    registry.input("mass_t", output_type="double")

    @registry.summary("participant_count", output_type="int")
    def participant_count(context):
        return len(context.products)

    return registry


def test_one_process_run_persists_rows_and_summaries_for_all_products(config_factory):
    raw = config_factory().model_dump()
    raw["lifecycle"] = {"source": "explicit", "product_id": None, "rules": []}
    config = type(config_factory()).model_validate(raw)
    product_fields = fields()
    store = SQLiteStore(
        config_factory.sqlite_path, config.aggregation_storage_schema,
        {"participant_count": "int", "mass_t": "double"},
    )
    store.initialize()
    processor = ProcessProcessor(config, store, hooks=ThreeProductHooks(), product_fields=product_fields)
    processor.start()
    processor.handle(update("position", 0.0, 0))
    processor.handle(ProcessEvent(EventType.PROCESS_START, 1))
    run_id = processor.current_product.run_id

    processor.handle(update("temperature", 800.0, 2))
    processor.handle(update("position", 1.0, 1_002))
    processor.handle(ProcessEvent(EventType.PROCESS_END, 1_003))

    summaries = store.products_for_run("TEST_LINE", run_id)
    assert {row["product_id"] for row in summaries} == {"P001", "P002", "P003"}
    assert {row["run_id"] for row in summaries} == {run_id}
    assert {row["participant_count"] for row in summaries} == {3}
    assert {row["mass_t"] for row in summaries} == {2.0, 3.0, 4.0}

    windows = store.windows_for_run("TEST_LINE", run_id)
    assert len(windows) == 3
    assert {row["product_id"] for row in windows} == {"P001", "P002", "P003"}
    assert {(row["segment_no"], row["window_no"]) for row in windows} == {(1, 0)}


def test_multi_product_process_is_restored_and_reconciled_as_one_run(config_factory):
    raw = config_factory().model_dump()
    raw["lifecycle"] = {"source": "explicit", "product_id": None, "rules": []}
    config = type(config_factory()).model_validate(raw)
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()

    product_fields = fields()
    first = ProcessProcessor(config, store, hooks=ThreeProductHooks(), product_fields=product_fields)
    first.start()
    first.handle(update("position", 0.0, 0))
    first.handle(ProcessEvent(EventType.PROCESS_START, 1))
    run_id = first.current_product.run_id

    recovered = ProcessProcessor(config, store, hooks=ThreeProductHooks(), product_fields=product_fields)
    recovered.start()
    assert recovered.awaiting_reconciliation is True
    assert {product.product_id for product in recovered.current_products} == {
        "P001", "P002", "P003",
    }

    recovered.handle(ProcessEvent(EventType.PROCESS_START, 2))

    assert recovered.awaiting_reconciliation is False
    assert recovered.current_product.run_id == run_id
    assert len(store.products_for_run("TEST_LINE", run_id)) == 3

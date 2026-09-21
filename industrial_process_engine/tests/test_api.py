import logging

from fastapi.testclient import TestClient

from industrial_process_engine.api.app import create_app
from industrial_process_engine.domain import (
    EventType, ProcessEvent, ProcessTimeRecord, SignalUpdate, WindowQuality,
)
from industrial_process_engine.engine import ProcessEngine


def test_engine_logs_startup_and_shutdown(config_factory, caplog):
    engine = ProcessEngine(config_factory(), sqlite_path=config_factory.sqlite_path)

    with caplog.at_level(logging.INFO, logger="industrial_process_engine.engine"):
        engine.start()
        engine.graceful_shutdown()

    messages = [record.getMessage() for record in caplog.records]
    assert any("Engine started process_id=TEST_LINE" in message for message in messages)
    assert any("Engine stopped process_id=TEST_LINE" in message for message in messages)


def test_health_status_and_queued_control(config_factory):
    runtime = ProcessEngine(config_factory(), sqlite_path=config_factory.sqlite_path)
    runtime.start()
    try:
        client = TestClient(create_app(runtime))
        assert client.get("/openapi.json").json()["info"] == {
            "title": "Test L2",
            "description": "Test material aggregation service",
            "version": "0.1.0",
        }
        assert client.get("/health/live").status_code == 200
        ready = client.get("/health/ready")
        assert ready.status_code == 200
        assert ready.json()["inputs_ok"] is True
        status = client.get("/api/v1/status").json()
        assert status["process_id"] == "TEST_LINE"
        assert status["service_state"] == "RUNNING"
        assert status["mqtt_enabled"] is False
        assert status["opcua_enabled"] is False
        assert status["observed_ts"] >= status["service_started_ts"]
        assert status["uptime_s"] >= 0
        assert status["production_state"] == "IDLE"
        assert status["in_process_product_count"] == 0
        assert status["metrics"] == {}
        assert client.get("/api/v1/runs/current").status_code == 404
        assert client.get("/api/v1/processes/current").status_code == 404
        assert client.post("/api/v1/control/pause").status_code == 202
        runtime.queue.join()
        assert client.get("/api/v1/status").json()["service_state"] == "PAUSED"
        assert client.post("/api/v1/control/resume").status_code == 202
        runtime.queue.join()
        assert client.get("/api/v1/status").json()["service_state"] == "RUNNING"
    finally:
        runtime.graceful_shutdown()


def test_status_metrics_alias_units_and_bad_quality(config_factory):
    raw = config_factory().model_dump()
    raw["mappings"][1]["unit"] = "°C"
    raw["status"] = {"metrics": {"temperature_live": "temperature", "lot": "product_id"}}
    config = type(config_factory()).model_validate(raw)
    runtime = ProcessEngine(config, sqlite_path=config_factory.sqlite_path)
    runtime.store.initialize()
    runtime.processor.start()

    assert runtime.status()["metrics"] == {
        "temperature_live": {"value": None, "unit": "°C"},
        "lot": {"value": None},
    }
    runtime.processor.handle(SignalUpdate("temperature", 850.0, True, 1, "temp", "test"))
    runtime.processor.handle(SignalUpdate("product_id", "COIL-1", True, 1, "pid", "test"))
    assert runtime.status()["metrics"] == {
        "temperature_live": {"value": 850.0, "unit": "°C"},
        "lot": {"value": "COIL-1"},
    }
    runtime.processor.handle(SignalUpdate("temperature", None, False, 2, "temp", "test"))
    assert runtime.status()["metrics"]["temperature_live"]["value"] is None


def test_status_counts_draining_products_as_processing(config_factory):
    raw = config_factory().model_dump()
    raw["spatial"] = {
        "origin": "entry", "line_length_m": 5.0, "stations": {},
    }
    config = type(config_factory()).model_validate(raw)
    runtime = ProcessEngine(config, sqlite_path=config_factory.sqlite_path)
    runtime.store.initialize()
    runtime.processor.start()
    runtime.processor.handle(SignalUpdate("position", 0.0, True, 0, "pos", "test"))
    runtime.processor.handle(SignalUpdate("product_id", "COIL-1", True, 1, "pid", "test"))
    runtime.processor.handle(ProcessEvent(EventType.PROCESS_START, 1))
    runtime.processor.handle(SignalUpdate("position", 2.0, True, 2, "pos", "test"))
    runtime.processor.handle(ProcessEvent(EventType.PROCESS_END, 3))

    status = runtime.status()
    assert status["current_run"] is None
    assert status["spatial"]["products"][0]["state"] == "DRAINING"
    assert status["production_state"] == "PROCESSING"
    assert status["in_process_product_count"] == 1


def test_products_are_addressed_by_run_id_and_filterable_by_product_id(config_factory):
    runtime = ProcessEngine(config_factory(), sqlite_path=config_factory.sqlite_path)
    runtime.store.initialize()
    runtime.processor.start()
    runtime.processor.handle(SignalUpdate("position", 0, True, 0, "position", "test"))
    runtime.processor.handle(SignalUpdate("product_id", "REWORK", True, 1, "product_id", "test"))
    runtime.processor.handle(ProcessEvent(EventType.PROCESS_START, 2))
    run_id = runtime.processor.current_product.run_id
    runtime.processor.handle(ProcessEvent(EventType.PROCESS_END, 3))
    client = TestClient(create_app(runtime))

    detail = client.get(f"/api/v1/runs/{run_id}")
    assert detail.status_code == 200
    assert [product["product_id"] for product in detail.json()["products"]] == ["REWORK"]
    assert detail.json()["run_id"] == run_id
    assert [row["run_id"] for row in client.get(
        "/api/v1/products", params={"product_id": "REWORK"},
    ).json()] == [run_id]
    assert client.get(f"/api/v1/processes/{run_id}").status_code == 404


def test_local_storage_reset_requires_paused_idle_service_and_recreates_tables(config_factory):
    runtime = ProcessEngine(config_factory(), sqlite_path=config_factory.sqlite_path)
    runtime.start()
    try:
        client = TestClient(create_app(runtime))
        assert client.delete("/api/v1/admin/local-storage").status_code == 409

        runtime.enqueue(SignalUpdate("position", 0, True, 0, "position", "test"))
        runtime.enqueue(SignalUpdate("product_id", "RESET-ME", True, 1, "product_id", "test"))
        runtime.enqueue(ProcessEvent(EventType.PROCESS_START, 2))
        runtime.enqueue(ProcessEvent(EventType.PROCESS_END, 3))
        runtime.queue.join()
        assert runtime.store.list_products()
        assert runtime.store.list_events()

        client.post("/api/v1/control/pause")
        runtime.queue.join()
        response = client.delete("/api/v1/admin/local-storage")
        assert response.status_code == 200
        assert response.json() == {"status": "reset", "storage": "sqlite"}
        assert runtime.sink._initialized is False
        assert runtime.processor.signal_state == {}
        assert runtime.processor.position and runtime.processor.position.raw_position is None

        expected_tables = {
            "product_data", "process_data_time", "product_summary", "product_relation",
            "process_run", "checkpoint", "process_time_checkpoint", "signal_state", "event_log",
        }
        with runtime.store.connection() as db:
            tables = {
                row[0] for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            }
            assert tables == expected_tables
            assert all(db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0 for table in tables)

        client.post("/api/v1/control/resume")
        runtime.queue.join()
        runtime.enqueue(SignalUpdate("product_id", "ACTIVE", True, 4, "product_id", "test"))
        runtime.enqueue(ProcessEvent(EventType.PROCESS_START, 5))
        runtime.queue.join()
        client.post("/api/v1/control/pause")
        runtime.queue.join()
        rejected = client.delete("/api/v1/admin/local-storage")
        assert rejected.status_code == 409
        assert "active" in rejected.json()["detail"]
    finally:
        runtime.graceful_shutdown()


def test_process_time_endpoint_requires_range_and_does_not_add_product_context(config_factory):
    engine = ProcessEngine(config_factory(), sqlite_path=config_factory.sqlite_path)
    engine.store.initialize()
    engine.store.persist_process_time(ProcessTimeRecord(
        "TEST_LINE", 10_000, 15_000, {}, WindowQuality.DATA_GAP,
    ))
    client = TestClient(create_app(engine))

    assert client.get("/api/v1/process-data/time").status_code == 422
    assert client.get(
        "/api/v1/process-data/time", params={"from_ts": 20_000, "to_ts": 10_000},
    ).status_code == 422
    rows = client.get(
        "/api/v1/process-data/time", params={"from_ts": 0, "to_ts": 20_000, "limit": 10},
    ).json()
    assert len(rows) == 1
    assert "product_id" not in rows[0]
    assert "run_id" not in rows[0]

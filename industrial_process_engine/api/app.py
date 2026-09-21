from __future__ import annotations

import time

from fastapi import FastAPI, HTTPException, Query, Response, status
from pydantic import BaseModel, Field

from industrial_process_engine.domain import CommandType, EventType, ProcessEvent, ServiceState
from industrial_process_engine.engine import ProcessEngine, StorageResetRejected


class ProductLifecycleRequest(BaseModel):
    timestamp_ms: int | None = Field(default=None, ge=0)
    context: dict[str, object] = Field(default_factory=dict)
    parent_product_ids: list[str] = Field(default_factory=list)


class ProcessLifecycleRequest(BaseModel):
    timestamp_ms: int | None = Field(default=None, ge=0)


def create_app(engine: ProcessEngine) -> FastAPI:
    metadata = engine.config.service
    app = FastAPI(
        title=metadata.title,
        description=metadata.description,
        version=metadata.version,
    )

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/health/ready")
    def ready(response: Response) -> dict[str, object]:
        sqlite_ok = engine.store.healthcheck()
        inputs_ok = engine.inputs_ready and not engine.waiting_for_initial_inputs
        is_ready = (
            sqlite_ok and inputs_ok
            and engine.processor.service_state not in {ServiceState.STARTING, ServiceState.ERROR}
        )
        if not is_ready:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "ready" if is_ready else "not_ready", "sqlite_ok": sqlite_ok,
            "inputs_ok": inputs_ok, "datasources": engine.datasource_status(),
        }

    @app.get("/api/v1/status")
    def service_status() -> dict[str, object]:
        return engine.status()

    @app.get("/api/v1/runs/current")
    def current_run() -> dict[str, object]:
        value = service_status()["current_run"]
        if value is None:
            raise HTTPException(status_code=404, detail="No active process")
        return value

    @app.get("/api/v1/products")
    def products(
        state: str | None = None,
        sync_state: str | None = None,
        product_id: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> list[dict[str, object]]:
        return engine.store.list_products(state, sync_state, product_id, limit)

    @app.get("/api/v1/runs")
    def runs(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, object]]:
        return engine.store.list_runs(engine.config.process_id, limit)

    @app.get("/api/v1/runs/{run_id}")
    def process(run_id: str) -> dict[str, object]:
        value = engine.store.get_process(engine.config.process_id, run_id)
        if value is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return value

    @app.get("/api/v1/process-data/time")
    def process_data_time(
        from_ts: int = Query(ge=0), to_ts: int = Query(ge=0),
        limit: int = Query(default=1000, ge=1, le=10_000),
    ) -> list[dict[str, object]]:
        if to_ts <= from_ts:
            raise HTTPException(status_code=422, detail="to_ts must be greater than from_ts")
        return engine.store.list_process_time(engine.config.process_id, from_ts, to_ts, limit)

    @app.post("/api/v1/runs/start", status_code=202)
    def start_run(request: ProcessLifecycleRequest) -> dict[str, str]:
        engine.enqueue(ProcessEvent(
            EventType.PROCESS_START,
            request.timestamp_ms or time.time_ns() // 1_000_000,
            source="api",
        ))
        return {"status": "accepted", "event": "PROCESS_START"}

    @app.post("/api/v1/runs/current/end", status_code=202)
    def end_run(request: ProcessLifecycleRequest) -> dict[str, str]:
        engine.enqueue(ProcessEvent(
            EventType.PROCESS_END,
            request.timestamp_ms or time.time_ns() // 1_000_000,
            source="api",
        ))
        return {"status": "accepted", "event": "PROCESS_END"}

    @app.post("/api/v1/products/{product_id}/enter", status_code=202)
    def product_enter(product_id: str, request: ProductLifecycleRequest) -> dict[str, str]:
        engine.product_enter(
            product_id, request.context, request.timestamp_ms, request.parent_product_ids,
        )
        return {"status": "accepted", "product_id": product_id, "event": "PRODUCT_ENTER"}

    @app.post("/api/v1/products/{product_id}/update", status_code=202)
    def product_update(product_id: str, request: ProductLifecycleRequest) -> dict[str, str]:
        engine.product_update(product_id, request.context, request.timestamp_ms)
        return {"status": "accepted", "product_id": product_id, "event": "PRODUCT_UPDATE"}

    @app.post("/api/v1/products/{product_id}/exit", status_code=202)
    def product_exit(product_id: str, request: ProductLifecycleRequest) -> dict[str, str]:
        engine.product_exit(product_id, request.timestamp_ms)
        return {"status": "accepted", "product_id": product_id, "event": "PRODUCT_EXIT"}

    @app.post("/api/v1/products/{product_id}/abort", status_code=202)
    def product_abort(product_id: str, request: ProductLifecycleRequest) -> dict[str, str]:
        engine.product_abort(product_id, request.timestamp_ms)
        return {"status": "accepted", "product_id": product_id, "event": "PRODUCT_ABORT"}

    @app.get("/api/v1/events")
    def events(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, object]]:
        return engine.store.list_events(limit)

    def enqueue_control(command: CommandType) -> dict[str, str]:
        engine.command(command)
        return {"status": "accepted", "command": str(command)}

    @app.post("/api/v1/control/pause", status_code=202)
    def pause() -> dict[str, str]:
        return enqueue_control(CommandType.PAUSE)

    @app.post("/api/v1/control/resume", status_code=202)
    def resume() -> dict[str, str]:
        return enqueue_control(CommandType.RESUME)

    @app.post("/api/v1/control/stop", status_code=202)
    def stop() -> dict[str, str]:
        return enqueue_control(CommandType.STOP)

    @app.post("/api/v1/control/restart", status_code=202)
    def restart() -> dict[str, str]:
        return enqueue_control(CommandType.RESTART)

    @app.post("/api/v1/runs/{run_id}/retry-sync", status_code=202)
    def retry_sync(run_id: str) -> dict[str, str]:
        if not engine.retry_run(run_id):
            raise HTTPException(status_code=409, detail="Only completed runs can be retried")
        return {"status": "accepted", "run_id": run_id}

    @app.post("/api/v1/sync/retry-all", status_code=202)
    def retry_all() -> dict[str, int | str]:
        count = engine.store.retry_all(engine.config.process_id)
        engine.sync_worker.wake()
        return {"status": "accepted", "products": count}

    @app.delete("/api/v1/admin/local-storage")
    def reset_local_storage() -> dict[str, str]:
        try:
            engine.reset_local_storage()
        except StorageResetRejected as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except TimeoutError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error
        return {"status": "reset", "storage": "sqlite"}

    return app

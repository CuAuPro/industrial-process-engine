from __future__ import annotations

from collections.abc import Callable
import logging
from typing import Any

from industrial_process_engine.domain import EventType, ProcessEvent, ProductState
from .base import ProcessModelController

log = logging.getLogger("industrial_process_engine.processing.process_processor")


class TransformationModelController(ProcessModelController):
    name = "transformation"
    close_on_last_product_exit = False

    def __init__(self) -> None:
        self.run_id: str | None = None
        self.start_ts: int | None = None

    @property
    def active_run_id(self) -> str | None:
        return self.run_id

    @property
    def active_start_ts(self) -> int | None:
        return self.start_ts

    def begin(self, timestamp_ms: int, run_id_factory: Callable[[int], str]) -> str:
        if self.run_id is not None:
            raise ValueError("A transformation run is already active")
        self.run_id = run_id_factory(timestamp_ms)
        self.start_ts = timestamp_ms
        return self.run_id

    def finish(self) -> str | None:
        run_id = self.run_id
        self.run_id = None
        self.start_ts = None
        return run_id

    def persist_product(
        self, store: Any, process_id: str, product: Any,
        parent_product_ids: tuple[str, ...], timestamp_ms: int,
    ) -> None:
        store.start_product_with_relations(
            process_id, product, parent_product_ids, timestamp_ms,
        )

    def snapshot(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "start_ts": self.start_ts}

    def restore(self, payload: dict[str, Any]) -> None:
        self.run_id = payload.get("run_id")
        self.start_ts = payload.get("start_ts")

    def status(self) -> dict[str, Any]:
        return {"genealogy": True, "close_run": "process_end"}

    def handle_run_event(self, processor: Any, event: ProcessEvent) -> bool:
        if event.event_type == EventType.PROCESS_START:
            self._start(processor, event)
            return True
        if event.event_type in {EventType.PROCESS_END, EventType.PROCESS_ABORT}:
            self._end(processor, event)
            return True
        return False

    def _start(self, processor: Any, event: ProcessEvent) -> None:
        try:
            run_id = self.begin(event.timestamp_ms, processor.run_id_factory)
            processor.store.start_run(processor.config.process_id, run_id, event.timestamp_ms)
        except Exception as error:
            rejected_run_id = self.run_id
            if self.run_id is not None:
                self.finish()
            processor.store.log_event(
                event.timestamp_ms, processor.config.process_id, "PROCESS_START_REJECTED",
                str(error)[:500], None, rejected_run_id,
            )
            return
        processor.participant_ids.clear()
        processor.store.log_event(
            event.timestamp_ms, processor.config.process_id, "PROCESS_START",
            "Transformation run started", None, run_id,
        )
        log.info("Started transformation run run_id=%s", run_id)
        processor.checkpoint(event.timestamp_ms)

    def _end(self, processor: Any, event: ProcessEvent) -> None:
        run_id = self.run_id
        if run_id is None:
            processor.store.log_event(
                event.timestamp_ms, processor.config.process_id, "PROCESS_END_IGNORED",
                "No transformation run is active",
            )
            return
        aborted = event.event_type == EventType.PROCESS_ABORT
        for product in tuple(processor.current_products):
            processor._exit_product(ProcessEvent(
                EventType.PRODUCT_ABORT if aborted else EventType.PRODUCT_EXIT,
                event.timestamp_ms, product.product_id, source=event.source,
            ), product.product_id, aborted)
        if aborted:
            processor.store.fail_process(
                processor.config.process_id, run_id, ProductState.ABORTED,
                event.timestamp_ms, "Transformation aborted",
            )
        else:
            processor.store.complete_run(processor.config.process_id, run_id, event.timestamp_ms)
        processor.product_fields.clear_run(run_id)
        self.finish()
        processor.participant_ids.clear()
        processor.store.delete_checkpoint(processor.config.process_id)
        processor.store.log_event(
            event.timestamp_ms, processor.config.process_id, str(event.event_type),
            "Transformation run ended", None, run_id,
        )
        log.info("Ended transformation run run_id=%s event=%s", run_id, event.event_type)

    def validate_enter(
        self, product_id: str, parent_product_ids: tuple[str, ...],
        active_product_count: int, parent_exists: Callable[[str], bool],
    ) -> tuple[str, ...]:
        del active_product_count
        if self.run_id is None:
            raise ValueError("PROCESS_START is required before PRODUCT_ENTER")
        if len(parent_product_ids) != len(set(parent_product_ids)):
            raise ValueError("Duplicate parent_product_ids are not allowed")
        if product_id in parent_product_ids:
            raise ValueError("A product cannot be its own parent")
        missing = [parent for parent in parent_product_ids if not parent_exists(parent)]
        if missing:
            raise ValueError(f"Unknown parent products in current run: {missing}")
        return parent_product_ids

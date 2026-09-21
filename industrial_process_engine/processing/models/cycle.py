from __future__ import annotations

from collections.abc import Callable
import logging
from typing import Any

from industrial_process_engine.domain import EventType, ProcessEvent, ProductState

from .base import ProcessModelController

log = logging.getLogger("industrial_process_engine.processing.process_processor")


class CycleModelController(ProcessModelController):
    """Piece/batch cycle policy with explicit membership and run closure semantics."""

    name = "cycle"

    def __init__(self, membership: str, close_run: str) -> None:
        self.membership = membership
        self.close_run = close_run
        self._run_id: str | None = None
        self._start_ts: int | None = None

    @property
    def close_on_last_product_exit(self) -> bool:
        return self.close_run == "last_product_exit"

    @property
    def active_run_id(self) -> str | None:
        return self._run_id

    @property
    def active_start_ts(self) -> int | None:
        return self._start_ts

    def validate_enter(
        self, product_id: str, parent_product_ids: tuple[str, ...],
        active_product_count: int, parent_exists: Callable[[str], bool],
    ) -> tuple[str, ...]:
        if self.membership == "single" and active_product_count:
            raise ValueError("A single-membership cycle can contain only one active product")
        if self.close_run == "process_end" and self._run_id is None:
            raise ValueError("PROCESS_START is required before PRODUCT_ENTER")
        return super().validate_enter(
            product_id, parent_product_ids, active_product_count, parent_exists,
        )

    def handle_run_event(self, processor: Any, event: ProcessEvent) -> bool:
        if self.close_run != "process_end":
            return False
        if event.event_type == EventType.PROCESS_START:
            self._start(processor, event)
            return True
        if event.event_type in {EventType.PROCESS_END, EventType.PROCESS_ABORT}:
            self._end(processor, event)
            return True
        return False

    def _start(self, processor: Any, event: ProcessEvent) -> None:
        if self._run_id is not None:
            processor.store.log_event(
                event.timestamp_ms, processor.config.process_id, "PROCESS_START_REJECTED",
                "A cycle run is already active", None, self._run_id,
            )
            return
        run_id = processor.run_id_factory(event.timestamp_ms)
        try:
            processor.store.start_run(processor.config.process_id, run_id, event.timestamp_ms)
        except Exception as error:
            processor.store.log_event(
                event.timestamp_ms, processor.config.process_id, "PROCESS_START_REJECTED",
                str(error)[:500], None, run_id,
            )
            return
        self._run_id = run_id
        self._start_ts = event.timestamp_ms
        processor.participant_ids.clear()
        processor.store.log_event(
            event.timestamp_ms, processor.config.process_id, "PROCESS_START",
            "Cycle run started", None, run_id,
        )
        log.info("Started cycle run run_id=%s", run_id)
        processor.checkpoint(event.timestamp_ms)

    def _end(self, processor: Any, event: ProcessEvent) -> None:
        run_id = self._run_id
        if run_id is None:
            processor.store.log_event(
                event.timestamp_ms, processor.config.process_id, "PROCESS_END_IGNORED",
                "No cycle run is active",
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
                event.timestamp_ms, "Cycle aborted",
            )
        else:
            processor.store.complete_run(processor.config.process_id, run_id, event.timestamp_ms)
        processor.product_fields.clear_run(run_id)
        self._run_id = None
        self._start_ts = None
        processor.participant_ids.clear()
        processor.store.delete_checkpoint(processor.config.process_id)
        processor.store.log_event(
            event.timestamp_ms, processor.config.process_id, str(event.event_type),
            "Cycle run ended", None, run_id,
        )
        log.info("Ended cycle run run_id=%s event=%s", run_id, event.event_type)

    def snapshot(self) -> dict[str, Any]:
        return {"run_id": self._run_id, "start_ts": self._start_ts}

    def restore(self, payload: dict[str, Any]) -> None:
        self._run_id = payload.get("run_id")
        self._start_ts = payload.get("start_ts")

    def status(self) -> dict[str, Any]:
        return {"membership": self.membership, "close_run": self.close_run}

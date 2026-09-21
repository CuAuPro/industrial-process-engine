from __future__ import annotations

from collections.abc import Callable
from typing import Any


class ProcessModelController:
    """Model-specific lifecycle policy composed by the common processor."""

    name = "base"
    close_on_last_product_exit = True

    @property
    def active_run_id(self) -> str | None:
        return None

    @property
    def active_start_ts(self) -> int | None:
        return None

    def handle_run_event(self, processor: Any, event: Any) -> bool:
        """Handle a model-owned run boundary and report whether it was consumed."""
        del processor, event
        return False

    def validate_enter(
        self, product_id: str, parent_product_ids: tuple[str, ...],
        active_product_count: int, parent_exists: Callable[[str], bool],
    ) -> tuple[str, ...]:
        del product_id, active_product_count, parent_exists
        if parent_product_ids:
            raise ValueError("parent_product_ids require a transformation process")
        return ()

    def persist_product(
        self, store: Any, process_id: str, product: Any,
        parent_product_ids: tuple[str, ...], timestamp_ms: int,
    ) -> None:
        del timestamp_ms
        if parent_product_ids:
            raise ValueError("parent_product_ids require a transformation process")
        store.start_product(process_id, product)

    def snapshot(self) -> dict[str, Any]:
        return {}

    def restore(self, payload: dict[str, Any]) -> None:
        del payload

    def status(self) -> dict[str, Any]:
        return {}

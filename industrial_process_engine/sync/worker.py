from __future__ import annotations

import logging
import threading
import time

from industrial_process_engine.config import AppConfig
from industrial_process_engine.domain import SyncState
from industrial_process_engine.storage.questdb import QuestDBSink
from industrial_process_engine.storage.sqlite import SQLiteStore

log = logging.getLogger(__name__)


class SyncWorker:
    def __init__(self, config: AppConfig, store: SQLiteStore, sink: QuestDBSink) -> None:
        self.config = config
        self.store = store
        self.sink = sink
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if not self.config.questdb.enabled:
            return
        self._thread = threading.Thread(target=self._run, name="questdb-sync", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=10)

    def wake(self) -> None:
        self._wake.set()

    def _run(self) -> None:
        backoff = self.config.questdb.retry_interval_s
        while not self._stop.is_set():
            failed = False
            time_rows = self.store.pending_process_time(self.config.process_id)
            if time_rows:
                try:
                    self.sink.upload_process_time(time_rows)
                    self.store.set_process_time_sync_state(
                        self.config.process_id,
                        [int(row["ts_start"]) for row in time_rows], SyncState.SYNCED,
                    )
                except Exception as exc:
                    failed = True
                    message = str(exc)[:500]
                    self.store.set_process_time_sync_state(
                        self.config.process_id,
                        [int(row["ts_start"]) for row in time_rows], SyncState.FAILED, message,
                    )
                    log.exception("QuestDB process-time sync failed")
            pending = self.store.pending_runs(self.config.process_id)
            run_ids = [run["run_id"] for run in pending]
            for run_id in run_ids:
                if self._stop.is_set():
                    return
                products = self.store.products_for_run(self.config.process_id, run_id)
                product_ids = ",".join(product["product_id"] for product in products)
                try:
                    windows = self.store.windows_for_run(self.config.process_id, run_id)
                    relations = self.store.relations_for_run(self.config.process_id, run_id)
                    self.sink.upload(windows, products, relations)
                    self.store.set_sync_state(self.config.process_id, run_id, SyncState.SYNCED)
                    self.store.log_event(
                        time.time_ns() // 1_000_000, self.config.process_id,
                        "QUESTDB_SYNCED", "Process synchronized", None, run_id,
                    )
                    backoff = self.config.questdb.retry_interval_s
                except Exception as exc:
                    failed = True
                    message = str(exc)[:500]
                    self.store.set_sync_state(self.config.process_id, run_id, SyncState.FAILED, message)
                    self.store.log_event(
                        time.time_ns() // 1_000_000, self.config.process_id,
                        "QUESTDB_SYNC_FAILED", message, None, run_id,
                    )
                    log.exception("QuestDB sync failed for run %s (products %s)", run_id, product_ids)
                    break
            wait_for = backoff if failed else self.config.questdb.retry_interval_s
            if failed:
                backoff = min(backoff * 2, self.config.questdb.max_backoff_s)
            self._wake.wait(wait_for)
            self._wake.clear()

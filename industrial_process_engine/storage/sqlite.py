from __future__ import annotations

import json
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from industrial_process_engine.domain import (
    ProcessTimeRecord, ProductContext, ProductState, SyncState, WindowRecord,
)


class SQLiteStore:
    SQLITE_TYPES = {
        "double": "REAL", "int": "INTEGER", "long": "INTEGER",
        "symbol": "TEXT", "varchar": "TEXT", "char": "TEXT",
    }

    def __init__(
        self, path: str, product_schema: dict[str, str], summary_schema: dict[str, str] | None = None,
        process_time_schema: dict[str, str] | None = None,
    ) -> None:
        self.path = Path(path)
        self.product_schema = product_schema
        self.summary_schema = summary_schema or {}
        self.process_time_schema = process_time_schema or {}
        self.product_variables = list(product_schema)
        self.summary_variables = list(self.summary_schema)
        self.process_time_variables = list(self.process_time_schema)
        for identifier, value_type in {
            **product_schema, **self.summary_schema, **self.process_time_schema,
        }.items():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
                raise ValueError(f"unsafe SQL identifier: {identifier}")
            if value_type not in self.SQLITE_TYPES:
                raise ValueError(f"unsupported SQLite output type: {value_type}")

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _dynamic_columns(schema: dict[str, str], types: dict[str, str]) -> str:
        return "".join(f',\n                    "{name}" {types[value_type]}' for name, value_type in schema.items())

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        summary_columns = self._dynamic_columns(self.summary_schema, self.SQLITE_TYPES)
        product_columns = self._dynamic_columns(self.product_schema, self.SQLITE_TYPES)
        process_time_columns = self._dynamic_columns(self.process_time_schema, self.SQLITE_TYPES)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(f"""
                CREATE TABLE IF NOT EXISTS process_run (
                    process_id TEXT NOT NULL, run_id TEXT NOT NULL, start_ts INTEGER NOT NULL,
                    end_ts INTEGER, drained_ts INTEGER, material_length_m REAL,
                    state TEXT NOT NULL, sync_state TEXT NOT NULL,
                    sync_blocked INTEGER NOT NULL DEFAULT 0,
                    error_message TEXT, PRIMARY KEY(process_id, run_id)
                );
                CREATE TABLE IF NOT EXISTS product_summary (
                    run_id TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    process_id TEXT NOT NULL,
                    start_ts INTEGER NOT NULL,
                    end_ts INTEGER,
                    drained_ts INTEGER,
                    material_length_m REAL,
                    processing_time_s REAL,
                    start_mode TEXT NOT NULL DEFAULT 'normal',
                    state TEXT NOT NULL,
                    sync_state TEXT NOT NULL,
                    error_message TEXT{summary_columns},
                    PRIMARY KEY (process_id, run_id, product_id)
                );
                CREATE TABLE IF NOT EXISTS product_data (
                    process_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    segment_no INTEGER NOT NULL,
                    window_no INTEGER NOT NULL,
                    axis TEXT NOT NULL,
                    ts_start INTEGER NOT NULL,
                    ts_end INTEGER NOT NULL,
                    elapsed_start_s REAL NOT NULL,
                    elapsed_end_s REAL NOT NULL,
                    position_start_m REAL,
                    position_end_m REAL{product_columns},
                    quality TEXT NOT NULL,
                    PRIMARY KEY (process_id, run_id, product_id, segment_no, window_no),
                    FOREIGN KEY (process_id, run_id, product_id)
                        REFERENCES product_summary(process_id, run_id, product_id)
                );
                CREATE TABLE IF NOT EXISTS process_data_time (
                    process_id TEXT NOT NULL,
                    ts_start INTEGER NOT NULL,
                    ts_end INTEGER NOT NULL,
                    quality TEXT NOT NULL,
                    sync_state TEXT NOT NULL DEFAULT 'PENDING',
                    error_message TEXT{process_time_columns},
                    PRIMARY KEY(process_id, ts_start)
                );
                CREATE TABLE IF NOT EXISTS product_relation (
                    process_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    parent_product_id TEXT NOT NULL,
                    child_product_id TEXT NOT NULL,
                    created_ts INTEGER NOT NULL,
                    PRIMARY KEY(process_id,run_id,parent_product_id,child_product_id),
                    FOREIGN KEY(process_id,run_id,parent_product_id)
                        REFERENCES product_summary(process_id,run_id,product_id),
                    FOREIGN KEY(process_id,run_id,child_product_id)
                        REFERENCES product_summary(process_id,run_id,product_id)
                );
                CREATE TABLE IF NOT EXISTS checkpoint (
                    process_id TEXT PRIMARY KEY,
                    run_id TEXT,
                    product_id TEXT,
                    payload TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS process_time_checkpoint (
                    process_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS signal_state (
                    process_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    value TEXT,
                    quality INTEGER NOT NULL,
                    timestamp_ms INTEGER NOT NULL,
                    PRIMARY KEY (process_id, name)
                );
                CREATE TABLE IF NOT EXISTS event_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    process_id TEXT NOT NULL,
                    run_id TEXT,
                    product_id TEXT,
                    event_type TEXT NOT NULL,
                    message TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_run_sync ON process_run(process_id,state,sync_state);
                CREATE INDEX IF NOT EXISTS ix_summary_state ON product_summary(state, sync_state);
                CREATE INDEX IF NOT EXISTS ix_summary_product ON product_summary(process_id, product_id, start_ts DESC);
                CREATE INDEX IF NOT EXISTS ix_event_ts ON event_log(ts DESC);
                CREATE INDEX IF NOT EXISTS ix_event_run ON event_log(process_id, run_id, ts DESC);
                CREATE INDEX IF NOT EXISTS ix_process_time_pending
                    ON process_data_time(process_id,sync_state,ts_start);
            """)
    def reset(self) -> None:
        """Drop and recreate every local service table."""
        with self.connection() as db:
            try:
                db.executescript("""
                    BEGIN IMMEDIATE;
                    DROP TABLE IF EXISTS product_data;
                    DROP TABLE IF EXISTS process_data_time;
                    DROP TABLE IF EXISTS product_relation;
                    DROP TABLE IF EXISTS product_summary;
                    DROP TABLE IF EXISTS process_run;
                    DROP TABLE IF EXISTS checkpoint;
                    DROP TABLE IF EXISTS process_time_checkpoint;
                    DROP TABLE IF EXISTS signal_state;
                    DROP TABLE IF EXISTS event_log;
                    COMMIT;
                """)
            except Exception:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise
        self.initialize()

    def start_product(
        self, process_id: str, product: ProductContext, *, start_mode: str = "normal",
    ) -> None:
        self.start_products(process_id, (product,), start_mode=start_mode)

    def start_product_with_relations(
        self, process_id: str, product: ProductContext,
        parent_product_ids: tuple[str, ...], created_ts: int, *, start_mode: str = "normal",
    ) -> None:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(
                    "INSERT INTO product_summary"
                    "(run_id,product_id,process_id,start_ts,start_mode,state,sync_state) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (product.run_id, product.product_id, process_id, product.start_ts,
                     start_mode, ProductState.ACTIVE, SyncState.PENDING),
                )
                for parent_id in parent_product_ids:
                    parent = db.execute(
                        "SELECT 1 FROM product_summary WHERE process_id=? AND run_id=? AND product_id=?",
                        (process_id, product.run_id, parent_id),
                    ).fetchone()
                    if parent is None:
                        raise ValueError(f"parent product is not part of current run: {parent_id}")
                    db.execute(
                        "INSERT INTO product_relation(process_id,run_id,parent_product_id,child_product_id,created_ts) "
                        "VALUES(?,?,?,?,?)",
                        (process_id, product.run_id, parent_id, product.product_id, created_ts),
                    )
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise

    def start_run(self, process_id: str, run_id: str, start_ts: int) -> None:
        with self.connection() as db:
            db.execute(
                "INSERT INTO process_run(process_id,run_id,start_ts,state,sync_state) "
                "VALUES(?,?,?,?,?)",
                (process_id, run_id, start_ts, ProductState.ACTIVE, SyncState.PENDING),
            )

    def product_exists(self, process_id: str, run_id: str, product_id: str) -> bool:
        with self.connection() as db:
            return db.execute(
                "SELECT 1 FROM product_summary WHERE process_id=? AND run_id=? AND product_id=?",
                (process_id, run_id, product_id),
            ).fetchone() is not None

    def start_products(
        self, process_id: str, products: tuple[ProductContext, ...], *,
        sync_blocked: bool = False, start_mode: str = "normal",
    ) -> None:
        with self.connection() as db:
            first = products[0]
            db.execute(
                "INSERT OR IGNORE INTO process_run"
                "(process_id,run_id,start_ts,state,sync_state,sync_blocked) VALUES(?,?,?,?,?,?)",
                (
                    process_id, first.run_id, first.start_ts, ProductState.ACTIVE,
                    SyncState.PENDING, int(sync_blocked),
                ),
            )
            db.executemany(
                """INSERT INTO product_summary
                   (run_id,product_id,process_id,start_ts,start_mode,state,sync_state)
                   VALUES(?,?,?,?,?,?,?)""",
                [(
                    product.run_id, product.product_id, process_id, product.start_ts,
                    start_mode, ProductState.ACTIVE, SyncState.PENDING,
                ) for product in products],
            )

    def complete_run(self, process_id: str, run_id: str, end_ts: int) -> None:
        with self.connection() as db:
            db.execute(
                "UPDATE process_run SET end_ts=?,state=?,sync_state=? WHERE process_id=? AND run_id=?",
                (end_ts, ProductState.COMPLETE, SyncState.PENDING, process_id, run_id),
            )
            db.execute("DELETE FROM checkpoint WHERE process_id=?", (process_id,))

    def mark_process_draining(
        self, process_id: str, run_id: str, end_ts: int, material_length_m: float,
    ) -> None:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE product_summary SET end_ts=?,processing_time_s=(?-start_ts)/1000.0,"
                "material_length_m=?,state=? WHERE process_id=? AND run_id=? AND state=?",
                (end_ts, end_ts, material_length_m, ProductState.DRAINING,
                 process_id, run_id, ProductState.ACTIVE),
            )
            db.execute(
                "UPDATE process_run SET end_ts=?,material_length_m=?,state=? WHERE process_id=? AND run_id=?",
                (end_ts, material_length_m, ProductState.DRAINING, process_id, run_id),
            )
            db.execute("COMMIT")

    def complete_draining_process(
        self, process_id: str, run_id: str, drained_ts: int,
        summaries: dict[str, dict[str, Any | None]],
    ) -> None:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                for product_id, summary_values in summaries.items():
                    assignments = ["drained_ts=?", "state=?", "sync_state=?"]
                    values: list[Any] = [drained_ts, ProductState.COMPLETE, SyncState.PENDING]
                    for name in self.summary_variables:
                        assignments.append(f'"{name}"=?')
                        values.append(summary_values.get(name))
                    values.extend([process_id, run_id, product_id, ProductState.DRAINING])
                    cursor = db.execute(
                        f"UPDATE product_summary SET {','.join(assignments)} "
                        "WHERE process_id=? AND run_id=? AND product_id=? AND state=?", values,
                    )
                    if cursor.rowcount != 1:
                        raise ValueError(f"draining product not found: {run_id}/{product_id}")
                db.execute(
                    "UPDATE process_run SET drained_ts=?,state=?,sync_state=? WHERE process_id=? AND run_id=?",
                    (drained_ts, ProductState.COMPLETE, SyncState.PENDING, process_id, run_id),
                )
                db.execute("COMMIT")
            except Exception:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise

    def abort_draining_process(
        self, process_id: str, run_id: str, drained_ts: int, reason: str,
    ) -> None:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE product_summary SET drained_ts=?,state=?,error_message=? "
                "WHERE process_id=? AND run_id=? AND state=?",
                (drained_ts, ProductState.ABORTED, reason, process_id, run_id, ProductState.DRAINING),
            )
            db.execute(
                "UPDATE process_run SET drained_ts=?,state=?,error_message=? WHERE process_id=? AND run_id=?",
                (drained_ts, ProductState.ABORTED, reason, process_id, run_id),
            )
            db.execute("DELETE FROM checkpoint WHERE process_id=?", (process_id,))
            db.execute("COMMIT")

    def persist_window(self, record: WindowRecord) -> None:
        columns = [
            "process_id", "run_id", "product_id", "segment_no", "window_no", "axis", "ts_start", "ts_end",
            "elapsed_start_s", "elapsed_end_s", "position_start_m", "position_end_m",
            *self.product_variables, "quality",
        ]
        values = self._window_values(record)
        with self.connection() as db:
            db.execute(
                f"INSERT OR REPLACE INTO product_data({self._quoted(columns)}) VALUES({self._marks(columns)})",
                values,
            )

    def persist_process_time(self, record: ProcessTimeRecord) -> None:
        columns = [
            "process_id", "ts_start", "ts_end", *self.process_time_variables,
            "quality", "sync_state",
        ]
        values = [
            record.process_id, record.ts_start, record.ts_end,
            *(record.values.get(name) for name in self.process_time_variables),
            record.quality, SyncState.PENDING,
        ]
        with self.connection() as db:
            db.execute(
                f"INSERT OR IGNORE INTO process_data_time"
                f"({self._quoted(columns)}) VALUES({self._marks(columns)})",
                values,
            )

    def save_process_time_checkpoint(
        self, process_id: str, payload: dict[str, Any], updated_at: int,
    ) -> None:
        with self.connection() as db:
            db.execute(
                "INSERT INTO process_time_checkpoint(process_id,payload,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(process_id) DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at",
                (process_id, json.dumps(payload), updated_at),
            )

    def load_process_time_checkpoint(self, process_id: str) -> dict[str, Any] | None:
        with self.connection() as db:
            row = db.execute(
                "SELECT payload FROM process_time_checkpoint WHERE process_id=?", (process_id,),
            ).fetchone()
        return json.loads(row["payload"]) if row else None

    def list_process_time(
        self, process_id: str, from_ts: int, to_ts: int, limit: int = 1000,
    ) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM process_data_time WHERE process_id=? "
                "AND ts_start>=? AND ts_start<? ORDER BY ts_start LIMIT ?",
                (process_id, from_ts, to_ts, min(max(limit, 1), 10_000)),
            ).fetchall()
        return [dict(row) for row in rows]

    def pending_process_time(self, process_id: str, limit: int = 1000) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM process_data_time WHERE process_id=? AND sync_state!=? "
                "ORDER BY ts_start LIMIT ?",
                (process_id, SyncState.SYNCED, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def set_process_time_sync_state(
        self, process_id: str, timestamps: list[int], state: SyncState,
        error: str | None = None,
    ) -> None:
        if not timestamps:
            return
        marks = ",".join("?" for _ in timestamps)
        with self.connection() as db:
            db.execute(
                f"UPDATE process_data_time SET sync_state=?,error_message=? "
                f"WHERE process_id=? AND ts_start IN ({marks})",
                (state, error, process_id, *timestamps),
            )

    def add_product_relations(
        self, process_id: str, run_id: str, child_product_id: str,
        parent_product_ids: tuple[str, ...], created_ts: int,
    ) -> None:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                for parent_id in parent_product_ids:
                    parent = db.execute(
                        "SELECT 1 FROM product_summary WHERE process_id=? AND run_id=? AND product_id=?",
                        (process_id, run_id, parent_id),
                    ).fetchone()
                    if parent is None:
                        raise ValueError(f"parent product is not part of current run: {parent_id}")
                    db.execute(
                        "INSERT INTO product_relation(process_id,run_id,parent_product_id,child_product_id,created_ts) "
                        "VALUES(?,?,?,?,?)",
                        (process_id, run_id, parent_id, child_product_id, created_ts),
                    )
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise

    def complete_product(
        self, process_id: str, run_id: str, end_ts: int, partial: WindowRecord | None,
        summary_values: dict[str, Any | None],
    ) -> None:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            if partial is not None:
                columns = [
                    "process_id", "run_id", "product_id", "segment_no", "window_no", "axis", "ts_start", "ts_end",
                    "elapsed_start_s", "elapsed_end_s", "position_start_m", "position_end_m",
                    *self.product_variables, "quality",
                ]
                db.execute(
                    f"INSERT OR REPLACE INTO product_data({self._quoted(columns)}) VALUES({self._marks(columns)})",
                    self._window_values(partial),
                )
            assignments = ["end_ts=?", "processing_time_s=(?-start_ts)/1000.0", "state=?", "sync_state=?"]
            values: list[Any] = [end_ts, end_ts, ProductState.COMPLETE, SyncState.PENDING]
            for name in self.summary_variables:
                assignments.append(f'"{name}"=?')
                values.append(summary_values.get(name))
            values.extend([process_id, run_id, ProductState.ACTIVE])
            cursor = db.execute(
                f"UPDATE product_summary SET {','.join(assignments)} WHERE process_id=? AND run_id=? AND state=?",
                values,
            )
            if cursor.rowcount != 1:
                db.execute("ROLLBACK")
                raise ValueError(f"active run not found: {run_id}")
            db.execute("DELETE FROM checkpoint WHERE process_id=?", (process_id,))
            db.execute("COMMIT")

    def finalize_product(
        self, process_id: str, run_id: str, product_id: str, end_ts: int,
        summary_values: dict[str, Any | None], state: ProductState = ProductState.COMPLETE,
        reason: str | None = None,
    ) -> None:
        assignments = ["end_ts=?", "processing_time_s=(?-start_ts)/1000.0", "state=?", "error_message=?"]
        values: list[Any] = [end_ts, end_ts, state, reason]
        for name in self.summary_variables:
            assignments.append(f'"{name}"=?')
            values.append(summary_values.get(name))
        values.extend([process_id, run_id, product_id, ProductState.ACTIVE])
        with self.connection() as db:
            cursor = db.execute(
                f"UPDATE product_summary SET {','.join(assignments)} WHERE process_id=? AND run_id=? AND product_id=? AND state=?",
                values,
            )
            if cursor.rowcount != 1:
                raise ValueError(f"active product not found: {run_id}/{product_id}")

    def update_product_summary_values(
        self, process_id: str, run_id: str, product_id: str, values: dict[str, Any],
    ) -> None:
        selected = {name: values[name] for name in self.summary_variables if name in values}
        if not selected:
            return
        assignments = ",".join(f'"{name}"=?' for name in selected)
        with self.connection() as db:
            db.execute(
                f"UPDATE product_summary SET {assignments} WHERE process_id=? AND run_id=? AND product_id=?",
                [*selected.values(), process_id, run_id, product_id],
            )

    def complete_process(
        self, process_id: str, run_id: str, end_ts: int,
        partials: tuple[WindowRecord, ...],
        summaries: dict[str, dict[str, Any | None]],
    ) -> None:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                columns = [
                    "process_id", "run_id", "product_id", "segment_no", "window_no",
                    "axis", "ts_start", "ts_end", "elapsed_start_s",
                    "elapsed_end_s", "position_start_m", "position_end_m",
                    *self.product_variables, "quality",
                ]
                for partial in partials:
                    db.execute(
                        f"INSERT OR REPLACE INTO product_data({self._quoted(columns)}) "
                        f"VALUES({self._marks(columns)})",
                        self._window_values(partial),
                    )
                assignments = [
                    "end_ts=?", "processing_time_s=(?-start_ts)/1000.0",
                    "state=?", "sync_state=?",
                ]
                for name in self.summary_variables:
                    assignments.append(f'"{name}"=?')
                for product_id, summary_values in summaries.items():
                    values: list[Any] = [
                        end_ts, end_ts, ProductState.COMPLETE, SyncState.PENDING,
                        *(summary_values.get(name) for name in self.summary_variables),
                        process_id, run_id, product_id, ProductState.ACTIVE,
                    ]
                    cursor = db.execute(
                        f"UPDATE product_summary SET {','.join(assignments)} "
                        "WHERE process_id=? AND run_id=? AND product_id=? AND state=?",
                        values,
                    )
                    if cursor.rowcount != 1:
                        raise ValueError(f"active product not found: {run_id}/{product_id}")
                db.execute(
                    "UPDATE process_run SET end_ts=?,state=?,sync_state=? WHERE process_id=? AND run_id=?",
                    (end_ts, ProductState.COMPLETE, SyncState.PENDING, process_id, run_id),
                )
                db.execute("DELETE FROM checkpoint WHERE process_id=?", (process_id,))
                db.execute("COMMIT")
            except Exception:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise

    def fail_product(self, process_id: str, run_id: str, state: ProductState, end_ts: int, reason: str) -> None:
        if state not in {ProductState.ERROR, ProductState.ABORTED}:
            raise ValueError("failure state must be ERROR or ABORTED")
        with self.connection() as db:
            db.execute(
                """UPDATE product_summary SET end_ts=?,processing_time_s=(?-start_ts)/1000.0,
                   state=?,error_message=? WHERE process_id=? AND run_id=?""",
                (end_ts, end_ts, state, reason, process_id, run_id),
            )
            db.execute(
                "UPDATE process_run SET end_ts=?,state=?,error_message=? WHERE process_id=? AND run_id=?",
                (end_ts, state, reason, process_id, run_id),
            )
            db.execute("DELETE FROM checkpoint WHERE process_id=?", (process_id,))

    def fail_process(self, process_id: str, run_id: str, state: ProductState, end_ts: int, reason: str) -> None:
        self.fail_product(process_id, run_id, state, end_ts, reason)

    def save_checkpoint(
        self, process_id: str, run_id: str | None, product_id: str | None,
        payload: dict[str, Any], updated_at: int,
    ) -> None:
        encoded = json.dumps(payload, separators=(",", ":"))
        with self.connection() as db:
            db.execute(
                """INSERT INTO checkpoint(process_id,run_id,product_id,payload,updated_at) VALUES(?,?,?,?,?)
                   ON CONFLICT(process_id) DO UPDATE SET run_id=excluded.run_id,product_id=excluded.product_id,
                   payload=excluded.payload,updated_at=excluded.updated_at""",
                (process_id, run_id, product_id, encoded, updated_at),
            )

    def load_checkpoint(self, process_id: str) -> dict[str, Any] | None:
        with self.connection() as db:
            row = db.execute(
                "SELECT run_id,product_id,payload,updated_at FROM checkpoint WHERE process_id=?", (process_id,),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(row["payload"])
        payload["run_id"] = row["run_id"]
        payload["product_id"] = row["product_id"]
        payload["updated_at"] = row["updated_at"]
        return payload

    def delete_checkpoint(self, process_id: str) -> None:
        with self.connection() as db:
            db.execute("DELETE FROM checkpoint WHERE process_id=?", (process_id,))

    def save_signal_state(
        self, process_id: str, name: str, value: Any, quality: bool, timestamp_ms: int,
    ) -> None:
        encoded = json.dumps(value, separators=(",", ":"))
        with self.connection() as db:
            db.execute(
                """INSERT INTO signal_state(process_id,name,value,quality,timestamp_ms) VALUES(?,?,?,?,?)
                   ON CONFLICT(process_id,name) DO UPDATE SET value=excluded.value,
                   quality=excluded.quality,timestamp_ms=excluded.timestamp_ms
                   WHERE excluded.timestamp_ms >= signal_state.timestamp_ms""",
                (process_id, name, encoded, int(quality), timestamp_ms),
            )

    def load_signal_state(self, process_id: str) -> dict[str, dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT name,value,quality,timestamp_ms FROM signal_state WHERE process_id=?", (process_id,),
            ).fetchall()
        return {
            row["name"]: {
                "value": json.loads(row["value"]),
                "quality": bool(row["quality"]),
                "timestamp_ms": int(row["timestamp_ms"]),
            }
            for row in rows
        }

    def log_event(
        self, ts: int, process_id: str, event_type: str, message: str,
        product_id: str | None = None, run_id: str | None = None,
    ) -> None:
        with self.connection() as db:
            db.execute(
                "INSERT INTO event_log(ts,process_id,run_id,product_id,event_type,message) VALUES(?,?,?,?,?,?)",
                (ts, process_id, run_id, product_id, event_type, message),
            )

    def list_products(
        self, state: str | None = None, sync_state: str | None = None,
        product_id: str | None = None, limit: int = 100,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        if state:
            conditions.append("state=?")
            params.append(state)
        if sync_state:
            conditions.append("sync_state=?")
            params.append(sync_state)
        if product_id:
            conditions.append("product_id=?")
            params.append(product_id)
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(min(max(limit, 1), 1000))
        with self.connection() as db:
            rows = db.execute(
                f"SELECT * FROM product_summary{where} ORDER BY start_ts DESC LIMIT ?", params,
            ).fetchall()
        return [dict(row) for row in rows]

    def list_runs(self, process_id: str, limit: int = 100) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM process_run WHERE process_id=? ORDER BY start_ts DESC LIMIT ?",
                (process_id, min(max(limit, 1), 1000)),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_product(self, process_id: str, run_id: str, include_windows: bool = True) -> dict[str, Any] | None:
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM product_summary WHERE process_id=? AND run_id=?", (process_id, run_id),
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            if include_windows:
                result["windows"] = [dict(window) for window in db.execute(
                    "SELECT * FROM product_data WHERE process_id=? AND run_id=? ORDER BY segment_no,window_no",
                    (process_id, run_id),
                ).fetchall()]
        return result

    def get_process(self, process_id: str, run_id: str, include_windows: bool = True) -> dict[str, Any] | None:
        products = self.products_for_run(process_id, run_id)
        with self.connection() as db:
            run = db.execute("SELECT * FROM process_run WHERE process_id=? AND run_id=?", (process_id, run_id)).fetchone()
        if not products and run is None:
            return None
        result: dict[str, Any] = {
            "process_id": process_id,
            "run_id": run_id,
            "start_ts": run["start_ts"] if run else min(product["start_ts"] for product in products),
            "end_ts": run["end_ts"] if run else max(
                (product["end_ts"] for product in products if product["end_ts"] is not None), default=None,
            ),
            "drained_ts": run["drained_ts"] if run else max(
                (product["drained_ts"] for product in products if product["drained_ts"] is not None), default=None,
            ),
            "material_length_m": run["material_length_m"] if run else None,
            "state": run["state"] if run else products[0]["state"],
            "sync_state": run["sync_state"] if run else products[0]["sync_state"],
            "products": products,
        }
        if include_windows:
            result["windows"] = self.windows_for_run(process_id, run_id)
        with self.connection() as db:
            result["relations"] = [dict(row) for row in db.execute(
                "SELECT * FROM product_relation WHERE process_id=? AND run_id=? "
                "ORDER BY created_ts,parent_product_id,child_product_id",
                (process_id, run_id),
            ).fetchall()]
        return result

    def products_for_run(self, process_id: str, run_id: str) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM product_summary WHERE process_id=? AND run_id=? ORDER BY product_id",
                (process_id, run_id),
            ).fetchall()
        return [dict(row) for row in rows]

    def relations_for_run(self, process_id: str, run_id: str) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM product_relation WHERE process_id=? AND run_id=? "
                "ORDER BY created_ts,parent_product_id,child_product_id",
                (process_id, run_id),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_latest_product(
        self, process_id: str, product_id: str, include_windows: bool = True,
    ) -> dict[str, Any] | None:
        """Return the newest run for a repeatable business product ID."""
        with self.connection() as db:
            row = db.execute(
                """SELECT * FROM product_summary WHERE process_id=? AND product_id=?
                   ORDER BY start_ts DESC, run_id DESC LIMIT 1""",
                (process_id, product_id),
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            if include_windows:
                result["windows"] = self.windows_for_product(process_id, row["run_id"], product_id)
            return result

    def list_events(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM event_log ORDER BY ts DESC,id DESC LIMIT ?", (min(max(limit, 1), 1000),),
            ).fetchall()
        return [dict(row) for row in rows]

    def pending_products(self, process_id: str, limit: int = 10) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                """SELECT * FROM product_summary WHERE process_id=? AND state=? AND sync_state!=?
                   ORDER BY end_ts LIMIT ?""",
                (process_id, ProductState.COMPLETE, SyncState.SYNCED, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def pending_runs(self, process_id: str, limit: int = 10) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM process_run WHERE process_id=? AND state=? AND sync_state!=? "
                "AND sync_blocked=0 ORDER BY end_ts LIMIT ?",
                (process_id, ProductState.COMPLETE, SyncState.SYNCED, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def unresolved_runs(self, process_id: str) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM process_run WHERE process_id=? AND sync_blocked=1 ORDER BY start_ts",
                (process_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def bind_product_id(
        self, process_id: str, run_id: str, old_product_id: str, new_product_id: str,
    ) -> None:
        """Atomically replace a provisional identity in every local persisted record."""
        if not new_product_id or new_product_id == "0":
            raise ValueError("a valid product ID is required")
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                run = db.execute(
                    "SELECT sync_state,sync_blocked FROM process_run WHERE process_id=? AND run_id=?",
                    (process_id, run_id),
                ).fetchone()
                if run is None or not bool(run["sync_blocked"]):
                    raise ValueError(f"run is not awaiting identity binding: {run_id}")
                if run["sync_state"] == SyncState.SYNCED:
                    raise ValueError(f"cannot bind an already synchronized run: {run_id}")
                collision = db.execute(
                    "SELECT 1 FROM product_summary WHERE process_id=? AND run_id=? AND product_id=?",
                    (process_id, run_id, new_product_id),
                ).fetchone()
                if collision is not None:
                    raise ValueError(f"product already exists in run: {run_id}/{new_product_id}")
                summary = db.execute(
                    "SELECT 1 FROM product_summary WHERE process_id=? AND run_id=? AND product_id=?",
                    (process_id, run_id, old_product_id),
                ).fetchone()
                if summary is None:
                    raise ValueError(f"provisional product not found: {run_id}/{old_product_id}")
                db.execute("PRAGMA defer_foreign_keys=ON")
                db.execute(
                    "UPDATE product_summary SET product_id=? "
                    "WHERE process_id=? AND run_id=? AND product_id=?",
                    (new_product_id, process_id, run_id, old_product_id),
                )
                db.execute(
                    "UPDATE product_data SET product_id=? "
                    "WHERE process_id=? AND run_id=? AND product_id=?",
                    (new_product_id, process_id, run_id, old_product_id),
                )
                db.execute(
                    "UPDATE event_log SET product_id=? "
                    "WHERE process_id=? AND run_id=? AND product_id=?",
                    (new_product_id, process_id, run_id, old_product_id),
                )
                db.execute(
                    "UPDATE checkpoint SET product_id=? "
                    "WHERE process_id=? AND run_id=? AND product_id=?",
                    (new_product_id, process_id, run_id, old_product_id),
                )
                db.execute(
                    "UPDATE process_run SET sync_blocked=0,sync_state=? "
                    "WHERE process_id=? AND run_id=?",
                    (SyncState.PENDING, process_id, run_id),
                )
                db.execute("COMMIT")
            except Exception:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise

    def windows_for_run(self, process_id: str, run_id: str) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM product_data WHERE process_id=? AND run_id=? "
                "ORDER BY product_id,segment_no,window_no",
                (process_id, run_id),
            ).fetchall()
        return [dict(row) for row in rows]

    def windows_for_product(self, process_id: str, run_id: str, product_id: str) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM product_data WHERE process_id=? AND run_id=? AND product_id=? "
                "ORDER BY segment_no,window_no",
                (process_id, run_id, product_id),
            ).fetchall()
        return [dict(row) for row in rows]

    def set_sync_state(self, process_id: str, run_id: str, state: SyncState, error: str | None = None) -> None:
        with self.connection() as db:
            db.execute(
                "UPDATE product_summary SET sync_state=?,error_message=? WHERE process_id=? AND run_id=?",
                (state, error, process_id, run_id),
            )
            db.execute(
                "UPDATE process_run SET sync_state=?,error_message=? WHERE process_id=? AND run_id=?",
                (state, error, process_id, run_id),
            )

    def retry_all(self, process_id: str) -> int:
        with self.connection() as db:
            runs = db.execute(
                "UPDATE process_run SET sync_state=? WHERE process_id=? AND state=? AND sync_state=?",
                (SyncState.PENDING, process_id, ProductState.COMPLETE, SyncState.FAILED),
            )
            db.execute(
                "UPDATE product_summary SET sync_state=? WHERE process_id=? AND state=? AND sync_state=?",
                (SyncState.PENDING, process_id, ProductState.COMPLETE, SyncState.FAILED),
            )
        return runs.rowcount

    def healthcheck(self) -> bool:
        try:
            with self.connection() as db:
                return db.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        except sqlite3.Error:
            return False

    def pending_count(self, process_id: str) -> int:
        with self.connection() as db:
            runs = int(db.execute(
                "SELECT count(*) FROM process_run WHERE process_id=? AND state=? AND sync_state!=?",
                (process_id, ProductState.COMPLETE, SyncState.SYNCED),
            ).fetchone()[0])
            time_rows = int(db.execute(
                "SELECT count(*) FROM process_data_time WHERE process_id=? AND sync_state!=?",
                (process_id, SyncState.SYNCED),
            ).fetchone()[0])
        return runs + time_rows

    def apply_retention(
        self, process_id: str, synced_before_ms: int, events_before_ms: int,
    ) -> tuple[int, int, int]:
        """Delete old synced runs and event rows, preserving all operational state."""
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                run_filter = """
                    process_id=? AND state=? AND sync_state=?
                    AND end_ts IS NOT NULL AND end_ts<?
                """
                run_params = (
                    process_id, ProductState.COMPLETE, SyncState.SYNCED, synced_before_ms,
                )
                windows = db.execute(
                    f"""DELETE FROM product_data WHERE process_id=? AND run_id IN (
                        SELECT run_id FROM product_summary WHERE {run_filter}
                    )""",
                    (process_id, *run_params),
                ).rowcount
                db.execute(
                    f"""DELETE FROM product_relation WHERE process_id=? AND run_id IN (
                        SELECT run_id FROM product_summary WHERE {run_filter}
                    )""",
                    (process_id, *run_params),
                )
                products = db.execute(
                    f"DELETE FROM product_summary WHERE {run_filter}", run_params,
                ).rowcount
                db.execute(f"DELETE FROM process_run WHERE {run_filter}", run_params)
                db.execute(
                    "DELETE FROM process_data_time WHERE process_id=? AND sync_state=? AND ts_end<?",
                    (process_id, SyncState.SYNCED, synced_before_ms),
                )
                events = db.execute(
                    "DELETE FROM event_log WHERE process_id=? AND ts<?",
                    (process_id, events_before_ms),
                ).rowcount
                db.execute("COMMIT")
            except Exception:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise
        return products, windows, events

    def _window_values(self, record: WindowRecord) -> list[Any]:
        values: list[Any] = [
            record.process_id, record.run_id, record.product_id, record.segment_no, record.window_no,
            record.axis,
            record.ts_start, record.ts_end, record.elapsed_start_s, record.elapsed_end_s,
            record.position_start_m, record.position_end_m,
        ]
        values.extend(record.values.get(name) for name in self.product_variables)
        values.append(str(record.quality))
        return values

    @staticmethod
    def _quoted(columns: list[str]) -> str:
        return ",".join(f'"{column}"' for column in columns)

    @staticmethod
    def _marks(columns: list[str]) -> str:
        return ",".join("?" for _ in columns)

from __future__ import annotations

import base64
import re
import urllib.parse
import urllib.request
from typing import Any

from industrial_process_engine.config import QuestDbConfig


class QuestDBSink:
    """Official QuestDB Python client sink for process data and summaries."""

    QUESTDB_TYPES = {
        "double": "DOUBLE", "int": "INT", "long": "LONG",
        "symbol": "SYMBOL", "varchar": "VARCHAR", "char": "CHAR",
    }

    def __init__(
        self, config: QuestDbConfig, product_schema: dict[str, str],
        summary_schema: dict[str, str] | None = None,
        process_time_schema: dict[str, str] | None = None,
        *, product_data_enabled: bool = False,
        process_data_time_enabled: bool = False,
        product_relations_enabled: bool = False,
    ) -> None:
        self.config = config
        self.product_schema = product_schema
        self.summary_schema = summary_schema or {}
        self.process_time_schema = process_time_schema or {}
        self.product_data_enabled = product_data_enabled
        self.process_data_time_enabled = process_data_time_enabled
        self.product_relations_enabled = product_relations_enabled
        for identifier in [
            config.product_data_table, config.product_summary_table,
            config.process_data_time_table, config.product_relation_table,
            *product_schema, *self.summary_schema, *self.process_time_schema,
        ]:
            if identifier is None:
                continue
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
                raise ValueError(f"unsafe QuestDB identifier: {identifier}")
        unsupported = sorted(
            (set(product_schema.values()) | set(self.summary_schema.values())
             | set(self.process_time_schema.values())) - self.QUESTDB_TYPES.keys()
        )
        if unsupported:
            raise ValueError(f"unsupported QuestDB output types: {unsupported}")
        self.healthy = False
        self._initialized = False

    def initialize(self) -> None:
        assert self.config.product_summary_table is not None
        product_dynamic = self._dynamic_sql(self.product_schema)
        summary_dynamic = self._dynamic_sql(self.summary_schema)
        process_time_dynamic = self._dynamic_sql(self.process_time_schema)
        product_sql = f"""CREATE TABLE IF NOT EXISTS {self.config.product_data_table} (
            timestamp TIMESTAMP, process_id SYMBOL, run_id SYMBOL, product_id SYMBOL, segment_no LONG,
            window_no LONG,
            axis SYMBOL, ts_end TIMESTAMP, elapsed_start_s DOUBLE,
            elapsed_end_s DOUBLE, position_start_m DOUBLE, position_end_m DOUBLE
            {product_dynamic}, quality SYMBOL
        ) TIMESTAMP(timestamp) PARTITION BY DAY WAL
        DEDUP UPSERT KEYS(timestamp,process_id,run_id,product_id,segment_no,window_no)"""
        summary_sql = f"""CREATE TABLE IF NOT EXISTS {self.config.product_summary_table} (
            timestamp TIMESTAMP, process_id SYMBOL, run_id SYMBOL, product_id SYMBOL, end_ts TIMESTAMP,
            processing_time_s DOUBLE, drained_ts TIMESTAMP, material_length_m DOUBLE,
            start_mode SYMBOL, state SYMBOL{summary_dynamic}
        ) TIMESTAMP(timestamp) PARTITION BY DAY WAL
        DEDUP UPSERT KEYS(timestamp,process_id,run_id,product_id)"""
        process_time_sql = f"""CREATE TABLE IF NOT EXISTS {self.config.process_data_time_table} (
            timestamp TIMESTAMP, process_id SYMBOL, ts_end TIMESTAMP,
            quality SYMBOL{process_time_dynamic}
        ) TIMESTAMP(timestamp) PARTITION BY DAY WAL
        DEDUP UPSERT KEYS(timestamp,process_id)"""
        relation_sql = f"""CREATE TABLE IF NOT EXISTS {self.config.product_relation_table} (
            timestamp TIMESTAMP, process_id SYMBOL, run_id SYMBOL,
            parent_product_id SYMBOL, child_product_id SYMBOL
        ) TIMESTAMP(timestamp) PARTITION BY DAY WAL
        DEDUP UPSERT KEYS(timestamp,process_id,run_id,parent_product_id,child_product_id)"""
        if self.product_data_enabled:
            assert self.config.product_data_table is not None
            self._execute_sql(product_sql)
        self._execute_sql(summary_sql)
        if self.process_data_time_enabled:
            assert self.config.process_data_time_table is not None
            self._execute_sql(process_time_sql)
        if self.product_relations_enabled:
            assert self.config.product_relation_table is not None
            self._execute_sql(relation_sql)
        self._initialized = True
        self.healthy = True

    def _execute_sql(self, sql: str) -> None:
        separator = "&" if "?" in self.config.query_url else "?"
        request = urllib.request.Request(
            f"{self.config.query_url}{separator}{urllib.parse.urlencode({'query': sql})}"
        )
        authorization = self._authorization_header()
        if authorization:
            request.add_header("Authorization", authorization)
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status >= 300:
                raise ConnectionError(f"QuestDB schema creation returned {response.status}")

    def _authorization_header(self) -> str | None:
        options = self.config.client_options
        if options.get("token"):
            return f"Bearer {options['token']}"
        if options.get("username") is not None and options.get("password") is not None:
            credentials = f"{options['username']}:{options['password']}".encode()
            return "Basic " + base64.b64encode(credentials).decode("ascii")
        return None

    def upload(
        self, windows: list[dict[str, Any]], summaries: list[dict[str, Any]],
        relations: list[dict[str, Any]] | None = None,
    ) -> None:
        if not self._initialized:
            self.initialize()
        try:
            from questdb import Sender, TimestampMicros, TimestampNanos
        except ImportError as exc:
            raise RuntimeError("Install the 'questdb' dependency to enable QuestDB sync") from exc
        try:
            with Sender.from_conf(self.config.client_conf) as sender:
                if windows:
                    assert self.product_data_enabled and self.config.product_data_table is not None
                assert self.config.product_summary_table is not None
                for window in windows:
                    columns: dict[str, Any] = {
                        "segment_no": int(window["segment_no"]),
                        "window_no": int(window["window_no"]),
                        "ts_end": TimestampMicros(int(window["ts_end"]) * 1_000),
                        "elapsed_start_s": float(window["elapsed_start_s"]),
                        "elapsed_end_s": float(window["elapsed_end_s"]),
                    }
                    if window.get("position_start_m") is not None:
                        columns["position_start_m"] = float(window["position_start_m"])
                    if window.get("position_end_m") is not None:
                        columns["position_end_m"] = float(window["position_end_m"])
                    symbols = {
                        "process_id": window["process_id"], "run_id": window["run_id"],
                        "product_id": window["product_id"],
                        "axis": window["axis"], "quality": window["quality"],
                    }
                    self._append_dynamic(window, self.product_schema, columns, symbols)
                    sender.row(
                        self.config.product_data_table, symbols=symbols, columns=columns,
                        at=TimestampNanos(int(window["ts_start"]) * 1_000_000),
                    )
                for summary in summaries:
                    summary_columns: dict[str, Any] = {
                        "end_ts": TimestampMicros(int(summary["end_ts"]) * 1_000),
                        "processing_time_s": float(summary["processing_time_s"]),
                    }
                    if summary.get("drained_ts") is not None:
                        summary_columns["drained_ts"] = TimestampMicros(int(summary["drained_ts"]) * 1_000)
                    if summary.get("material_length_m") is not None:
                        summary_columns["material_length_m"] = float(summary["material_length_m"])
                    summary_symbols = {
                        "process_id": summary["process_id"], "run_id": summary["run_id"],
                        "product_id": summary["product_id"],
                        "start_mode": summary.get("start_mode", "normal"),
                        "state": summary["state"],
                    }
                    self._append_dynamic(summary, self.summary_schema, summary_columns, summary_symbols)
                    sender.row(
                        self.config.product_summary_table, symbols=summary_symbols, columns=summary_columns,
                        at=TimestampNanos(int(summary["start_ts"]) * 1_000_000),
                    )
                for relation in relations or ():
                    assert (
                        self.product_relations_enabled
                        and self.config.product_relation_table is not None
                    )
                    sender.row(
                        self.config.product_relation_table,
                        symbols={
                            "process_id": relation["process_id"],
                            "run_id": relation["run_id"],
                            "parent_product_id": relation["parent_product_id"],
                            "child_product_id": relation["child_product_id"],
                        },
                        columns={},
                        at=TimestampNanos(int(relation["created_ts"]) * 1_000_000),
                    )
                sender.flush()
            self.healthy = True
        except Exception:
            self.healthy = False
            raise

    def upload_process_time(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if not self._initialized:
            self.initialize()
        assert self.process_data_time_enabled and self.config.process_data_time_table is not None
        from questdb import Sender, TimestampMicros, TimestampNanos
        try:
            with Sender.from_conf(self.config.client_conf) as sender:
                for row in rows:
                    columns: dict[str, Any] = {
                        "ts_end": TimestampMicros(int(row["ts_end"]) * 1_000),
                    }
                    symbols = {"process_id": row["process_id"], "quality": row["quality"]}
                    self._append_dynamic(row, self.process_time_schema, columns, symbols)
                    sender.row(
                        self.config.process_data_time_table, symbols=symbols, columns=columns,
                        at=TimestampNanos(int(row["ts_start"]) * 1_000_000),
                    )
                sender.flush()
            self.healthy = True
        except Exception:
            self.healthy = False
            raise

    def _append_dynamic(
        self, row: dict[str, Any], schema: dict[str, str], columns: dict[str, Any], symbols: dict[str, Any],
    ) -> None:
        for name, output_type in schema.items():
            value = row.get(name)
            if value is None:
                continue
            if output_type == "symbol":
                symbols[name] = str(value)
            else:
                columns[name] = value

    def _dynamic_sql(self, schema: dict[str, str]) -> str:
        return "".join(
            f", {name} {self.QUESTDB_TYPES[value_type]}" for name, value_type in schema.items()
        )

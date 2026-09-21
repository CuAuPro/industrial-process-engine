import base64
import sys
import types
import urllib.parse

from industrial_process_engine.config import QuestDbConfig
from industrial_process_engine.storage.questdb import QuestDBSink


def test_questdb_rest_auth_is_derived_from_official_client_conf():
    sink = QuestDBSink(
        QuestDbConfig(client_conf="http::addr=questdb:9000;username=l2;password=secret;"),
        {"temperature": "double"},
    )
    expected = base64.b64encode(b"l2:secret").decode("ascii")
    assert sink._authorization_header() == f"Basic {expected}"


def test_questdb_query_url_is_derived_from_official_client_conf():
    plain = QuestDbConfig(client_conf="http::addr=questdb:9000;")
    secure = QuestDbConfig(client_conf="https::addr=qdb.example.com:9443;token=secret;")

    assert plain.query_url == "http://questdb:9000/exec"
    assert secure.query_url == "https://qdb.example.com:9443/exec"


def test_questdb_rest_token_is_derived_from_official_client_conf():
    sink = QuestDBSink(
        QuestDbConfig(client_conf="http::addr=questdb:9000;token=rest-token;"),
        {"temperature": "double"},
    )
    assert sink._authorization_header() == "Bearer rest-token"


def test_explicit_output_types_map_to_questdb_schema():
    schema = {
        "average": "double", "counter": "int", "total": "long",
        "recipe": "symbol", "description": "varchar", "code": "char",
    }
    sink = QuestDBSink(QuestDbConfig(), schema)
    assert {name: sink.QUESTDB_TYPES[value_type] for name, value_type in schema.items()} == {
        "average": "DOUBLE", "counter": "INT", "total": "LONG",
        "recipe": "SYMBOL", "description": "VARCHAR", "code": "CHAR",
    }


def test_initialization_generates_public_process_tables(monkeypatch):
    queries = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def urlopen(request, timeout):
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)["query"][0]
        queries.append(query)
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", urlopen)
    sink = QuestDBSink(
        QuestDbConfig(
            product_data_table="line_data", process_data_time_table="line_time",
            product_summary_table="line_summary",
        ),
        {"temperature": "double"}, {"consumption_kwh": "double"},
        product_data_enabled=True, process_data_time_enabled=True,
    )
    sink.initialize()
    assert "CREATE TABLE IF NOT EXISTS line_data" in queries[0]
    assert "run_id SYMBOL" in queries[0]
    assert "segment_no LONG" in queries[0]
    assert "window_no LONG" in queries[0]
    assert "DEDUP UPSERT KEYS(timestamp,process_id,run_id,product_id,segment_no,window_no)" in queries[0]
    assert "temperature DOUBLE" in queries[0]
    assert "CREATE TABLE IF NOT EXISTS line_summary" in queries[1]
    assert "DEDUP UPSERT KEYS(timestamp,process_id,run_id,product_id)" in queries[1]
    assert "consumption_kwh DOUBLE" in queries[1]
    assert "CREATE TABLE IF NOT EXISTS line_time" in queries[2]
    assert queries[2].split("(", 1)[1].lstrip().startswith("timestamp TIMESTAMP")
    assert "DEDUP UPSERT KEYS(timestamp,process_id)" in queries[2]
    assert len(queries) == 3


def test_relation_table_is_created_only_when_enabled(monkeypatch):
    queries = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def urlopen(request, timeout):
        queries.append(
            urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)["query"][0]
        )
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", urlopen)
    sink = QuestDBSink(
        QuestDbConfig(
            product_summary_table="cut_summary", product_relation_table="cut_relation",
        ),
        {}, product_relations_enabled=True,
    )
    sink.initialize()
    assert len(queries) == 2
    assert "CREATE TABLE IF NOT EXISTS cut_summary" in queries[0]
    assert "CREATE TABLE IF NOT EXISTS cut_relation" in queries[1]


def test_upload_includes_segment_number(monkeypatch):
    rows = []

    class FakeSender:
        @classmethod
        def from_conf(cls, conf):
            return cls()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def row(self, table, *, symbols, columns, at):
            rows.append((table, symbols, columns, at))

        def flush(self):
            pass

    package = types.ModuleType("questdb")
    package.Sender = FakeSender
    package.TimestampMicros = lambda value: value
    package.TimestampNanos = lambda value: value
    monkeypatch.setitem(sys.modules, "questdb", package)

    sink = QuestDBSink(QuestDbConfig(
        product_data_table="product_data",
        process_data_time_table="process_data_time",
        product_summary_table="product_summary",
        product_relation_table="product_relation",
    ), {"temperature": "double"}, product_data_enabled=True,
        process_data_time_enabled=True, product_relations_enabled=True)
    sink._initialized = True
    sink.upload([{
        "process_id": "L", "run_id": "R", "product_id": "P", "segment_no": 3,
        "window_no": 2, "axis": "distance", "ts_start": 1000,
        "ts_end": 2000, "elapsed_start_s": 1.0, "elapsed_end_s": 2.0,
        "position_start_m": 200.0, "position_end_m": 300.0,
        "temperature": 800.0, "quality": "GOOD",
    }], [{
        "process_id": "L", "run_id": "R", "product_id": "P", "start_ts": 0,
        "end_ts": 2000, "processing_time_s": 2.0, "state": "COMPLETE",
    }], [{
        "process_id": "L", "run_id": "R", "parent_product_id": "PARENT",
        "child_product_id": "P", "created_ts": 1500,
    }])

    assert rows[0][2]["segment_no"] == 3
    assert rows[0][2]["window_no"] == 2
    assert rows[1][1]["start_mode"] == "normal"
    assert rows[2][0] == "product_relation"
    assert rows[2][1]["parent_product_id"] == "PARENT"

    sink.upload_process_time([{
        "process_id": "L", "ts_start": 5_000, "ts_end": 10_000,
        "quality": "DATA_GAP",
    }])
    assert rows[3][0] == "process_data_time"
    assert rows[3][1] == {"process_id": "L", "quality": "DATA_GAP"}
    assert "run_id" not in rows[3][1]
    assert "product_id" not in rows[3][1]

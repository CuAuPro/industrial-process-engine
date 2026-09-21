from __future__ import annotations

import queue
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from industrial_process_engine.api.app import create_app
from industrial_process_engine.config import OpcUaConfig, OpcUaMappingConfig
from industrial_process_engine.domain import SignalUpdate
from industrial_process_engine.input.opcua_client import OpcUaClient
from industrial_process_engine.engine import ProcessEngine


class FakeStatus:
    def __init__(self, good: bool = True) -> None:
        self.good = good

    def is_good(self) -> bool:
        return self.good

    def is_bad(self) -> bool:
        return not self.good

    def __str__(self) -> str:
        return "Good" if self.good else "Bad"


def data_value(value, *, good=True, timestamp=None):
    return SimpleNamespace(
        Value=SimpleNamespace(Value=value),
        StatusCode=FakeStatus(good),
        SourceTimestamp=timestamp,
        ServerTimestamp=None,
    )


class FakeNodeId:
    def __init__(self, value: str) -> None:
        self.value = value

    def to_string(self) -> str:
        return self.value


class FakeNode:
    def __init__(self, node_id: str) -> None:
        self.nodeid = FakeNodeId(node_id)


class FakeSubscription:
    def __init__(self, handler) -> None:
        self.handler = handler
        self.groups = []

    def subscribe_data_change(self, nodes, sampling_interval):
        self.groups.append(([node.nodeid.to_string() for node in nodes], sampling_interval))


class FakeClient:
    def __init__(self, values) -> None:
        self.values = values
        self.connected = False
        self.disconnected = False
        self.subscription = None
        self.read_calls = []
        self.user = None
        self.application_uri = None
        self.name = None
        self.description = None
        self.connect_error = None

    def set_user(self, value):
        self.user = value

    def set_password(self, value):
        self.password = value

    def set_security_string(self, value):
        self.security = value

    def setup_self_signed_certificate(self, key_path, certificate_path, **kwargs):
        self.generated_certificate = (key_path, certificate_path, kwargs)
        self.application_uri_at_generation = self.application_uri
        return certificate_path, key_path

    def connect(self):
        if self.connect_error:
            raise self.connect_error
        self.connected = True

    def disconnect(self):
        self.disconnected = True
        self.connected = False

    def check_connection(self):
        return None

    def get_node(self, node_id):
        return FakeNode(node_id)

    def create_subscription(self, period, handler):
        self.subscription = FakeSubscription(handler)
        self.period = period
        return self.subscription

    def read_attributes(self, nodes, attribute):
        node_ids = [node.nodeid.to_string() for node in nodes]
        self.read_calls.append(node_ids)
        return [self.values[node_id] for node_id in node_ids]


def mapping(name, node_id, signal_type="float", **values):
    return OpcUaMappingConfig(
        source="opcua", node_id=node_id, name=name, type=signal_type, **values,
    )


def make_client(
    mappings, values, output=None, config=None, application_uri=None, application_name=None,
):
    fake = FakeClient(values)
    client = OpcUaClient(
        config or OpcUaConfig(enabled=True), mappings, output or queue.Queue(),
        client_factory=lambda *args, **kwargs: fake, application_uri=application_uri,
        application_name=application_name,
    )
    return client, fake


def test_secure_authentication_generates_missing_application_certificate():
    config = OpcUaConfig(
        enabled=True, username="reader", password="secret",
        security_policy="Basic256Sha256", security_mode="SignAndEncrypt",
    )
    client, fake = make_client([], {}, config=config)

    client._configure_security(fake)

    assert fake.user == "reader"
    assert fake.password == "secret"
    assert fake.generated_certificate == (
        Path("data/opcua/client-private-key.pem"),
        Path("data/opcua/client-certificate.der"),
        {"subject_attrs": {"organizationName": "Industrial Process Engine"}},
    )
    assert fake.security == ",".join((
        "Basic256Sha256", "SignAndEncrypt",
        str(Path("data/opcua/client-certificate.der")),
        str(Path("data/opcua/client-private-key.pem")),
    ))


def test_application_uri_is_assigned_before_certificate_generation():
    config = OpcUaConfig(
        enabled=True, security_policy="Basic256Sha256", security_mode="SignAndEncrypt",
    )
    application_uri = "urn:FURNACE-PC:furnace_service"
    client, fake = make_client(
        [], {}, config=config, application_uri=application_uri,
        application_name="Furnace Service",
    )

    with client._lock:
        client._connect_locked()

    assert fake.application_uri == application_uri
    assert fake.application_uri_at_generation == application_uri
    assert fake.name == "Furnace Service"
    assert fake.description == "Furnace Service"


def test_secured_client_can_discover_server_certificate():
    config = OpcUaConfig(
        enabled=True, security_policy="Basic256Sha256", security_mode="SignAndEncrypt",
        certificate_path="certs/client.der", private_key_path="certs/client.pem",
    )
    client, fake = make_client([], {}, config=config)

    client._configure_security(fake)

    assert fake.security == (
        "Basic256Sha256,SignAndEncrypt,certs/client.der,certs/client.pem"
    )


def test_read_many_is_one_typed_batch_with_source_timestamps():
    timestamp = datetime(2026, 8, 21, 10, 0, tzinfo=UTC)
    mappings = [
        mapping("temperature", "ns=2;s=Temperature"),
        mapping("grade", "ns=2;s=Grade", "string", subscribe=False),
    ]
    client, fake = make_client(mappings, {
        "ns=2;s=Temperature": data_value("812.5", timestamp=timestamp),
        "ns=2;s=Grade": data_value("S355", timestamp=timestamp),
    })
    batch = client.read_many(("temperature", "grade"), "process_start")
    assert fake.read_calls == [["ns=2;s=Temperature", "ns=2;s=Grade"]]
    assert [update.value for update in batch.updates] == [812.5, "S355"]
    assert all(update.timestamp_ms == int(timestamp.timestamp() * 1000) for update in batch.updates)
    assert all(update.source_timestamp_ms == int(timestamp.timestamp() * 1000) for update in batch.updates)
    assert batch.reason == "process_start"
    assert batch.errors == ()


def test_bad_node_status_is_returned_without_reusing_an_old_value():
    mappings = [mapping("grade", "ns=2;s=Grade", "string", required=True, subscribe=False)]
    client, _ = make_client(mappings, {"ns=2;s=Grade": data_value("OLD", good=False)})
    batch = client.read_many(("grade",))
    assert batch.updates[0].value is None
    assert batch.updates[0].quality is False
    assert batch.errors and batch.errors[0].startswith("grade:")


def test_subscription_groups_sampling_intervals_and_injects_updates():
    output = queue.Queue()
    mappings = [
        mapping("speed", "ns=2;s=Speed", sampling_interval_ms=50),
        mapping("position", "ns=2;s=Position"),
        mapping("recipe", "ns=2;s=Recipe", "string", subscribe=False),
    ]
    config = OpcUaConfig(enabled=True, publishing_interval_ms=200, sampling_interval_ms=500)
    client, fake = make_client(mappings, {}, output, config)
    with client._lock:
        client._connect_locked()
    assert fake.subscription.groups == [(["ns=2;s=Speed"], 50.0), (["ns=2;s=Position"], 500.0)]
    notification = SimpleNamespace(monitored_item=SimpleNamespace(Value=data_value(12.5)))
    fake.subscription.handler.datachange_notification(FakeNode("ns=2;s=Speed"), 12.5, notification)
    batch = output.get_nowait()
    assert batch.updates[0].name == "speed"
    assert batch.updates[0].value == 12.5


def test_disconnect_invalidates_only_subscribed_nodes():
    output = queue.Queue()
    mappings = [
        mapping("speed", "ns=2;s=Speed"),
        mapping("recipe", "ns=2;s=Recipe", "string", subscribe=False),
    ]
    client, _ = make_client(mappings, {}, output)
    with client._lock:
        client._connect_locked()
        client._disconnect_locked(invalidate=True)
    batch = output.get_nowait()
    assert [update.name for update in batch.updates] == ["speed"]
    assert batch.updates[0].quality is False
    assert client.connected is False


def test_unknown_and_duplicate_explicit_names_are_rejected():
    client, _ = make_client([mapping("speed", "ns=2;s=Speed")], {})
    try:
        client.read_many(("missing",))
        raise AssertionError("unknown name should fail")
    except ValueError as error:
        assert "unknown" in str(error)
    try:
        client.read_many(("speed", "speed"))
        raise AssertionError("duplicate name should fail")
    except ValueError as error:
        assert "duplicate" in str(error)


def test_runtime_request_injects_on_demand_batch_in_fifo_order(config_factory):
    raw = config_factory().model_dump()
    raw["opcua"]["enabled"] = True
    raw["opcua"]["reconnect_interval_s"] = 0.05
    raw["mappings"].append({
        "source": "opcua", "node_id": "ns=2;s=Recipe", "name": "recipe",
        "type": "string", "subscribe": False,
    })
    config = type(config_factory()).model_validate(raw)
    fake = FakeClient({"ns=2;s=Recipe": data_value("R-17")})
    runtime = ProcessEngine(
        config, sqlite_path=config_factory.sqlite_path,
        opcua_client_factory=lambda *args, **kwargs: fake,
    )
    runtime.start()
    try:
        runtime.request_opcua_read(("recipe",), "manual_refresh")
        runtime.queue.join()
        assert runtime.processor.signal_state["recipe"].value == "R-17"
        assert fake.read_calls == [["ns=2;s=Recipe"]]
    finally:
        runtime.graceful_shutdown()


def test_runtime_waits_and_reports_untrusted_opcua_until_connection_succeeds(config_factory):
    raw = config_factory().model_dump()
    raw["opcua"].update({"enabled": True, "reconnect_interval_s": 0.05})
    raw["mappings"].append({
        "source": "opcua", "node_id": "ns=2;s=Recipe", "name": "recipe",
        "type": "string", "subscribe": False,
    })
    config = type(config_factory()).model_validate(raw)
    fake = FakeClient({})
    fake.connect_error = PermissionError("BadCertificateUntrusted")
    runtime = ProcessEngine(
        config, sqlite_path=config_factory.sqlite_path,
        opcua_client_factory=lambda *args, **kwargs: fake,
    )
    runtime.start()
    try:
        deadline = time.monotonic() + 2
        while runtime.opcua and runtime.opcua.last_error is None and time.monotonic() < deadline:
            time.sleep(0.01)
        client = TestClient(create_app(runtime))
        ready = client.get("/health/ready")
        assert ready.status_code == 503
        assert ready.json()["datasources"]["opcua"]["status"] == "WAITING_FOR_CONNECTION"
        status = client.get("/api/v1/status").json()
        assert status["waiting_for_initial_inputs"] is True
        assert "BadCertificateUntrusted" in status["datasources"]["opcua"]["last_error"]

        runtime.enqueue(SignalUpdate("temperature", 500, True, 1, "temp", "test"))
        time.sleep(0.1)
        assert "temperature" not in runtime.processor.signal_state

        fake.connect_error = None
        deadline = time.monotonic() + 2
        while runtime.waiting_for_initial_inputs and time.monotonic() < deadline:
            time.sleep(0.01)
        runtime.queue.join()
        assert runtime.processor.signal_state["temperature"].value == 500
        assert client.get("/health/ready").status_code == 200
        assert client.get("/api/v1/status").json()["datasources"]["opcua"]["last_error"] is None
    finally:
        runtime.graceful_shutdown()

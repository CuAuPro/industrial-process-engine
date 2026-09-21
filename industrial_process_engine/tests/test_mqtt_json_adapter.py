import json

import pytest

from industrial_process_engine.config import MqttConfig, MqttMappingConfig, MqttProductTopicsConfig
from industrial_process_engine.domain import EventType, ProcessEvent, SignalUpdate
from industrial_process_engine.input.mqtt_json_adapter import MqttJsonAdapter


def adapter(mappings, payload_format="value_array", **config):
    return MqttJsonAdapter(mappings, MqttConfig(
        client_id="", payload_format=payload_format, **config,
    ))


def test_maps_exact_topic_id_and_uses_observation_timestamp_for_freshness():
    mqtt_adapter = adapter([MqttMappingConfig(
        source="mqtt", topic="hv/scl", id="tag.1", name="load", type="float",
    )])
    payload = json.dumps({"timestamp": 2000, "values": [
        {"id": "tag.1", "v": "51.25", "q": True, "t": 1999},
        {"id": "unknown", "v": 1, "q": True, "t": 1999},
    ]})
    result = mqtt_adapter.parse("hv/scl", payload)
    assert result is not None
    assert result.updates == (
        SignalUpdate("load", 51.25, True, 2000, "tag.1", "hv/scl", 1999, 2000),
    )
    assert mqtt_adapter.parse("another/topic", payload) is None


def test_boolean_edge_declarations_remain_mapping_metadata():
    mapping = MqttMappingConfig(
        source="mqtt", topic="events", id="active", name="active", type="bool",
        true_event=EventType.PROCESS_START, false_event=EventType.PROCESS_END,
    )
    result = adapter([mapping]).parse(
        "events", json.dumps({"timestamp": 1, "values": [{"id": "active", "v": True, "q": True}]})
    )
    assert result is not None
    assert result.updates[0].name == "active"


def test_maps_flat_key_value_payload_with_iso_timestamp():
    mqtt_adapter = adapter([
        MqttMappingConfig(
            source="mqtt", topic="mill", id="MdrVAct", name="speed", type="float",
        ),
    ], payload_format="flat", flat_timestamp_path="Timestamp")
    result = mqtt_adapter.parse("mill", json.dumps({
        "MdrVAct": 120,
        "Timestamp": "2026-08-27T10:15:30.250+00:00",
    }))

    assert result is not None
    assert result.timestamp_ms == 1787825730250
    assert result.updates == (
        SignalUpdate("speed", 120.0, True, 1787825730250, "MdrVAct", "mill"),
    )


def test_invalid_envelope_is_rejected():
    with pytest.raises(ValueError, match="invalid MQTT JSON payload"):
        adapter([]).parse("x", b"not-json")


def test_values_format_is_explicit_and_fields_are_configurable():
    mqtt_adapter = adapter([
        MqttMappingConfig(
            source="mqtt", topic="custom", id="speed", name="speed", type="float",
        ),
    ], values_path="data.items", id_field="key", value_field="value",
        quality_field="good", item_timestamp_field="time",
        envelope_timestamp_path="data.timestamp")
    result = mqtt_adapter.parse("custom", json.dumps({"data": {
        "timestamp": 1000,
        "items": [{"key": "speed", "value": 3.5, "good": True, "time": 999}],
    }}))

    assert result is not None
    assert result.updates == (
        SignalUpdate("speed", 3.5, True, 1000, "speed", "custom", 999, 1000),
    )

    with pytest.raises(ValueError, match="must contain an array"):
        mqtt_adapter.parse("custom", json.dumps({"speed": 3.5}))


def test_parses_flat_product_tag_on_lifecycle_topic(monkeypatch):
    monkeypatch.setattr(
        "industrial_process_engine.input.mqtt_json_adapter.time.time_ns",
        lambda: 1_250_000_000,
    )
    mqtt_adapter = MqttJsonAdapter(
        [], MqttConfig(client_id="", payload_format="flat"),
        MqttProductTopicsConfig(
            enter_topic="furnace/product/enter",
            exit_topic="furnace/product/exit",
        ),
    )

    result = mqtt_adapter.parse(
        "furnace/product/enter", json.dumps({"ProductId": "PIECE-1"}),
    )

    assert result == ProcessEvent(
        EventType.PRODUCT_ENTER, 1250, "PIECE-1", source="mqtt",
    )


def test_parses_parent_product_ids_from_lifecycle_topic():
    mqtt_adapter = MqttJsonAdapter(
        [], MqttConfig(client_id="", payload_format="flat"),
        MqttProductTopicsConfig(
            enter_topic="cut/product/enter", exit_topic="cut/product/exit",
        ),
    )
    result = mqtt_adapter.parse("cut/product/enter", json.dumps({
        "Timestamp": 1234, "ProductId": "CHILD",
        "ParentProductIds": ["PARENT-A", "PARENT-B"],
    }))
    assert result == ProcessEvent(
        EventType.PRODUCT_ENTER, 1234, "CHILD", source="mqtt",
        parent_product_ids=("PARENT-A", "PARENT-B"),
    )


def test_rejects_empty_product_id_on_lifecycle_topic():
    mqtt_adapter = MqttJsonAdapter(
        [], MqttConfig(client_id="", payload_format="flat"),
        MqttProductTopicsConfig(
            enter_topic="furnace/product/enter",
            exit_topic="furnace/product/exit",
        ),
    )
    with pytest.raises(ValueError, match="product ID is empty"):
        mqtt_adapter.parse(
            "furnace/product/exit", json.dumps({"ProductId": "  "}),
        )


def test_parses_value_array_product_tag_with_quality_and_timestamp():
    mqtt_adapter = MqttJsonAdapter(
        [], MqttConfig(client_id="", payload_format="value_array"),
        MqttProductTopicsConfig(
            enter_topic="furnace/product/enter",
            exit_topic="furnace/product/exit",
            id="piece_id",
        ),
    )
    result = mqtt_adapter.parse("furnace/product/enter", json.dumps({
        "timestamp": 2000,
        "values": [{"id": "piece_id", "v": "PIECE-2", "q": True, "t": 1999}],
    }))

    assert result == ProcessEvent(
        EventType.PRODUCT_ENTER, 1999, "PIECE-2", source="mqtt",
    )

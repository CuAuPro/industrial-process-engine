import pytest
from pydantic import ValidationError

import industrial_process_engine.config as config_module
from industrial_process_engine.config import (
    MqttConfig, MqttMappingConfig, TrackingConfig, QuestDbConfig, apply_environment,
)
from industrial_process_engine.units import MeasurementUnit


@pytest.mark.parametrize("legacy_model", ["batch", "discrete"])
def test_legacy_process_models_are_rejected(config_factory, legacy_model):
    raw = config_factory().model_dump()
    raw["process"]["model"] = legacy_model
    with pytest.raises(ValidationError):
        type(config_factory()).model_validate(raw)


def test_cycle_requires_explicit_membership_and_close_policy(config_factory):
    raw = config_factory().model_dump()
    raw["process"] = {"id": raw["process"]["id"], "model": "cycle"}
    raw["tracking"] = None
    raw["streams"]["product_data"] = {"enabled": False}
    with pytest.raises(ValidationError, match="cycle requires membership and close_run"):
        type(config_factory()).model_validate(raw)


def test_topics_are_derived_from_mappings(config_factory):
    config = config_factory()
    assert config.topics == ["events", "process"]


def test_topics_include_mqtt_product_topics(config_factory):
    raw = config_factory().model_dump()
    raw["lifecycle"]["product_topics"] = {
        "enter_topic": "product/enter", "exit_topic": "product/exit",
    }
    config = type(config_factory()).model_validate(raw)
    assert config.topics == ["events", "process", "product/enter", "product/exit"]


def test_mqtt_product_topics_require_explicit_source(config_factory):
    raw = config_factory().model_dump()
    raw["lifecycle"].update({
        "source": "derived", "product_topics": {
            "enter_topic": "product/enter", "exit_topic": "product/exit",
        },
    })
    with pytest.raises(ValidationError, match="MQTT product topics"):
        type(config_factory()).model_validate(raw)


def test_product_topic_cannot_also_be_a_mapped_tag_topic(config_factory):
    raw = config_factory().model_dump()
    raw["lifecycle"]["product_topics"] = {
        "enter_topic": "process", "exit_topic": "product/exit",
    }
    with pytest.raises(ValidationError, match="cannot also contain mapped tags"):
        type(config_factory()).model_validate(raw)


def test_mapping_source_is_required(config_factory):
    raw = config_factory().model_dump()
    raw["mappings"][0].pop("source")
    with pytest.raises(ValidationError, match="discriminator"):
        type(config_factory()).model_validate(raw)


def test_misnested_or_unknown_configuration_is_rejected(config_factory):
    raw = config_factory().model_dump()
    raw["mqtt"]["opcua"] = {"enabled": True}
    with pytest.raises(ValidationError, match="extra_forbidden"):
        type(config_factory()).model_validate(raw)


def test_legacy_process_and_aggregation_keys_are_rejected(config_factory):
    config = config_factory()
    raw = config.model_dump()
    raw["line_id"] = raw.pop("process")["id"]
    raw["aggregation"] = {"mode": "distance", "interval_m": 1.0}
    with pytest.raises(ValidationError, match="process"):
        type(config).model_validate(raw)

    raw = config.model_dump()
    raw["mappings"][1]["aggregate"] = {
        "calculation": "weighted_mean", "output_type": "double",
    }
    with pytest.raises(ValidationError, match="aggregate"):
        type(config).model_validate(raw)


def test_product_id_requires_nested_signal_configuration(config_factory):
    raw = config_factory().model_dump()
    raw["lifecycle"]["product_id"] = "product_id"
    with pytest.raises(ValidationError, match="product_id"):
        type(config_factory()).model_validate(raw)


def test_opcua_mapping_supports_subscription_and_on_demand(config_factory):
    raw = config_factory().model_dump()
    raw["mappings"].extend([
        {
            "source": "opcua", "node_id": "ns=2;s=Recipe", "name": "recipe",
            "type": "string", "subscribe": False,
        },
        {
            "source": "opcua", "node_id": "ns=2;s=Pressure", "name": "pressure",
            "type": "float", "sampling_interval_ms": 50,
        },
    ])
    config = type(config_factory()).model_validate(raw)
    assert [mapping.name for mapping in config.opcua_mappings] == ["recipe", "pressure"]
    assert [mapping.name for mapping in config.subscribed_opcua_mappings] == ["pressure"]


def test_on_demand_opcua_mapping_cannot_declare_edge(config_factory):
    raw = config_factory().model_dump()
    raw["mappings"].append({
        "source": "opcua", "node_id": "ns=2;s=Start", "name": "start", "type": "bool",
        "subscribe": False, "true_event": "PROCESS_START",
    })
    with pytest.raises(ValidationError, match="on-demand"):
        type(config_factory()).model_validate(raw)


def test_distance_position_can_use_subscribed_opcua_but_not_on_demand(config_factory):
    raw = config_factory().model_dump()
    position = next(mapping for mapping in raw["mappings"] if mapping["name"] == "position")
    position.clear()
    position.update({
        "source": "opcua", "node_id": "ns=2;s=Position", "name": "position",
        "type": "float", "subscribe": True,
    })
    assert type(config_factory()).model_validate(raw).mapping_by_name["position"].source == "opcua"
    position["subscribe"] = False
    with pytest.raises(ValidationError, match="position signal"):
        type(config_factory()).model_validate(raw)


def test_secured_opcua_allows_server_managed_certificate_configuration(config_factory):
    raw = config_factory().model_dump()
    raw["opcua"].update({
        "security_policy": "Basic256Sha256", "security_mode": "SignAndEncrypt",
    })
    config = type(config_factory()).model_validate(raw)
    assert config.opcua.security_mode == "SignAndEncrypt"
    assert config.opcua.certificate_path is None

    raw["opcua"]["certificate_path"] = "certs/client.der"
    with pytest.raises(ValidationError, match="configured together"):
        type(config_factory()).model_validate(raw)


def test_opcua_application_uri_defaults_to_hostname_and_service(config_factory, monkeypatch):
    monkeypatch.setattr(config_module.socket, "gethostname", lambda: "FURNACE-PC")
    config = config_factory()

    assert config.opcua_application_uri == "urn:FURNACE-PC:test_l2"


def test_opcua_application_uri_can_be_overridden(config_factory, monkeypatch):
    explicit_uri = "urn:plant:line-client"
    monkeypatch.setenv("OPCUA_APPLICATION_URI", explicit_uri)

    assert apply_environment(config_factory()).opcua_application_uri == explicit_uri


def test_duplicate_mapping_is_rejected(config_factory):
    config = config_factory()
    raw = config.model_dump()
    raw["mappings"].append(raw["mappings"][0])
    with pytest.raises(ValidationError, match="duplicate"):
        type(config).model_validate(raw)


def test_config_allows_aggregation_to_be_supplied_by_python_derived_signal(config_factory):
    config = config_factory()
    raw = config.model_dump()
    for mapping in raw["mappings"]:
        mapping["outputs"]["product_data"] = []
    raw["streams"]["product_data"]["enabled"] = False
    validated = type(config).model_validate(raw)
    assert validated.aggregation_variables == []


def test_output_name_defaults_to_signal_and_can_be_overridden(config_factory):
    raw = config_factory().model_dump()
    temperature = next(mapping for mapping in raw["mappings"] if mapping["name"] == "temperature")
    temperature["outputs"]["product_data"] = [
        {"calculation": "weighted_mean"},
        {"name": "temperature_max", "calculation": "max"},
    ]

    assert [v.name for v in type(config_factory()).model_validate(raw).aggregation_variables] == [
        "temperature", "temperature_max",
    ]


def test_mqtt_client_id_must_be_present_but_may_be_empty():
    with pytest.raises(ValidationError):
        MqttConfig()
    assert MqttConfig(client_id="", payload_format="value_array").client_id == ""


def test_mqtt_payload_format_is_explicit():
    with pytest.raises(ValidationError, match="payload_format"):
        MqttConfig(client_id="")
    assert MqttConfig(client_id="", payload_format="flat").payload_format == "flat"


def test_mapping_type_is_explicitly_required():
    with pytest.raises(ValidationError):
        MqttMappingConfig(source="mqtt", topic="x", id="x", name="x")


def test_mapping_uses_string_not_python_str_abbreviation():
    assert MqttMappingConfig(source="mqtt", topic="x", id="x", name="x", type="string").type == "string"
    with pytest.raises(ValidationError):
        MqttMappingConfig(source="mqtt", topic="x", id="x", name="x", type="str")


def test_only_boolean_tags_can_declare_rising_and_falling_events():
    with pytest.raises(ValidationError, match="boolean tag"):
        MqttMappingConfig(
            source="mqtt", topic="x", id="x", name="x", type="int",
            true_event="PROCESS_START",
        )


def test_boolean_edges_cannot_declare_product_events_without_an_id():
    with pytest.raises(ValidationError, match="cannot identify product"):
        MqttMappingConfig(
            source="mqtt", topic="x", id="x", name="x", type="bool",
            true_event="PRODUCT_ENTER",
        )


def test_product_id_lifecycle_requires_a_string_tag(config_factory):
    raw = config_factory().model_dump()
    product_id = next(
        mapping for mapping in raw["mappings"] if mapping["name"] == "product_id"
    )
    product_id["type"] = "int"
    with pytest.raises(ValidationError, match="must be a string tag"):
        type(config_factory()).model_validate(raw)


def test_derived_rules_cannot_emit_product_events_without_an_id(config_factory):
    raw = config_factory().model_dump()
    raw["lifecycle"] = {
        "source": "derived",
        "rules": [{"when": "temperature > 100", "event": "PRODUCT_ENTER"}],
    }
    with pytest.raises(ValidationError, match="PRODUCT_ENTER"):
        type(config_factory()).model_validate(raw)


def test_aggregation_mapping_must_be_numeric(config_factory):
    config = config_factory()
    raw = config.model_dump()
    next(mapping for mapping in raw["mappings"] if mapping["name"] == "temperature")["type"] = "string"
    with pytest.raises(ValidationError, match="requires numeric input"):
        type(config).model_validate(raw)


def test_integer_input_can_be_stored_as_symbol(config_factory):
    config = config_factory()
    raw = config.model_dump()
    raw["mappings"].append({
        "source": "mqtt", "topic": "process", "id": "recipe", "name": "recipe_code", "type": "int",
        "outputs": {"product_data": [{
            "name": "recipe_code", "calculation": "last", "output_type": "symbol",
        }]},
    })
    validated = type(config).model_validate(raw)
    assert validated.aggregation_storage_schema["recipe_code"] == "symbol"


def test_float_to_integer_output_requires_explicit_policy(config_factory):
    config = config_factory()
    raw = config.model_dump()
    temperature = next(mapping for mapping in raw["mappings"] if mapping["name"] == "temperature")
    temperature["outputs"]["product_data"] = [{
        "name": "temperature", "calculation": "last", "output_type": "long",
    }]
    with pytest.raises(ValidationError, match="without a conversion policy"):
        type(config).model_validate(raw)


def test_deployment_environment_overrides_connections(config_factory, monkeypatch):
    monkeypatch.setenv("MQTT_ENABLED", "true")
    monkeypatch.setenv("MQTT_HOST", "broker.internal")
    monkeypatch.setenv("MQTT_PORT", "2883")
    monkeypatch.setenv("MQTT_CLIENT_ID", "scl-production")
    monkeypatch.setenv("QUESTDB_ENABLED", "true")
    monkeypatch.setenv("QUESTDB_CLIENT_CONF", "http::addr=questdb:9000;")
    monkeypatch.setenv("OPCUA_ENABLED", "true")
    monkeypatch.setenv("OPCUA_ENDPOINT", "opc.tcp://plc:4840")
    monkeypatch.setenv("OPCUA_PUBLISHING_INTERVAL_MS", "400")
    monkeypatch.setenv("OPCUA_USERNAME", "reader")
    monkeypatch.setenv("OPCUA_PASSWORD", "secret")
    config = apply_environment(config_factory(questdb={
        "enabled": False,
        "product_data_table": "line_data",
        "product_summary_table": "line_summary",
    }))
    assert config.mqtt.host == "broker.internal"
    assert config.mqtt.enabled is True
    assert config.mqtt.port == 2883
    assert config.mqtt.client_id == "scl-production"
    assert config.questdb.enabled is True
    assert config.questdb.client_conf == "http::addr=questdb:9000;"
    assert config.questdb.product_data_table == "line_data"
    assert config.questdb.process_data_time_table is None
    assert config.questdb.product_summary_table == "line_summary"
    assert config.opcua.enabled is True
    assert config.opcua.endpoint == "opc.tcp://plc:4840"
    assert config.opcua.username == "reader"
    assert config.opcua.publishing_interval_ms == 400


def test_questdb_requires_http_client_configuration():
    with pytest.raises(ValidationError, match="must use http:: or https::"):
        QuestDbConfig(client_conf="tcp::addr=questdb:9009;")


def test_enabled_questdb_requires_only_used_table_names(config_factory):
    raw = config_factory().model_dump()
    raw["questdb"] = {"enabled": True}
    with pytest.raises(ValidationError, match="product_summary_table.*product_data_table"):
        type(config_factory()).model_validate(raw)

    raw["questdb"].update({
        "product_summary_table": "line_summary",
        "product_data_table": "line_data",
    })
    config = type(config_factory()).model_validate(raw)
    assert config.questdb.product_relation_table is None
    assert config.questdb.process_data_time_table is None


def test_transformation_questdb_requires_relation_table(config_factory):
    raw = config_factory().model_dump()
    raw["process"] = {"id": "TEST_LINE", "model": "transformation"}
    raw["tracking"] = None
    raw["streams"]["product_data"] = {"enabled": False}
    raw["questdb"] = {"enabled": True, "product_summary_table": "line_summary"}
    with pytest.raises(ValidationError, match="product_relation_table"):
        type(config_factory()).model_validate(raw)


def test_time_aggregation_requires_seconds_and_no_position(config_factory):
    raw = config_factory().model_dump()
    raw["streams"]["product_data"] = {
        "enabled": True, "axis": "time", "interval_s": 10, "stale_after_ms": 5000,
    }
    raw["tracking"] = None
    config = type(config_factory()).model_validate(raw)
    assert config.aggregation.interval == 10


@pytest.mark.parametrize("unit", list(MeasurementUnit))
def test_numeric_mapping_accepts_supported_units(unit):
    mapping = MqttMappingConfig(
        source="mqtt", topic="process", id="speed", name="speed",
        type="float", unit=unit,
    )
    assert mapping.unit is unit


@pytest.mark.parametrize("signal_type", ["string", "bool"])
def test_mapping_units_are_restricted_to_numeric_signals(signal_type):
    with pytest.raises(ValidationError, match="Input should be"):
        MqttMappingConfig(
            source="mqtt", topic="process", id="speed", name="speed",
            type="float", unit="furlong/fortnight",
        )
    with pytest.raises(ValidationError, match="numeric mapping"):
        MqttMappingConfig(
            source="mqtt", topic="process", id="mode", name="mode",
            type=signal_type, unit="m/min",
        )


@pytest.mark.parametrize("unit", ["mm/s", "mm/min", "cm/s", "cm/min", "m/s", "m/min"])
def test_tracking_accepts_every_linear_speed_unit(config_factory, unit):
    raw = config_factory().model_dump()
    raw["tracking"] = {"source": "speed", "speed_signal": "speed"}
    raw["mappings"].append({
        "source": "mqtt", "topic": "process", "id": "speed",
        "name": "speed", "type": "float", "unit": unit,
    })
    assert type(config_factory()).model_validate(raw).speed_unit.value == unit


@pytest.mark.parametrize("unit", [None, "rpm"])
def test_tracking_requires_a_linear_speed_unit(config_factory, unit):
    raw = config_factory().model_dump()
    raw["tracking"] = {"source": "speed", "speed_signal": "speed"}
    mapping = {
        "source": "mqtt", "topic": "process", "id": "speed",
        "name": "speed", "type": "float",
    }
    if unit is not None:
        mapping["unit"] = unit
    raw["mappings"].append(mapping)
    with pytest.raises(ValidationError, match="linear-speed unit"):
        type(config_factory()).model_validate(raw)


def test_status_metric_aliases_must_reference_mapped_signals(config_factory):
    with pytest.raises(ValidationError, match="unknown signals"):
        config_factory(status={"metrics": {"speed": "missing"}})


def test_time_aggregation_rejects_position_configuration(config_factory):
    raw = config_factory().model_dump()
    raw["streams"]["product_data"] = {
        "enabled": True, "axis": "time", "interval_s": 10, "stale_after_ms": 5000,
    }
    with pytest.raises(ValidationError, match="does not use position"):
        type(config_factory()).model_validate(raw)


def test_direct_position_reset_requires_end_on_product_id_change(config_factory):
    raw = config_factory().model_dump()
    raw["tracking"]["reset_ratio"] = 0.10
    raw["lifecycle"]["product_id"]["on_change"] = "PROCESS_START"
    with pytest.raises(ValidationError, match="requires product_id.on_change PROCESS_END"):
        type(config_factory()).model_validate(raw)


def test_position_reset_ratio_is_not_valid_for_speed_integration(config_factory):
    raw = config_factory().model_dump()
    raw["tracking"] = {
        "source": "speed", "speed_signal": "speed",
        "reset_ratio": 0.10,
    }
    raw["mappings"].append({
        "source": "mqtt", "topic": "process", "id": "speed", "name": "speed",
        "type": "float", "unit": "m/min",
    })
    raw["lifecycle"]["product_id"]["on_change"] = "PROCESS_END"
    with pytest.raises(ValidationError, match="only valid for direct position"):
        type(config_factory()).model_validate(raw)


def test_position_reset_coordination_requires_explicit_lifecycle(config_factory):
    raw = config_factory().model_dump()
    raw["tracking"]["reset_ratio"] = 0.10
    raw["lifecycle"]["source"] = "derived"
    raw["lifecycle"]["product_id"]["on_change"] = "PROCESS_END"
    with pytest.raises(ValidationError, match="requires explicit lifecycle"):
        type(config_factory()).model_validate(raw)


def test_segment_scope_requires_direct_reset_and_start_lifecycle(config_factory):
    raw = config_factory().model_dump()
    raw["tracking"].update({
        "reset_ratio": 0.10, "reset_scope": "segment",
        "segment_start": "segment_change_and_position_reset", "segment_signal": "pass_no",
    })
    raw["mappings"].append({
        "source": "mqtt", "topic": "process", "id": "pass", "name": "pass_no", "type": "int",
    })
    raw["lifecycle"]["product_id"]["on_change"] = "PROCESS_END"
    with pytest.raises(ValidationError, match="requires product_id.on_change PROCESS_START"):
        type(config_factory()).model_validate(raw)


def test_signal_segment_mode_requires_continuous_mapping(config_factory):
    raw = config_factory().model_dump()
    raw["tracking"].update({
        "reset_ratio": 0.10, "reset_scope": "segment",
        "segment_start": "segment_change_and_position_reset", "segment_signal": "pass_no",
    })
    raw["lifecycle"]["product_id"]["on_change"] = "PROCESS_START"
    with pytest.raises(ValidationError, match="segment_signal must use MQTT or subscribed OPC UA"):
        type(config_factory()).model_validate(raw)


def test_signal_segment_mode_rejects_on_demand_opcua_mapping(config_factory):
    raw = config_factory().model_dump()
    raw["tracking"].update({
        "reset_ratio": 0.10, "reset_scope": "segment",
        "segment_start": "segment_change", "segment_signal": "pass_no",
    })
    raw["lifecycle"]["product_id"]["on_change"] = "PROCESS_START"
    raw["mappings"].append({
        "source": "opcua", "node_id": "ns=2;s=Pass", "name": "pass_no",
        "type": "int", "subscribe": False,
    })
    with pytest.raises(ValidationError, match="segment_signal must use MQTT or subscribed OPC UA"):
        type(config_factory()).model_validate(raw)


def test_position_reset_segment_mode_requires_signal(config_factory):
    raw = config_factory().model_dump()
    raw["tracking"].update({
        "reset_ratio": 0.10, "reset_scope": "segment",
        "segment_start": "position_reset", "segment_signal": None,
    })
    raw["lifecycle"]["product_id"]["on_change"] = "PROCESS_START"
    with pytest.raises(ValidationError, match="requires segment_signal"):
        type(config_factory()).model_validate(raw)


def test_segment_signal_mapping_must_be_integer(config_factory):
    raw = config_factory().model_dump()
    raw["tracking"].update({
        "reset_ratio": 0.10, "reset_scope": "segment",
        "segment_start": "segment_change_and_position_reset", "segment_signal": "pass_no",
    })
    raw["lifecycle"]["product_id"]["on_change"] = "PROCESS_START"
    raw["mappings"].append({
        "source": "mqtt", "topic": "process", "id": "pass", "name": "pass_no", "type": "string",
    })
    with pytest.raises(ValidationError, match="segment_signal mapping type must be int"):
        type(config_factory()).model_validate(raw)

from __future__ import annotations

import math
import os
import re
import socket
from pathlib import Path
from typing import Annotated, Any, Literal
from urllib.parse import quote

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from industrial_process_engine.domain import EventType
from industrial_process_engine.units import LINEAR_SPEED_UNITS, MeasurementUnit

SignalType = Literal["float", "int", "bool", "string"]
OutputType = Literal["double", "int", "long", "symbol", "varchar", "char"]
AggregationCalculation = Literal["weighted_mean", "first", "last", "min", "max"]
PRODUCT_DATA_COLUMNS = {
    "process_id", "run_id", "product_id", "segment_no", "window_no", "axis", "ts_start", "ts_end",
    "elapsed_start_s", "elapsed_end_s", "position_start_m", "position_end_m", "quality",
}
PROCESS_DATA_TIME_COLUMNS = {
    "process_id", "ts_start", "ts_end", "quality", "sync_state", "error_message",
}


class StrictConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MqttConfig(StrictConfigModel):
    enabled: bool = True
    host: str = "localhost"
    port: int = 1883
    client_id: str
    payload_format: Literal["value_array", "flat"]
    values_path: str = Field(default="values", min_length=1)
    id_field: str = Field(default="id", min_length=1)
    value_field: str = Field(default="v", min_length=1)
    quality_field: str = Field(default="q", min_length=1)
    item_timestamp_field: str = Field(default="t", min_length=1)
    envelope_timestamp_path: str = Field(default="timestamp", min_length=1)
    flat_timestamp_path: str = Field(default="Timestamp", min_length=1)
    username: str | None = None
    password: str | None = None
    keepalive: int = 60
    qos: Literal[0, 1, 2] = 1


class ServiceConfig(StrictConfigModel):
    name: str = Field(min_length=1)
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    version: str = Field(min_length=1)


class ProcessConfig(StrictConfigModel):
    id: str = Field(min_length=1)
    model: Literal["cycle", "continuous", "transformation"]
    membership: Literal["single", "multiple"] | None = None
    close_run: Literal["last_product_exit", "process_end"] | None = None

    @model_validator(mode="after")
    def validate_model_options(self) -> "ProcessConfig":
        if self.model == "cycle":
            if self.membership is None or self.close_run is None:
                raise ValueError("cycle requires membership and close_run")
        elif self.membership is not None or self.close_run is not None:
            raise ValueError("membership and close_run are only valid for cycle processes")
        return self


class StreamOutputConfig(StrictConfigModel):
    name: str | None = None
    calculation: AggregationCalculation
    output_type: OutputType | None = None

    @model_validator(mode="after")
    def validate_output_name(self) -> "StreamOutputConfig":
        if self.name is not None:
            _validate_signal_name(self.name)
        return self


class MappingOutputsConfig(StrictConfigModel):
    output_type: OutputType | None = None
    product_data: list[StreamOutputConfig] = Field(default_factory=list)
    process_data_time: list[StreamOutputConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_output_type(self) -> "MappingOutputsConfig":
        if any(
            output.output_type is None and self.output_type is None
            for output in [*self.product_data, *self.process_data_time]
        ):
            raise ValueError("configured outputs require output_type")
        return self

    def output_type_for(self, output: StreamOutputConfig) -> OutputType:
        value = output.output_type or self.output_type
        assert value is not None
        return value


class BaseMappingConfig(StrictConfigModel):
    source: str
    name: str
    type: SignalType
    unit: MeasurementUnit | None = None
    required: bool = False
    true_event: EventType | None = None
    false_event: EventType | None = None
    outputs: MappingOutputsConfig = Field(default_factory=MappingOutputsConfig)
    station: str | None = None
    spatial_offset_m: float | None = None

    @model_validator(mode="after")
    def validate_name(self) -> "BaseMappingConfig":
        _validate_signal_name(self.name)
        if self.unit is not None and self.type not in {"float", "int"}:
            raise ValueError("units require a numeric mapping")
        if self.station is not None:
            _validate_signal_name(self.station)
        if self.spatial_offset_m is not None and (
            not math.isfinite(self.spatial_offset_m) or self.spatial_offset_m < 0
        ):
            raise ValueError("spatial_offset_m must be finite and non-negative")
        edge_events = (self.true_event, self.false_event)
        if any(edge_events) and self.type != "bool":
            raise ValueError("true_event and false_event require a boolean tag")
        if any(
            event in {
                EventType.PRODUCT_ENTER, EventType.PRODUCT_UPDATE,
                EventType.PRODUCT_EXIT, EventType.PRODUCT_ABORT,
            }
            for event in edge_events
        ):
            raise ValueError("boolean tag edges cannot identify product lifecycle events")
        return self

class MqttMappingConfig(BaseMappingConfig):
    source: Literal["mqtt"]
    topic: str
    id: str


class OpcUaMappingConfig(BaseMappingConfig):
    source: Literal["opcua"]
    node_id: str = Field(min_length=1)
    subscribe: bool = True
    sampling_interval_ms: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_events(self) -> "OpcUaMappingConfig":
        if not self.subscribe and (self.true_event or self.false_event):
            raise ValueError("on-demand OPC UA mappings cannot declare lifecycle edges")
        return self


MappingConfig = Annotated[MqttMappingConfig | OpcUaMappingConfig, Field(discriminator="source")]


class RuleConfig(StrictConfigModel):
    when: str
    event: Literal[
        EventType.PROCESS_START, EventType.PROCESS_END, EventType.PROCESS_ABORT,
        EventType.LINE_START, EventType.LINE_STOP,
    ]
    debounce_ms: int = Field(default=0, ge=0)


class LateBindingConfig(StrictConfigModel):
    enabled: bool = False
    start_on_position_reset: bool = False
    placeholder_prefix: str = Field(default="UNASSIGNED", min_length=1)
    timeout_s: float | None = Field(default=None, gt=0)
    block_remote_sync: bool = True

    @model_validator(mode="after")
    def validate_policy(self) -> "LateBindingConfig":
        if not self.placeholder_prefix.strip() or self.placeholder_prefix.strip() == "0":
            raise ValueError("late-binding placeholder_prefix must be a valid non-zero ID")
        if self.enabled and not self.block_remote_sync:
            raise ValueError("late binding requires block_remote_sync=true")
        return self


class ProductIdConfig(StrictConfigModel):
    signal: str = "product_id"
    on_change: Literal[
        EventType.PROCESS_START, EventType.PROCESS_END, EventType.PROCESS_ABORT,
    ] | None = None
    on_clear: Literal[EventType.PROCESS_END, EventType.PROCESS_ABORT] | None = None
    late_binding: LateBindingConfig = Field(default_factory=LateBindingConfig)

    @model_validator(mode="after")
    def validate_signal(self) -> "ProductIdConfig":
        _validate_signal_name(self.signal)
        return self


class MqttProductTopicsConfig(StrictConfigModel):
    enter_topic: str = Field(min_length=1)
    exit_topic: str = Field(min_length=1)
    abort_topic: str | None = Field(default=None, min_length=1)
    id: str = Field(default="ProductId", min_length=1)
    parent_ids: str = Field(default="ParentProductIds", min_length=1)

    @property
    def topic_events(self) -> dict[str, EventType]:
        configured = {
            self.enter_topic: EventType.PRODUCT_ENTER,
            self.exit_topic: EventType.PRODUCT_EXIT,
        }
        if self.abort_topic:
            configured[self.abort_topic] = EventType.PRODUCT_ABORT
        return configured

    @model_validator(mode="after")
    def validate_unique_topics(self) -> "MqttProductTopicsConfig":
        topics = [
            self.enter_topic, self.exit_topic, self.abort_topic,
        ]
        present = [topic for topic in topics if topic]
        if len(present) != len(set(present)):
            raise ValueError("MQTT product topics must be unique")
        return self


class LifecycleConfig(StrictConfigModel):
    source: Literal["explicit", "derived", "both"] = "derived"
    product_id: ProductIdConfig | None = None
    product_topics: MqttProductTopicsConfig | None = None
    rules: list[RuleConfig] = Field(default_factory=list)


SecurityPolicy = Literal["none", "Basic256Sha256", "Aes128Sha256RsaOaep", "Aes256Sha256RsaPss"]
SecurityMode = Literal["none", "Sign", "SignAndEncrypt"]


class OpcUaConfig(StrictConfigModel):
    enabled: bool = False
    endpoint: str = "opc.tcp://localhost:4840"
    application_uri: str | None = Field(default=None, min_length=1)
    timeout_s: float = Field(default=5.0, gt=0)
    reconnect_interval_s: float = Field(default=5.0, gt=0)
    publishing_interval_ms: float = Field(default=250.0, gt=0)
    sampling_interval_ms: float = Field(default=250.0, gt=0)
    username: str | None = None
    password: str | None = None
    security_policy: SecurityPolicy = "none"
    security_mode: SecurityMode = "none"
    certificate_path: str | None = None
    private_key_path: str | None = None
    private_key_password: str | None = None
    server_certificate_path: str | None = None

    @model_validator(mode="after")
    def validate_security(self) -> "OpcUaConfig":
        secure_paths = (self.certificate_path, self.private_key_path, self.server_certificate_path)
        if self.security_policy == "none":
            if self.security_mode != "none" or any(secure_paths):
                raise ValueError("OPC UA policy none requires mode none and no security certificates")
        elif self.security_mode == "none":
            raise ValueError("secured OPC UA policy requires Sign or SignAndEncrypt mode")
        if bool(self.certificate_path) != bool(self.private_key_path):
            raise ValueError("OPC UA client certificate and private key must be configured together")
        if self.private_key_password and not self.private_key_path:
            raise ValueError("OPC UA private key password requires a private key")
        if self.password and not self.username:
            raise ValueError("OPC UA password requires a username")
        return self


class TrackingConfig(StrictConfigModel):
    source: Literal["direct", "speed"] = "direct"
    signal: str = "position"
    speed_signal: str = "speed"
    reverse_policy: Literal["ignore", "hold", "lost"] = "hold"
    reset_ratio: float | None = Field(default=None, gt=0, lt=1)
    reset_scope: Literal["product", "segment"] = "product"
    segment_start: Literal[
        "segment_change_and_position_reset", "segment_change", "position_reset",
    ] = "segment_change_and_position_reset"
    segment_signal: str | None = None
    max_forward_jump_m: float = Field(default=10.0, gt=0)
    stopped_epsilon: float = Field(default=1e-6, ge=0)
    stale_after_ms: int = Field(default=5000, gt=0)
    fallback_to_speed: bool = False

    @model_validator(mode="after")
    def validate_segment(self) -> "TrackingConfig":
        if self.segment_signal is not None:
            _validate_signal_name(self.segment_signal)
        if self.reset_scope == "segment":
            if not self.segment_signal:
                raise ValueError("segment reset scope requires segment_signal")
            if self.source == "direct" and self.reset_ratio is None:
                raise ValueError("direct segment reset scope requires reset_ratio")
            if self.source == "speed" and self.segment_start != "segment_change":
                raise ValueError("speed segment tracking requires segment_start=segment_change")
            if self.source == "speed" and self.reset_ratio is not None:
                raise ValueError("speed segment tracking does not use reset_ratio")
        elif self.segment_signal is not None:
            raise ValueError("segment_signal is only valid with reset_scope segment")
        return self


class AggregationVariableConfig(StrictConfigModel):
    name: str
    source_name: str | None = None
    calculation: AggregationCalculation
    output_type: OutputType
    spatial_offset_m: float = 0.0

    @model_validator(mode="after")
    def validate_name(self) -> "AggregationVariableConfig":
        _validate_signal_name(self.name)
        if self.source_name is None:
            self.source_name = self.name
        else:
            _validate_signal_name(self.source_name)
        if not math.isfinite(self.spatial_offset_m) or self.spatial_offset_m < 0:
            raise ValueError("spatial_offset_m must be finite and non-negative")
        return self


class SpatialStationConfig(StrictConfigModel):
    offset_m: float = Field(ge=0)

    @model_validator(mode="after")
    def validate_offset(self) -> "SpatialStationConfig":
        if not math.isfinite(self.offset_m):
            raise ValueError("station offset_m must be finite")
        return self


class SpatialConfig(StrictConfigModel):
    origin: str = "process_entry"
    line_length_m: float = Field(gt=0)
    stations: dict[str, SpatialStationConfig] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_names(self) -> "SpatialConfig":
        _validate_signal_name(self.origin)
        if not math.isfinite(self.line_length_m):
            raise ValueError("spatial line_length_m must be finite")
        for name in self.stations:
            _validate_signal_name(name)
        outside = [
            name for name, station in self.stations.items()
            if station.offset_m > self.line_length_m
        ]
        if outside:
            raise ValueError(f"stations exceed spatial line_length_m: {sorted(outside)}")
        return self


class AggregationConfig(StrictConfigModel):
    mode: Literal["distance", "time"] = "distance"
    interval_m: float | None = Field(default=None, gt=0)
    interval_s: float | None = Field(default=None, gt=0)
    stale_after_ms: int = Field(default=5000, ge=0)

    @model_validator(mode="after")
    def validate_interval(self) -> "AggregationConfig":
        if self.mode == "distance":
            if self.interval_m is None or self.interval_s is not None:
                raise ValueError("distance aggregation requires interval_m and forbids interval_s")
        elif self.interval_s is None or self.interval_m is not None:
            raise ValueError("time aggregation requires interval_s and forbids interval_m")
        return self

    @property
    def interval(self) -> float:
        value = self.interval_m if self.mode == "distance" else self.interval_s
        assert value is not None
        return value


class ProductDataStreamConfig(StrictConfigModel):
    enabled: bool = True
    axis: Literal["distance", "time"] = "distance"
    interval_m: float | None = Field(default=None, gt=0)
    interval_s: float | None = Field(default=None, gt=0)
    stale_after_ms: int = Field(default=5000, ge=0)

    @model_validator(mode="after")
    def validate_interval(self) -> "ProductDataStreamConfig":
        if not self.enabled:
            return self
        if self.axis == "distance" and (self.interval_m is None or self.interval_s is not None):
            raise ValueError("distance product_data requires interval_m and forbids interval_s")
        if self.axis == "time" and (self.interval_s is None or self.interval_m is not None):
            raise ValueError("time product_data requires interval_s and forbids interval_m")
        return self

    @property
    def mode(self) -> Literal["distance", "time"]:
        return self.axis

    @property
    def interval(self) -> float:
        value = self.interval_m if self.axis == "distance" else self.interval_s
        assert value is not None
        return value


class ProcessDataTimeStreamConfig(StrictConfigModel):
    enabled: bool = False
    interval_s: float | None = Field(default=None, gt=0)
    stale_after_ms: int = Field(default=5000, ge=0)
    when: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_interval(self) -> "ProcessDataTimeStreamConfig":
        if self.enabled and self.interval_s is None:
            raise ValueError("enabled process_data_time requires interval_s")
        return self


class StreamsConfig(StrictConfigModel):
    product_data: ProductDataStreamConfig | None = None
    process_data_time: ProcessDataTimeStreamConfig = Field(default_factory=ProcessDataTimeStreamConfig)


class StatusConfig(StrictConfigModel):
    metrics: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_metrics(self) -> "StatusConfig":
        for name, signal in self.metrics.items():
            _validate_signal_name(name)
            _validate_signal_name(signal)
        return self


class QuestDbConfig(StrictConfigModel):
    enabled: bool = False
    client_conf: str = "http::addr=localhost:9000;"
    retry_interval_s: float = Field(default=30.0, gt=0)
    max_backoff_s: float = Field(default=300.0, gt=0)
    product_data_table: str | None = None
    process_data_time_table: str | None = None
    product_summary_table: str | None = None
    product_relation_table: str | None = None

    @model_validator(mode="after")
    def validate_tables(self) -> "QuestDbConfig":
        tables = [table for table in (
            self.product_data_table, self.process_data_time_table,
            self.product_summary_table, self.product_relation_table,
        ) if table is not None]
        if len(tables) != len(set(tables)):
            raise ValueError("QuestDB table names must be different")
        if not re.match(r"^https?::", self.client_conf):
            raise ValueError("QuestDB client_conf must use http:: or https::")
        if not self.client_options.get("addr"):
            raise ValueError("QuestDB client_conf requires addr")
        return self

    @property
    def client_options(self) -> dict[str, str]:
        options_text = self.client_conf.split("::", 1)[1]
        return {
            key.strip(): value
            for item in options_text.split(";")
            if "=" in item
            for key, value in [item.split("=", 1)]
        }

    @property
    def query_url(self) -> str:
        protocol = self.client_conf.split("::", 1)[0]
        return f"{protocol}://{self.client_options['addr']}/exec"


class LocalRetentionConfig(StrictConfigModel):
    synced_days: int = Field(default=7, gt=0)
    event_log_days: int = Field(default=30, gt=0)


class AppConfig(StrictConfigModel):
    service: ServiceConfig
    process: ProcessConfig
    mqtt: MqttConfig = Field(default_factory=MqttConfig)
    opcua: OpcUaConfig = Field(default_factory=OpcUaConfig)
    mappings: list[MappingConfig]
    lifecycle: LifecycleConfig = Field(default_factory=LifecycleConfig)
    streams: StreamsConfig = Field(default_factory=StreamsConfig)
    status: StatusConfig = Field(default_factory=StatusConfig)
    tracking: TrackingConfig | None = None
    spatial: SpatialConfig | None = None
    questdb: QuestDbConfig = Field(default_factory=QuestDbConfig)
    local_retention: LocalRetentionConfig = Field(default_factory=LocalRetentionConfig)

    @model_validator(mode="after")
    def validate_mappings(self) -> "AppConfig":
        if self.streams.product_data is None:
            if self.process.model == "continuous":
                self.streams.product_data = ProductDataStreamConfig(
                    enabled=True, axis="distance", interval_m=1.0,
                )
            else:
                self.streams.product_data = ProductDataStreamConfig(enabled=False)
        if self.questdb.enabled:
            required_tables = ["product_summary_table"]
            if self.aggregation.enabled:
                required_tables.append("product_data_table")
            if self.streams.process_data_time.enabled:
                required_tables.append("process_data_time_table")
            if self.process.model == "transformation":
                required_tables.append("product_relation_table")
            missing_tables = [
                name for name in required_tables if getattr(self.questdb, name) is None
            ]
            if missing_tables:
                raise ValueError(
                    f"enabled QuestDB requires explicit table names: {missing_tables}"
                )
        mqtt_keys = [(m.topic, m.id) for m in self.mqtt_mappings]
        if len(mqtt_keys) != len(set(mqtt_keys)):
            raise ValueError("duplicate MQTT (topic, id) mapping")
        opcua_node_ids = [m.node_id for m in self.opcua_mappings]
        if len(opcua_node_ids) != len(set(opcua_node_ids)):
            raise ValueError("duplicate OPC UA node_id mapping")
        mapped_name_list = [mapping.name for mapping in self.mappings]
        if len(mapped_name_list) != len(set(mapped_name_list)):
            raise ValueError("duplicate normalized mapping name")
        mapped_names = set(mapped_name_list)
        missing_status_signals = sorted(set(self.status.metrics.values()) - mapped_names)
        if missing_status_signals:
            raise ValueError(
                f"status metrics reference unknown signals: {missing_status_signals}"
            )
        if self.position is not None and (
            self.position.source == "speed" or self.position.fallback_to_speed
        ):
            speed_mapping = self.mapping_by_name.get(self.position.speed_signal)
            if speed_mapping is None or not self.is_continuous_mapping(speed_mapping):
                raise ValueError("speed tracking requires a continuous speed_signal mapping")
            if speed_mapping.type not in {"float", "int"}:
                raise ValueError("speed tracking signal must be numeric")
            if speed_mapping.unit not in LINEAR_SPEED_UNITS:
                raise ValueError("speed tracking signal requires a linear-speed unit")
        if self.lifecycle.product_topics and self.lifecycle.source not in {"explicit", "both"}:
            raise ValueError("MQTT product topics require lifecycle.source explicit or both")
        if self.lifecycle.product_topics:
            product_topics = set(self.lifecycle.product_topics.topic_events)
            mapped_topics = {mapping.topic for mapping in self.mqtt_mappings}
            overlap = product_topics.intersection(mapped_topics)
            if overlap:
                raise ValueError(
                    f"MQTT product topics cannot also contain mapped tags: {sorted(overlap)}"
                )
        if self.aggregation.enabled and self.aggregation.mode == "distance":
            if self.position is None:
                raise ValueError("distance aggregation requires position configuration")
            position_name = self.position.signal if self.position.source == "direct" else self.position.speed_signal
            if position_name not in mapped_names:
                raise ValueError(f"missing mapping for position signal: {position_name}")
            position_mapping = self.mapping_by_name[position_name]
            if not self.is_continuous_mapping(position_mapping):
                raise ValueError("distance position signal must use MQTT or subscribed OPC UA")
            if self.position.reset_ratio is not None:
                if self.position.source != "direct":
                    raise ValueError("position reset_ratio is only valid for direct position")
                if self.position.reset_scope == "product":
                    if self.lifecycle.source not in {"explicit", "both"}:
                        raise ValueError("position reset coordination requires explicit lifecycle events")
                    if (
                        self.lifecycle.product_id is None
                        or self.lifecycle.product_id.on_change != EventType.PROCESS_END
                    ):
                        raise ValueError(
                            "product-scoped position reset requires product_id.on_change PROCESS_END"
                        )
                else:
                    if self.lifecycle.source not in {"explicit", "both"}:
                        raise ValueError("segment reset scope requires explicit lifecycle events")
                    if (
                        self.lifecycle.product_id is None
                        or self.lifecycle.product_id.on_change != EventType.PROCESS_START
                    ):
                        raise ValueError(
                            "segment reset scope requires product_id.on_change PROCESS_START"
                        )
                    if self.position.segment_signal:
                        segment_mapping = self.mapping_by_name.get(self.position.segment_signal)
                        if segment_mapping is None or not self.is_continuous_mapping(segment_mapping):
                            raise ValueError("segment_signal must use MQTT or subscribed OPC UA")
                        if segment_mapping.type != "int":
                            raise ValueError("segment_signal mapping type must be int")
        elif self.aggregation.enabled and self.position is not None:
            raise ValueError("time aggregation does not use position configuration")
        if (not self.aggregation.enabled or self.aggregation.mode != "distance") and (
            self.spatial is not None
            or any(m.station is not None or m.spatial_offset_m is not None for m in self.mappings)
        ):
            raise ValueError("spatial configuration is only valid for distance aggregation")
        if self.spatial is None and any(
            mapping.station is not None or mapping.spatial_offset_m is not None
            for mapping in self.mappings
        ):
            raise ValueError("station and spatial_offset_m require spatial configuration")
        station_names = set(self.spatial.stations) if self.spatial else set()
        origin = self.spatial.origin if self.spatial else None
        for mapping in self.mappings:
            if mapping.station is not None and mapping.station != origin and mapping.station not in station_names:
                raise ValueError(f"unknown station for {mapping.name}: {mapping.station}")
        aggregate_names = [variable.name for variable in self.aggregation_variables]
        if len(aggregate_names) != len(set(aggregate_names)):
            raise ValueError("product_data output names must be unique")
        time_names = [variable.name for variable in self.process_time_variables]
        if len(time_names) != len(set(time_names)):
            raise ValueError("process_data_time output names must be unique")
        if time_names and not self.streams.process_data_time.enabled:
            raise ValueError("process_data_time outputs require the stream to be enabled")
        if self.process.model != "continuous" and self.tracking is not None:
            raise ValueError("tracking is only valid for continuous processes")
        if self.process.model == "continuous" and self.aggregation.enabled and self.aggregation.mode == "distance" and self.tracking is None:
            raise ValueError("continuous distance product_data requires tracking")
        if self.process.model == "cycle" and self.aggregation.enabled and self.aggregation.axis != "time":
            raise ValueError("cycle product_data must use the time axis")
        if (
            self.process.model == "cycle" and self.process.close_run == "process_end"
            and self.lifecycle.source not in {"explicit", "both"}
        ):
            raise ValueError("cycle close_run=process_end requires explicit lifecycle events")
        if self.process.model == "transformation" and self.lifecycle.source not in {"explicit", "both"}:
            raise ValueError("transformation lifecycle requires explicit events")
        reserved = set(aggregate_names).intersection(PRODUCT_DATA_COLUMNS)
        if reserved:
            raise ValueError(f"aggregated signal names collide with product_data columns: {sorted(reserved)}")
        reserved_time = set(time_names).intersection(PROCESS_DATA_TIME_COLUMNS)
        if reserved_time:
            raise ValueError(
                "aggregated signal names collide with process_data_time columns: "
                f"{sorted(reserved_time)}"
            )
        for mapping in self.mappings:
            for output in [*mapping.outputs.product_data, *mapping.outputs.process_data_time]:
                validate_aggregate(
                    mapping.name, mapping.type, output.calculation,
                    mapping.outputs.output_type_for(output),
                )
        if self.lifecycle.product_id:
            product_mapping = self.mapping_by_name.get(self.lifecycle.product_id.signal)
            if product_mapping is None or not self.is_continuous_mapping(product_mapping):
                raise ValueError("product ID lifecycle requires a continuous mapped string tag")
            if product_mapping.type != "string":
                raise ValueError("product ID lifecycle signal must be a string tag")
            late_binding = self.lifecycle.product_id.late_binding
            if late_binding.enabled and late_binding.start_on_position_reset and (
                self.position is None
                or self.position.source != "direct"
                or self.position.reset_ratio is None
                or self.position.reset_scope != "product"
            ):
                raise ValueError(
                    "late-binding start_on_position_reset requires a direct, "
                    "product-scoped position reset"
                )
        return self

    @property
    def process_id(self) -> str:
        return self.process.id

    @property
    def position(self) -> TrackingConfig | None:
        return self.tracking

    @property
    def aggregation(self) -> ProductDataStreamConfig:
        assert self.streams.product_data is not None
        return self.streams.product_data

    @property
    def topics(self) -> list[str]:
        topics = {mapping.topic for mapping in self.mqtt_mappings}
        if self.lifecycle.product_topics:
            topics.update(self.lifecycle.product_topics.topic_events)
        return sorted(topics)

    @property
    def mqtt_mappings(self) -> list[MqttMappingConfig]:
        return [mapping for mapping in self.mappings if isinstance(mapping, MqttMappingConfig)]

    @property
    def opcua_mappings(self) -> list[OpcUaMappingConfig]:
        return [mapping for mapping in self.mappings if isinstance(mapping, OpcUaMappingConfig)]

    @property
    def subscribed_opcua_mappings(self) -> list[OpcUaMappingConfig]:
        return [mapping for mapping in self.opcua_mappings if mapping.subscribe]

    @property
    def opcua_application_uri(self) -> str:
        if self.opcua.application_uri is not None:
            return self.opcua.application_uri
        hostname = quote(socket.gethostname(), safe="-._~")
        service_name = quote(self.service.name, safe="-._~")
        return f"urn:{hostname}:{service_name}"

    @property
    def mapping_by_name(self) -> dict[str, BaseMappingConfig]:
        return {mapping.name: mapping for mapping in self.mappings}

    @property
    def signal_names(self) -> set[str]:
        return set(self.mapping_by_name)

    @property
    def speed_unit(self) -> MeasurementUnit | None:
        if self.position is None:
            return None
        mapping = self.mapping_by_name.get(self.position.speed_signal)
        return mapping.unit if mapping is not None else None

    @staticmethod
    def is_continuous_mapping(mapping: BaseMappingConfig) -> bool:
        return isinstance(mapping, MqttMappingConfig) or (
            isinstance(mapping, OpcUaMappingConfig) and mapping.subscribe
        )

    @property
    def aggregation_variables(self) -> list[AggregationVariableConfig]:
        return [
            AggregationVariableConfig(
                name=output.name or mapping.name,
                source_name=mapping.name,
                spatial_offset_m=self.effective_spatial_offset(mapping),
                calculation=output.calculation,
                output_type=mapping.outputs.output_type_for(output),
            )
            for mapping in self.mappings
            for output in mapping.outputs.product_data
        ]

    @property
    def process_time_variables(self) -> list[AggregationVariableConfig]:
        return [
            AggregationVariableConfig(
                name=output.name or mapping.name,
                source_name=mapping.name,
                calculation=output.calculation,
                output_type=mapping.outputs.output_type_for(output),
            )
            for mapping in self.mappings
            for output in mapping.outputs.process_data_time
        ]

    def effective_spatial_offset(self, mapping: BaseMappingConfig) -> float:
        if mapping.spatial_offset_m is not None:
            return mapping.spatial_offset_m
        if mapping.station is not None and self.spatial is not None:
            if mapping.station == self.spatial.origin:
                return 0.0
            return self.spatial.stations[mapping.station].offset_m
        return 0.0

    @property
    def aggregation_storage_schema(self) -> dict[str, str]:
        return {variable.name: variable.output_type for variable in self.aggregation_variables}

    @property
    def process_time_storage_schema(self) -> dict[str, str]:
        return {variable.name: variable.output_type for variable in self.process_time_variables}

def _validate_signal_name(name: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"invalid signal name: {name!r}")


def validate_aggregate(
    name: str, source_type: SignalType, calculation: AggregationCalculation,
    output_type: OutputType,
) -> None:
    if calculation == "weighted_mean":
        if source_type not in {"float", "int"} or output_type != "double":
            raise ValueError(f"{name}: weighted_mean requires numeric input and double output")
    if calculation in {"min", "max"} and source_type not in {"float", "int"}:
        raise ValueError(f"{name}: {calculation} requires numeric input")
    if output_type == "double" and source_type not in {"float", "int"}:
        raise ValueError(f"{name}: double output requires numeric input")
    if output_type in {"int", "long"} and source_type != "int":
        raise ValueError(
            f"{name}: {source_type} input cannot be stored as {output_type} without a conversion policy"
        )


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as stream:
        raw: dict[str, Any] = yaml.safe_load(stream) or {}
    return apply_environment(AppConfig.model_validate(raw))


def apply_environment(config: AppConfig) -> AppConfig:
    """Apply deployment-only overrides without mixing them into line semantics."""
    raw = config.model_dump()
    overrides: tuple[tuple[str, str, str, Any], ...] = (
        ("MQTT_ENABLED", "mqtt", "enabled", _parse_bool),
        ("MQTT_HOST", "mqtt", "host", str),
        ("MQTT_PORT", "mqtt", "port", int),
        ("MQTT_CLIENT_ID", "mqtt", "client_id", str),
        ("MQTT_USERNAME", "mqtt", "username", str),
        ("MQTT_PASSWORD", "mqtt", "password", str),
        ("OPCUA_ENABLED", "opcua", "enabled", _parse_bool),
        ("OPCUA_ENDPOINT", "opcua", "endpoint", str),
        ("OPCUA_APPLICATION_URI", "opcua", "application_uri", str),
        ("OPCUA_TIMEOUT_S", "opcua", "timeout_s", float),
        ("OPCUA_RECONNECT_INTERVAL_S", "opcua", "reconnect_interval_s", float),
        ("OPCUA_PUBLISHING_INTERVAL_MS", "opcua", "publishing_interval_ms", float),
        ("OPCUA_SAMPLING_INTERVAL_MS", "opcua", "sampling_interval_ms", float),
        ("OPCUA_USERNAME", "opcua", "username", str),
        ("OPCUA_PASSWORD", "opcua", "password", str),
        ("OPCUA_SECURITY_POLICY", "opcua", "security_policy", str),
        ("OPCUA_SECURITY_MODE", "opcua", "security_mode", str),
        ("OPCUA_CERTIFICATE_PATH", "opcua", "certificate_path", str),
        ("OPCUA_PRIVATE_KEY_PATH", "opcua", "private_key_path", str),
        ("OPCUA_PRIVATE_KEY_PASSWORD", "opcua", "private_key_password", str),
        ("OPCUA_SERVER_CERTIFICATE_PATH", "opcua", "server_certificate_path", str),
        ("QUESTDB_ENABLED", "questdb", "enabled", _parse_bool),
        ("QUESTDB_CLIENT_CONF", "questdb", "client_conf", str),
    )
    for environment_name, section, field_name, converter in overrides:
        value = os.getenv(environment_name)
        if value is not None:
            raw[section][field_name] = converter(value)
    return AppConfig.model_validate(raw)


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean environment value: {value!r}")

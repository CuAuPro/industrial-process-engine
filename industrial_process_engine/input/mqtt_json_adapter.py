from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from industrial_process_engine.config import MqttConfig, MqttMappingConfig, MqttProductTopicsConfig
from industrial_process_engine.domain import EventType, ProcessEvent, SignalBatch, SignalUpdate
from .conversion import coerce_signal_value

log = logging.getLogger(__name__)
_MISSING = object()


class MqttJsonAdapter:
    """Convert configured MQTT tag payloads and raw product-ID topics."""

    def __init__(
        self, mappings: Iterable[MqttMappingConfig], config: MqttConfig,
        product_topics: MqttProductTopicsConfig | None = None,
    ) -> None:
        self._mappings = {(mapping.topic, mapping.id): mapping for mapping in mappings}
        self._config = config
        self._product_topics = product_topics

    def parse(self, topic: str, payload: bytes | str) -> SignalBatch | ProcessEvent | None:
        product_event_type = (
            self._product_topics.topic_events.get(topic)
            if self._product_topics is not None else None
        )
        try:
            raw = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
            raise ValueError(f"invalid MQTT JSON payload on {topic}: {exc}") from exc
        if not isinstance(raw, dict):
            raise ValueError(f"invalid MQTT JSON payload on {topic}: expected an object")

        received_ms = time.time_ns() // 1_000_000
        if product_event_type is not None:
            return self._product_topic_event(
                product_event_type, raw, topic, received_ms,
            )
        if self._config.payload_format == "value_array":
            items, batch_timestamp = self._values_items(raw, topic, received_ms)
        else:
            batch_timestamp = _timestamp_ms(
                _read_path(raw, self._config.flat_timestamp_path), received_ms,
            )
            items = (
                (str(source_id), value, True, batch_timestamp)
                for source_id, value in raw.items()
            )

        output: list[SignalUpdate] = []
        for source_id, raw_value, quality, timestamp in items:
            mapping = self._mappings.get((topic, source_id))
            if mapping is None:
                log.debug("Ignoring unmapped MQTT ID %s on %s", source_id, topic)
                continue
            try:
                value = coerce_signal_value(raw_value, mapping.type)
                valid = coerce_signal_value(quality, "bool")
            except (TypeError, ValueError) as exc:
                log.warning("Invalid value for %s: %s", mapping.name, exc)
                continue
            output.append(SignalUpdate(
                mapping.name, value, valid, batch_timestamp, source_id, topic,
                timestamp if self._config.payload_format == "value_array" else None,
                batch_timestamp if self._config.payload_format == "value_array" else None,
            ))
        if not output:
            return None
        return SignalBatch(tuple(output), batch_timestamp, "mqtt", reason=f"mqtt:{topic}")

    def _product_topic_event(
        self, event_type: EventType, raw: dict[str, Any], topic: str,
        received_ms: int,
    ) -> ProcessEvent | None:
        assert self._product_topics is not None
        raw_parent_ids: Any = []
        if self._config.payload_format == "value_array":
            items, _ = self._values_items(raw, topic, received_ms)
            matches = [
                item for item in items if item[0] == self._product_topics.id
            ]
            if len(matches) != 1:
                raise ValueError(
                    f"invalid MQTT product-topic payload on {topic}: expected one "
                    f"value with ID {self._product_topics.id!r}"
                )
            _, raw_product_id, quality, timestamp = matches[0]
            parent_match = next(
                (item for item in items if item[0] == self._product_topics.parent_ids), None,
            )
            raw_parent_ids = parent_match[1] if parent_match else []
            try:
                quality_valid = coerce_signal_value(quality, "bool")
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"invalid MQTT product-topic quality on {topic}: {quality!r}"
                ) from exc
            if not quality_valid:
                log.warning("Ignoring bad-quality product ID on %s", topic)
                return None
        else:
            raw_product_id = raw.get(self._product_topics.id, _MISSING)
            if raw_product_id is _MISSING:
                raise ValueError(
                    f"invalid MQTT product-topic payload on {topic}: missing "
                    f"key {self._product_topics.id!r}"
                )
            timestamp = _timestamp_ms(
                _read_path(raw, self._config.flat_timestamp_path), received_ms,
            )
            raw_parent_ids = raw.get(self._product_topics.parent_ids, [])
        if not isinstance(raw_product_id, str):
            raise ValueError(
                f"invalid MQTT product-topic payload on {topic}: product ID must be a string"
            )
        product_id = raw_product_id.strip()
        if not product_id:
            raise ValueError(f"invalid MQTT product-topic payload on {topic}: product ID is empty")
        if not isinstance(raw_parent_ids, list) or not all(
            isinstance(parent_id, str) for parent_id in raw_parent_ids
        ):
            raise ValueError("ParentProductIds must be an array of strings")
        parent_ids = tuple(parent_id.strip() for parent_id in raw_parent_ids if parent_id.strip())
        return ProcessEvent(
            event_type, timestamp, product_id, source="mqtt",
            parent_product_ids=parent_ids,
        )

    def _values_items(
        self, raw: dict[str, Any], topic: str, received_ms: int,
    ) -> tuple[list[tuple[str, Any, Any, int]], int]:
        values = _read_path(raw, self._config.values_path)
        if not isinstance(values, list):
            raise ValueError(
                f"invalid MQTT JSON payload on {topic}: "
                f"{self._config.values_path!r} must contain an array"
            )
        batch_timestamp = _timestamp_ms(
            _read_path(raw, self._config.envelope_timestamp_path), received_ms,
        )
        items: list[tuple[str, Any, Any, int]] = []
        for index, item in enumerate(values):
            if not isinstance(item, dict):
                raise ValueError(
                    f"invalid MQTT JSON payload on {topic}: "
                    f"item {index} in {self._config.values_path!r} must be an object"
                )
            source_id = _read_path(item, self._config.id_field)
            value = _read_path(item, self._config.value_field)
            if source_id is _MISSING or value is _MISSING:
                raise ValueError(
                    f"invalid MQTT JSON payload on {topic}: item {index} requires "
                    f"{self._config.id_field!r} and {self._config.value_field!r}"
                )
            quality = _read_path(item, self._config.quality_field)
            item_timestamp = _read_path(item, self._config.item_timestamp_field)
            items.append((
                str(source_id), value,
                True if quality is _MISSING else quality,
                _timestamp_ms(item_timestamp, batch_timestamp),
            ))
        return items, batch_timestamp


def _read_path(value: Any, path: str) -> Any:
    current = value
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return _MISSING
        current = current[part]
    return current


def _timestamp_ms(value: Any, fallback: int) -> int:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(parsed.timestamp() * 1000)
        except ValueError:
            pass
    return fallback

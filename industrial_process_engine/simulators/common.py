from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class PublishedMessage:
    topic: str
    payload: dict[str, Any]
    timestamp_ms: int | None = None

    def json(self) -> str:
        return json.dumps(self.payload, separators=(",", ":"))


def envelope(timestamp_ms: int, values: dict[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": timestamp_ms,
        "values": [
            {"id": name, "v": value, "q": True, "t": timestamp_ms}
            for name, value in values.items()
        ],
    }


def retimestamp_value_array(message: PublishedMessage, timestamp_ms: int) -> PublishedMessage:
    """Give a generated value-array message its actual live publication time."""
    payload = dict(message.payload)
    payload["timestamp"] = timestamp_ms
    payload["values"] = [
        {**item, "t": timestamp_ms} for item in payload.get("values", [])
    ]
    return PublishedMessage(message.topic, payload, timestamp_ms)


def mqtt_client(args: Any) -> Any:
    import paho.mqtt.client as mqtt

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=args.client_id)
    if args.username:
        client.username_pw_set(args.username, args.password)
    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()
    return client

from __future__ import annotations

import logging
import queue
from typing import Any

import paho.mqtt.client as mqtt

from industrial_process_engine.config import AppConfig
from .mqtt_json_adapter import MqttJsonAdapter

log = logging.getLogger(__name__)


class MqttInput:
    def __init__(self, config: AppConfig, adapter: MqttJsonAdapter, output: queue.Queue[Any]) -> None:
        self.config = config
        self.adapter = adapter
        self.output = output
        self.connected = False
        self.last_error: str | None = None
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=config.mqtt.client_id)
        if config.mqtt.username:
            self.client.username_pw_set(config.mqtt.username, config.mqtt.password)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    def start(self) -> None:
        self.client.connect_async(self.config.mqtt.host, self.config.mqtt.port, self.config.mqtt.keepalive)
        self.client.loop_start()

    def stop(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()
        self.connected = False

    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        self.connected = reason_code == 0
        if self.connected:
            self.last_error = None
            for topic in self.config.topics:
                client.subscribe(topic, qos=self.config.mqtt.qos)
            log.info("Connected to MQTT and subscribed to %s", self.config.topics)
        else:
            self.last_error = f"MQTT connection failed: {reason_code}"
            log.error("MQTT connection failed: %s", reason_code)

    def _on_disconnect(self, client: mqtt.Client, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        self.connected = False
        self.last_error = None if reason_code == 0 else f"MQTT disconnected: {reason_code}"
        log.warning("MQTT disconnected: %s", reason_code)

    @property
    def connection_status(self) -> str:
        return "CONNECTED" if self.connected else "WAITING_FOR_CONNECTION"

    def _on_message(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
        try:
            item = self.adapter.parse(message.topic, message.payload)
            if item is not None:
                self.output.put_nowait(item)
        except (ValueError, queue.Full):
            log.exception("Could not accept MQTT message on %s", message.topic)

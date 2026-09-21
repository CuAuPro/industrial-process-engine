from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Callable, Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

from industrial_process_engine.config import OpcUaConfig, OpcUaMappingConfig
from industrial_process_engine.domain import SignalBatch, SignalUpdate
from .conversion import coerce_signal_value

log = logging.getLogger(__name__)


class OpcUaClient:
    """Persistent OPC UA datasource with subscriptions and explicit batch reads."""

    def __init__(
        self,
        config: OpcUaConfig,
        mappings: Iterable[OpcUaMappingConfig],
        output: queue.Queue[Any],
        client_factory: Callable[..., Any] | None = None,
        *,
        application_uri: str | None = None,
        application_name: str | None = None,
    ) -> None:
        self.config = config
        self.mappings = list(mappings)
        self.mapping_by_name = {mapping.name: mapping for mapping in self.mappings}
        self.mapping_by_node = {mapping.node_id: mapping for mapping in self.mappings}
        self.output = output
        self.application_uri = application_uri
        self.application_name = application_name
        self._client_factory = client_factory or self._default_client_factory
        self._client: Any | None = None
        self._subscription: Any | None = None
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._supervisor: threading.Thread | None = None
        self.connected = False
        self.last_error: str | None = None

    def start(self) -> None:
        if self._supervisor and self._supervisor.is_alive():
            return
        self._stop.clear()
        self._supervisor = threading.Thread(
            target=self._supervise, name="opcua-client", daemon=True,
        )
        self._supervisor.start()

    def stop(self) -> None:
        self._stop.set()
        if self._supervisor:
            self._supervisor.join(timeout=max(2.0, self.config.timeout_s + 1.0))
        with self._lock:
            self._disconnect_locked(invalidate=False)

    def read_many(self, names: Iterable[str], reason: str = "explicit_read") -> SignalBatch:
        requested = tuple(names)
        if not requested:
            return SignalBatch((), self._now(), "opcua", reason)
        if len(requested) != len(set(requested)):
            raise ValueError("duplicate OPC UA mapping name in read request")
        mappings: list[OpcUaMappingConfig] = []
        for name in requested:
            mapping = self.mapping_by_name.get(name)
            if mapping is None:
                raise ValueError(f"unknown OPC UA mapping: {name}")
            mappings.append(mapping)

        requested_at = self._now()
        with self._lock:
            try:
                if not self.connected:
                    self._connect_locked()
                assert self._client is not None
                nodes = [self._client.get_node(mapping.node_id) for mapping in mappings]
                data_values = self._read_attributes(nodes)
                if len(data_values) != len(mappings):
                    raise RuntimeError("OPC UA batch read returned an unexpected result count")
            except Exception as error:
                self._disconnect_locked(invalidate=True)
                message = f"OPC UA batch read failed: {error}"
                self.last_error = message
                log.warning(message)
                updates = tuple(self._bad_update(mapping, requested_at) for mapping in mappings)
                return SignalBatch(updates, requested_at, "opcua", reason, (message,))

        updates: list[SignalUpdate] = []
        errors: list[str] = []
        for mapping, data_value in zip(mappings, data_values, strict=True):
            update, error = self._from_data_value(mapping, data_value, requested_at)
            updates.append(update)
            if error:
                errors.append(error)
        return SignalBatch(tuple(updates), requested_at, "opcua", reason, tuple(errors))

    def _supervise(self) -> None:
        while not self._stop.is_set():
            with self._lock:
                try:
                    if not self.connected:
                        self._connect_locked()
                    elif self._client is not None and hasattr(self._client, "check_connection"):
                        self._client.check_connection()
                except Exception as error:
                    self.last_error = f"{type(error).__name__}: {error}"
                    log.exception("OPC UA connection unavailable")
                    self._disconnect_locked(invalidate=True)
            self._stop.wait(self.config.reconnect_interval_s)

    def _connect_locked(self) -> None:
        if self.connected:
            return
        if self._client is not None:
            self._disconnect_locked(invalidate=False)
        client = self._client_factory(
            self.config.endpoint,
            timeout=self.config.timeout_s,
            sync_wrapper_timeout=self.config.timeout_s + 1.0,
        )
        if self.application_uri is not None:
            client.application_uri = self.application_uri
        if self.application_name is not None:
            identity_client = getattr(client, "aio_obj", client)
            identity_client.name = self.application_name
            identity_client.description = self.application_name
        self._configure_security(client)
        try:
            client.connect()
            self._client = client
            subscribed = [mapping for mapping in self.mappings if mapping.subscribe]
            if subscribed:
                self._subscription = client.create_subscription(
                    self.config.publishing_interval_ms, _SubscriptionHandler(self),
                )
                groups: dict[float, list[tuple[OpcUaMappingConfig, Any]]] = {}
                for mapping in subscribed:
                    interval = mapping.sampling_interval_ms or self.config.sampling_interval_ms
                    groups.setdefault(interval, []).append((mapping, client.get_node(mapping.node_id)))
                subscription_failures: list[OpcUaMappingConfig] = []
                for interval, entries in groups.items():
                    results = self._subscription.subscribe_data_change(
                        [node for _, node in entries], sampling_interval=interval,
                    )
                    result_list = results if isinstance(results, list) else [results]
                    for (mapping, _), result in zip(entries, result_list, strict=False):
                        if hasattr(result, "is_good") and not result.is_good():
                            subscription_failures.append(mapping)
            self.connected = True
            self.last_error = None
            if subscribed and subscription_failures:
                timestamp_ms = self._now()
                names = [mapping.name for mapping in subscription_failures]
                self.output.put_nowait(SignalBatch(
                    tuple(self._bad_update(mapping, timestamp_ms) for mapping in subscription_failures),
                    timestamp_ms, "opcua", "subscription_failed",
                    (f"OPC UA monitored-item creation failed: {names}",),
                ))
            log.info("Connected to OPC UA %s", self.config.endpoint)
        except Exception:
            try:
                client.disconnect()
            except Exception:
                pass
            self._client = None
            self._subscription = None
            raise

    @property
    def connection_status(self) -> str:
        return "CONNECTED" if self.connected else "WAITING_FOR_CONNECTION"

    def _configure_security(self, client: Any) -> None:
        if self.config.username:
            client.set_user(self.config.username)
        if self.config.password:
            client.set_password(self.config.password)
        if self.config.security_policy != "none":
            certificate_path = self.config.certificate_path
            private_key_path = self.config.private_key_path
            if not certificate_path or not private_key_path:
                certificate_path, private_key_path = client.setup_self_signed_certificate(
                    Path("data/opcua/client-private-key.pem"),
                    Path("data/opcua/client-certificate.der"),
                    subject_attrs={"organizationName": "Industrial Process Engine"},
                )
                log.info(
                    "Using generated OPC UA application certificate %s",
                    certificate_path,
                )
            private_key = str(private_key_path)
            if self.config.private_key_password:
                private_key = f"{private_key}::{self.config.private_key_password}"
            security_parts = [
                self.config.security_policy,
                self.config.security_mode,
                str(certificate_path),
                private_key,
            ]
            if self.config.server_certificate_path:
                security_parts.append(str(self.config.server_certificate_path))
            client.set_security_string(",".join(security_parts))

    def _read_attributes(self, nodes: list[Any]) -> list[Any]:
        assert self._client is not None
        try:
            from asyncua import ua
            return list(self._client.read_attributes(nodes, ua.AttributeIds.Value))
        except ImportError:
            return list(self._client.read_attributes(nodes, "Value"))

    def _subscription_update(self, node: Any, value: Any, data: Any) -> None:
        node_id = node.nodeid.to_string()
        mapping = self.mapping_by_node.get(node_id)
        if mapping is None:
            log.warning("Ignoring notification for unmapped OPC UA node %s", node_id)
            return
        data_value = getattr(getattr(data, "monitored_item", None), "Value", None)
        received_at = self._now()
        source_timestamp_ms, server_timestamp_ms = self._timestamps_ms(data_value)
        timestamp_ms = source_timestamp_ms or server_timestamp_ms or received_at
        quality = self._is_good(data_value)
        if not quality:
            timestamp_ms = received_at
        try:
            converted = coerce_signal_value(value, mapping.type) if quality else None
        except (TypeError, ValueError) as error:
            quality = False
            converted = None
            log.warning("Invalid OPC UA value for %s: %s", mapping.name, error)
        update = SignalUpdate(
            mapping.name, converted, quality, timestamp_ms, mapping.node_id, "opcua",
            source_timestamp_ms, server_timestamp_ms,
        )
        try:
            self.output.put_nowait(SignalBatch((update,), timestamp_ms, "opcua", "subscription"))
        except queue.Full:
            log.exception("Could not enqueue OPC UA update for %s", mapping.name)

    def _subscription_status(self, status: Any) -> None:
        code = getattr(status, "Status", status)
        if hasattr(code, "is_bad") and code.is_bad():
            was_connected = self.connected
            self.connected = False
            self.last_error = f"OPC UA subscription status: {code}"
            if was_connected:
                self._invalidate_subscriptions()

    def _disconnect_locked(self, invalidate: bool) -> None:
        was_connected = self.connected
        self.connected = False
        client, self._client = self._client, None
        self._subscription = None
        if client is not None:
            try:
                client.disconnect()
            except Exception:
                log.debug("OPC UA disconnect failed", exc_info=True)
        if invalidate and was_connected:
            self._invalidate_subscriptions()

    def _invalidate_subscriptions(self) -> None:
        timestamp_ms = self._now()
        updates = tuple(
            self._bad_update(mapping, timestamp_ms) for mapping in self.mappings if mapping.subscribe
        )
        if not updates:
            return
        try:
            self.output.put_nowait(SignalBatch(
                updates, timestamp_ms, "opcua", "disconnect",
                ("OPC UA connection lost; subscribed signals invalidated",),
            ))
        except queue.Full:
            log.exception("Could not enqueue OPC UA disconnect invalidation")

    def _from_data_value(
        self, mapping: OpcUaMappingConfig, data_value: Any, fallback_ts: int,
    ) -> tuple[SignalUpdate, str | None]:
        source_timestamp_ms, server_timestamp_ms = self._timestamps_ms(data_value)
        timestamp_ms = source_timestamp_ms or server_timestamp_ms or fallback_ts
        if not self._is_good(data_value):
            status = getattr(data_value, "StatusCode", "bad status")
            return self._bad_update(
                mapping, fallback_ts, source_timestamp_ms, server_timestamp_ms,
            ), f"{mapping.name}: OPC UA status {status}"
        try:
            variant = getattr(data_value, "Value", None)
            raw = getattr(variant, "Value", None)
            value = coerce_signal_value(raw, mapping.type)
            return SignalUpdate(
                mapping.name, value, True, timestamp_ms, mapping.node_id, "opcua",
                source_timestamp_ms, server_timestamp_ms,
            ), None
        except (TypeError, ValueError) as error:
            return self._bad_update(
                mapping, fallback_ts, source_timestamp_ms, server_timestamp_ms,
            ), f"{mapping.name}: {error}"

    @staticmethod
    def _is_good(data_value: Any) -> bool:
        if data_value is None:
            return True
        status = getattr(data_value, "StatusCode", None)
        return status is None or not hasattr(status, "is_good") or bool(status.is_good())

    @staticmethod
    def _timestamps_ms(data_value: Any) -> tuple[int | None, int | None]:
        if data_value is None:
            return None, None
        source = getattr(data_value, "SourceTimestamp", None)
        server = getattr(data_value, "ServerTimestamp", None)
        source_ms = int(source.timestamp() * 1000) if isinstance(source, datetime) else None
        server_ms = int(server.timestamp() * 1000) if isinstance(server, datetime) else None
        return source_ms, server_ms

    @staticmethod
    def _bad_update(
        mapping: OpcUaMappingConfig, timestamp_ms: int,
        source_timestamp_ms: int | None = None, server_timestamp_ms: int | None = None,
    ) -> SignalUpdate:
        return SignalUpdate(
            mapping.name, None, False, timestamp_ms, mapping.node_id, "opcua",
            source_timestamp_ms, server_timestamp_ms,
        )

    @staticmethod
    def _now() -> int:
        return time.time_ns() // 1_000_000

    @staticmethod
    def _default_client_factory(*args: Any, **kwargs: Any) -> Any:
        from asyncua.sync import Client
        return Client(*args, **kwargs)


class _SubscriptionHandler:
    def __init__(self, owner: OpcUaClient) -> None:
        self.owner = owner

    def datachange_notification(self, node: Any, value: Any, data: Any) -> None:
        self.owner._subscription_update(node, value, data)

    def status_change_notification(self, status: Any) -> None:
        self.owner._subscription_status(status)

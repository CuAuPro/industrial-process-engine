from __future__ import annotations

import argparse
import random
import time
from dataclasses import dataclass
from typing import Iterator

from .common import PublishedMessage, mqtt_client

PROCESS_TOPIC = "demo/cutting/process"
ENTER_TOPIC = "demo/cutting/product/enter"
EXIT_TOPIC = "demo/cutting/product/exit"


@dataclass(frozen=True, slots=True)
class Settings:
    children: int = 3
    interval_s: float = 0.5
    seed: int | None = None


class TransformationCuttingLineSimulator:
    """Generate one parent coil and multiple child sheets in one explicit run."""

    def __init__(self, settings: Settings, start_timestamp_ms: int | None = None) -> None:
        if settings.children < 1 or settings.interval_s <= 0:
            raise ValueError("children and interval_s must be positive")
        self.settings = settings
        self.timestamp_ms = start_timestamp_ms or time.time_ns() // 1_000_000
        self.random = random.Random(settings.seed)

    def messages(self) -> Iterator[PublishedMessage]:
        parent_id = "COIL-PARENT-001"
        yield self._process(True, 0.0)
        self._advance()
        yield self._product(ENTER_TOPIC, parent_id)
        self._advance()
        yield self._product(EXIT_TOPIC, parent_id)
        self._advance()
        for number in range(1, self.settings.children + 1):
            child_id = f"SHEET-{number:04d}"
            yield self._product(ENTER_TOPIC, child_id, [parent_id])
            self._advance()
            yield self._process(True, 35.0 + self.random.uniform(-3.0, 3.0))
            self._advance()
            yield self._product(EXIT_TOPIC, child_id)
            self._advance()
        yield self._process(False, 0.0)

    def _process(self, active: bool, force: float) -> PublishedMessage:
        return PublishedMessage(PROCESS_TOPIC, {
            "Timestamp": self.timestamp_ms, "RunActive": active,
            "CutForce": round(force, 3),
        }, self.timestamp_ms)

    def _product(
        self, topic: str, product_id: str, parents: list[str] | None = None,
    ) -> PublishedMessage:
        payload: dict[str, object] = {
            "Timestamp": self.timestamp_ms, "ProductId": product_id,
        }
        if parents:
            payload["ParentProductIds"] = parents
        return PublishedMessage(topic, payload, self.timestamp_ms)

    def _advance(self) -> None:
        self.timestamp_ms += round(self.settings.interval_s * 1000)


def publish(args: argparse.Namespace) -> None:
    simulator = TransformationCuttingLineSimulator(Settings(
        args.children, args.interval_s, args.seed,
    ))
    client = None if args.dry_run else mqtt_client(args)
    try:
        for message in simulator.messages():
            if client is None:
                print(message.topic, message.json())
            else:
                payload = {**message.payload, "Timestamp": time.time_ns() // 1_000_000}
                client.publish(message.topic, PublishedMessage(message.topic, payload).json(), qos=args.qos).wait_for_publish()
            if not args.no_sleep:
                time.sleep(args.interval_s)
    finally:
        if client is not None:
            client.loop_stop()
            client.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a transformation cutting-line run")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=1)
    parser.add_argument("--client-id", default="transformation-cutting-line-simulator")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--children", type=int, default=3)
    parser.add_argument("--interval-s", type=float, default=0.5)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-sleep", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    publish(parse_args())

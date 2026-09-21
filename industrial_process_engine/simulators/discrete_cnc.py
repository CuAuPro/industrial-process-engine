from __future__ import annotations

import argparse
import random
import time
from dataclasses import dataclass
from typing import Iterator

from .common import PublishedMessage, mqtt_client

PROCESS_TOPIC = "demo/cnc/process"
ENTER_TOPIC = "demo/cnc/product/enter"
EXIT_TOPIC = "demo/cnc/product/exit"


@dataclass(frozen=True, slots=True)
class Settings:
    parts: int = 3
    samples_per_part: int = 8
    interval_s: float = 0.5
    seed: int | None = None


class DiscreteCncSimulator:
    """Generate one product lifecycle for every CNC machining cycle."""

    def __init__(self, settings: Settings, start_timestamp_ms: int | None = None) -> None:
        if settings.parts < 1 or settings.samples_per_part < 1 or settings.interval_s <= 0:
            raise ValueError("parts, samples_per_part and interval_s must be positive")
        self.settings = settings
        self.timestamp_ms = start_timestamp_ms or time.time_ns() // 1_000_000
        self.random = random.Random(settings.seed)

    def messages(self) -> Iterator[PublishedMessage]:
        for number in range(1, self.settings.parts + 1):
            product_id = f"CNC-{number:04d}"
            yield self._product(ENTER_TOPIC, product_id)
            self._advance()
            for sample in range(self.settings.samples_per_part):
                phase = sample / max(1, self.settings.samples_per_part - 1)
                load = 20.0 + 55.0 * (1.0 - abs(2.0 * phase - 1.0))
                load += self.random.uniform(-2.0, 2.0)
                temperature = 25.0 + 30.0 * phase + 0.1 * load
                oil_pressure = 3.5 + 0.02 * load + self.random.uniform(-0.1, 0.1)
                yield PublishedMessage(PROCESS_TOPIC, {
                    "Timestamp": self.timestamp_ms, "SpindleLoad": round(load, 3),
                    "Temperature": round(temperature, 3),
                    "OilPressure": round(oil_pressure, 3),
                }, self.timestamp_ms)
                self._advance()
            yield self._product(EXIT_TOPIC, product_id)
            self._advance()

    def _product(self, topic: str, product_id: str) -> PublishedMessage:
        return PublishedMessage(
            topic, {"Timestamp": self.timestamp_ms, "ProductId": product_id},
            self.timestamp_ms,
        )

    def _advance(self) -> None:
        self.timestamp_ms += round(self.settings.interval_s * 1000)


def publish(args: argparse.Namespace) -> None:
    simulator = DiscreteCncSimulator(Settings(
        args.parts, args.samples_per_part, args.interval_s, args.seed,
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
    parser = argparse.ArgumentParser(description="Publish discrete CNC demo cycles")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=1)
    parser.add_argument("--client-id", default="discrete-cnc-simulator")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--parts", type=int, default=3)
    parser.add_argument("--samples-per-part", type=int, default=8)
    parser.add_argument("--interval-s", type=float, default=0.5)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-sleep", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    publish(parse_args())

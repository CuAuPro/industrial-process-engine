from __future__ import annotations

import argparse
import math
import random
import time
from dataclasses import dataclass
from typing import Iterator, Literal

from .common import PublishedMessage, mqtt_client
from .consumption_metrics import furnace_consumption_metrics as consumption_metrics


PROCESS_TOPIC = "demo/furnace/process"
ENTER_TOPIC = "demo/furnace/product/enter"
EXIT_TOPIC = "demo/furnace/product/exit"


@dataclass(frozen=True, slots=True)
class Settings:
    mode: Literal["batch", "pusher"] = "batch"
    batch_size: int = 3
    batch_cycles: int = 1
    products: int = 6
    pusher_capacity: int = 3
    push_interval_s: float = 12.0
    initial_temperature_c: float = 20.0
    setpoint_c: float = 900.0
    ramp_rate_c_s: float = 60.0
    max_natural_gas_flow_m3_h: float = 120.0
    piece_time_constant_s: float = 8.0
    soak_time_s: float = 5.0
    soak_tolerance_c: float = 8.0
    interval_s: float = 0.5
    product_id: str | None = None
    seed: int | None = None

    def __post_init__(self) -> None:
        if self.mode not in {"batch", "pusher"}:
            raise ValueError("mode must be batch or pusher")
        integer_values = (self.batch_size, self.batch_cycles, self.products, self.pusher_capacity)
        if any(value < 1 for value in integer_values):
            raise ValueError("piece counts, cycles, and pusher capacity must be at least one")
        positive = (
            self.push_interval_s, self.ramp_rate_c_s,
            self.max_natural_gas_flow_m3_h, self.piece_time_constant_s, self.interval_s,
        )
        if any(value <= 0 for value in positive):
            raise ValueError("timing and thermal parameters must be positive")
        if self.setpoint_c <= self.initial_temperature_c:
            raise ValueError("setpoint must exceed initial temperature")
        if self.soak_time_s < 0 or self.soak_tolerance_c < 0:
            raise ValueError("soak time and tolerance cannot be negative")


@dataclass(slots=True)
class Piece:
    product_id: str
    temperature_c: float
    entered_at_ms: int
    position: int = 1


class FurnaceSimulator:
    """Generate batch or pusher furnace cycles with first-order heating physics."""

    def __init__(self, settings: Settings, start_timestamp_ms: int | None = None) -> None:
        self.settings = settings
        self.timestamp_ms = (
            time.time_ns() // 1_000_000 if start_timestamp_ms is None else start_timestamp_ms
        )
        self.furnace_temperature_c = settings.initial_temperature_c
        self.natural_gas_meter_m3 = 0.0
        self.natural_gas_flow_m3_h = 0.0
        self.random = random.Random(settings.seed)
        self.active: list[Piece] = []
        self.push_number = 0
        self._id_number = 0

    def messages(self) -> Iterator[PublishedMessage]:
        yield self._process_message(soaking=False)
        if self.settings.mode == "batch":
            yield from self._batch_messages()
        else:
            yield from self._pusher_messages()

    def _batch_messages(self) -> Iterator[PublishedMessage]:
        for cycle in range(self.settings.batch_cycles):
            pieces = [self._new_piece() for _ in range(self.settings.batch_size)]
            self.active.extend(pieces)
            for piece in pieces:
                yield self._product_message(ENTER_TOPIC, piece.product_id)

            soaking_s = 0.0
            while soaking_s < self.settings.soak_time_s - 1e-9:
                self._advance_thermal(self.settings.interval_s)
                at_temperature = min(
                    piece.temperature_c for piece in self.active
                ) >= self.settings.setpoint_c - self.settings.soak_tolerance_c
                soaking_s = soaking_s + self.settings.interval_s if at_temperature else 0.0
                yield self._process_message(soaking=at_temperature)

            for piece in tuple(self.active):
                yield self._product_message(EXIT_TOPIC, piece.product_id)
            self.active.clear()
            yield self._process_message(soaking=False)
            if cycle + 1 < self.settings.batch_cycles:
                self._advance_clock(self.settings.interval_s)

    def _pusher_messages(self) -> Iterator[PublishedMessage]:
        inserted = 0
        first = self._new_piece()
        self.active.append(first)
        inserted += 1
        yield self._product_message(ENTER_TOPIC, first.product_id)
        yield self._process_message(soaking=False)

        while self.active or inserted < self.settings.products:
            elapsed_s = 0.0
            while elapsed_s < self.settings.push_interval_s - 1e-9:
                step_s = min(
                    self.settings.interval_s, self.settings.push_interval_s - elapsed_s,
                )
                self._advance_thermal(step_s)
                elapsed_s += step_s
                yield self._process_message(soaking=False)

            self.push_number += 1
            for piece in self.active:
                piece.position += 1
            exiting = [
                piece for piece in self.active
                if piece.position > self.settings.pusher_capacity
            ]
            self.active = [
                piece for piece in self.active
                if piece.position <= self.settings.pusher_capacity
            ]
            for piece in exiting:
                yield self._product_message(EXIT_TOPIC, piece.product_id)

            if inserted < self.settings.products:
                piece = self._new_piece()
                self.active.append(piece)
                inserted += 1
                yield self._product_message(ENTER_TOPIC, piece.product_id)
            yield self._process_message(soaking=False)

    def _advance_thermal(self, elapsed_s: float) -> None:
        burner_power_pct = self._burner_power_pct()
        self.natural_gas_flow_m3_h = (
            self.settings.max_natural_gas_flow_m3_h * burner_power_pct / 100.0
        )
        self.natural_gas_meter_m3 += self.natural_gas_flow_m3_h * elapsed_s / 3600.0
        furnace_delta = min(
            self.settings.ramp_rate_c_s * elapsed_s,
            self.settings.setpoint_c - self.furnace_temperature_c,
        )
        self.furnace_temperature_c += max(0.0, furnace_delta)
        response = 1.0 - math.exp(-elapsed_s / self.settings.piece_time_constant_s)
        for piece in self.active:
            piece.temperature_c += (
                self.furnace_temperature_c - piece.temperature_c
            ) * response
        self._advance_clock(elapsed_s)

    def _process_message(self, soaking: bool) -> PublishedMessage:
        power_pct = self._burner_power_pct()
        self.natural_gas_flow_m3_h = (
            self.settings.max_natural_gas_flow_m3_h * power_pct / 100.0
        )
        payload: dict[str, object] = {
            "FurnaceMode": self.settings.mode,
            "FurnaceTemperatureC": round(self.furnace_temperature_c, 3),
            "TemperatureSetpointC": round(self.settings.setpoint_c, 3),
            "BurnerPowerPct": round(power_pct, 3),
            "NaturalGasMeterM3": round(self.natural_gas_meter_m3, 6),
            "NaturalGasFlowM3H": round(self.natural_gas_flow_m3_h, 3),
            "ActivePieceCount": len(self.active),
            "PushNumber": self.push_number,
            "Soaking": soaking,
        }
        if self.active:
            temperatures = [piece.temperature_c for piece in self.active]
            residence_s = [
                (self.timestamp_ms - piece.entered_at_ms) / 1000.0 for piece in self.active
            ]
            payload.update({
                "PieceTemperatureAvgC": round(sum(temperatures) / len(temperatures), 3),
                "PieceTemperatureMinC": round(min(temperatures), 3),
                "PieceTemperatureMaxC": round(max(temperatures), 3),
                "MaximumResidenceS": round(max(residence_s), 3),
            })
        return PublishedMessage(PROCESS_TOPIC, payload, self.timestamp_ms)

    def _burner_power_pct(self) -> float:
        error_c = max(0.0, self.settings.setpoint_c - self.furnace_temperature_c)
        heating_demand = min(100.0, error_c / 2.0)
        holding_demand = 12.0 if self.active else 0.0
        return max(heating_demand, holding_demand)

    def _product_message(self, topic: str, product_id: str) -> PublishedMessage:
        return PublishedMessage(topic, {"ProductId": product_id}, self.timestamp_ms)

    def _new_piece(self) -> Piece:
        self._id_number += 1
        if self.settings.product_id:
            product_id = (
                self.settings.product_id if self._total_products() == 1
                else f"{self.settings.product_id}-{self._id_number:03d}"
            )
        else:
            product_id = f"FURNACE-{self.random.getrandbits(48):012X}"
        return Piece(product_id, self.settings.initial_temperature_c, self.timestamp_ms)

    def _total_products(self) -> int:
        if self.settings.mode == "batch":
            return self.settings.batch_size * self.settings.batch_cycles
        return self.settings.products

    def _advance_clock(self, elapsed_s: float) -> None:
        self.timestamp_ms += round(elapsed_s * 1000)


def publish(args: argparse.Namespace) -> None:
    simulator = FurnaceSimulator(Settings(
        mode=args.mode,
        batch_size=args.batch_size,
        batch_cycles=args.batch_cycles,
        products=args.products,
        pusher_capacity=args.pusher_capacity,
        push_interval_s=args.push_interval_s,
        initial_temperature_c=args.initial_temperature_c,
        setpoint_c=args.setpoint_c,
        ramp_rate_c_s=args.ramp_rate_c_s,
        max_natural_gas_flow_m3_h=args.max_natural_gas_flow_m3_h,
        piece_time_constant_s=args.piece_time_constant_s,
        soak_time_s=args.soak_time_s,
        soak_tolerance_c=args.soak_tolerance_c,
        interval_s=args.interval_s,
        product_id=args.product_id,
        seed=args.seed,
    ))
    client = None if args.dry_run else mqtt_client(args)
    previous_timestamp_ms: int | None = None
    try:
        for message in simulator.messages():
            assert message.timestamp_ms is not None
            current_timestamp_ms = message.timestamp_ms
            if not args.no_sleep and previous_timestamp_ms is not None:
                time.sleep(max(0.0, (current_timestamp_ms - previous_timestamp_ms) / 1000.0))
            if client is None:
                print(message.topic, message.json())
            else:
                info = client.publish(message.topic, message.json(), qos=args.qos)
                info.wait_for_publish()
            previous_timestamp_ms = current_timestamp_ms
    finally:
        if client is not None:
            client.loop_stop()
            client.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a batch or pusher furnace cycle")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=1)
    parser.add_argument("--client-id", default="furnace-simulator")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--mode", choices=("batch", "pusher"), default="batch")
    parser.add_argument("--batch-size", type=int, default=3)
    parser.add_argument("--batch-cycles", type=int, default=1)
    parser.add_argument("--products", type=int, default=6, help="Pieces inserted in pusher mode")
    parser.add_argument("--pusher-capacity", type=int, default=3)
    parser.add_argument("--push-interval-s", type=float, default=12.0)
    parser.add_argument("--initial-temperature-c", type=float, default=20.0)
    parser.add_argument("--setpoint-c", type=float, default=900.0)
    parser.add_argument("--ramp-rate-c-s", type=float, default=60.0)
    parser.add_argument("--max-natural-gas-flow-m3-h", type=float, default=120.0)
    parser.add_argument("--piece-time-constant-s", type=float, default=8.0)
    parser.add_argument("--soak-time-s", type=float, default=5.0)
    parser.add_argument("--soak-tolerance-c", type=float, default=8.0)
    parser.add_argument("--interval-s", type=float, default=0.5)
    parser.add_argument("--product-id", help="Fixed ID or numbered prefix")
    parser.add_argument("--seed", type=int, help="Reproduce generated product IDs")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-sleep", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    publish(parse_args())

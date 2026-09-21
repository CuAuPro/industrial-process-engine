from __future__ import annotations

import argparse
import math
import random
import time
from dataclasses import dataclass
from typing import Iterator

from industrial_process_engine.hooks import ProcessHooks
from industrial_process_engine.processing.derived_signals import DerivedSignalRegistry
from industrial_process_engine.processing.product_fields import ProductFieldRegistry

from .common import PublishedMessage, envelope, mqtt_client, retimestamp_value_array
from .consumption_metrics import continuous_line_consumption_metrics as consumption_metrics


PROCESS_TOPIC = "demo/continuous-line/process"
EVENT_TOPIC = "demo/continuous-line/events"
# Self-contained application declarations for running this shipped demo from
# main.py without importing production-specific hooks or coil fields.
derived_signals = DerivedSignalRegistry()
product_fields = ProductFieldRegistry()
AppHooks = ProcessHooks
hooks = AppHooks()


@dataclass(frozen=True, slots=True)
class Settings:
    products: int = 3
    product_id: str | None = None
    product_length_m: float = 50.0
    speed_m_min: float = 60.0
    interval_s: float = 0.5
    seed: int | None = None

    def __post_init__(self) -> None:
        if self.products < 1:
            raise ValueError("products must be at least one")
        if self.product_length_m <= 0 or self.speed_m_min <= 0 or self.interval_s <= 0:
            raise ValueError("length, speed, and interval must be positive")


class ContinuousLineSimulator:
    """Generate products whose PLC distance counter resets before each new ID."""

    def __init__(self, settings: Settings, start_timestamp_ms: int | None = None) -> None:
        self.settings = settings
        self.timestamp_ms = (
            time.time_ns() // 1_000_000 if start_timestamp_ms is None else start_timestamp_ms
        )
        self.id_random = random.Random(settings.seed)
        self.electricity_meter_kwh = 0.0
        self.natural_gas_meter_m3 = 0.0
        self.electrical_power_kw = 0.0
        self.natural_gas_flow_m3_h = 0.0
        self.process_sample_no = 0

    def messages(self) -> Iterator[PublishedMessage]:
        step_m = self.settings.speed_m_min / 60 * self.settings.interval_s
        for number in range(1, self.settings.products + 1):
            product_id = self._product_id(number)
            # The paired YAML uses this reset as PROCESS_START. Product-ID change
            # only closes the preceding run and identifies the pending product.
            yield self._process(0.0, 0.0)
            self._advance()
            yield PublishedMessage(
                EVENT_TOPIC, envelope(self.timestamp_ms, {"ProductId": product_id}),
            )
            self._advance()
            product_position_m = 0.0
            while product_position_m < self.settings.product_length_m - 1e-9:
                movement = min(step_m, self.settings.product_length_m - product_position_m)
                product_position_m += movement
                yield self._process(product_position_m, self.settings.speed_m_min)
                self._advance()
            yield self._process(product_position_m, 0.0)
            self._advance()
            yield PublishedMessage(
                EVENT_TOPIC, envelope(self.timestamp_ms, {"ProductId": ""}),
            )
            self._advance()

    def continuous_scans(self) -> Iterator[PublishedMessage]:
        """Continue normal PLC scans after the finite product scenario."""
        while True:
            yield self._process(self.settings.product_length_m, self.settings.speed_m_min)
            self._advance()

    def _product_id(self, number: int) -> str:
        if self.settings.product_id:
            if self.settings.products == 1:
                return self.settings.product_id
            return f"{self.settings.product_id}-{number:03d}"
        return f"LINE-{self.id_random.getrandbits(48):012X}"

    def _process(self, position_m: float, speed_m_min: float) -> PublishedMessage:
        # PLC-style scan values: every tag is always published. The simulator
        # does not associate either load with a product or material position;
        # station offsets and product alignment belong exclusively to the L2.
        phase = self.process_sample_no * 0.17
        brush_values: dict[str, float] = {}
        actual_loads: list[float] = []
        set_loads: list[float] = []
        for brush_no in (1, 2):
            brush_phase = phase + (brush_no - 1) * 0.43
            set_load = 48.0 + brush_no * 2.0 + 3.0 * math.sin(brush_phase)
            # Repeatable PLC-side test disturbance. Brush 1 is 6 m downstream,
            # so a head position of 10-13 m maps to material meters 4-7 there.
            if brush_no == 1 and 10.0 <= position_m < 13.0:
                set_load *= 1.25
            # Brush 2 is 12 m downstream, so this maps to material meters 8-13.
            if brush_no == 2 and 20.0 <= position_m < 25.0:
                set_load *= 1.25
            actual_load = set_load + 0.8 * math.sin(brush_phase * 1.9)
            set_speed = 900.0 + brush_no * 35.0 + 25.0 * math.sin(brush_phase * 0.7)
            actual_speed = set_speed + 6.0 * math.sin(brush_phase * 1.4)
            set_position = 18.0 + brush_no * 1.5 + 1.2 * math.sin(brush_phase * 0.5)
            actual_position = set_position + 0.25 * math.sin(brush_phase * 1.6)
            brush_values.update({
                f"Brush{brush_no}SetLoad": round(set_load, 4),
                f"Brush{brush_no}ActualLoad": round(actual_load, 4),
                f"Brush{brush_no}SetSpeed": round(set_speed, 4),
                f"Brush{brush_no}ActualSpeed": round(actual_speed, 4),
                f"Brush{brush_no}SetPosition": round(set_position, 4),
                f"Brush{brush_no}ActualPosition": round(actual_position, 4),
            })
            set_loads.append(set_load)
            actual_loads.append(actual_load)
        self.process_sample_no += 1
        moving = abs(speed_m_min) > 1e-9
        self.electrical_power_kw = 25.0 + (sum(actual_loads) * 0.9 if moving else 0.0)
        self.natural_gas_flow_m3_h = 1.0 + (
            18.0 + sum(set_loads) * 0.06 if moving else 0.0
        )
        return PublishedMessage(PROCESS_TOPIC, envelope(self.timestamp_ms, {
            "PositionM": round(position_m, 4),
            "SpeedMMin": round(speed_m_min, 4),
            **brush_values,
            "ElectricityMeterKWh": round(self.electricity_meter_kwh, 6),
            "ElectricalPowerKW": round(self.electrical_power_kw, 4),
            "NaturalGasMeterM3": round(self.natural_gas_meter_m3, 6),
            "NaturalGasFlowM3H": round(self.natural_gas_flow_m3_h, 4),
        }))

    def _advance(self) -> None:
        elapsed_h = self.settings.interval_s / 3600.0
        self.electricity_meter_kwh += self.electrical_power_kw * elapsed_h
        self.natural_gas_meter_m3 += self.natural_gas_flow_m3_h * elapsed_h
        self.timestamp_ms += round(self.settings.interval_s * 1000)


def publish(args: argparse.Namespace) -> None:
    simulator = ContinuousLineSimulator(Settings(
        products=args.products, product_id=args.product_id,
        product_length_m=args.product_length_m, speed_m_min=args.speed_m_min,
        interval_s=args.interval_s, seed=args.seed,
    ))
    client = None if args.dry_run else mqtt_client(args)
    last_live_timestamp_ms = 0
    try:
        messages = simulator.messages()
        if client is not None:
            from itertools import chain
            messages = chain(messages, simulator.continuous_scans())
        for message in messages:
            if client is None:
                print(message.topic, message.json())
            else:
                last_live_timestamp_ms = max(
                    last_live_timestamp_ms + 1, time.time_ns() // 1_000_000,
                )
                message = retimestamp_value_array(message, last_live_timestamp_ms)
                info = client.publish(message.topic, message.json(), qos=args.qos)
                info.wait_for_publish()
            if not args.no_sleep:
                time.sleep(args.interval_s)
    finally:
        if client is not None:
            client.loop_stop()
            client.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a demo continuous-line cycle")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=1)
    parser.add_argument("--client-id", default="continuous-line-simulator")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--products", type=int, default=3)
    parser.add_argument(
        "--product-id", help="Fixed ID for one product; numbered prefix for multiple products",
    )
    parser.add_argument("--product-length-m", type=float, default=50.0)
    parser.add_argument("--speed-m-min", type=float, default=60.0)
    parser.add_argument("--interval-s", type=float, default=0.5)
    parser.add_argument("--seed", type=int, help="Reproduce generated product IDs")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-sleep", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    publish(parse_args())

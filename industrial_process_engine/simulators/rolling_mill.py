from __future__ import annotations

import argparse
import math
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

from .common import PublishedMessage, mqtt_client
from .consumption_metrics import rolling_mill_consumption_metrics as consumption_metrics


PROCESS_TOPIC = "demo/rolling-mill/process"
EVENT_TOPIC = "demo/rolling-mill/events"
PRODUCT_ID_TAG = "ProductId"


@dataclass(frozen=True, slots=True)
class Settings:
    products: int = 3
    product_id: str | None = None
    passes: int = 3
    initial_length_m: float = 12.0
    initial_thickness_mm: float = 2.0
    reduction_fraction: float = 0.15
    entry_speed_m_min: float = 120.0
    strip_width_m: float = 1.2
    work_roll_radius_m: float = 0.25
    flow_stress_mpa: float = 300.0
    interval_s: float = 0.25
    reverse_distance_m: float = 1.5
    seed: int | None = None

    def __post_init__(self) -> None:
        positive = (
            self.initial_length_m, self.initial_thickness_mm, self.entry_speed_m_min,
            self.strip_width_m, self.work_roll_radius_m, self.flow_stress_mpa,
            self.interval_s,
        )
        if self.products < 1:
            raise ValueError("products must be at least one")
        if self.passes < 1:
            raise ValueError("passes must be at least one")
        if any(value <= 0 for value in positive):
            raise ValueError("physical dimensions, speed, stress, and interval must be positive")
        if not 0 < self.reduction_fraction < 1:
            raise ValueError("reduction_fraction must be between zero and one")
        if self.reverse_distance_m < 0:
            raise ValueError("reverse distance cannot be negative")


@dataclass(frozen=True, slots=True)
class PassPlan:
    pass_number: int
    entry_length_m: float
    exit_length_m: float
    entry_thickness_mm: float
    exit_thickness_set_mm: float
    entry_speed_m_min: float
    exit_speed_m_min: float
    force_set_kn: float

    @property
    def reduction_fraction(self) -> float:
        return 1.0 - self.exit_thickness_set_mm / self.entry_thickness_mm


class RollingMillSimulator:
    """Generate a finite multi-pass cycle with approximate rolling physics."""

    def __init__(self, settings: Settings, start_timestamp_ms: int | None = None) -> None:
        self.settings = settings
        self.timestamp_ms = (
            time.time_ns() // 1_000_000 if start_timestamp_ms is None else start_timestamp_ms
        )
        self.random = random.Random(settings.seed)
        self.id_random = random.Random(settings.seed)
        self.electricity_meter_kwh = 0.0
        self.electrical_power_kw = 35.0
        self.pass_plans = self._build_pass_plans()

    def _build_pass_plans(self) -> tuple[PassPlan, ...]:
        plans: list[PassPlan] = []
        entry_length_m = self.settings.initial_length_m
        entry_thickness_mm = self.settings.initial_thickness_mm
        for pass_number in range(1, self.settings.passes + 1):
            exit_thickness_mm = entry_thickness_mm * (1.0 - self.settings.reduction_fraction)
            # Constant width and approximate volume conservation: L_in*h_in = L_out*h_out.
            exit_length_m = entry_length_m * entry_thickness_mm / exit_thickness_mm
            # Steady mass flow through the roll gap: v_in*h_in = v_out*h_out.
            exit_speed_m_min = (
                self.settings.entry_speed_m_min * entry_thickness_mm / exit_thickness_mm
            )
            draft_m = (entry_thickness_mm - exit_thickness_mm) / 1000.0
            contact_length_m = math.sqrt(self.settings.work_roll_radius_m * draft_m)
            contact_area_m2 = self.settings.strip_width_m * contact_length_m
            force_set_kn = self.settings.flow_stress_mpa * 1_000 * contact_area_m2
            plans.append(PassPlan(
                pass_number=pass_number,
                entry_length_m=entry_length_m,
                exit_length_m=exit_length_m,
                entry_thickness_mm=entry_thickness_mm,
                exit_thickness_set_mm=exit_thickness_mm,
                entry_speed_m_min=self.settings.entry_speed_m_min,
                exit_speed_m_min=exit_speed_m_min,
                force_set_kn=force_set_kn,
            ))
            entry_length_m = exit_length_m
            entry_thickness_mm = exit_thickness_mm
        return tuple(plans)

    def messages(self) -> Iterator[PublishedMessage]:
        for product_number in range(1, self.settings.products + 1):
            first = self.pass_plans[0]
            yield self._process_message(first, 0.0, 0.0)
            self._advance_clock()
            yield self._event_message(self._product_id(product_number))
            self._advance_clock()

            for plan in self.pass_plans:
                exit_travel_m = 0.0
                if plan.pass_number > 1:
                    yield self._process_message(plan, exit_travel_m, 0.0)
                    self._advance_clock()

                reversed_once = False
                reverse_remaining_m = 0.0
                step_m = plan.exit_speed_m_min / 60.0 * self.settings.interval_s
                reverse_at_m = plan.exit_length_m * 0.45
                while exit_travel_m < plan.exit_length_m - 1e-9:
                    should_reverse = plan.pass_number == 2 and (
                        reverse_remaining_m > 0
                        or (not reversed_once and exit_travel_m >= reverse_at_m)
                    )
                    if should_reverse and reverse_remaining_m <= 0:
                        reverse_remaining_m = min(self.settings.reverse_distance_m, exit_travel_m)
                        reversed_once = True
                    if reverse_remaining_m > 0:
                        movement_m = min(step_m, reverse_remaining_m)
                        reverse_remaining_m -= movement_m
                        exit_travel_m = max(0.0, exit_travel_m - movement_m)
                        speed_m_min = -plan.exit_speed_m_min
                    else:
                        exit_travel_m += min(step_m, plan.exit_length_m - exit_travel_m)
                        speed_m_min = plan.exit_speed_m_min
                    yield self._process_message(plan, exit_travel_m, speed_m_min)
                    self._advance_clock()

                yield self._process_message(plan, exit_travel_m, 0.0)
                self._advance_clock()

            yield self._event_message("")
            self._advance_clock()

    def _product_id(self, number: int) -> str:
        if self.settings.product_id:
            if self.settings.products == 1:
                return self.settings.product_id
            return f"{self.settings.product_id}-{number:03d}"
        return f"ROLL-{self.id_random.getrandbits(48):012X}"

    def _process_message(
        self, plan: PassPlan, exit_travel_m: float, speed_m_min: float,
    ) -> PublishedMessage:
        progress = min(1.0, max(0.0, exit_travel_m / plan.exit_length_m))
        entry_travel_m = progress * plan.entry_length_m
        force_actual_kn = plan.force_set_kn * (
            1.0 + 0.018 * math.sin(progress * math.tau)
        ) + self.random.uniform(-20, 20)
        thickness_error_mm = (0.025 / plan.pass_number) * math.exp(-3.0 * progress)
        thickness_error_mm += self.random.uniform(-0.002, 0.002)
        exit_thickness_actual_mm = plan.exit_thickness_set_mm + thickness_error_mm
        speed_m_s = abs(speed_m_min) / 60.0
        self.electrical_power_kw = 35.0 if speed_m_s <= 1e-9 else (
            60.0
            + abs(force_actual_kn) * speed_m_s * plan.reduction_fraction * 0.8
        )
        return PublishedMessage(PROCESS_TOPIC, {
            "RgcTrfRef": round(plan.force_set_kn, 3),
            "RgcTrfAct": round(force_actual_kn, 3),
            # Actual exit thickness minus its setpoint.
            "AgcdHEx": round(thickness_error_mm, 6),
            "PrrNrOfPasses": self.settings.passes,
            "PrrPassNumber": plan.pass_number,
            "MdrVAct": round(speed_m_min, 3),
            "ElectricityMeterKWh": round(self.electricity_meter_kwh, 6),
            "ElectricalPowerKW": round(self.electrical_power_kw, 3),
            "Timestamp": self._formatted_time(),
            "EntryLengthM": round(plan.entry_length_m, 6),
            "ExitLengthM": round(plan.exit_length_m, 6),
            "EntryTravelM": round(entry_travel_m, 6),
            "ExitTravelM": round(exit_travel_m, 6),
            "EntryThicknessMm": round(plan.entry_thickness_mm, 6),
            "ExitThicknessSetMm": round(plan.exit_thickness_set_mm, 6),
            "ExitThicknessActMm": round(exit_thickness_actual_mm, 6),
            # Diagnostic alternative to speed integration; intentionally not mapped.
            "InternalDistanceM": round(exit_travel_m, 6),
        })

    def _event_message(self, product_id: str) -> PublishedMessage:
        return PublishedMessage(EVENT_TOPIC, {
            PRODUCT_ID_TAG: product_id,
            "Timestamp": self._formatted_time(),
        })

    def _formatted_time(self) -> str:
        return datetime.fromtimestamp(
            self.timestamp_ms / 1000, timezone.utc,
        ).isoformat(timespec="milliseconds")

    def _advance_clock(self) -> None:
        self.electricity_meter_kwh += (
            self.electrical_power_kw * self.settings.interval_s / 3600.0
        )
        self.timestamp_ms += round(self.settings.interval_s * 1000)


def publish(args: argparse.Namespace) -> None:
    simulator = RollingMillSimulator(Settings(
        products=args.products,
        product_id=args.product_id,
        passes=args.passes,
        initial_length_m=args.initial_length_m,
        initial_thickness_mm=args.initial_thickness_mm,
        reduction_fraction=args.reduction_fraction,
        entry_speed_m_min=args.entry_speed_m_min,
        strip_width_m=args.strip_width_m,
        work_roll_radius_m=args.work_roll_radius_m,
        flow_stress_mpa=args.flow_stress_mpa,
        interval_s=args.interval_s,
        reverse_distance_m=args.reverse_distance_m,
        seed=args.seed,
    ))
    client = None if args.dry_run else mqtt_client(args)
    try:
        for message in simulator.messages():
            if client is None:
                print(message.topic, message.json())
            else:
                info = client.publish(message.topic, message.json(), qos=args.qos)
                info.wait_for_publish()
            if not args.no_sleep:
                time.sleep(args.interval_s)
    finally:
        if client is not None:
            client.loop_stop()
            client.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a demo reversing rolling-mill cycle")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=1)
    parser.add_argument("--client-id", default="rolling-mill-simulator")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--products", type=int, default=3)
    parser.add_argument(
        "--product-id", help="Fixed ID for one product; numbered prefix for multiple products",
    )
    parser.add_argument("--passes", type=int, default=3)
    parser.add_argument("--initial-length-m", type=float, default=12.0)
    parser.add_argument("--initial-thickness-mm", type=float, default=2.0)
    parser.add_argument("--reduction-fraction", type=float, default=0.15)
    parser.add_argument("--entry-speed-m-min", type=float, default=120.0)
    parser.add_argument("--strip-width-m", type=float, default=1.2)
    parser.add_argument("--work-roll-radius-m", type=float, default=0.25)
    parser.add_argument("--flow-stress-mpa", type=float, default=300.0)
    parser.add_argument("--interval-s", type=float, default=0.25)
    parser.add_argument("--reverse-distance-m", type=float, default=1.5)
    parser.add_argument("--seed", type=int, help="Reproduce IDs and process noise")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-sleep", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    publish(parse_args())

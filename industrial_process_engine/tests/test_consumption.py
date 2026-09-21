from __future__ import annotations

import pytest

from industrial_process_engine.domain import ProductContext, ProductFieldValue, SignalValue
from industrial_process_engine.processing.consumption import ConsumptionAllocator, ConsumptionMetricRegistry


def product(product_id: str, mass_t: float | None, timestamp_ms: int = 0) -> ProductContext:
    context = {}
    if mass_t is not None:
        context["mass_t"] = ProductFieldValue(mass_t, True, timestamp_ms)
    return ProductContext("RUN", product_id, timestamp_ms, context)


def counter_signal(value: float, timestamp_ms: int, quality: bool = True) -> dict[str, SignalValue]:
    return {"meter": SignalValue(value, quality, timestamp_ms)}


def test_empty_registry_does_not_track_or_reject_timestamps() -> None:
    allocator = ConsumptionAllocator("LINE", ConsumptionMetricRegistry())

    assert allocator.advance(2_000, None, (), {}) == []
    assert allocator.advance(1_000, None, (), {}) == []
    assert allocator.last_advanced_ms is None


def test_delayed_source_sample_does_not_rewind_or_fail_allocator() -> None:
    registry = ConsumptionMetricRegistry()
    registry.counter(
        "electricity", source_signal="meter", mass_field=None,
        cumulative_field="consumption_kwh", rate_field="consumption_rate_kw",
        stale_after_ms=5_000,
    )
    allocator = ConsumptionAllocator("LINE", registry)
    p1 = product("P1", None)
    allocator.advance(1_000, "RUN", (p1,), counter_signal(100, 1_000))
    allocator.advance(2_000, "RUN", (p1,), {})

    assert allocator.advance(1_500, "RUN", (p1,), counter_signal(105, 1_500)) == []
    assert allocator.last_advanced_ms == 2_000
    assert allocator.states["electricity"].last_value == 100

    allocator.advance(3_000, "RUN", (p1,), counter_signal(110, 3_000))
    assert allocator.totals["electricity"]["P1"] == pytest.approx(10)


def test_specific_field_is_not_generated_without_mass() -> None:
    registry = ConsumptionMetricRegistry()
    registry.counter(
        "electricity", source_signal="meter", mass_field=None,
        cumulative_field="consumption_kwh", rate_field="consumption_rate_kw",
        specific_field="specific_consumption_kwh_t", stale_after_ms=5_000,
    )

    assert registry["electricity"].specific_field is None
    assert registry["electricity"].summary_quality is False


def test_summary_quality_is_explicitly_opted_in() -> None:
    registry = ConsumptionMetricRegistry()
    registry.counter(
        "electricity", source_signal="meter", mass_field=None,
        cumulative_field="consumption_kwh", rate_field="consumption_rate_kw",
        stale_after_ms=5_000, summary_quality=True,
    )

    assert registry["electricity"].summary_quality is True


def test_counter_conserves_energy_across_staggered_mass_membership() -> None:
    registry = ConsumptionMetricRegistry()
    registry.counter(
        "electricity", source_signal="meter", mass_field="mass_t",
        cumulative_field="consumption_kwh", rate_field="consumption_rate_kw",
        specific_field="specific_consumption_kwh_t", stale_after_ms=5_000,
    )
    allocator = ConsumptionAllocator("LINE", registry)
    p1, p2 = product("P1", 2), product("P2", 1, 3_600_000)

    allocator.advance(0, "RUN", (p1,), counter_signal(100, 0))
    rows = allocator.advance(3_600_000, "RUN", (p1,), counter_signal(130, 3_600_000))
    rows += allocator.advance(7_200_000, "RUN", (p1, p2), counter_signal(160, 7_200_000))
    rows += allocator.advance(10_800_000, "RUN", (p2,), counter_signal(190, 10_800_000))

    assert allocator.totals["electricity"] == pytest.approx({"P1": 50, "P2": 40})
    assert sum(row.allocated_consumption for row in rows) == pytest.approx(90)
    assert {row.allocation_policy for row in rows} == {"MASS"}


def test_equal_fallback_and_future_only_mass_change() -> None:
    registry = ConsumptionMetricRegistry()
    registry.counter(
        "electricity", source_signal="meter", mass_field="mass_t",
        cumulative_field="consumption_kwh", rate_field="consumption_rate_kw",
        specific_field="specific_consumption_kwh_t", stale_after_ms=5_000,
    )
    allocator = ConsumptionAllocator("LINE", registry)
    p1, p2 = product("P1", 2), product("P2", None)
    allocator.advance(0, "RUN", (p1, p2), counter_signal(0, 0))
    rows = allocator.advance(3_600_000, "RUN", (p1, p2), counter_signal(20, 3_600_000))
    p2.context["mass_t"] = ProductFieldValue(1, True, 3_600_000)
    rows += allocator.advance(7_200_000, "RUN", (p1, p2), counter_signal(50, 7_200_000))

    assert allocator.totals["electricity"] == pytest.approx({"P1": 30, "P2": 20})
    assert [row.allocation_policy for row in rows] == ["EQUAL", "EQUAL", "MASS", "MASS"]


def test_rate_stops_at_stale_limit_and_records_gap() -> None:
    registry = ConsumptionMetricRegistry()
    registry.rate(
        "electricity", source_signal="power", mass_field=None,
        cumulative_field="consumption_kwh", rate_field="consumption_rate_kw",
        specific_field="specific_consumption_kwh_t", stale_after_ms=5_000,
        refresh_interval_ms=1_000,
    )
    allocator = ConsumptionAllocator("LINE", registry)
    p1 = product("P1", None)
    allocator.advance(0, "RUN", (p1,), {"power": SignalValue(36, True, 0)})
    rows = allocator.advance(10_000, "RUN", (p1,), {})

    assert allocator.totals["electricity"]["P1"] == pytest.approx(0.05)
    assert any(row.reason == "STALE_RATE" and row.quality == "DATA_GAP" for row in rows)
    assert p1.context["consumption_rate_kw"].quality is False


def test_counter_reset_preserves_total_and_marks_quality_gap() -> None:
    registry = ConsumptionMetricRegistry()
    registry.counter(
        "electricity", source_signal="meter", mass_field=None,
        cumulative_field="consumption_kwh", rate_field="consumption_rate_kw",
        specific_field="specific_consumption_kwh_t", stale_after_ms=5_000,
    )
    allocator = ConsumptionAllocator("LINE", registry)
    p1 = product("P1", None)
    allocator.advance(0, "RUN", (p1,), counter_signal(100, 0))
    allocator.advance(1_000, "RUN", (p1,), counter_signal(110, 1_000))
    rows = allocator.advance(2_000, "RUN", (p1,), counter_signal(2, 2_000))

    assert allocator.totals["electricity"]["P1"] == pytest.approx(10)
    assert any(row.reason == "COUNTER_RESET" for row in rows)
    assert p1.context["consumption_kwh"].value == pytest.approx(10)
    assert p1.context["consumption_kwh"].quality is False

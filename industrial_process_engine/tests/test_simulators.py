import pytest

from industrial_process_engine.config import TrackingConfig, load_config
from industrial_process_engine.processing.position_tracker import PositionTracker
from industrial_process_engine.engine import ProcessEngine
from industrial_process_engine.units import MeasurementUnit
from industrial_process_engine.simulators.continuous_line import (
    ContinuousLineSimulator,
    Settings as LineSettings,
    derived_signals as continuous_line_derived_signals,
    hooks as continuous_line_hooks,
    product_fields as continuous_line_product_fields,
)
from industrial_process_engine.simulators.consumption_metrics import (
    continuous_line_consumption_metrics,
    furnace_consumption_metrics,
    rolling_mill_consumption_metrics,
)
from industrial_process_engine.simulators.furnace import (
    ENTER_TOPIC as FURNACE_ENTER_TOPIC,
    EXIT_TOPIC as FURNACE_EXIT_TOPIC,
    PROCESS_TOPIC as FURNACE_PROCESS_TOPIC,
    FurnaceSimulator,
    Settings as FurnaceSettings,
)
from industrial_process_engine.simulators.rolling_mill import (
    EVENT_TOPIC,
    PROCESS_TOPIC,
    RollingMillSimulator,
    Settings as MillSettings,
)


def values(message):
    if "values" not in message.payload:
        return message.payload
    return {item["id"]: item["v"] for item in message.payload["values"]}


def timestamp_ms(message):
    if "timestamp" in message.payload:
        return message.payload["timestamp"]
    return round(__import__("datetime").datetime.fromisoformat(
        message.payload["Timestamp"],
    ).timestamp() * 1000)


def test_rolling_mill_uses_separate_product_topic_and_reverses_in_pass_two():
    settings = MillSettings(
        products=1, passes=3, initial_length_m=6, entry_speed_m_min=120,
        interval_s=0.25, reverse_distance_m=1,
        seed=42,
    )
    simulator = RollingMillSimulator(settings, start_timestamp_ms=1_000)
    messages = list(simulator.messages())
    process = [message for message in messages if message.topic == PROCESS_TOPIC]
    events = [message for message in messages if message.topic == EVENT_TOPIC]
    rows = [values(message) for message in process]

    event_ids = [values(message)["ProductId"] for message in events]
    assert event_ids[0].startswith("ROLL-")
    assert event_ids[1] == ""
    assert all("values" not in message.payload for message in process + events)
    assert all("ProductId" not in row for row in rows)
    assert {
        "RgcTrfRef", "RgcTrfAct", "AgcdHEx", "PrrNrOfPasses",
        "PrrPassNumber", "MdrVAct", "Timestamp",
    } < set(rows[0])
    assert "InternalDistanceM" in rows[0]
    assert {row["PrrPassNumber"] for row in rows} == {1, 2, 3}
    assert {row["PrrNrOfPasses"] for row in rows} == {3}
    assert any(row["MdrVAct"] < 0 for row in rows if row["PrrPassNumber"] == 2)
    assert rows[-1]["ElectricityMeterKWh"] > rows[0]["ElectricityMeterKWh"]
    assert max(row["ElectricalPowerKW"] for row in rows) > 100
    for row in rows:
        assert row["AgcdHEx"] == pytest.approx(
            row["ExitThicknessActMm"] - row["ExitThicknessSetMm"], abs=1e-6,
        )

    for plan in simulator.pass_plans:
        assert plan.exit_thickness_set_mm < plan.entry_thickness_mm
        assert plan.exit_length_m > plan.entry_length_m
        assert plan.exit_speed_m_min > plan.entry_speed_m_min
        assert plan.force_set_kn > 0
        assert plan.entry_length_m * plan.entry_thickness_mm == pytest.approx(
            plan.exit_length_m * plan.exit_thickness_set_mm,
        )
    for previous, current in zip(simulator.pass_plans, simulator.pass_plans[1:]):
        assert current.entry_length_m == pytest.approx(previous.exit_length_m)
        assert current.entry_thickness_mm == pytest.approx(previous.exit_thickness_set_mm)


def test_integrated_speed_holds_reverse_until_distance_is_recovered():
    settings = MillSettings(
        products=1, passes=2, initial_length_m=6, entry_speed_m_min=120,
        interval_s=0.25, reverse_distance_m=1,
        seed=42,
    )
    simulator = RollingMillSimulator(settings, start_timestamp_ms=0)
    messages = list(simulator.messages())
    pass_two = [
        message for message in messages
        if message.topic == PROCESS_TOPIC and values(message)["PrrPassNumber"] == 2
    ]
    tracker = PositionTracker(
        TrackingConfig(
            source="speed", speed_signal="speed",
            reverse_policy="hold", max_forward_jump_m=2,
        ),
        MeasurementUnit.METRES_PER_MINUTE,
    )
    tracker.start_product(timestamp_ms(pass_two[0]))
    movements = [
        tracker.observe("speed", values(message)["MdrVAct"], timestamp_ms(message))
        for message in pass_two
    ]

    assert any(movement is None for movement in movements[1:])
    plan = simulator.pass_plans[1]
    sampling_step_m = plan.exit_speed_m_min / 60 * settings.interval_s
    assert tracker.position_m == pytest.approx(plan.exit_length_m, abs=sampling_step_m)
    assert values(pass_two[-1])["InternalDistanceM"] == pytest.approx(plan.exit_length_m)


def test_continuous_line_and_both_demo_configs_are_valid():
    messages = list(ContinuousLineSimulator(
        LineSettings(products=2, product_length_m=2, interval_s=0.5, seed=42),
        start_timestamp_ms=0,
    ).messages())
    event_ids = [
        values(message)["ProductId"] for message in messages
        if message.topic.endswith("/events")
    ]
    generated_ids = event_ids[::2]
    assert event_ids[1::2] == ["", ""]
    assert len(set(generated_ids)) == 2
    assert all(product_id.startswith("LINE-") for product_id in generated_ids)
    process = [
        values(message) for message in messages
        if message.topic.endswith("/process")
    ]
    expected_tags = {
        "PositionM", "SpeedMMin",
        "Brush1SetLoad", "Brush1ActualLoad",
        "Brush1SetSpeed", "Brush1ActualSpeed",
        "Brush1SetPosition", "Brush1ActualPosition",
        "Brush2SetLoad", "Brush2ActualLoad",
        "Brush2SetSpeed", "Brush2ActualSpeed",
        "Brush2SetPosition", "Brush2ActualPosition",
        "ElectricityMeterKWh", "ElectricalPowerKW",
        "NaturalGasMeterM3", "NaturalGasFlowM3H",
    }
    assert all(expected_tags <= set(row) for row in process)
    positions = [row["PositionM"] for row in process]
    assert positions.count(0.0) >= 2
    assert max(positions) == pytest.approx(2.0)
    assert any(
        current == 0.0 and previous == pytest.approx(2.0)
        for previous, current in zip(positions, positions[1:])
    )
    assert process[-1]["ElectricityMeterKWh"] > process[0]["ElectricityMeterKWh"]
    assert process[-1]["NaturalGasMeterM3"] > process[0]["NaturalGasMeterM3"]

    package = "industrial_process_engine/simulators"
    continuous = load_config(f"{package}/continuous_line.yaml")
    assert continuous.process_id == "CONTINUOUS_LINE_DEMO"
    assert continuous.effective_spatial_offset(
        continuous.mapping_by_name["brush_1_actual_load"],
    ) == 6.0
    assert continuous.effective_spatial_offset(
        continuous.mapping_by_name["brush_2_actual_load"],
    ) == 12.0
    rolling = load_config(f"{package}/rolling_mill.yaml")
    assert rolling.process_id == "ROLLING_MILL_DEMO"
    assert rolling.position and rolling.position.source == "speed"
    assert "internal_distance_m" not in rolling.mapping_by_name


def test_rolling_mill_demo_completes_three_integrated_passes(tmp_path):
    config = load_config("industrial_process_engine/simulators/rolling_mill.yaml")
    runtime = ProcessEngine(
        config, sqlite_path=str(tmp_path / "rolling.db"),
        consumption_metrics=rolling_mill_consumption_metrics,
    )
    runtime.store.initialize()
    runtime.processor.start()
    for message in RollingMillSimulator(
        MillSettings(products=1, passes=3, initial_length_m=4, seed=42),
        start_timestamp_ms=1_000,
    ).messages():
        runtime.processor.handle(runtime.adapter.parse(message.topic, message.json()))

    product = runtime.store.list_products()[0]
    windows = runtime.store.windows_for_run(config.process_id, product["run_id"])
    assert product["state"] == "COMPLETE"
    assert product["electricity_kwh"] > 0
    assert "electricity_consumption_quality" not in product
    assert {window["segment_no"] for window in windows} == {1, 2, 3}


def test_rolling_mill_generates_multiple_unique_product_cycles():
    messages = RollingMillSimulator(
        MillSettings(
            products=3, passes=1, initial_length_m=1,
            entry_speed_m_min=120, interval_s=0.25, seed=7,
        ),
        start_timestamp_ms=0,
    ).messages()
    event_ids = [
        values(message)["ProductId"] for message in messages
        if message.topic == EVENT_TOPIC
    ]

    generated_ids = event_ids[::2]
    assert event_ids[1::2] == ["", "", ""]
    assert len(set(generated_ids)) == 3
    assert all(product_id.startswith("ROLL-") for product_id in generated_ids)


def test_batch_furnace_heats_and_soaks_three_simultaneous_pieces():
    messages = list(FurnaceSimulator(FurnaceSettings(
        mode="batch", batch_size=3, batch_cycles=1,
        initial_temperature_c=20, setpoint_c=100, ramp_rate_c_s=100,
        piece_time_constant_s=0.5, soak_time_s=1, soak_tolerance_c=2,
        interval_s=0.25, seed=11,
    ), start_timestamp_ms=0).messages())
    entered_messages = [message for message in messages if message.topic == FURNACE_ENTER_TOPIC]
    exited_messages = [message for message in messages if message.topic == FURNACE_EXIT_TOPIC]
    process = [message.payload for message in messages if message.topic == FURNACE_PROCESS_TOPIC]

    entered = [message.payload["ProductId"] for message in entered_messages]
    exited = [message.payload["ProductId"] for message in exited_messages]
    assert len(entered) == len(exited) == 3
    assert len(set(entered)) == 3
    assert set(entered) == set(exited)
    assert len({message.timestamp_ms for message in entered_messages}) == 1
    assert any(row["ActivePieceCount"] == 3 for row in process)
    assert any(row["Soaking"] for row in process)
    assert max(row["FurnaceTemperatureC"] for row in process) == 100
    assert process[-1]["NaturalGasMeterM3"] > process[0]["NaturalGasMeterM3"]
    assert max(row["NaturalGasFlowM3H"] for row in process) > 0


def test_pusher_furnace_adds_advances_and_drains_products():
    messages = list(FurnaceSimulator(FurnaceSettings(
        mode="pusher", products=5, pusher_capacity=3,
        push_interval_s=1, interval_s=0.5, seed=12,
    ), start_timestamp_ms=0).messages())
    process = [message.payload for message in messages if message.topic == FURNACE_PROCESS_TOPIC]

    entered = [
        message.payload["ProductId"] for message in messages
        if message.topic == FURNACE_ENTER_TOPIC
    ]
    exited = [
        message.payload["ProductId"] for message in messages
        if message.topic == FURNACE_EXIT_TOPIC
    ]
    assert len(entered) == len(exited) == 5
    assert len(set(entered)) == 5
    assert set(entered) == set(exited)
    assert max(row["ActivePieceCount"] for row in process) == 3
    assert max(row["PushNumber"] for row in process) == 7
    assert process[-1]["ActivePieceCount"] == 0


def test_furnace_yaml_processes_simultaneous_batch_products(tmp_path, monkeypatch):
    config = load_config("industrial_process_engine/simulators/furnace.yaml")
    runtime = ProcessEngine(
        config, sqlite_path=str(tmp_path / "furnace.db"),
        consumption_metrics=furnace_consumption_metrics,
    )
    runtime.store.initialize()
    runtime.processor.start()
    simulator = FurnaceSimulator(FurnaceSettings(
        mode="batch", batch_size=3,
        initial_temperature_c=20, setpoint_c=100, ramp_rate_c_s=100,
        piece_time_constant_s=0.5, soak_time_s=1, soak_tolerance_c=2,
        interval_s=0.25, seed=13,
    ), start_timestamp_ms=1_000)
    clock_ms = [1_000]
    monkeypatch.setattr("time.time_ns", lambda: clock_ms[0] * 1_000_000)
    for message in simulator.messages():
        assert message.timestamp_ms is not None
        clock_ms[0] = message.timestamp_ms
        runtime.processor.handle(runtime.adapter.parse(message.topic, message.json()))

    products = runtime.store.list_products()
    assert len(products) == 3
    assert {product["state"] for product in products} == {"COMPLETE"}
    assert len({product["run_id"] for product in products}) == 1
    assert all(product["natural_gas_m3"] > 0 for product in products)
    assert all("natural_gas_consumption_quality" not in product for product in products)


def test_continuous_line_consumption_reaches_product_summary(tmp_path):
    config = load_config("industrial_process_engine/simulators/continuous_line.yaml")
    runtime = ProcessEngine(
        config, sqlite_path=str(tmp_path / "continuous.db"),
        hooks=continuous_line_hooks,
        derived_signals=continuous_line_derived_signals,
        product_fields=continuous_line_product_fields,
        consumption_metrics=continuous_line_consumption_metrics,
    )
    runtime.store.initialize()
    runtime.processor.start()
    simulator = ContinuousLineSimulator(
        LineSettings(
            products=2, product_length_m=3, speed_m_min=120,
            interval_s=0.25, seed=14,
        ),
        start_timestamp_ms=1_000,
    )
    for message in simulator.messages():
        runtime.processor.handle(runtime.adapter.parse(message.topic, message.json()))
    continuous_scans = simulator.continuous_scans()
    for _ in range(60):
        message = next(continuous_scans)
        runtime.processor.handle(runtime.adapter.parse(message.topic, message.json()))
        if all(product["state"] == "COMPLETE" for product in runtime.store.list_products()):
            break

    products = runtime.store.list_products()
    assert len(products) == 2
    assert {product["state"] for product in products} == {"COMPLETE"}
    assert all(product["material_length_m"] == pytest.approx(3.0) for product in products)
    assert all(product["drained_ts"] > product["end_ts"] for product in products)
    assert all(product["electricity_kwh"] > 0 for product in products)
    assert all(product["natural_gas_m3"] > 0 for product in products)
    assert all(runtime.store.windows_for_run(config.process_id, product["run_id"]) for product in products)
    assert all(
        all(
            row["brush_2_actual_load"] is not None
            for row in runtime.store.windows_for_run(config.process_id, product["run_id"])
        )
        for product in products
    )

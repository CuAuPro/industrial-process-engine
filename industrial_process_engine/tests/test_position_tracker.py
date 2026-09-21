import pytest

from industrial_process_engine.config import TrackingConfig
from industrial_process_engine.domain import TrackingStatus
from industrial_process_engine.processing.position_tracker import PositionTracker
from industrial_process_engine.units import MeasurementUnit


def test_reverse_policy_defaults_to_hold():
    assert TrackingConfig().reverse_policy == "hold"


def test_direct_position_stop_reverse_and_jump():
    tracker = PositionTracker(TrackingConfig(source="direct", signal="position", reverse_policy="lost", max_forward_jump_m=2))
    tracker.observe("position", 100, 0)
    tracker.start_product(0)
    movement = tracker.observe("position", 100.5, 1000)
    assert movement and movement.end_m == 0.5 and movement.status == TrackingStatus.OK
    assert tracker.observe("position", 100.5, 2000) is None
    reverse = tracker.observe("position", 100.4, 3000)
    assert reverse and reverse.status == TrackingStatus.LOST and reverse.start_m == reverse.end_m
    jump = tracker.observe("position", 104, 4000)
    assert jump and jump.status == TrackingStatus.LOST
    following = tracker.observe("position", 104.5, 4500)
    assert following and following.status == TrackingStatus.LOST


def test_bad_position_quality_makes_tracking_loss_sticky():
    tracker = PositionTracker(TrackingConfig(source="direct", signal="position"))
    tracker.observe("position", 0, 0)
    tracker.start_product(0)
    tracker.invalidate("position")
    movement = tracker.observe("position", 1, 1000)
    assert movement and movement.status == TrackingStatus.LOST
    assert tracker.observe("position", 2, 2000).status == TrackingStatus.LOST


def test_large_forward_jump_is_a_recoverable_signal_gap():
    tracker = PositionTracker(TrackingConfig(
        source="direct", signal="position", max_forward_jump_m=10,
    ))
    tracker.observe("position", 10, 0)
    tracker.start_product(0)
    tracker.observe("position", 12, 1000)

    jump = tracker.observe("position", 40, 2000)
    assert jump is not None
    assert jump.signal_gap is True
    assert jump.status == TrackingStatus.LOST
    assert jump.start_m == 2
    assert jump.end_m == 30

    following = tracker.observe("position", 41, 3000)
    assert following is not None
    assert following.signal_gap is False
    assert following.status == TrackingStatus.OK


def test_hold_reverse_policy_waits_for_previous_maximum():
    tracker = PositionTracker(TrackingConfig(
        source="direct", signal="position", reverse_policy="hold",
    ))
    tracker.observe("position", 100, 0)
    tracker.start_product(0)
    assert tracker.observe("position", 101, 1000).end_m == 1

    assert tracker.observe("position", 100.5, 2000) is None
    assert tracker.observe("position", 100.9, 3000) is None
    assert tracker.position_m == 1

    resumed = tracker.observe("position", 101.2, 4000)
    assert resumed is not None
    assert resumed.start_m == 1
    assert resumed.end_m == pytest.approx(1.2)
    assert resumed.status == TrackingStatus.OK


def test_direct_reset_uses_relative_discontinuity_between_raw_readings():
    tracker = PositionTracker(TrackingConfig(
        source="direct", signal="position", reset_ratio=0.10,
    ))

    assert tracker.is_direct_reset(400, 395) is False
    assert tracker.is_direct_reset(5, 1) is False
    assert tracker.is_direct_reset(400, 2) is True
    assert tracker.is_direct_reset(400, 0.5) is True
    assert tracker.is_direct_reset(400, 0) is True


def test_speed_is_integrated_in_configured_units():
    tracker = PositionTracker(
        TrackingConfig(source="speed", speed_signal="speed"),
        MeasurementUnit.METRES_PER_MINUTE,
    )
    tracker.start_product(0)
    tracker.observe("speed", 60, 0)
    movement = tracker.observe("speed", 120, 1500)
    assert movement is not None
    assert movement.start_m == 0
    assert movement.end_m == 2.25


@pytest.mark.parametrize(("units", "speed"), [
    (MeasurementUnit.METRES_PER_SECOND, 1.0),
    (MeasurementUnit.METRES_PER_MINUTE, 60.0),
    (MeasurementUnit.CENTIMETRES_PER_SECOND, 100.0),
    (MeasurementUnit.CENTIMETRES_PER_MINUTE, 6_000.0),
    (MeasurementUnit.MILLIMETRES_PER_SECOND, 1_000.0),
    (MeasurementUnit.MILLIMETRES_PER_MINUTE, 60_000.0),
])
def test_all_linear_speed_units_convert_to_metres_per_second(units, speed):
    tracker = PositionTracker(
        TrackingConfig(source="speed", speed_signal="speed"), units,
    )
    tracker.start_product(0)
    tracker.observe("speed", speed, 0)
    movement = tracker.observe("speed", speed, 1_000)
    assert movement is not None
    assert movement.end_m == pytest.approx(1.0)


def test_speed_hold_policy_integrates_reverse_and_waits_for_catch_up():
    tracker = PositionTracker(
        TrackingConfig(source="speed", speed_signal="speed", reverse_policy="hold"),
        MeasurementUnit.METRES_PER_SECOND,
    )
    tracker.start_product(0)
    tracker.observe("speed", 1, 0)
    assert tracker.observe("speed", 1, 1000).end_m == 1

    # The signed path returns from 1 m to 0 m while material progress stays at 1 m.
    assert tracker.observe("speed", -1, 2000) is None
    assert tracker.observe("speed", -1, 3000) is None
    assert tracker.speed_path_m == pytest.approx(0)
    assert tracker.position_m == pytest.approx(1)

    # Forward motion first recovers the reversed distance. Only travel beyond
    # the previous 1 m maximum becomes new material progress.
    assert tracker.observe("speed", 1, 4000) is None
    assert tracker.observe("speed", 1, 5000) is None
    resumed = tracker.observe("speed", 1, 5500)
    assert resumed is not None
    assert resumed.start_m == pytest.approx(1)
    assert resumed.end_m == pytest.approx(1.5)


def test_speed_hold_state_is_restored_during_reversal():
    tracker = PositionTracker(
        TrackingConfig(source="speed", speed_signal="speed", reverse_policy="hold"),
        MeasurementUnit.METRES_PER_SECOND,
    )
    tracker.start_product(0)
    tracker.observe("speed", 1, 0)
    tracker.observe("speed", 1, 1000)
    tracker.observe("speed", -1, 2000)
    tracker.observe("speed", -1, 3000)

    restored = PositionTracker(tracker.config, tracker.speed_unit)
    restored.restore(
        tracker.position_m, tracker.raw_position, tracker.last_ts, str(tracker.status),
        tracker.speed_m_s, tracker.speed_valid, tracker.speed_path_m, tracker.furthest_position_m,
    )
    assert restored.speed_path_m == pytest.approx(0)
    assert restored.furthest_position_m == pytest.approx(1)

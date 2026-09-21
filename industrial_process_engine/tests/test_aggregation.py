from industrial_process_engine.aggregation.aggregator import Aggregator
from industrial_process_engine.config import AggregationVariableConfig
from industrial_process_engine.domain import SignalValue, TrackingStatus, WindowQuality


def signal(value, timestamp=0, quality=True):
    return {"temperature": SignalValue(value, quality, timestamp)}


def mean_variable(name="temperature"):
    return AggregationVariableConfig(
        name=name, calculation="weighted_mean", output_type="double"
    )


def test_distance_weighted_average():
    agg = Aggregator("L", [mean_variable()], "distance", 1.0, 10_000)
    agg.start("RUN", "P", 0)
    assert agg.add_movement(0, 0.2, 0, 200, signal(800)) == []
    records = agg.add_movement(0.2, 1.0, 200, 1000, signal(850, 200))
    assert len(records) == 1
    assert records[0].values["temperature"] == 840
    assert records[0].quality == WindowQuality.GOOD


def test_one_update_splits_across_multiple_boundaries():
    agg = Aggregator("L", [mean_variable()], "distance", 1.0, 10_000)
    agg.start("RUN", "P", 0)
    records = agg.add_movement(0, 2.3, 0, 2300, signal(10))
    assert [(r.window_no, r.position_start_m, r.position_end_m) for r in records] == [
        (0, 0.0, 1.0), (1, 1.0, 2.0)
    ]
    partial = agg.finalize_partial(2300)
    assert partial is not None
    assert partial.window_no == 2
    assert partial.position_end_m == 2.3
    assert partial.quality == WindowQuality.PARTIAL


def test_segment_resets_window_and_position_but_keeps_product_elapsed_time():
    agg = Aggregator("L", [mean_variable()], "distance", 1.0, 10_000)
    agg.start("RUN", "P", 0)
    agg.add_movement(0, 0.5, 0, 500, signal(10))
    partial = agg.pause_segment(500)
    assert partial is not None
    assert (partial.segment_no, partial.window_no, partial.position_end_m) == (1, 0, 0.5)

    agg.start_segment(2, 600)
    record = agg.add_movement(0, 1, 600, 1600, signal(20, 600))[0]
    assert (record.segment_no, record.window_no) == (2, 0)
    assert (record.position_start_m, record.position_end_m) == (0, 1)
    assert (record.elapsed_start_s, record.elapsed_end_s) == (0.6, 1.6)


def test_stale_data_and_tracking_loss_are_explicit():
    stale = Aggregator("L", [mean_variable()], "distance", 1.0, 100)
    stale.start("RUN", "P", 0)
    stale_record = stale.add_movement(0, 1, 1000, 2000, signal(10, 0))[0]
    assert stale_record.quality == WindowQuality.DATA_GAP
    assert stale_record.values["temperature"] is None
    lost = Aggregator("L", [mean_variable()], "distance", 1.0, 10000)
    lost.start("RUN", "P", 0)
    assert lost.add_movement(0, 1, 0, 1000, signal(10), TrackingStatus.LOST)[0].quality == WindowQuality.TRACKING_LOST


def test_partial_variable_coverage_is_null_not_partial_mean():
    agg = Aggregator("L", [mean_variable()], "distance", 1.0, 10_000)
    agg.start("RUN", "P", 0)
    agg.add_movement(0, 0.5, 0, 500, signal(800))
    record = agg.add_movement(0.5, 1.0, 500, 1000, signal(0, 500, quality=False))[0]
    assert record.quality == WindowQuality.DATA_GAP
    assert record.values["temperature"] is None


def test_first_last_and_text_output_types():
    specifications = [
        AggregationVariableConfig(name="recipe", calculation="last", output_type="symbol"),
        AggregationVariableConfig(name="code", calculation="first", output_type="char"),
        AggregationVariableConfig(name="description", calculation="last", output_type="varchar"),
        AggregationVariableConfig(name="counter", calculation="max", output_type="long"),
    ]
    agg = Aggregator("L", specifications, "distance", 1.0, 10_000)
    agg.start("RUN", "P", 0)
    first = {
        "recipe": SignalValue(12, True, 0),
        "code": SignalValue("A", True, 0),
        "description": SignalValue("start", True, 0),
        "counter": SignalValue(3, True, 0),
    }
    second = {
        "recipe": SignalValue(15, True, 500),
        "code": SignalValue("B", True, 500),
        "description": SignalValue("finish", True, 500),
        "counter": SignalValue(9, True, 500),
    }
    agg.add_movement(0, 0.5, 0, 500, first)
    record = agg.add_movement(0.5, 1.0, 500, 1000, second)[0]
    assert record.values == {
        "recipe": "15",
        "code": "A",
        "description": "finish",
        "counter": 9,
    }


def test_invalid_char_becomes_null_and_data_gap():
    specification = AggregationVariableConfig(
        name="temperature", calculation="last", output_type="char"
    )
    agg = Aggregator("L", [specification], "distance", 1.0, 10_000)
    agg.start("RUN", "P", 0)
    record = agg.add_movement(0, 1, 0, 1000, signal(123))[0]
    assert record.values["temperature"] is None
    assert record.quality == WindowQuality.DATA_GAP


def test_first_and_last_values_remain_valid_when_old():
    specifications = [
        AggregationVariableConfig(name="setpoint", calculation="last", output_type="long"),
        AggregationVariableConfig(name="mode", calculation="first", output_type="symbol"),
    ]
    agg = Aggregator("L", specifications, "distance", 1.0, stale_after_ms=100)
    agg.start("RUN", "P", 0)
    record = agg.add_movement(
        0,
        1,
        10_000,
        11_000,
        {
            "setpoint": SignalValue(42, True, 0),
            "mode": SignalValue("AUTO", True, 0),
        },
    )[0]
    assert record.values == {"setpoint": 42, "mode": "AUTO"}
    assert record.quality == WindowQuality.GOOD


def test_held_value_is_invalidated_by_bad_source_quality():
    specification = AggregationVariableConfig(
        name="mode", calculation="last", output_type="symbol"
    )
    agg = Aggregator("L", [specification], "distance", 1.0, stale_after_ms=100)
    agg.start("RUN", "P", 0)
    record = agg.add_movement(
        0, 1, 0, 1000, {"mode": SignalValue("AUTO", False, 0)}
    )[0]
    assert record.values["mode"] is None
    assert record.quality == WindowQuality.DATA_GAP

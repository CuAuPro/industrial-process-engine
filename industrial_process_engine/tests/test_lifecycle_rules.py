import pytest

from industrial_process_engine.config import RuleConfig
from industrial_process_engine.domain import EventType, SignalValue
from industrial_process_engine.processing.lifecycle_rules import LifecycleRuleEvaluator, SafeCondition


def test_threshold_debounce_and_rearm():
    evaluator = LifecycleRuleEvaluator([RuleConfig(when="weight > 100 and enabled", event=EventType.PROCESS_START, debounce_ms=1000)])
    state = {"weight": SignalValue(150, True, 0), "enabled": SignalValue(True, True, 0)}
    assert evaluator.update(state, 0) == []
    assert evaluator.update(state, 999) == []
    assert [e.event_type for e in evaluator.update(state, 1000)] == [EventType.PROCESS_START]
    assert evaluator.update(state, 2000) == []
    state["weight"] = SignalValue(0, True, 2100)
    assert evaluator.update(state, 2100) == []
    state["weight"] = SignalValue(150, True, 2200)
    assert evaluator.update(state, 2200) == []
    assert evaluator.update(state, 3200)[0].event_type == EventType.PROCESS_START


def test_unsafe_expression_is_rejected():
    with pytest.raises(ValueError, match="unsupported"):
        SafeCondition("__import__('os').system('whoami')")

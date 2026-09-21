from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Any

from industrial_process_engine.config import RuleConfig
from industrial_process_engine.domain import ProcessEvent, SignalValue


class SafeCondition:
    """A deliberately small expression evaluator for configured conditions."""

    _allowed = (
        ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.UnaryOp, ast.Not,
        ast.Compare, ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
        ast.Name, ast.Load, ast.Constant,
    )

    def __init__(self, expression: str) -> None:
        self.expression = expression
        self._tree = ast.parse(expression, mode="eval")
        for node in ast.walk(self._tree):
            if not isinstance(node, self._allowed):
                raise ValueError(f"unsupported condition expression element: {type(node).__name__}")
        self.names = {node.id for node in ast.walk(self._tree) if isinstance(node, ast.Name)}

    def evaluate(self, values: dict[str, Any]) -> bool:
        return bool(self._eval(self._tree.body, values))

    def _eval(self, node: ast.AST, values: dict[str, Any]) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            return values.get(node.id)
        if isinstance(node, ast.BoolOp):
            items = [bool(self._eval(v, values)) for v in node.values]
            return all(items) if isinstance(node.op, ast.And) else any(items)
        if isinstance(node, ast.UnaryOp):
            return not bool(self._eval(node.operand, values))
        if isinstance(node, ast.Compare):
            left = self._eval(node.left, values)
            for operator, comparator in zip(node.ops, node.comparators):
                right = self._eval(comparator, values)
                if left is None or right is None:
                    return False
                match operator:
                    case ast.Eq(): result = left == right
                    case ast.NotEq(): result = left != right
                    case ast.Lt(): result = left < right
                    case ast.LtE(): result = left <= right
                    case ast.Gt(): result = left > right
                    case ast.GtE(): result = left >= right
                    case _: raise ValueError("unsupported comparison")
                if not result:
                    return False
                left = right
            return True
        raise ValueError(f"unsupported expression: {ast.dump(node)}")


@dataclass
class _RuleState:
    config: RuleConfig
    condition: SafeCondition
    candidate_since: int | None = None
    fired: bool = False


class LifecycleRuleEvaluator:
    def __init__(self, rules: list[RuleConfig]) -> None:
        self._rules = [_RuleState(rule, SafeCondition(rule.when)) for rule in rules]

    def update(self, signal_state: dict[str, SignalValue], timestamp_ms: int) -> list[ProcessEvent]:
        values = {name: state.value for name, state in signal_state.items() if state.quality}
        events: list[ProcessEvent] = []
        for rule in self._rules:
            active = rule.condition.evaluate(values)
            if not active:
                rule.candidate_since = None
                rule.fired = False
                continue
            if rule.candidate_since is None:
                rule.candidate_since = timestamp_ms
            if not rule.fired and timestamp_ms - rule.candidate_since >= rule.config.debounce_ms:
                events.append(ProcessEvent(rule.config.event, timestamp_ms, source="derived"))
                rule.fired = True
        return events

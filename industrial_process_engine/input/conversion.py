from __future__ import annotations

from typing import Any


def coerce_signal_value(value: Any, target: str) -> Any:
    if target == "string":
        return str(value)
    if target == "bool":
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "on", "yes"}:
                return True
            if normalized in {"false", "0", "off", "no"}:
                return False
            raise ValueError(f"cannot convert {value!r} to bool")
        return bool(value)
    if target == "int":
        return int(value)
    if target == "float":
        return float(value)
    raise ValueError(f"unsupported signal type: {target}")

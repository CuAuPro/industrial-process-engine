from __future__ import annotations

import secrets


_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_RANDOM_SPACE = len(_ALPHABET) ** 5


def _base36(value: int) -> str:
    if value < 0:
        raise ValueError("run ID timestamp cannot be negative")
    if value == 0:
        return "0"
    encoded = ""
    while value:
        value, remainder = divmod(value, len(_ALPHABET))
        encoded = _ALPHABET[remainder] + encoded
    return encoded


def generate_run_id(timestamp_ms: int) -> str:
    """Return a short, time-sortable ID for one processing occurrence."""
    return f"{_base36(timestamp_ms).zfill(9)}{_base36(secrets.randbelow(_RANDOM_SPACE)).zfill(5)}"

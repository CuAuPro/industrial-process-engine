import re

from industrial_process_engine.processing.run_id import generate_run_id


def test_run_ids_are_short_time_sortable_and_unique(monkeypatch):
    timestamp_ms = 1_787_296_316_309
    suffixes = iter(range(1_000))
    monkeypatch.setattr(
        "industrial_process_engine.processing.run_id.secrets.randbelow",
        lambda _: next(suffixes),
    )
    run_ids = {generate_run_id(timestamp_ms) for _ in range(1_000)}

    assert len(run_ids) == 1_000
    assert all(re.fullmatch(r"[0-9A-Z]{14}", run_id) for run_id in run_ids)
    monkeypatch.setattr(
        "industrial_process_engine.processing.run_id.secrets.randbelow", lambda _: 0,
    )
    assert generate_run_id(timestamp_ms - 1) < min(run_ids)

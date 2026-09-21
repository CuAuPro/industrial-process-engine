from __future__ import annotations

import pytest

from industrial_process_engine.config import AppConfig


@pytest.fixture
def config_factory(tmp_path):
    def make(**overrides):
        raw = {
            "service": {
                "name": "test_l2",
                "title": "Test L2",
                "description": "Test material aggregation service",
                "version": "0.1.0",
            },
            "process": {"id": "TEST_LINE", "model": "continuous"},
            "mqtt": {
                "enabled": False, "host": "localhost", "client_id": "",
                "payload_format": "value_array",
            },
            "mappings": [
                {"source": "mqtt", "topic": "process", "id": "pos", "name": "position", "type": "float"},
                {
                    "source": "mqtt", "topic": "process", "id": "temp", "name": "temperature", "type": "float",
                    "outputs": {"output_type": "double", "product_data": [{
                        "calculation": "weighted_mean",
                    }]},
                },
                {"source": "mqtt", "topic": "events", "id": "pid", "name": "product_id", "type": "string"},
                {
                    "source": "mqtt", "topic": "events", "id": "active",
                    "name": "product_active", "type": "bool",
                    "true_event": "PROCESS_START", "false_event": "PROCESS_END",
                },
            ],
            "lifecycle": {"source": "explicit", "product_id": {"signal": "product_id"}},
            "tracking": {"source": "direct", "signal": "position", "max_forward_jump_m": 10},
            "streams": {"product_data": {
                "enabled": True, "axis": "distance", "interval_m": 1.0,
                "stale_after_ms": 5000,
            }},
            "questdb": {"enabled": False},
        }
        raw.update(overrides)
        return AppConfig.model_validate(raw)

    make.sqlite_path = str(tmp_path / "l2.db")
    return make

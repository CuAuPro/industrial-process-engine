import json
import pytest

from industrial_process_engine.cli import init_project
from industrial_process_engine.config import load_config
from industrial_process_engine.input.mqtt_json_adapter import MqttJsonAdapter
from industrial_process_engine.simulators.discrete_cnc import DiscreteCncSimulator, Settings


def test_init_project_creates_valid_config_without_overwriting(tmp_path):
    project = tmp_path / "my-line"
    init_project(project)

    config = load_config(project / "config/settings.yaml")
    assert config.service.name == "my_line"
    assert config.process.id == "MY_LINE"
    assert (project / "requirements.txt").read_text(encoding="utf-8").strip() == "industrial-process-engine"
    assert (project / "requirements-dev.txt").read_text(encoding="utf-8").startswith("-e ../industrial_process_engine")
    assert (project / "application/derived_signals.py").is_file()
    assert (project / "application/product_fields.py").is_file()
    assert (project / "application/consumption_metrics.py").is_file()
    assert (project / "application/hooks.py").is_file()
    assert (project / "tests/test_settings.py").is_file()
    assert (project / "Dockerfile").is_file()
    assert (project / "pyproject.toml").is_file()
    assert (project / ".dockerignore").is_file()
    assert (project / ".env.example").is_file()
    assert (project / "docker-build.sh").is_file()
    assert (project / "azure-pipelines.yml").is_file()

    with pytest.raises(FileExistsError):
        init_project(project)


def test_init_simulator_messages_match_starter_config(tmp_path):
    project = tmp_path / "my-line"
    init_project(project)
    config = load_config(project / "config/settings.yaml")
    adapter = MqttJsonAdapter(config.mqtt_mappings, config.mqtt, config.lifecycle.product_topics)

    messages = list(DiscreteCncSimulator(Settings(parts=1, samples_per_part=12)).messages())
    assert len(messages) == 14
    parsed = [adapter.parse(message.topic, json.dumps(message.payload)) for message in messages]
    assert all(item is not None for item in parsed)
    assert parsed[0].product_id == "CNC-0001"
    assert {update.name for update in parsed[1].updates} == {
        "spindle_load", "temperature", "oil_pressure",
    }
    assert parsed[-1].product_id == "CNC-0001"

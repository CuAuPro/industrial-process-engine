from pathlib import Path

from industrial_process_engine import load_config


def test_settings_load():
    import main  # Also checks application extension imports.

    assert main.AppHooks
    config = load_config(Path(__file__).parents[1] / "config/settings.yaml")
    assert config.service.name == "__SERVICE_NAME__"

from __future__ import annotations

import os
import sys

from industrial_process_engine.environment import load_env


def test_load_env_defaults_to_entry_script_directory(tmp_path, monkeypatch):
    variable_name = "IPR_TEST_ENTRY_ENV"
    monkeypatch.delenv(variable_name, raising=False)
    monkeypatch.setattr(
        sys.modules["__main__"], "__file__", str(tmp_path / "main.py"), raising=False,
    )
    (tmp_path / ".env").write_text(f"{variable_name}=loaded\n", encoding="utf-8")

    assert load_env() is True
    assert os.environ[variable_name] == "loaded"


def test_load_env_preserves_existing_environment_by_default(tmp_path, monkeypatch):
    variable_name = "IPR_TEST_ENV_PRECEDENCE"
    env_path = tmp_path / ".env"
    env_path.write_text(f"{variable_name}=from-file\n", encoding="utf-8")
    monkeypatch.setenv(variable_name, "from-process")

    assert load_env(env_path) is True
    assert os.environ[variable_name] == "from-process"

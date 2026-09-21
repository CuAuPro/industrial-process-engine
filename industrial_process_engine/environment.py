from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv as _load_dotenv


def load_env(path: str | Path | None = None, *, override: bool = False) -> bool:
    """Load deployment variables without overriding the process environment.

    By default, ``.env`` is resolved beside the running entry-point script.
    An explicit path may be supplied by tests or alternative launchers.
    """
    if path is None:
        main_file = getattr(sys.modules.get("__main__"), "__file__", None)
        base_directory = Path(main_file).resolve().parent if main_file else Path.cwd()
        env_path = base_directory / ".env"
    else:
        env_path = Path(path).resolve()
    return _load_dotenv(dotenv_path=env_path, override=override)

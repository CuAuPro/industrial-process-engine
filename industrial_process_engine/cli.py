from __future__ import annotations

import argparse
import re
from importlib.resources import files
from pathlib import Path


TEMPLATE_FILES = (
    "main.py", "pyproject.toml", "requirements.txt", "requirements-dev.txt", ".gitignore",
    ".env.example",
    ".dockerignore", "README.md", "Dockerfile", "docker-compose.yaml",
    "docker-compose.dev.yml", "docker-build.sh", "azure-pipelines.yml",
    "config/settings.yaml", "application/__init__.py",
    "application/derived_signals.py", "application/product_fields.py",
    "application/consumption_metrics.py", "application/hooks.py",
    "tests/test_settings.py",
)


def init_project(path: Path) -> None:
    name = path.name
    if not re.fullmatch(r"[a-z][a-z0-9_-]*", name):
        raise ValueError("project name must start with a lowercase letter and contain only lowercase letters, digits, _ or -")
    target = path.resolve()
    if target.exists():
        raise FileExistsError(f"destination already exists: {target}")

    template = files("industrial_process_engine").joinpath("templates", "basic")
    replacements = {"__SERVICE_NAME__": name.replace("-", "_"), "__PROCESS_ID__": name.replace("-", "_").upper()}
    for filename in TEMPLATE_FILES:
        content = template.joinpath(*filename.split("/")).read_text(encoding="utf-8")
        for old, new in replacements.items():
            content = content.replace(old, new)
        output = target / filename
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(content, encoding="utf-8", newline="\n")
    print(f"Created {target}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Industrial Process Engine project tools")
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init", help="Create a starter application")
    init.add_argument("directory", type=Path)
    args = parser.parse_args()
    try:
        init_project(args.directory)
    except (ValueError, FileExistsError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()

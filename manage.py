from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PYPROJECT = ROOT / "pyproject.toml"
PACKAGE_INIT = ROOT / "industrial_process_engine" / "__init__.py"
VERSION_PATTERN = re.compile(r'^version = "(\d+)\.(\d+)\.(\d+)"$', re.MULTILINE)
INIT_VERSION_PATTERN = re.compile(r'^__version__ = "\d+\.\d+\.\d+"$', re.MULTILINE)


def run(*arguments: str) -> None:
    subprocess.run([sys.executable, *arguments], cwd=ROOT, check=True)


def current_version() -> tuple[int, int, int]:
    match = VERSION_PATTERN.search(PYPROJECT.read_text(encoding="utf-8"))
    if match is None:
        raise RuntimeError("Cannot find project version in pyproject.toml")
    return tuple(int(part) for part in match.groups())


def bump(part: str) -> str:
    major, minor, patch = current_version()
    if part == "major":
        major, minor, patch = major + 1, 0, 0
    elif part == "minor":
        minor, patch = minor + 1, 0
    else:
        patch += 1
    version = f"{major}.{minor}.{patch}"

    pyproject = VERSION_PATTERN.sub(f'version = "{version}"', PYPROJECT.read_text(encoding="utf-8"), count=1)
    PYPROJECT.write_text(pyproject, encoding="utf-8")
    package_init = INIT_VERSION_PATTERN.sub(
        f'__version__ = "{version}"', PACKAGE_INIT.read_text(encoding="utf-8"), count=1,
    )
    PACKAGE_INIT.write_text(package_init, encoding="utf-8")
    print(f"Version bumped to {version}")
    return version


def clean_build_outputs() -> None:
    for directory in (ROOT / "build", ROOT / "dist"):
        if directory.exists():
            shutil.rmtree(directory)
    for directory in ROOT.glob("*.egg-info"):
        shutil.rmtree(directory)


def build() -> None:
    clean_build_outputs()
    run("-m", "pytest")
    run("-m", "build")
    artifacts = sorted(str(path) for path in (ROOT / "dist").iterdir())
    run("-m", "twine", "check", *artifacts)


def publish(repository: str) -> None:
    artifacts = sorted(str(path) for path in (ROOT / "dist").iterdir())
    command = ["-m", "twine", "upload"]
    if repository == "testpypi":
        command.extend(["--repository", "testpypi"])
    run(*command, *artifacts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and publish industrial-process-engine")
    parser.add_argument("action", choices=("build", "patch", "minor", "major", "publish"))
    parser.add_argument(
        "--publish", action="store_true",
        help="Publish after build; useful with patch, minor, or major",
    )
    parser.add_argument(
        "--repository", choices=("pypi", "testpypi"), default="pypi",
        help="Package index (default: PyPI); package name comes from pyproject.toml",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.action in {"patch", "minor", "major"}:
        bump(args.action)
    build()
    if args.action == "publish" or args.publish:
        publish(args.repository)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import importlib
import logging
from pathlib import Path
from typing import Any

import uvicorn

from industrial_process_engine import ProcessEngine, create_app, load_config


DEMO_MODULES = {
    "continuous-line": "continuous_line",
    "rolling-mill": "rolling_mill",
    "furnace": "furnace",
    "cnc": "discrete_cnc",
    "cutting-line": "transformation_cutting_line",
}


def demo_config_path(demo: str) -> Path:
    module_name = DEMO_MODULES[demo]
    yaml_name = "cutting_line.yaml" if demo == "cutting-line" else f"{module_name}.yaml"
    return Path(__file__).with_name(yaml_name)


def demo_extensions(demo: str) -> dict[str, Any]:
    module_name = DEMO_MODULES[demo]
    module = importlib.import_module(
        f"industrial_process_engine.simulators.{module_name}"
    )
    return {
        "derived_signals": getattr(module, "derived_signals", None),
        "product_fields": getattr(module, "product_fields", None),
        "hooks": getattr(module, "hooks", None),
        "consumption_metrics": getattr(module, "consumption_metrics", None),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a packaged Industrial Process Engine demo and REST API",
    )
    parser.add_argument("demo", choices=tuple(DEMO_MODULES))
    parser.add_argument("--sqlite", help="SQLite path; defaults to data/<demo>.db")
    parser.add_argument("--api-host", default="127.0.0.1")
    parser.add_argument("--api-port", type=int, default=8000)
    parser.add_argument("--log-level", default="info")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sqlite_path = args.sqlite or str(Path("data") / f"{args.demo}.db")
    config = load_config(demo_config_path(args.demo))
    engine = ProcessEngine(config, sqlite_path=sqlite_path, **demo_extensions(args.demo))
    app = create_app(engine)
    engine.start()
    try:
        uvicorn.run(
            app, host=args.api_host, port=args.api_port, log_level=args.log_level,
        )
    finally:
        engine.graceful_shutdown()


if __name__ == "__main__":
    main()

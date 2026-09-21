import argparse
import logging
import os
import threading
import time

import uvicorn

from application.consumption_metrics import consumption_metrics
from application.derived_signals import derived_signals
from application.hooks import AppHooks
from application.product_fields import product_fields
from industrial_process_engine import ProcessEngine, create_app, load_config, load_env


def main() -> None:
    load_env()
    config = load_config("config/settings.yaml")
    parser = argparse.ArgumentParser(description=config.service.description)
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    engine = ProcessEngine(
        config, sqlite_path="data/process.db",
        hooks=AppHooks(), derived_signals=derived_signals, product_fields=product_fields,
        consumption_metrics=consumption_metrics,
    )
    engine.start()
    server = uvicorn.Server(uvicorn.Config(
        create_app(engine), host=os.getenv("API_HOST", "127.0.0.1"), port=8080,
        log_level=args.log_level.lower(),
    ))

    def watch_restart() -> None:
        while not server.should_exit:
            if engine.processor.restart_requested:
                server.should_exit = True
                return
            time.sleep(0.2)

    threading.Thread(target=watch_restart, name="restart-monitor", daemon=True).start()
    try:
        server.run()
    finally:
        engine.graceful_shutdown()


if __name__ == "__main__":
    main()

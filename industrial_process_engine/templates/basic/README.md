# __SERVICE_NAME__

Edit `config/settings.yaml` with the real MQTT mappings and product lifecycle topics.
The starter configuration matches IPE's packaged CNC simulator: one cycle per
product, with spindle load, temperature, and oil pressure signals.
Add process-specific calculations and hooks in the separate `application/` modules.

## Quickstart with simulated MQTT data

Start a local MQTT broker on port 1883 (for example, `mosquitto -p 1883`).
Then create an environment and install the requirements:

```sh
python -m venv .venv
```

Activate `.venv` and install the requirements:

```sh
python -m pip install -r requirements.txt
```

Run the application and simulator in separate terminals from this directory.
Wait for the application's MQTT connection log before starting the simulator:

```sh
# Terminal 1: application and API
python main.py

# Terminal 2: two sample product cycles
python -m industrial_process_engine.simulators.discrete_cnc --parts 2 --samples-per-part 12
```

Check `http://127.0.0.1:8080/api/v1/products` for the completed products and
`http://127.0.0.1:8080/api/v1/runs` for process data. Use
Add `--dry-run --no-sleep` to the simulator command to inspect MQTT payloads
without a broker. Its topics match `config/settings.yaml`; update that file
when connecting the application to your own process.

The API docs are at http://127.0.0.1:8080/docs. Copy `.env.example` to `.env` and
uncomment only the overrides you need; `.env` is ignored by Git. For engine
development, keep the engine checkout beside this project and install the
editable requirements instead:

```sh
python -m pip install -r requirements-dev.txt
python -m pytest
```

For a local Docker build, set `MQTT_HOST` in `.env` to a broker reachable from
the container, then run:

```sh
docker compose -f docker-compose.yaml -f docker-compose.dev.yml up --build
```

The development override exposes the API on `127.0.0.1:8080`. The base Compose
file runs `${APP_IMAGE:-__SERVICE_NAME__:latest}` without publishing a port.

To build an image tagged with the application version and `latest`:

```sh
sh ./docker-build.sh registry.example.com/industrial-apps/__SERVICE_NAME__
```

In `azure-pipelines.yml`, set `imageRepository` and `registryConnection` to your
registry image path and Azure DevOps Docker registry service connection. Azure
Pipelines can use this YAML from a GitHub repository; select that repository
when creating the pipeline in Azure DevOps.

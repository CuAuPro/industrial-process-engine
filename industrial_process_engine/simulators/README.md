# Packaged simulator quick start

Each demo has a YAML configuration, an engine/API command, and a matching MQTT
publisher. Start an MQTT broker on `localhost:1883`, then run the two commands in separate
terminals.

## Continuous line: ID ends, position reset starts

```bash
python -m industrial_process_engine.simulators.run_engine continuous-line
python -m industrial_process_engine.simulators.continuous_line --products 2 --seed 1
```

Configuration: `continuous_line.yaml`. The publisher resets `PositionM` before publishing
the next `ProductId`. The YAML uses `on_change: PROCESS_END`, `reset_ratio: 0.1`, and
`reset_scope: product`; therefore either MQTT arrival order is safe.

## Rolling mill: continuous material with passes

```bash
python -m industrial_process_engine.simulators.run_engine rolling-mill
python -m industrial_process_engine.simulators.rolling_mill --products 1 --passes 3 --seed 1
```

Configuration: `rolling_mill.yaml`. Product-ID change/clear starts and ends the run.
`pass_number` changes create segments; speed integration provides distance and reverse
motion follows the configured policy.

## Furnace: multiple product membership

```bash
python -m industrial_process_engine.simulators.run_engine furnace
python -m industrial_process_engine.simulators.furnace --mode batch --batch-size 3 --seed 1
```

Configuration: `furnace.yaml`. Product enter/exit topics add and remove pieces. With
`membership: multiple` and `close_run: last_product_exit`, the final exit closes the run.
Use `--mode pusher --products 6 --pusher-capacity 3` for staggered membership.

## CNC: one summary per piece

```bash
python -m industrial_process_engine.simulators.run_engine cnc
python -m industrial_process_engine.simulators.discrete_cnc --parts 3 --seed 1
```

Configuration: `discrete_cnc.yaml`. Each enter/exit pair is a `cycle` with
`membership: single`. Product windows are disabled, while process-time spindle load is
still collected.

## Cutting line: transformation genealogy

```bash
python -m industrial_process_engine.simulators.run_engine cutting-line
python -m industrial_process_engine.simulators.transformation_cutting_line --children 3 --seed 1
```

Configuration: `cutting_line.yaml`. `RunActive=true/false` emits `PROCESS_START` and
`PROCESS_END`. Product topics first register a parent coil and then children containing
`ParentProductIds`.

## Useful options

The engine runner accepts `--sqlite`, `--api-host`, `--api-port`, and `--log-level`.
Publishers accept MQTT connection options and `--dry-run --no-sleep`. Connection settings
in YAML can also be overridden with `MQTT_HOST`, `MQTT_PORT`, `MQTT_USERNAME`, and
`MQTT_PASSWORD`.

The API is available at `http://127.0.0.1:8000` by default:

```bash
curl http://127.0.0.1:8000/api/v1/status
curl http://127.0.0.1:8000/api/v1/products
curl http://127.0.0.1:8000/api/v1/runs
```

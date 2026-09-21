# Industrial Process Engine

`industrial-process-engine` is a single-writer Python engine for collecting industrial
signals from MQTT and OPC UA, following product lifecycle, and producing two independent
views of the same process:

- `product_data`: time- or distance-windowed data attributed to products;
- `process_data_time`: wall-clock data attributed only to the process/asset.

The engine also writes one `product_summary` row per product and, for transformations,
parent-child `product_relation` rows. SQLite is the durable local source of truth;
completed records can be synchronized idempotently to QuestDB.

The package is new and intentionally has no compatibility layer for the former project
name or YAML schema.

## Install

```bash
python -m pip install industrial-process-engine
```

Before the first PyPI release, install this checkout with `python -m pip install -e .`.

## Start a project

```bash
industrial-process-engine init my_line
cd my_line
python -m venv .venv
```

Activate the virtual environment (`.venv\Scripts\Activate.ps1` in PowerShell or
`source .venv/bin/activate` in a Unix shell), then run:

```bash
python -m pip install -r requirements.txt
python main.py
```

The command creates `main.py`, `config/settings.yaml`, separate application
extension modules, a config test,
requirements for PyPI and editable development,
Docker files, and an Azure Pipelines example. Set the registry path and service
connection in its YAML before enabling the pipeline.
To try the generated application, start a local MQTT broker on port 1883 and run
`python -m industrial_process_engine.simulators.discrete_cnc --parts 2 --samples-per-part 12`
in a second terminal while `python main.py` is running.
The sample products appear at `http://127.0.0.1:8080/api/v1/products`.
Edit the sample MQTT mapping and lifecycle topics for the real process. The init
command will not overwrite an existing directory.

For local engine changes, keep the engine checkout beside the new application and
install it in the application's virtual environment:

```bash
python -m pip install -r requirements-dev.txt
```

This replaces the PyPI installation with an editable one. Use the regular
`requirements.txt` installation when working only on the application.

```python
from industrial_process_engine import ProcessEngine, load_config

engine = ProcessEngine(load_config("process.yaml"), sqlite_path="data/process.db")
engine.start()
```

Use a new SQLite path or manually remove the old development database. The engine does
not migrate or detect old schemas.

## Quick start with a simulator

Install the checkout and start an MQTT broker on `localhost:1883`:

```bash
python -m pip install -e ".[dev]"
```

Then use two terminals. The first command runs the engine, its packaged YAML and the REST
API on `http://127.0.0.1:8000`; the second publishes the matching PLC-like messages:

```bash
# Terminal 1
python -m industrial_process_engine.simulators.run_engine continuous-line

# Terminal 2
python -m industrial_process_engine.simulators.continuous_line --products 2 --seed 1
```

Inspect the result while it runs:

```bash
curl http://127.0.0.1:8000/api/v1/status
curl http://127.0.0.1:8000/api/v1/products
curl http://127.0.0.1:8000/api/v1/runs
```

Every demo uses its own default SQLite file under `data/`. Add `--sqlite my-demo.db` to
the engine command to choose another path. `MQTT_HOST`, `MQTT_PORT`, username and password
environment overrides work as described in the advanced guide. To inspect generated MQTT
payloads without a broker or engine, add `--dry-run --no-sleep` to a simulator command.

## Process models

| Model | Run boundary | Product membership | Typical `product_data` | Tracking |
|---|---|---|---|---|
| `cycle` | last product exit or explicit process end | `single` or `multiple` | disabled by default; time is optional | no |
| `continuous` | product/run lifecycle | material moving through a line | distance, enabled by default | optional except for distance data |
| `transformation` | only `PROCESS_START`/`PROCESS_END` | parents and children enter/exit independently | disabled by default; time is optional | no |

A furnace batch and a CNC operation use the same lifecycle mechanics, so they are both
`cycle`. Their intent is explicit rather than encoded in two nearly identical model
names:

```yaml
# CNC: one workpiece closes the run when it leaves.
process: {id: CNC_1, model: cycle, membership: single, close_run: last_product_exit}

# Furnace batch: several workpieces share the run.
process: {id: FURNACE_1, model: cycle, membership: multiple, close_run: last_product_exit}

# Recipe cycle: membership may become empty, but PROCESS_END owns the boundary.
process: {id: REACTOR_1, model: cycle, membership: multiple, close_run: process_end}
```

A rolling mill is a `continuous` process. Passes, direction policy, position reset and
sensor locations are tracking/spatial configuration—not a separate processor type.

`ProcessProcessor` is the single-writer orchestrator. Its reusable functional components
are split into `SignalAggregationCoordinator`, `LifecycleCoordinator`,
`TrackingCoordinator`, and `ProcessorStateManager`. A `CycleModelController`,
`ContinuousModelController`, or `TransformationModelController` owns only model-specific
run transitions and validation. Derived signals, product fields, consumption, storage and
sync remain independent support modules shared by every strategy.

## Lifecycle: what starts and ends what

Process and product events are deliberately separate:

| Event | Meaning |
|---|---|
| `PROCESS_START` | Open a run, or start the first product-driven run |
| `PROCESS_END` | Complete the current run according to its model |
| `PROCESS_ABORT` | Fail/abort the current run |
| `PRODUCT_ENTER` / `PRODUCT_EXIT` | Add or remove membership; the first enter also opens a `last_product_exit` cycle |
| `PRODUCT_UPDATE` | Change context fields of an active product |

`lifecycle.source` controls which configured automatic lifecycle mechanisms are accepted:

- `explicit`: MQTT product topics, product-ID transitions and mapping edges;
- `derived`: expressions in `lifecycle.rules`;
- `both`: accept both mechanisms.

REST lifecycle endpoints are direct operator/application commands and remain available.

The simplest sequential line starts on a non-empty product ID and ends when it clears.
Changing directly from `A` to `B` completes `A` and starts `B`:

```yaml
lifecycle:
  source: explicit
  product_id:
    signal: lot
    on_change: PROCESS_START
    on_clear: PROCESS_END
```

Some PLCs publish the next ID and reset a per-product distance counter independently. In
that case the ID transition is configured only as `PROCESS_END`; the direct counter reset
is the coordinated start boundary:

```yaml
lifecycle:
  source: explicit
  product_id:
    signal: lot
    on_change: PROCESS_END
    on_clear: PROCESS_END

tracking:
  source: direct
  signal: position
  reset_ratio: 0.1
  reset_scope: product
```

This works in either arrival order. If the new ID arrives first, it is held as pending
until reset. If reset arrives first, the old run closes and the following ID starts at the
reset boundary. The packaged `continuous-line` demo exercises this exact configuration.

For an explicit machine-cycle boundary, map a boolean edge:

```yaml
- source: mqtt
  topic: machine/process
  id: CycleActive
  name: cycle_active
  type: bool
  true_event: PROCESS_START
  false_event: PROCESS_END
```

For batch membership and transformations, use product topics instead:

```yaml
lifecycle:
  source: explicit
  product_topics:
    enter_topic: process/product/enter
    exit_topic: process/product/exit
    abort_topic: process/product/abort
    id: ProductId
    parent_ids: ParentProductIds
```

See the lifecycle section in [README_ADVANCED.md](README_ADVANCED.md) for REST examples,
derived rules, late product-ID binding, position-reset sequencing and model restrictions.

## Minimal YAML

```yaml
service:
  name: mill_engine
  title: Mill Process Engine
  description: Product and process data collection
  version: 0.1.0

process:
  id: ROLLING_MILL_1
  model: continuous

mqtt:
  enabled: true
  host: localhost
  client_id: rolling-mill-engine
  payload_format: value_array

opcua: {enabled: false}
questdb: {enabled: false}

lifecycle:
  source: explicit
  product_id:
    signal: product_id
    on_change: PROCESS_START
    on_clear: PROCESS_END

tracking:
  source: speed
  speed_signal: speed

streams:
  product_data:
    enabled: true
    axis: distance
    interval_m: 1.0
    stale_after_ms: 2000
  process_data_time:
    enabled: true
    interval_s: 5.0
    stale_after_ms: 2000
    when: "speed > 0"  # optional; omit to store continuously

mappings:
  - {source: mqtt, topic: mill/process, id: Speed, name: speed, type: float, unit: m/min}
  - {source: mqtt, topic: mill/events, id: ProductId, name: product_id, type: string}
  - source: mqtt
    topic: mill/process
    id: ForceActual
    name: rolling_force
    type: float
    outputs:
      output_type: double
      product_data:
        - {calculation: weighted_mean}
      process_data_time:
        - {calculation: max}
```

One input is evaluated once and can feed both streams or several calculations in one
stream. Omit an output `name` to reuse the input signal name; specify it only for multiple
outputs or a deliberate rename. `outputs.output_type` is shared by its stream outputs;
an individual output may override it when necessary. Supported calculations are
`weighted_mean`, `first`, `last`, `min`, and `max`. Output names must be unique within
each stream, but the same name may be used once in each stream.

Numeric mappings may declare a `unit`. Supported symbols are `%`, `ratio`, `count`,
`ms`, `s`, `min`, `h`, `mm`, `cm`, `m`, `mm/s`, `mm/min`, `cm/s`, `cm/min`, `m/s`,
`m/min`, `rpm`, `°C`, `K`, `g`, `kg`, `t`, `kg/h`, `t/h`, `N`, `kN`, `MN`, `N/mm`,
`kN/m`, `N·m`, `kN·m`, `Pa`, `kPa`, `MPa`, `bar`, `A`, `kA`, `V`, `kV`, `Hz`,
`W`, `kW`, `MW`, `Wh`, `kWh`, `MWh`, `L`, `m³`, `L/min`, `m³/h`, `€`, `€/h`, and
`€/t`. String and boolean mappings cannot have units. Speed tracking requires one of
the six linear-speed units on its `speed_signal` mapping.

`product_data.axis` is `distance` or `time`. `station` and `spatial_offset_m` apply only
to distance-based product data. `process_data_time` is aligned to Unix time: a 10-second
interval closes on `:00`, `:10`, `:20`, and so on. It runs whenever the service is
`RUNNING`, even with no active product. Optional `when` stores only bucket portions where
the condition is true and skips fully inactive buckets. Empty or insufficiently covered buckets contain
`NULL` dynamic values and `quality=DATA_GAP`. Downtime is not backfilled after restart.

## Derived signals and processing extensions

Support modules are first-class APIs and remain part of the engine:

```python
from industrial_process_engine import DerivedSignalRegistry, DerivedSignalResult

derived = DerivedSignalRegistry()

@derived.register(
    "power_kw",
    value_type="float",
    outputs={
        "output_type": "double",
        "product_data": [
            {"calculation": "weighted_mean"}
        ],
        "process_data_time": [
            {"calculation": "max"}
        ],
    },
)
def power(state, timestamp_ms):
    voltage = state.get("voltage")
    current = state.get("current")
    if not voltage or not current or not voltage.quality or not current.quality:
        return DerivedSignalResult(None, False)
    return DerivedSignalResult(float(voltage.value) * float(current.value) / 1000.0)
```

Pass the registry as `ProcessEngine(..., derived_signals=derived)`. A derived calculator
runs once per input change; its result is then offered to all configured outputs.

The package also keeps these extension points:

- `ProductFieldRegistry` for captured, calculated, managed and summary fields;
- `ConsumptionMetricRegistry` for counters, rates and product allocation;
- `ProcessHooks` for lifecycle preparation and notifications;
- MQTT/OPC UA adapters, lifecycle rules and position/global transport tracking;
- distance, spatial, product-time and process-time aggregators;
- SQLite checkpoints/retention and QuestDB retry/backoff sync.

### Startup lifecycle options

`ProcessHooks.on_startup()` is optional and returns `None` by default:

| Desired startup behavior | Hook behavior | Result |
|---|---|---|
| Wait for the next normal lifecycle event or product-ID change | Omit `on_startup`, or return `None` | No new run is created at startup |
| Resume the current product with speed tracking | Return a `LifecycleSnapshot` containing `PROCESS_START` and the product ID | The run starts at distance zero and integrates new speed samples |
| Resume the current product with direct-distance tracking | Return the same snapshot without a position | The engine waits for the first good direct position and uses it as the starting axis |
| Start immediately from a direct position already read by the application | Include that good position in the startup snapshot | The supplied position is used as the starting axis |

For example, a direct position of 200 m stores 200–201 m first and does not
invent 0–200 m. An application that still uses other hooks can disable startup
recovery by removing only its `on_startup` override and any mapping used solely
by that method.

This hook does not disable durable checkpoint recovery. To wait strictly for a
brand-new product after a restart, first abort the saved active run or reset
local storage; otherwise the normal lifecycle input may reconcile that run.

See [README_ADVANCED.md](README_ADVANCED.md) for complete configuration combinations.

## Rolling mill as continuous material flow

```yaml
process: {id: ROLLING_MILL_1, model: continuous}
mappings:
  - {source: mqtt, topic: mill/process, id: Speed, name: speed, type: float, unit: m/min}
tracking:
  source: speed
  speed_signal: speed
  reset_scope: segment
  segment_signal: pass_number
  segment_start: segment_change
  reverse_policy: hold
spatial:
  origin: stand
  line_length_m: 8.0
  stations:
    pyrometer: {offset_m: 4.5}
```

Sensors at the stand use `station: stand` (offset `0.0`). Other sensors use a named
station or `spatial_offset_m`. Each pass becomes a separate segment while still using the
common continuous material-flow implementation.

## Transformation genealogy

Transformation runs must be started and ended explicitly. Product exit never closes the
run. A child can reference one or more products already present in that active run:

```json
{
  "timestamp_ms": 1710000000000,
  "parent_product_ids": ["COIL-123"],
  "context": {}
}
```

For MQTT lifecycle topics the equivalent field is `ParentProductIds`. Unknown parents,
self-links, duplicate parent IDs and duplicate relations are rejected. Child insertion and
its relation rows are one SQLite transaction.

## Data tables and correlation

- `process_run`: internal run state;
- `product_summary`: product identity, run, start/end/state, `start_mode` and summary fields;
- `product_data`: product windows, segment, time, optional position, values and quality;
- `process_data_time`: process ID, absolute bucket times, values, quality and sync state;
- `product_relation`: parent-child genealogy within a transformation run.

QuestDB always receives `product_summary`, plus only the enabled stream tables.
`product_relation` is created only for transformation processes. Process-time retries use
`(process_id, ts_start)` as their idempotent key.

Overlap `process_data_time.ts_start/ts_end` with `product_summary.start_ts/end_ts` for the
same `process_id` to see active products. Precise station-to-product attribution comes
from distance-based `product_data`; no ambiguous product ID is stored in process time.
`start_mode` is `startup_partial` when a startup hook creates a new run whose earlier
processing is unavailable, otherwise `normal`. Checkpoint-restored runs keep their row.

## REST API

- `GET /api/v1/status`
- `GET /api/v1/runs/current`
- `GET /api/v1/runs`
- `GET /api/v1/runs/{run_id}`
- `GET /api/v1/products`
- `GET /api/v1/process-data/time?from_ts=...&to_ts=...&limit=...`
- `POST /api/v1/runs/start`
- `POST /api/v1/runs/current/end`
- `POST /api/v1/products/{product_id}/enter|update|exit|abort`

There are no legacy `/api/v1/processes...` routes.

Configure compact dashboard measurements by mapping a stable status name to an input
signal. The mapping supplies optional unit metadata:

```yaml
status:
  metrics:
    speed: speed
```

Status includes `observed_ts`, `service_started_ts`, `uptime_s`, `production_state`,
`in_process_product_count`, and `metrics`. Active and draining products both count as
`PROCESSING`. A missing or bad-quality signal is returned with `value: null`; `unit` is
omitted when its mapping has none.

## Shipped simulators

```bash
python -m industrial_process_engine.simulators.run_engine furnace
python -m industrial_process_engine.simulators.run_engine continuous-line
python -m industrial_process_engine.simulators.run_engine rolling-mill
python -m industrial_process_engine.simulators.run_engine cnc
python -m industrial_process_engine.simulators.run_engine cutting-line
```

Run the matching publisher in another terminal:

| Demo engine | Publisher | Lifecycle demonstrated |
|---|---|---|
| `furnace` | `simulators.furnace --mode batch --batch-size 3` | explicit multi-product enter/exit |
| `continuous-line` | `simulators.continuous_line --products 2` | ID ends, distance reset starts |
| `rolling-mill` | `simulators.rolling_mill --products 1 --passes 3` | ID start/end and pass segments |
| `cnc` | `simulators.discrete_cnc --parts 3` | one enter/exit cycle per piece |
| `cutting-line` | `simulators.transformation_cutting_line --children 3` | explicit run plus genealogy |

Prefix publisher names with `python -m industrial_process_engine.`. Each YAML file also
contains its matching two-terminal quick-start commands and explains its lifecycle choice.
The packaged [simulator guide](industrial_process_engine/simulators/README.md) describes
the expected event sequence for every demo.

## Development

```bash
python -m pip install -e ".[dev]"
python -m pytest
python manage.py build
```

`manage.py build` runs tests, builds the wheel and source distribution, and checks
both artifacts. To upload after configuring Twine credentials:

```bash
python manage.py publish --repository testpypi
python manage.py publish
```

Use `python manage.py patch`, `minor`, or `major` to bump the version before a new
release; add `--publish` to upload in the same command. The distribution and
import names are `industrial-process-engine` and `industrial_process_engine`.

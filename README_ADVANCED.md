# Industrial Process Engine: advanced configuration

This document describes supported model, stream and processing combinations for version
`0.1.0`. Configuration is strict: unknown keys are rejected. In particular, `line_id`,
top-level `aggregation`, and mapping-level `aggregate` are not accepted.

## Combination matrix

| Capability | `cycle` | `continuous` | `transformation` |
|---|---:|---:|---:|
| `product_summary` | always | always | always |
| time `product_data` | optional | yes | optional |
| distance `product_data` | no | yes | no |
| `process_data_time` | optional | optional | optional |
| tracking | no | optional; required for distance | no |
| spatial stations | no | distance stream only | no |
| pass/segment reset | no | yes | no |
| multiple active products | when `membership: multiple` | yes without a single spatial origin | yes |
| parent-child relations | no | no | yes |
| empty membership keeps run open | when `close_run: process_end` | draining may keep it open | yes |

For every model, either product windows may be disabled or process-time data may be
disabled. Product summaries still exist, so summary-only `cycle` and `transformation`
processes are valid. An enabled stream must have at least one mapping or Python-derived
output after the engine composes configuration and application extensions.

## Common document structure

```yaml
service:
  name: unique_service_name
  title: Human-readable title
  description: What this engine instance observes
  version: 0.1.0

process:
  id: STABLE_PROCESS_ID
  model: cycle  # cycle | continuous | transformation
  membership: single  # required only for cycle: single | multiple
  close_run: last_product_exit  # required only for cycle: last_product_exit | process_end

mqtt:
  enabled: false
  host: localhost
  port: 1883
  client_id: process-engine
  payload_format: value_array  # value_array | flat

opcua:
  enabled: false

questdb:
  enabled: false
  client_conf: "http::addr=localhost:9000;"
  product_summary_table: product_summary

mappings: []
lifecycle: {source: explicit}
streams: {}
```

When QuestDB is enabled, table names have no defaults and must be explicit for each
active output: `product_summary_table` always, `product_data_table` when product data is
enabled, `process_data_time_table` when process time is enabled, and
`product_relation_table` only for transformation processes. Unused tables are not created.

### Environment variables

All supported deployment overrides are listed below. They are optional and override the
corresponding YAML value:

| Variable | YAML field |
|---|---|
| `MQTT_ENABLED` | `mqtt.enabled` |
| `MQTT_HOST` | `mqtt.host` |
| `MQTT_PORT` | `mqtt.port` |
| `MQTT_CLIENT_ID` | `mqtt.client_id` |
| `MQTT_USERNAME` | `mqtt.username` |
| `MQTT_PASSWORD` | `mqtt.password` |
| `OPCUA_ENABLED` | `opcua.enabled` |
| `OPCUA_ENDPOINT` | `opcua.endpoint` |
| `OPCUA_APPLICATION_URI` | `opcua.application_uri` |
| `OPCUA_TIMEOUT_S` | `opcua.timeout_s` |
| `OPCUA_RECONNECT_INTERVAL_S` | `opcua.reconnect_interval_s` |
| `OPCUA_PUBLISHING_INTERVAL_MS` | `opcua.publishing_interval_ms` |
| `OPCUA_SAMPLING_INTERVAL_MS` | `opcua.sampling_interval_ms` |
| `OPCUA_USERNAME` | `opcua.username` |
| `OPCUA_PASSWORD` | `opcua.password` |
| `OPCUA_SECURITY_POLICY` | `opcua.security_policy` |
| `OPCUA_SECURITY_MODE` | `opcua.security_mode` |
| `OPCUA_CERTIFICATE_PATH` | `opcua.certificate_path` |
| `OPCUA_PRIVATE_KEY_PATH` | `opcua.private_key_path` |
| `OPCUA_PRIVATE_KEY_PASSWORD` | `opcua.private_key_password` |
| `OPCUA_SERVER_CERTIFICATE_PATH` | `opcua.server_certificate_path` |
| `QUESTDB_ENABLED` | `questdb.enabled` |
| `QUESTDB_CLIENT_CONF` | `questdb.client_conf` (address and optional credentials) |

Boolean values accept `1/0`, `true/false`, `yes/no`, or `on/off` (case-insensitive).
`load_env()` reads `.env` beside the running entry-point script by default and preserves
variables already present in the process environment. QuestDB table names are YAML-only
process configuration.

## Stream output syntax

```yaml
mappings:
  - source: opcua
    node_id: ns=2;s=Temperature
    name: temperature
    type: float
    subscribe: true
    sampling_interval_ms: 100
    outputs:
      output_type: double
      product_data:
        - {name: temperature_first, calculation: first}
        - {name: temperature_avg, calculation: weighted_mean}
        - {name: temperature_last, calculation: last}
      process_data_time:
        - {name: temperature_min, calculation: min}
        - {name: temperature_max, calculation: max}
```

Input types are `float`, `int`, `bool`, and `string`. Output types are `double`, `int`,
`long`, `symbol`, `varchar`, and `char`. Numeric calculations and conversions are
validated. Declare the common type once as `outputs.output_type`; an individual output
may override it. Omitted output names reuse the input signal name; explicit names allow
multiple outputs or renaming. Each output name is unique within its stream, but the same
name may appear once in each independent table.

Required signals participate in whole-window quality. Missing, bad-quality or stale
required coverage makes the window `DATA_GAP`. A completely empty process-time bucket is
still written, with all dynamic values `NULL`.

## Cycle: batch/furnace

Batch normally uses product-relative time windows and may assign the same windows to
multiple products present in one run.

```yaml
process:
  id: FURNACE_1
  model: cycle
  membership: multiple
  close_run: last_product_exit
lifecycle:
  source: explicit
  product_topics:
    enter_topic: furnace/product/enter
    exit_topic: furnace/product/exit
streams:
  product_data:
    enabled: true
    axis: time
    interval_s: 10
    stale_after_ms: 3000
  process_data_time:
    enabled: true
    interval_s: 5
    stale_after_ms: 3000
mappings:
  - source: mqtt
    topic: furnace/process
    id: Temperature
    name: temperature
    type: float
    required: true
    outputs:
      output_type: double
      product_data:
        - {name: temperature_avg, calculation: weighted_mean}
      process_data_time:
        - {name: temperature_max, calculation: max}
```

Valid cycle stream variants:

- product time + process time;
- product time only;
- summary + process time, with `product_data.enabled: false`;
- summary only, with both streams disabled.

Tracking and spatial configuration are invalid for `cycle`.

`membership` controls concurrency independently from the run boundary. `single` permits
one active product; `multiple` permits a batch. `close_run: last_product_exit` opens the
run with the first product and closes it when membership becomes empty.

| `membership` | `close_run` | Suitable example |
|---|---|---|
| `single` | `last_product_exit` | CNC piece, press stroke |
| `multiple` | `last_product_exit` | furnace charge, autoclave batch |
| `single` | `process_end` | machine cycle with setup/cooldown outside product presence |
| `multiple` | `process_end` | recipe or batch whose authoritative boundary is a PLC cycle signal |

With `close_run: process_end`, `PROCESS_START` must occur before products enter and
`PROCESS_END` closes the run. This permits an empty interval before, between, or after
products in the same cycle. It requires `lifecycle.source: explicit` or `both`.

## Continuous line

Distance windows require tracking:

```yaml
process: {id: PICKLING_LINE_1, model: continuous}
tracking:
  source: direct
  signal: position
  max_forward_jump_m: 10
  reverse_policy: hold
streams:
  product_data:
    enabled: true
    axis: distance
    interval_m: 1
  process_data_time:
    enabled: true
    interval_s: 5
```

Speed integration is an alternative:

```yaml
tracking:
  source: speed
  speed_signal: speed
  reverse_policy: hold
```

The mapped numeric `speed` signal must declare a supported linear unit, for example
`unit: m/min`.

A continuous process can instead use time-based product data and no tracking:

```yaml
streams:
  product_data: {enabled: true, axis: time, interval_s: 2}
  process_data_time: {enabled: false}
```

Valid continuous variants include distance or time product data, product data plus
process time, either stream alone, and summary-only operation. Tracking may still support
lifecycle/diagnostics when product data is disabled, but spatial output requires an enabled
distance stream.

### Spatial transport

```yaml
spatial:
  origin: entry
  line_length_m: 25
  stations:
    rinse: {offset_m: 8}
    dryer: {offset_m: 20}

mappings:
  - source: mqtt
    topic: line/process
    id: DryerTemperature
    name: dryer_temperature
    type: float
    station: dryer
    outputs:
      output_type: double
      product_data:
        - {name: dryer_temperature_avg, calculation: weighted_mean}
```

`station` resolves to its offset. `spatial_offset_m` can override it. Values are shifted
onto the material coordinate before aggregation. After `PRODUCT_EXIT`, spatial draining
can keep downstream attribution active until the material reaches `line_length_m`.

### Rolling passes

Rolling is the same continuous model with segment policy:

```yaml
process: {id: REVERSING_MILL_1, model: continuous}
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
  line_length_m: 8
  stations:
    pyrometer: {offset_m: 4.5}
```

Direct position counters may also use configured reset detection. Segment numbers are
preserved in `product_data`; there is no rolling-specific processor.

## Cycle: CNC/discrete machining

The default is one summary row per cycle/piece and no product windows:

```yaml
process:
  id: CNC_7
  model: cycle
  membership: single
  close_run: last_product_exit
lifecycle:
  source: explicit
  product_topics:
    enter_topic: cnc/product/enter
    exit_topic: cnc/product/exit
streams:
  product_data: {enabled: false}
  process_data_time: {enabled: true, interval_s: 5}
mappings:
  - source: mqtt
    topic: cnc/process
    id: SpindleLoad
    name: spindle_load
    type: float
    outputs:
      output_type: double
      process_data_time:
        - {name: spindle_load_max, calculation: max}
```

To retain cycle curves, enable time product data:

```yaml
streams:
  product_data: {enabled: true, axis: time, interval_s: 0.5}
```

Here `membership: single` enforces one active workpiece. A multi-fixture machining cycle
can instead use `multiple`; use `close_run: process_end` when the machine's cycle signal,
rather than the final part exit, is authoritative. Distance data, tracking, spatial
stations and genealogy are invalid.

## Transformation / cutting line

The run boundary is independent of product membership. `PROCESS_START` opens the run;
`PRODUCT_ENTER`/`PRODUCT_EXIT` manage materials; only `PROCESS_END` closes it.

```yaml
process: {id: CUTTING_LINE_1, model: transformation}
lifecycle:
  source: explicit
  product_topics:
    enter_topic: cutting/product/enter
    exit_topic: cutting/product/exit
    abort_topic: cutting/product/abort
    id: ProductId
    parent_ids: ParentProductIds
streams:
  product_data: {enabled: false}
  process_data_time: {enabled: true, interval_s: 5}
```

An input coil first enters the active run. A child later enters with:

```json
{"ProductId":"SHEET-001","ParentProductIds":["COIL-123"],"Timestamp":1710000000000}
```

Multiple parents and children are supported. Every parent must already exist in the same
active run. Parent IDs must be unique; self-links and duplicate relation rows are rejected.

Valid transformation variants are summary-only, summary + process time, optional time
product data, or both streams. Distance tracking/spatial output is not supported.

## Lifecycle sources

`lifecycle.source` is `explicit`, `derived`, or `both` where the model allows it.
Transformation and `cycle` with `close_run: process_end` require `explicit` or `both`
because their run boundary must be observable.

There are two different event scopes:

- `PROCESS_START`, `PROCESS_END`, and `PROCESS_ABORT` control a run boundary;
- `PRODUCT_ENTER`, `PRODUCT_UPDATE`, `PRODUCT_EXIT`, and `PRODUCT_ABORT` control product
  membership and context.

For ordinary product-driven `continuous` processing, `PROCESS_START` creates a run and
its initial product. For `cycle` with `close_run: process_end` and for `transformation`,
`PROCESS_START` can create an empty run; products then enter and exit independently.

### Lifecycle mechanisms

| Mechanism | Configuration | Events produced |
|---|---|---|
| REST | `/runs/start`, `/runs/current/end`, product endpoints | explicit process or product event |
| mapped boolean edge | mapping `true_event` / `false_event` | process event |
| product-ID tag | `lifecycle.product_id` | configured process event |
| MQTT product topics | `lifecycle.product_topics` | product membership event |
| derived rule | `lifecycle.rules` | process or line event |

`source: explicit` enables mapping/product-ID driven lifecycle and product topics;
`source: derived` enables rules; `source: both` enables both. Direct REST operator calls
remain available. Product events require a product ID and are therefore not emitted by
anonymous boolean edges or derived rules.

### Product-ID tag: start on change

This is the normal sequential case:

```yaml
lifecycle:
  source: explicit
  product_id:
    signal: lot
    on_change: PROCESS_START
    on_clear: PROCESS_END
```

The `lot` mapping must be a continuously received MQTT or subscribed OPC UA `string`.
Transitions behave as follows:

| Old value | New value | Result |
|---|---|---|
| empty | `A` | start product/run `A` |
| `A` | `B` | complete `A`, then start `B` |
| `A` | empty | end `A` |
| `A` | `A` | no lifecycle change |

`on_clear` may be `PROCESS_END` or `PROCESS_ABORT`. `on_change` may be
`PROCESS_START`, `PROCESS_END`, `PROCESS_ABORT`, or omitted.

### Product ID ends; direct distance reset starts

Use this when a PLC has a per-product position counter and the ID and counter reset may
arrive in either order:

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
  max_forward_jump_m: 2.0
```

A reset is detected when the previous raw position is positive and the new non-negative
position is at most `previous_position * reset_ratio`. For example, `50.0 -> 2.0` is a
reset with `reset_ratio: 0.1`; `50.0 -> 48.0` is normal reverse motion and follows
`reverse_policy`.

The coordination is deterministic:

1. If ID `B` arrives before the reset, run `A` ends and `B` remains pending. The reset
   starts `B` at the reset timestamp.
2. If the reset arrives before ID `B`, run `A` ends at reset. The next ID starts `B` with
   a rebased zero distance axis.
3. The first trustworthy direct-position reading establishes the initial baseline, so
   the first valid ID can start normally.

This combination requires `lifecycle.source: explicit` or `both`, direct tracking,
`reset_scope: product`, and `product_id.on_change: PROCESS_END`. The packaged
`continuous_line.yaml` and simulator demonstrate it end to end.

If the reset must start collecting before the new ID is known, enable late binding:

```yaml
lifecycle:
  source: explicit
  product_id:
    signal: lot
    on_change: PROCESS_END
    on_clear: PROCESS_END
    late_binding:
      enabled: true
      start_on_position_reset: true
      placeholder_prefix: UNASSIGNED
      timeout_s: 300
      block_remote_sync: true
```

The reset starts a provisional product such as `UNASSIGNED-1710000000000`. The later ID
atomically rewrites its local product, windows and event references. Remote sync remains
blocked until binding succeeds.

### Boolean process edge

A PLC cycle/run boolean can directly emit process boundaries:

```yaml
- source: mqtt
  topic: machine/process
  id: CycleActive
  name: cycle_active
  type: bool
  true_event: PROCESS_START
  false_event: PROCESS_END
```

The mapping must be boolean. The event fires on an actual edge (and is also used for
restart reconciliation), not on every repeated scan.

### Explicit product topics

Use product topics for furnace batches, multiple pieces in a cycle, and transformation
genealogy:

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

Example flat MQTT payloads:

```json
{"Timestamp":1710000000000,"ProductId":"COIL-123"}
{"Timestamp":1710000001000,"ProductId":"SHEET-001","ParentProductIds":["COIL-123"]}
```

`ParentProductIds` is meaningful only for `transformation`. For other models it is
rejected.

### REST lifecycle

```bash
curl -X POST http://127.0.0.1:8000/api/v1/runs/start \
  -H "Content-Type: application/json" -d '{"timestamp_ms":1710000000000}'

curl -X POST http://127.0.0.1:8000/api/v1/products/COIL-123/enter \
  -H "Content-Type: application/json" -d '{"context":{"grade":"S355"}}'

curl -X POST http://127.0.0.1:8000/api/v1/products/COIL-123/exit \
  -H "Content-Type: application/json" -d '{}'

curl -X POST http://127.0.0.1:8000/api/v1/runs/current/end \
  -H "Content-Type: application/json" -d '{}'
```

For `transformation`, enter a child with
`{"parent_product_ids":["COIL-123"],"context":{}}`.

### Derived lifecycle rules

```yaml
lifecycle:
  source: derived
  rules:
    - {when: "cycle_active and pressure_bar > 5", event: PROCESS_START, debounce_ms: 500}
    - {when: "not cycle_active", event: PROCESS_END, debounce_ms: 500}
```

Rules are evaluated against normalized signal names. Use `source: both` when PLC edges or
product-ID transitions and derived fallback rules must coexist.

## Python-derived outputs

Python-derived signals use exactly the same two-stream output model:

```python
signals.register(
    "rolling_power",
    value_type="float",
    station="stand",
    outputs={
        "output_type": "double",
        "product_data": [
            {"name": "power_avg", "calculation": "weighted_mean"}
        ],
        "process_data_time": [
            {"name": "power_avg", "calculation": "weighted_mean"},
            {"name": "power_max", "calculation": "max"},
        ],
    },
)
```

Output collisions are checked after YAML mappings and Python registries are composed. A
stream may be populated entirely by Python-derived outputs.

## Time semantics and restart

Product-time buckets begin/end with product lifecycle and may be partial. Process-time
buckets are absolute wall-clock buckets and do not contain `run_id` or `product_id`.

On service restart:

- closed buckets remain immutable and idempotently syncable;
- downtime buckets are not generated;
- an in-progress bucket is restored only when restart occurs in that same Unix bucket;
- unknown coverage caused by restart produces `DATA_GAP`;
- beginning in a later bucket starts at that current bucket and marks partial coverage as
  `DATA_GAP` when it closes.

## Persistence, sync and retention

SQLite checkpoints product processing and process-time aggregation independently.
Completed product runs and pending process-time buckets have separate sync queues. The
QuestDB worker applies retry and exponential backoff. Product relations sync with their
completed run; process-time data syncs as soon as a bucket is closed.

Retention removes old, successfully synchronized product windows/summaries/relations and
old synchronized process-time rows. Active, pending or failed state is preserved.

## API filtering and correlation

`GET /api/v1/process-data/time` requires `from_ts` and `to_ts`, rejects an inverted range,
and caps `limit` at 10,000. It does not perform an implicit product join.

Conceptual overlap query:

```sql
SELECT t.*, p.product_id, p.run_id
FROM process_data_time t
JOIN product_summary p
  ON p.process_id = t.process_id
 AND p.start_ts < t.ts_end
 AND COALESCE(p.end_ts, 9223372036854775807) > t.ts_start;
```

This finds temporal overlap, not precise spatial attribution. Use `product_data` for the
latter.

## Demo configurations

The packaged YAML and runnable generators are:

| Model | YAML | Python module |
|---|---|---|
| cycle (multiple/batch) | `furnace.yaml` | `simulators.furnace` |
| continuous | `continuous_line.yaml` | `simulators.continuous_line` |
| continuous rolling | `rolling_mill.yaml` | `simulators.rolling_mill` |
| cycle (single/CNC) | `discrete_cnc.yaml` | `simulators.discrete_cnc` |
| transformation | `cutting_line.yaml` | `simulators.transformation_cutting_line` |

Start any packaged engine configuration with:

```bash
python -m industrial_process_engine.simulators.run_engine continuous-line
```

Then run the matching publisher from the table in another terminal. Replace
`continuous-line` with `rolling-mill`, `furnace`, `cnc`, or `cutting-line`. The complete
copy/paste command list is packaged in
[`industrial_process_engine/simulators/README.md`](industrial_process_engine/simulators/README.md).

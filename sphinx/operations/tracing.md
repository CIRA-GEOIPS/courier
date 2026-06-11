# Courier Distributed Tracing

Courier instruments its plugin pipeline with [OpenTelemetry](https://opentelemetry.io/)
distributed tracing so operators can follow a file from detection to execution
across all services and plugin instances. Every message traversal produces a
single distributed trace, linked by `courier.correlation_id` and parent-child
span relationships propagated via [W3C Trace Context](https://www.w3.org/TR/trace-context/)
headers on every broker message.

## 1. Tracing Architecture Overview

### Pipeline Integration

Tracing is woven into the Courier plugin pipeline at two precise boundaries —
`Service.emit()` and `Service.consume()` — and nowhere else. Plugins never
touch W3C headers directly; they receive a parsed `Context` object and create
child spans from it.

```
┌──────────────┐     emit() + headers      ┌──────────────┐
│              │ ──────────────────────────▶│              │
│ Data Monitor │  FILE_FOUND fanout exch.   │ Job Builder  │
│   (root)     │                            │  (child)     │
│              │                            │              │
└──────────────┘                            └──────┬───────┘
                                                   │
                                           emit() + headers
                                                   │ JobReady-* queue
                                                   ▼
                                            ┌──────────────┐
                                            │              │
                                            │  Dispatcher  │
                                            │   (child)    │
                                            │              │
                                            └──────────────┘
```

### Boundary Pattern

| Boundary              | Function              | Role                                           |
|-----------------------|-----------------------|------------------------------------------------|
| Producer side         | `Service.emit()`      | Calls `inject_trace_headers()`, attaches W3C `traceparent` / `tracestate` to every broker message header |
| Consumer side         | `Service.consume()`   | Calls `extract_context(headers)`, returns a parsed OpenTelemetry `Context` as `(body, parent_ctx)` |
| Plugin entry point    | `handle_incoming_files` / `handle_incoming_jobs` | Passes `parent_ctx` to `tracer.start_as_current_span(…, context=parent_ctx)` |

This design means:

- **Plugins are oblivious to wire format** — they work with a typed `Context`, not raw headers.
- **A single injection/extraction pair per message** — no scattered propagation logic.
- **Fail-safe extraction** — garbage `traceparent` values never crash the pipeline; `extract_context()` degrades to an empty `Context` and logs a DEBUG message.

### Trace Root Semantics

- **Data monitors** produce **fresh root traces**. Their `find_and_emit_files()` loop creates spans without a parent context, because file detection is the origin of each trace.
- **Job builders** inherit parent context via `parent_ctx` extracted from incoming broker messages. Every `job_builder.build_job` span is a child of the data monitor's `data_monitor.emit_file` span.
- **Dispatchers** inherit parent context from the builder's `job_builder.emit_job` span. Every `dispatcher.dispatch_job` span continues the same trace.

### Correlation

Each distributed trace is linked by:

1. **`courier.correlation_id`** — a unique per-file identifier carried as a span attribute on every span in the trace. Set by the data monitor when a file is first detected. This is the primary key for trace-to-trace queries.
2. **Parent-child span relationships** — enforced by W3C `traceparent` propagation, forming a causal DAG from data monitor → builder → dispatcher.

### Relationship to Existing Observability

| Pillar              | System                 | Role                                   |
|---------------------|------------------------|----------------------------------------|
| **Metrics**         | Prometheus (via `prometheus_client`) | Counts, durations, health gauges — aggregated view |
| **Logs**            | Structured logging (optionally Grafana Loki) | Per-event detail, stack traces, diagnostic messages |
| **Traces**          | OpenTelemetry OTLP     | Causal chain of spans across service boundaries, enriched with attributes |

Traces do not replace metrics or logs. They complement them: use Prometheus for dashboards and alerts, use tracing when you need to follow **a single file** through the pipeline to understand latency breakdown or failure propagation.

---

## 2. Configuration Reference

### Fields

All four tracing fields live on the immutable `ServiceConfig` dataclass (`src/courier/config.py`).

| Field                      | Environment Variable               | Default                                    | Description                                                      |
|----------------------------|------------------------------------|--------------------------------------------|------------------------------------------------------------------|
| `tracing_enabled`          | `COURIER_TRACING_ENABLED`          | `true`                                     | Master toggle. Set to `"false"` to disable all tracing.         |
| `tracing_endpoint`         | `OTEL_EXPORTER_OTLP_ENDPOINT`      | `http://localhost:4318/v1/traces`          | OTLP HTTP collector endpoint. Falls back through `COURIER_TRACING_ENDPOINT` then the default. |
| `tracing_service_name`     | `COURIER_TRACING_SERVICE_NAME`     | `""` (falls back to `service_id`)          | `service.name` resource attribute for all exported spans.        |
| `tracing_sample_rate`      | `COURIER_TRACING_SAMPLE_RATE`      | `1.0`                                      | Float between 0.0 and 1.0. Controls the root sampling decision; children inherit the decision via `ParentBased`. |

### Disabling Tracing

Two independent mechanisms disable tracing:

```bash
# Method 1: Courier's own toggle
export COURIER_TRACING_ENABLED=false

# Method 2: OpenTelemetry SDK convention
export OTEL_TRACES_EXPORTER=none
```

When either is active, `init_tracing()` installs a `NoOpTracerProvider`. All `get_tracer()` calls return no-op tracers whose spans have invalid span contexts and are never exported. No network traffic leaves the process.

### Sampling Behavior

Sampling is controlled by `tracing_sample_rate` and uses OpenTelemetry's `ParentBased` composite sampler:

| Rate    | Sampler Configuration                             | Behavior                                                |
|---------|---------------------------------------------------|---------------------------------------------------------|
| `1.0`   | `ParentBased(root=ALWAYS_ON)`                     | All new root spans are sampled. Children always sampled if parent was. |
| `0.5`   | `ParentBased(root=TraceIdRatioBased(0.5))`        | ~50% of root spans sampled. Children inherit decision.   |
| `0.0`   | `ParentBased(root=TraceIdRatioBased(0.0))`        | No root spans sampled. `ParentBased` still samples children of sampled remote parents (none, at this rate). |

**Key property**: When a root span is sampled, every child span in the same trace is also sampled (guaranteed by `ParentBased`). This means you always get complete traces, never fragments.

### Service Name Fallback

```python
service_name = config.tracing_service_name or config.service_id
```

If `COURIER_TRACING_SERVICE_NAME` is unset or empty, the `service_id` field is used as the `service.name` resource attribute. The `service_id` itself defaults to `watcher-service-<random-hex8>` when `SERVICE_ID` is not set.

### Endpoint Format

The endpoint must be an OTLP HTTP endpoint. Example values:

```
http://localhost:4318/v1/traces           # Default (local collector)
http://jaeger:4318/v1/traces              # Docker Compose service
https://otel-collector.example.com:4318/v1/traces  # Production
```

The `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable takes precedence over `COURIER_TRACING_ENDPOINT`, following the standard OpenTelemetry SDK environment variable convention.

---

## 3. Span Naming Conventions & Attribute Schema

### Span Types

| Span Name                            | Emitting Plugin Type | What It Wraps                                            |
|--------------------------------------|-----------------------|----------------------------------------------------------|
| `data_monitor.process_file`          | `DataMonitor`         | Full lifecycle of processing a single detected file: metadata enrichment + emission |
| `data_monitor.add_metadata`          | `DataMonitor`         | Applying metadata matchers to a file (child of `process_file`) |
| `data_monitor.emit_file`             | `DataMonitor`         | Publishing a file message to the fanout exchange (child of `process_file`) |
| `job_builder.build_job`              | `JobBuilder`          | Processing an incoming file across all job groups        |
| `job_builder.process_job_group`      | `JobBuilder`          | Adding a file to one job group and emitting ready jobs (child of `build_job`) |
| `job_builder.emit_job`               | `JobBuilder`          | Fan-out emission of a ready job to all targets (child of `process_job_group`) |
| `job_builder.emit_one`               | `JobBuilder`          | Publishing a job to a single target queue (child of `emit_job`) |
| `dispatcher.dispatch_job`            | `Dispatcher`          | Consuming a job from the ready queue and orchestrating execution |
| `dispatcher.execute_job`             | `Dispatcher`          | Running the plugin's `get_execution_log()` logic (child of `dispatch_job`) |
| `dispatcher.emit_execution_log`      | `Dispatcher`          | Publishing an execution log record (child of `dispatch_job`) |
| `metadata_router.route_file`         | `MetadataRouterBuilder` | Routing an incoming file to the first matching route's job group |

**Note on `metadata_router.route_file`**: The `MetadataRouterBuilder` (a `JobBuilder` subclass in `src/courier/plugins/classes/job_builders/metadata_router.py`) replaces `job_builder.build_job` with its own top-level span. It does not emit `build_job` or `process_job_group` — those are only used by the base `JobBuilder`.

### Attribute Keys

All Courier-specific attributes use the `courier.*` namespace. Plugin identity attributes use `plugin.*`.

| Key                              | Type     | Carried By                                                                                             |
|----------------------------------|----------|--------------------------------------------------------------------------------------------------------|
| `courier.correlation_id`         | `str`    | `emit_job`, `dispatch_job`, `execute_job`                                                              |
| `courier.file.path`              | `str`    | `process_file`, `emit_file`, `build_job`, `route_file`                                                 |
| `courier.file.hostname`          | `str`    | `emit_file`                                                                                            |
| `courier.file.source`            | `str`    | `process_file`                                                                                         |
| `courier.file.instrument`        | `str`    | _(reserved — not yet emitted by built-in plugins)_                                                     |
| `courier.num_matchers`           | `int`    | `add_metadata`                                                                                         |
| `courier.job.id`                 | `str`    | `emit_job`, `dispatch_job`, `execute_job`                                                              |
| `courier.job.name`               | `str`    | `emit_job`                                                                                             |
| `courier.job.targets`            | `str`    | _(reserved — intended for multi-target fan-out spans)_                                                 |
| `courier.job.file_count`         | `int`    | _(reserved — intended for job composition spans)_                                                      |
| `courier.job_group.name`         | `str`    | `process_job_group`                                                                                    |
| `courier.execution_log.return_code` | `str` | `emit_execution_log`                                                                                   |
| `courier.execution_log.hostname` | `str`    | _(reserved — intended for execution host tracking)_                                                    |
| `courier.dispatch_latency`       | `float`  | _(reserved — intended for builder-to-dispatcher latency recording)_                                     |
| `courier.target`                 | `str`    | `emit_one`                                                                                             |
| `plugin.name`                    | `str`    | `process_file`, span events (`plugin.started`, `plugin.stopped`, etc.)                                |
| `plugin.version`                 | `str`    | `process_file`                                                                                         |
| `plugin.family`                  | `str`    | `process_file`                                                                                         |

**Reserved attributes**: Keys marked "reserved" are defined in `src/courier/tracing.py` and available for use by custom plugins but not yet emitted by any built-in plugin. Third-party plugin authors should use these constants rather than inventing their own keys to ensure cross-plugin trace queryability.

### Span Events

Span events represent discrete moments within a span's lifetime. They are additive — a span may carry zero or more events.

| Event Name                   | Location                                      | Trigger Condition                                                   |
|------------------------------|-----------------------------------------------|---------------------------------------------------------------------|
| `plugin.started`             | `PluginManager._start_plugin()`               | Plugin thread reaches `RUNNING` state after health check passes     |
| `plugin.stopped`             | `PluginManager._stop_plugin()`                | Plugin has been stopped gracefully and thread joined                |
| `plugin.health_check_failed` | `PluginManager._monitor_plugins()`            | Periodic health check returns `False` for a `RUNNING` plugin       |
| `plugin.restarting`          | `PluginManager._handle_failed_plugin()`       | Failed plugin is within restart budget; restart is being attempted  |
| `file.found`                 | `DataMonitor.find_and_emit_files()`           | File passed metadata enrichment successfully, before emit           |
| `file.emitted`               | `DataMonitor.find_and_emit_files()`           | File message published to the fanout exchange                       |
| `job.ready`                  | `JobBuilder._process_job_group()`             | `JobGroup.ready_jobs()` returned this job as ready for dispatch     |
| `job.emitted`                | `JobBuilder._process_job_group()`             | Job published to all target dispatcher queues                       |
| `job.executed`               | `Dispatcher.handle_incoming_jobs()`           | `get_execution_log()` returned execution results                    |

**PluginManager events** use standalone spans (`plugin_lifecycle`) rather than being attached to pipeline spans, because plugin lifecycle management is a separate concern from message processing.

### Slow Detection Thresholds

Operators can detect slow plugins by filtering spans by duration in their tracing backend. The following thresholds serve as starting points for alerting:

| Pipeline Stage      | Span Name(s)                         | Threshold | Rationale                                      |
|---------------------|--------------------------------------|-----------|------------------------------------------------|
| File scan           | `data_monitor.process_file`          | 10 s      | Covers filesystem I/O + metadata enrichment    |
| Job build           | `job_builder.build_job`              | 5 s       | Covers grouping logic + across all job groups  |
| Message emit        | `job_builder.emit_one`               | 2 s       | Single broker publish with publisher confirm   |
| Execution           | `dispatcher.execute_job`             | 30 s      | Plugin execution (e.g. script run, API call)   |

These are not hard-coded into Courier; they are recommended query filters for tracing backends (Jaeger, Grafana Tempo, Honeycomb, etc.).

### OpenTelemetry Messaging Semantic Conventions

Courier targets alignment with the [OTel messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/) where practical. The following conventional attributes are relevant to Courier's workflow and may be added in a future release:

- `messaging.system` — `"rabbitmq"` (or the name of the broker in use)
- `messaging.destination` — queue or exchange name
- `messaging.destination_kind` — `"queue"` or `"topic"`
- `messaging.operation` — `"process"` (for consumers), `"publish"` (for producers)
- `messaging.message.id` — correlation ID or job identifier

These are not currently emitted. The `courier.*` namespace attributes provide equivalent information today.

---

## 4. Operations Guide

### 4.1 Setting Up an OTLP Collector

The simplest way to get started is Jaeger's all-in-one Docker image, which bundles an OTLP collector and query UI:

```yaml
# docker-compose.yml
version: "3.8"
services:
  jaeger:
    image: jaegertracing/all-in-one:1.68
    ports:
      - "16686:16686"   # Jaeger UI
      - "4318:4318"     # OTLP HTTP receiver
    environment:
      - COLLECTOR_OTLP_ENABLED=true
```

With this running, Courier's default `tracing_endpoint` (`http://localhost:4318/v1/traces`) connects directly. Open `http://localhost:16686` to access the Jaeger UI.

For production, deploy the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) as a sidecar or daemonset pointed at your tracing backend (Grafana Tempo, Honeycomb, Datadog, etc.).

### 4.2 Querying Traces

#### Follow a File Through the Pipeline

To trace a specific file's journey, filter by the `courier.correlation_id` attribute:

**Jaeger UI**:
1. Open the **Search** tab.
2. In the **Tags** field, enter: `courier.correlation_id = <your-correlation-id>`
3. Click **Find Traces**.

**Jaeger HTTP API**:
```bash
curl "http://localhost:16686/api/traces?service=courier&tags=%7B%22courier.correlation_id%22%3A%22<id>%22%7D"
```

The resulting trace shows every span from file detection (`data_monitor.process_file`) through builder (`job_builder.*`) to dispatcher (`dispatcher.*`), with their parent-child nesting and durations.

#### Identifying Slow Plugins

To find spans that exceeded the slow thresholds:

**In Jaeger UI**:
1. Set **Min Duration** to the threshold (e.g., `10s` for file scan).
2. Optionally filter by **Operation** = `data_monitor.process_file`.
3. Sort by **Longest First**.
4. Expand a slow span to see its child spans and pinpoint which sub-operation consumed the most time.

**In Grafana Tempo** (TraceQL):
```
{ span.name = "data_monitor.process_file" && duration > 10s }
```

### 4.3 Monitoring OTLP Export Health

The OTLP exporter logs a `WARNING` message when span export fails:

```
WARNING  courier.tracing  Failed to export N spans to OTLP endpoint http://...:4318/v1/traces
```

This is triggered by the custom error callback installed on the `OTLPSpanExporter`. No spans are lost silently — the `BatchSpanProcessor` retries on transient failures. Monitor for these warnings in your log aggregation.

Cross-reference with Prometheus metrics:

- If `courier_tracing` WARNINGs correlate with the Jaeger/Collector target being down (`up{job="jaeger"} == 0`), investigate the collector.
- If WARNINGs appear without collector downtime, check network latency or collector throughput limits.
- The `BatchSpanProcessor` buffers spans in memory; a sustained export outage causes gradual memory growth until spans are dropped (also logged).

---

### 4.4 Production Monitoring Workflows

This section covers day-to-day monitoring practices for operators running Courier in
production. It assumes you have an OTLP collector receiving spans and a query backend
(Jaeger, Grafana Tempo, or equivalent) available.

#### What to Monitor Day-to-Day

Operators should establish a baseline across three dimensions and watch for deviations:

| Dimension          | What to Watch                                         | Why It Matters                                        |
|--------------------|-------------------------------------------------------|-------------------------------------------------------|
| **Latency trends** | P50/P95/P99 span duration per plugin type, over time  | Reveals gradual degradation before it becomes an outage |
| **Error rates**    | Count of spans containing an `exception.escaped` event or `courier.execution_log.return_code != "0"` | Signals failing file processing that may not trigger a service-level alert |
| **Throughput**     | Spans created per minute, broken down by span name    | Drops indicate a stalled plugin or broker backpressure |

**Quick Jaeger dashboard query** — show all spans in the last hour with their durations:

```bash
curl -s "http://localhost:16686/api/traces?service=courier&lookback=1h&limit=100" \
  | jq '.data[].spans[] | {name: .operationName, duration_ms: (.duration / 1000)}'
```

**Grafana with Tempo** — use Explore to run a TraceQL query that surfaces every slow
operation across all services:

```
{ duration > 30s }
```

Pair this with a time-series panel in Grafana that plots span count per plugin type,
using a Prometheus metric or the Tempo metrics-generator.

#### Spotting Degradation

**Spans trending slower over time.** Compare current P95 latency to a 7-day rolling
average. In Jaeger, this is a manual comparison across two time windows; in Grafana
Tempo with the metrics-generator, use a query like:

```promql
histogram_quantile(0.95, sum(rate(span_latency_bucket{span_name=~"dispatcher.*"}[5m])) by (le))
```

If the P95 crosses your threshold (the recommended starting point is 30 s for
`dispatcher.execute_job`), investigate the span's child spans to isolate the cause.

**Gap detection — missing stages in a trace.** A healthy file trace contains at least:
`process_file` -> `build_job`/`route_file` -> `dispatch_job` -> `execute_job`.
Missing stages indicate:

| Missing Span                 | Likely Cause                                              |
|------------------------------|-----------------------------------------------------------|
| No `build_job` after `emit_file` | Job Builder is down or the fanout exchange routing is broken |
| No `dispatch_job` after `emit_job` | Dispatcher is not consuming its queue, or routing key mismatch |
| No `execute_job` under `dispatch_job` | The plugin crashed before invocation or raised an unhandled exception |
| No `emit_execution_log` after `execute_job` | Plugin completed but the log publish failed (broker issue) |

To detect gaps programmatically, query for traces that have a `data_monitor.emit_file`
span but are missing a corresponding `dispatcher.execute_job` within a configurable
window (e.g., 5 minutes after the file was emitted). This is most easily done with a
script that queries the Jaeger API:

```bash
#!/bin/bash
# find-orphaned-files.sh — detect file traces without a dispatcher
END=$(date -u +%s)000000  # microseconds
START=$(date -u -d '1 hour ago' +%s)000000

# Get all file-emitted traces
curl -s "http://localhost:16686/api/traces?service=courier&operation=data_monitor.emit_file&start=$START&end=$END&limit=500" \
  | jq -r '.data[].traceID' > /tmp/all_file_traces.txt

# Get all dispatched traces
curl -s "http://localhost:16686/api/traces?service=courier&operation=dispatcher.execute_job&start=$START&end=$END&limit=500" \
  | jq -r '.data[].traceID' > /tmp/all_dispatch_traces.txt

# Comm shows tracelDs present in file traces but absent from dispatch traces
comm -23 <(sort /tmp/all_file_traces.txt) <(sort /tmp/all_dispatch_traces.txt)
```

**Backpressure signals.** Courier does not expose queue depth as a span attribute (see
the reserved `courier.dispatch_latency` key), but you can infer backpressure by
measuring the wall-clock gap between `job_builder.emit_one` and
`dispatcher.dispatch_job` within the same trace. If this gap grows over time, the
dispatcher is falling behind the builder. In Jaeger, expand a trace and note the
timestamps of these two spans; a gap exceeding 30 seconds warrants investigation.

For environments with Prometheus, correlate with broker metrics:

```promql
rabbitmq_queue_messages{queue=~"JobReady-.*"} > 100
```

A sustained queue depth above 100 messages means the dispatcher pool is undersized or a
plugin is blocking.

#### Setting Up Alerts

Translate span conditions into actionable alerts. All examples below assume your
tracing backend feeds a time-series database (Prometheus via Tempo metrics-generator,
or Mimir), but the logic applies to any alerting system.

**Alert 1: Slow plugin execution**

Condition: P95 duration of `dispatcher.execute_job` exceeds 30 s for 5 minutes.

```yaml
# Prometheus alert rule
- alert: CourierSlowExecution
  expr: histogram_quantile(0.95, rate(span_latency_bucket{span_name="dispatcher.execute_job"}[5m])) > 30
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Dispatcher execution P95 > 30 s"
    description: "P95 of dispatcher.execute_job is {{ $value }}s. Check the dispatcher's host and plugin health."
```

**Alert 2: Span with error status**

Condition: Any span has `status_code = STATUS_CODE_ERROR` (2). In TraceQL:

```
{ status.code = 2 }
```

Prometheus rule:

```yaml
- alert: CourierSpanError
  expr: rate(span_status_code_total{status_code="2"}[5m]) > 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Spans with error status detected"
    description: "{{ $value }} spans/sec with ERROR status in the last 5 minutes. Query TraceQL: { status.code = 2 }"
```

**Alert 3: Plugin health check failures**

Condition: The span event `plugin.health_check_failed` appears more than twice in 10
minutes for the same plugin instance.

```yaml
- alert: CourierPluginUnhealthy
  expr: rate(span_event_total{event_name="plugin.health_check_failed"}[10m]) > 0.2
  for: 10m
  labels:
    severity: critical
  annotations:
    summary: "Plugin health check failing repeatedly"
    description: "Event plugin.health_check_failed triggered {{ $value }}/s. The plugin may be restarting in a loop."
```

**Alert 4: Export pipeline stalled**

Condition: The Jaeger/Tempo backend receives zero spans from Courier for 5 minutes,
but the Courier process is still running (check via separate uptime metric).

```yaml
- alert: CourierTracingSilent
  expr: absent(span_latency_count) == 1
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "No spans received from Courier"
    description: "No spans in 5 minutes. Check COURIER_TRACING_ENABLED and OTLP collector health."
```

#### Root-Causing With Span Hierarchy

When an alert fires, use the span tree to isolate the bottleneck. The general process:

1. **Find a representative slow trace.** Use Jaeger search with `Min Duration > 30s`
   sorted by Longest First.
2. **Expand the trace and read top-down.** A `dispatch_job` span that is 45 s long
   with an `execute_job` child of 44 s tells you the execution itself is slow. If
   `execute_job` is 2 s but `dispatch_job` is 45 s, the time was spent waiting —
   inspect upstream spans.
3. **Follow parent spans upstream.** If the dispatcher is slow but execution is fast,
   check the builder span. Does `job_builder.process_job_group` show excessive
   duration? That would indicate the builder is queuing work or blocked on broker
   publishes.
4. **Check sibling spans for fan-out skew.** The `job_builder.emit_job` span may
   contain multiple `emit_one` children targeting different queues. If one child is
   much slower than the others, the target queue or its consumer is the bottleneck.
5. **Cross-reference with span events.** Look for `job.ready`, `job.emitted`, and
   `job.executed` events within the trace. The wall time between `job.emitted` and
   `job.executed` is the queue waiting time. If it dominates the trace, the dispatcher
   is starved or the broker is slow.

**Concrete TraceQL root-cause queries:**

```
# Is the execution itself slow?
{ span.name = "dispatcher.execute_job" && duration > 30s }

# Or is the builder queuing?
{ span.name = "job_builder.build_job" && duration > 10s }

# Find traces where the queue wait dominates
{ span.name = "dispatcher.dispatch_job" && duration > 30s } >> { span.name = "dispatcher.execute_job" && duration < 5s }
```

This last query uses TraceQL's `>>` (child-of) operator: it finds traces where the
parent `dispatch_job` is long but the child `execute_job` is short, pointing to queue
wait time as the culprit.

#### Common Operational Queries

**"Show me all traces where execution took more than 30 seconds"**

Jaeger UI: set `Min Duration = 30s`, `Operation = dispatcher.execute_job`, click Find
Traces.

TraceQL (Grafana Tempo):
```
{ span.name = "dispatcher.execute_job" && duration > 30s }
```

curl equivalent:
```bash
curl -s "http://localhost:16686/api/traces?service=courier&operation=dispatcher.execute_job&minDuration=30s&limit=50" \
  | jq '.data[] | {traceID, spans: [.spans[] | {name: .operationName, dur: (.duration / 1e6)}]}'
```

**"Which plugins are producing the most spans?"**

Aggregate span count by `plugin.name` attribute. Use a PromQL query from the
metrics-generator:

```promql
topk(10, sum by (plugin_name) (rate(span_latency_count[1h])))
```

To get this from raw Jaeger data, iterate traces and count by the `plugin.name`
attribute on `process_file` spans:

```bash
curl -s "http://localhost:16686/api/traces?service=courier&operation=data_monitor.process_file&limit=500&lookback=1h" \
  | jq '[.data[].spans[] | select(.operationName == "data_monitor.process_file") | .tags[] | select(.key == "plugin.name") | .value] | group_by(.) | map({plugin: .[0], count: length}) | sort_by(-.count)'
```

**"What is the P95 latency per plugin type?"**

If using Tempo metrics-generator with `plugin.name` as a span attribute promoted to a
metric label:

```promql
histogram_quantile(0.95, sum by (plugin_name) (rate(span_latency_bucket{span_name="dispatcher.execute_job"}[1h])))
```

Without the metrics-generator, export raw span data and compute percentiles offline
(see the audit export scripts in Section 4.5 for the extraction pattern).

#### Correlating Traces With Logs

**Trace -> Logs.** Every Courier span carries the W3C `trace_id` and `span_id`.
Configure your logging framework to include these in structured log entries. With
Courier's structured logging, add a log filter in Loki or your log aggregator:

```logql
{courier_trace_id="a1b2c3d4e5f67890"}
```

In Jaeger, expand any span and copy the Trace ID. Paste it into your log search tool
to see every log line emitted during that trace's lifetime.

**Logs -> Trace.** When you find an error in logs, extract the trace ID and search
Jaeger:

```bash
# From a log line containing the trace ID
TRACE_ID=$(grep "FileNotFoundError" /var/log/courier/*.log | jq -r '.courier_trace_id' | head -1)

# Open the trace in Jaeger
curl -s "http://localhost:16686/api/traces/$TRACE_ID" | jq '.data[0].spans[] | {name: .operationName, duration_ms: (.duration / 1000)}'
```

**Enabling trace context in Courier logs.** Courier's logging module can include trace
context automatically when it detects an active span. If your deployment uses the
`courier.tracing` module, ensure the log handler extracts span context via
OpenTelemetry's logging integration. A minimal setup in Python:

```python
from opentelemetry import trace
from opentelemetry.trace import format_trace_id

def trace_log_filter(record):
    span = trace.get_current_span()
    if span.get_span_context().is_valid:
        record.courier_trace_id = format_trace_id(span.get_span_context().trace_id)
    return True
```

#### Production Readiness Checklist

Before enabling tracing in production, verify each item:

- [ ] **Collector HA.** The OTLP collector is deployed with at least two replicas. A
  single Jaeger all-in-one is acceptable for dev; for production, use the OpenTelemetry
  Collector with a highly available backend (Grafana Tempo with MinIO/S3, or a
  commercial vendor).
- [ ] **Sampling rate tuned for volume.** Measure your peak file throughput
  (files/minute). Each file produces 5–12 spans. At 10,000 files/hour with 8 spans
  each, that is 80,000 spans/hour. Adjust `COURIER_TRACING_SAMPLE_RATE` so your
  collector and backend can handle the write volume. Start with `1.0` (100%) in
  staging, then reduce in production if needed. Never sample below `0.1` (10%) without
  confirming audit requirements are still met.
- [ ] **Retention policy aligned with audit needs.** Tempo's default retention is 24 h.
  For compliance use cases (see Section 4.5), configure retention to at least 90 days.
  For operational monitoring only, 7 days is usually sufficient.
- [ ] **Resource limits on the collector.** The OTLP collector's `BatchSpanProcessor`
  in Courier buffers spans client-side. If the collector is slow, Courier's memory
  grows. Set a memory limit on the collector pod/container and monitor
  `otelcol_exporter_send_failed_spans`.
- [ ] **End-to-end test trace.** Before declaring production ready, manually inject a
  test file and verify the complete trace appears in Jaeger/Tempo with all expected
  spans from `data_monitor.process_file` through `dispatcher.emit_execution_log`.
- [ ] **Alert rules deployed.** At minimum, deploy the "Span with error status" and
  "Slow plugin execution" alerts described above.
- [ ] **Dashboard bookmarked.** Save a direct link to the P95 latency dashboard and
  the "orphaned file" script output (see gap detection above) in your runbook.

---

### 4.5 Audit Trail Workflows

Distributed tracing is also Courier's audit trail system. Because every file is
assigned a `courier.correlation_id` and every processing stage emits a span in a
causal chain, the trace DAG doubles as a tamper-evident processing record. This
section covers compliance and data lineage workflows.

#### Building an Audit Trail for a Specific File

Given a file path, reconstruct its complete processing history:

1. **Identify the file.** Locate your file within the monitored directory. The file's
   hostname and path together uniquely identify it.
2. **Find the trace.** Search Jaeger for the `courier.correlation_id` if you have it.
   If you only have the filename, search by `courier.file.path`:

   **Jaeger UI:** Tags field: `courier.file.path = /data/incoming/report_2026-06-11.csv`

   **HTTP API:**
   ```bash
   curl -s "http://localhost:16686/api/traces?service=courier&tags=%7B%22courier.file.path%22%3A%22/data/incoming/report_2026-06-11.csv%22%7D" \
     | jq '.data[] | {traceID, spans: [.spans[] | {name: .operationName, start: (.startTime / 1000 | strftime("%Y-%m-%dT%H:%M:%SZ")), dur_ms: (.duration / 1000)}]}'
   ```

3. **Reconstruct the timeline.** Each span has a `startTime` and `duration` in
   microseconds. Sort spans by start time to produce the processing chronology:

   ```
   # Example timeline for /data/incoming/report_2026-06-11.csv
   14:32:01.123  data_monitor.process_file       (5.2 s)   File detected, metadata applied
   14:32:01.450  data_monitor.add_metadata        (0.3 s)   Metadata matchers evaluated
   14:32:04.890  data_monitor.emit_file           (1.2 s)   File published to fanout exchange
   14:32:06.100  metadata_router.route_file       (0.8 s)   Routed to "reports" job group
   14:32:06.950  job_builder.emit_job             (0.5 s)   Job fanned out to dispatcher queue
   14:32:07.500  dispatcher.dispatch_job          (28.1 s)  Job consumed and executed
   14:32:35.600  dispatcher.execute_job           (27.8 s)  Script executed against the file
   14:32:35.650  dispatcher.emit_execution_log    (0.05 s)  Execution log published
   ```

4. **Record the trace.** Export the full trace as JSON (see "Exporting Audit Data"
   below) and store it alongside your compliance records.

#### Data Lineage Queries

These queries answer "what happened to this data?" — the core of any data lineage
system.

**"Which files did this dispatcher execute in the last hour?"**

Find all `dispatcher.execute_job` spans and extract the `courier.correlation_id` and
`courier.file.path` attributes:

```bash
curl -s "http://localhost:16686/api/traces?service=courier&operation=dispatcher.execute_job&lookback=1h&limit=200" \
  | jq '[.data[].spans[] | select(.operationName == "dispatcher.execute_job") | {
      traceID: .traceID,
      correlation_id: (.tags[]? | select(.key == "courier.correlation_id") | .value),
      file_path: (.tags[]? | select(.key == "courier.file.path") | .value),
      timestamp: (.startTime / 1000 | strftime("%Y-%m-%dT%H:%M:%SZ"))
    }]'
```

**"Show me every file that passed through the metadata_router."**

Filter by `span.name = metadata_router.route_file` and collect file paths:

```bash
curl -s "http://localhost:16686/api/traces?service=courier&operation=metadata_router.route_file&lookback=1h&limit=200" \
  | jq '[.data[].spans[] | select(.operationName == "metadata_router.route_file") | {
      traceID: .traceID,
      file_path: (.tags[]? | select(.key == "courier.file.path") | .value),
      job_group: (.tags[]? | select(.key == "courier.job_group.name") | .value)
    }] | unique_by(.traceID)'
```

**"What is the complete processing history of file X?"**

Expands the first query in "Building an Audit Trail" into a fully attributed record:

```bash
FILE_PATH="/data/incoming/report_2026-06-11.csv"
ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('{\"courier.file.path\":\"' + '$FILE_PATH' + '\"}'))")

curl -s "http://localhost:16686/api/traces?service=courier&tags=$ENCODED" \
  | jq --arg fp "$FILE_PATH" '
    .data[] | {
      file: $fp,
      correlation_id: ([.spans[].tags[] | select(.key == "courier.correlation_id") | .value] | first),
      timeline: [.spans[] | {
        stage: .operationName,
        start: (.startTime / 1000 | strftime("%Y-%m-%dT%H:%M:%S.%fZ")),
        duration_ms: (.duration / 1000),
        return_code: ([.tags[]? | select(.key == "courier.execution_log.return_code") | .value] | first // "N/A"),
        hostname: ([.tags[]? | select(.key == "courier.file.hostname") | .value] | first // "N/A")
      }] | sort_by(.start)
    }'
```

#### Exporting Audit Data

**Via Jaeger HTTP API — span-level export to NDJSON.**

This script exports every span in a time window as newline-delimited JSON (NDJSON),
one span per line, suitable for ingestion into a SIEM, data warehouse, or long-term
archive:

```bash
#!/bin/bash
# export-audit-spans.sh — export Courier spans as NDJSON for audit archive
# Usage: ./export-audit-spans.sh 2026-06-11T00:00:00Z 2026-06-11T23:59:59Z > audit_20260611.ndjson

START=$(date -d "$1" +%s)000000
END=$(date -d "$2" +%s)000000
BASE="http://localhost:16686/api/traces?service=courier&start=$START&end=$END&limit=200"

cursor_start="$START"
while true; do
    resp=$(curl -s "${BASE}&start=${cursor_start}")
    count=$(echo "$resp" | jq '.data | length')
    if [ "$count" -eq 0 ]; then
        break
    fi

    echo "$resp" | jq -c '
      .data[].spans[] | {
        trace_id: .traceID,
        span_id: .spanID,
        parent_span_id: (.references[]? | select(.refType == "CHILD_OF") | .spanID // null),
        operation: .operationName,
        start_time_us: .startTime,
        duration_us: .duration,
        correlation_id: ([.tags[]? | select(.key == "courier.correlation_id") | .value] | first // null),
        file_path: ([.tags[]? | select(.key == "courier.file.path") | .value] | first // null),
        job_id: ([.tags[]? | select(.key == "courier.job.id") | .value] | first // null),
        return_code: ([.tags[]? | select(.key == "courier.execution_log.return_code") | .value] | first // null),
        hostname: ([.tags[]? | select(.key == "courier.file.hostname") | .value] | first // null),
        plugin_name: ([.tags[]? | select(.key == "plugin.name") | .value] | first // null),
        plugin_version: ([.tags[]? | select(.key == "plugin.version") | .value] | first // null)
      }
    '

    # Advance cursor using the startTime of the last span
    last_start=$(echo "$resp" | jq -r '[.data[-1].spans[].startTime] | max')
    cursor_start="$last_start"
    sleep 0.2  # polite rate limiting
done
```

**Via OTLP Collector — pipeline to SIEM or data warehouse.**

Configure the OpenTelemetry Collector to fan-out traces to both your query backend and
a long-term store. Example collector config that exports to Tempo for querying AND
writes spans to a file (which can be ingested by a SIEM):

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

exporters:
  otlp/tempo:
    endpoint: tempo:4317
    tls:
      insecure: true
  file/audit:
    path: /var/log/otel/spans.ndjson
    rotation:
      max_megabytes: 500
      max_backups: 30

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp/tempo, file/audit]
```

For direct SIEM ingestion, replace `file/audit` with the appropriate exporter
(`kafka`, `splunk_hec`, `elasticsearch`, or the `otlphttp` exporter pointed at your
SIEM's OTLP ingest endpoint).

#### Compliance Considerations

**What constitutes a complete audit record?**

A single file's processing is auditable when the trace contains all of these attributes
across its spans:

| Attribute                         | Source Span(s)                                | Required For                                  |
|-----------------------------------|-----------------------------------------------|-----------------------------------------------|
| `courier.correlation_id`          | Present on all pipeline spans                 | Uniquely identifies the file's processing event |
| `courier.file.path`               | `data_monitor.process_file`, `emit_file`      | What file was processed                       |
| `courier.file.hostname`           | `data_monitor.emit_file`                      | Which machine detected the file               |
| `plugin.name` / `plugin.version`  | `data_monitor.process_file`                   | Which plugin processed it, and at what version |
| `courier.job_group.name`          | `job_builder.process_job_group`               | Which pipeline rule routed the file           |
| `courier.execution_log.return_code` | `dispatcher.emit_execution_log`            | Whether execution succeeded or failed         |
| Start time and duration (per span) | Every span                                    | When each stage occurred and how long it took |
| W3C `trace_id` + `span_id`        | Every span (intrinsic to OTLP)                | Chain-of-custody proof (see below)            |

**Retention requirements by regime:**

| Regime            | Minimum Trace Retention | Notes                                                  |
|--------------------|-------------------------|--------------------------------------------------------|
| General operations | 7 days                  | Enough for incident response and trend analysis        |
| SOC 2              | 90 days                 | Aligns with typical log retention requirements         |
| HIPAA              | 6 years                 | May require exporting traces to WORM-compliant storage |
| GDPR (processing) | Aligned with data retention policy | Delete traces when the associated file data is deleted |
| Internal audit     | 1 year                  | Common enterprise standard                             |

Configure your tracing backend's retention accordingly. For Tempo:

```yaml
# tempo.yaml
compactor:
  compaction:
    block_retention: 2160h   # 90 days
```

For Jaeger with Elasticsearch backend, configure index lifecycle management (ILM) to
match.

#### Chain of Custody

The combination of W3C Trace Context propagation and parent-child span relationships
provides a cryptographic-strength chain of custody:

1. **Root span creation.** When a Data Monitor detects a file, it creates a root span
   with a new W3C `trace_id` (128-bit random) and a `span_id` (64-bit random).
2. **Context injection.** `Service.emit()` serializes the current span context into a
   `traceparent` header (`00-{trace_id}-{span_id}-01`) and attaches it to the broker
   message.
3. **Context extraction.** `Service.consume()` extracts the `traceparent` header and
   produces a parent `Context` object. The consuming plugin creates a child span with
   its own new `span_id` and the same `trace_id`, recording the parent's `span_id` in
   a `CHILD_OF` reference.
4. **Continuous chain.** Every hop repeats steps 2–3. The result is an unbroken
   sequence of parent-child links from file detection to execution log.

This proves that **file A detected at stage 1 is the same file A processed at stage
3** because:
- The `trace_id` is invariant across all stages.
- The parent-child references form a directed, acyclic graph with no missing edges
  (if any stage is skipped, the trace DAG has a visible gap).
- The `courier.correlation_id` on every span provides a human-readable secondary key
  that cross-validates the W3C chain.

To verify chain integrity for a specific audit:

```bash
# Verify that a trace has no missing stages by checking span references
curl -s "http://localhost:16686/api/traces/$TRACE_ID" | jq '
  .data[0].spans | map({
    name: .operationName,
    has_parent: (.references | any(.refType == "CHILD_OF")),
    children: [.[] | select(.references[]?.refType == "CHILD_OF" and .references[]?.spanID == .spanID) | .operationName]
  })
'
```

#### Tamper Evidence

Distributed tracing is inherently tamper-evident because:

1. **Missing spans are visible holes.** The trace DAG for a file must contain specific
   stages. An operator or auditor can enumerate expected spans and flag traces where
   required spans are absent (see the gap detection script in Section 4.4).
2. **Span order is chronological.** `startTime` is set by the SDK at span creation and
   cannot be retroactively modified in an exported trace (the collector timestamps
   spans on receive, but the span's own `startTime` is preserved as client-side
   evidence).
3. **Correlation ID immutability.** `courier.correlation_id` is set once by the data
   monitor and carried as an attribute — if it changes mid-trace, the spans with the
   new value will belong to a different trace. Any attempt to splice spans from
   different traces is detectable by comparing correlation IDs.
4. **OTLP delivery provides completeness.** The `BatchSpanProcessor` retries failed
   exports and logs WARNINGs on persistent failure. If a span was created but not
   delivered, the log gap is evidence of the delivery failure.

A script to audit trace completeness across a batch of correlation IDs:

```bash
#!/bin/bash
# audit-trace-completeness.sh — check that every correlation_id has all required stages
REQUIRED_STAGES=("data_monitor.process_file" "dispatcher.execute_job" "dispatcher.emit_execution_log")

for corr_id in "$@"; do
    ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('{\"courier.correlation_id\":\"' + '$corr_id' + '\"}'))")
    stages=$(curl -s "http://localhost:16686/api/traces?service=courier&tags=$ENCODED" \
        | jq -r '.data[0].spans[].operationName' | sort -u)

    for required in "${REQUIRED_STAGES[@]}"; do
        if ! echo "$stages" | grep -qx "$required"; then
            echo "MISSING: correlation_id=$corr_id is missing stage $required"
        fi
    done
done
```

#### Example Audit Report Query

Generate a CSV of all files processed by a specific dispatcher between two timestamps,
with their complete processing timeline:

```bash
#!/bin/bash
# audit-dispatcher-report.sh — CSV report for compliance
# Usage: ./audit-dispatcher-report.sh "2026-06-11T08:00:00Z" "2026-06-11T12:00:00Z"

START=$(date -d "$1" +%s)000000
END=$(date -d "$2" +%s)000000

echo "correlation_id,file_path,stage,start_time,duration_ms,return_code,hostname"

curl -s "http://localhost:16686/api/traces?service=courier&operation=dispatcher.execute_job&start=$START&end=$END&limit=500" \
  | jq -r '
    .data[] | . as $trace |
    $trace.spans[] | {
      corr: ([.tags[]? | select(.key == "courier.correlation_id") | .value] | first // "N/A"),
      file: ([.tags[]? | select(.key == "courier.file.path") | .value] | first // "N/A"),
      stage: .operationName,
      start: (.startTime / 1000 | strftime("%Y-%m-%dT%H:%M:%S.%fZ")),
      dur_ms: (.duration / 1000),
      rc: ([.tags[]? | select(.key == "courier.execution_log.return_code") | .value] | first // "N/A"),
      host: ([.tags[]? | select(.key == "courier.file.hostname") | .value] | first // "N/A")
    } | [
      .corr, .file, .stage, .start, (.dur_ms | tostring), .rc, .host
    ] | @csv
  '
```

The output is a CSV with one row per span, ready for import into Excel, a database, or
a compliance reporting system. Each row captures the correlation ID, file path, stage
name, precise timestamp, duration in milliseconds, return code, and originating
hostname — the full processing fingerprint of every file.

For a complete file-centric view (all stages for each file, not just dispatcher
spans), retrieve the full trace by correlation ID and flatten all spans. This is the
pattern used by the "complete processing history" query earlier in this section,
piped through `jq` with `@csv` output formatting.

---

## 5. Development Guide

### Writing Tests with `_ListExporter`

Courier's test suite uses a custom `_ListExporter` (defined in `tests/unit_tests/test_tracing.py`) because OpenTelemetry SDK 1.42+ removed `InMemorySpanExporter`. The pattern:

```python
from opentelemetry.sdk.trace.export import SpanExportResult, SpanExporter

def _make_list_exporter():
    """Create a span exporter + span list pair for round-trip verification."""
    spans: list = []

    class _ListExporter(SpanExporter):
        def export(self, span_data):
            spans.extend(span_data)
            return SpanExportResult.SUCCESS

        def shutdown(self):
            pass

    return _ListExporter(), spans
```

Usage in tests:

```python
exporter, captured_spans = _make_list_exporter()
provider = TracerProvider(
    active_span_processor=SimpleSpanProcessor(exporter),
)
# ... exercise code that creates spans ...
assert len(captured_spans) == 2
```

The `_ListExporter` is in-process and synchronous (`SimpleSpanProcessor`), so spans are available for assertion immediately after the operations under test complete.

### Test Isolation with `reset_tracing()` and `_force_noop_global_provider()`

The tracing module stores a global `_tracer_provider` singleton. Without cleanup, one test's initialized provider leaks into the next test. Two utilities enforce isolation:

**`reset_tracing()`** — clears Courier's own module-level singleton:
```python
from courier.tracing import reset_tracing
reset_tracing()  # calls shutdown_tracing(), clears _tracer_provider
```

**`_force_noop_global_provider()`** — resets the OpenTelemetry API's process-wide gate (`_TRACER_PROVIDER_SET_ONCE`) and installs a fresh `NoOpTracerProvider`:
```python
def _force_noop_global_provider():
    from opentelemetry.util._once import Once
    import opentelemetry.trace
    opentelemetry.trace._TRACER_PROVIDER_SET_ONCE = Once()
    set_tracer_provider(NoOpTracerProvider())
```

The test suite uses both in an `autouse` fixture:
```python
@pytest.fixture(autouse=True)
def _reset_tracing_after_test():
    reset_tracing()
    _force_noop_global_provider()
    yield
    reset_tracing()
    _force_noop_global_provider()
```

**Important**: `_force_noop_global_provider()` reaches into the `opentelemetry.util._once` private module. This is a necessary workaround — the OTel API intentionally prevents replacing the global provider after the first call. Tests that need to re-initialize tracing must use this pattern.

### Adding Tracing to a New Plugin Type

To instrument a new plugin with tracing:

1. **Get a tracer** at module level or in `__init__`:
   ```python
   from courier.tracing import get_tracer
   tracer = get_tracer(__name__)
   ```

2. **Create a span** with `start_as_current_span`:
   ```python
   with tracer.start_as_current_span(
       "my_plugin.do_work",
       attributes={"courier.correlation_id": correlation_id},
   ):
       do_work()
   ```

3. **Add attributes** on every span:
   - Always include `ATTR_CORRELATION_ID` if the work is tied to a specific file or job.
   - Use the constants from `courier.tracing` — never hard-code attribute key strings.

4. **Add span events** for lifecycle milestones:
   ```python
   from opentelemetry.trace import get_current_span

   get_current_span().add_event(
       "work.completed",
       attributes={"result.count": str(n)},
   )
   ```

5. **Pass parent context** when consuming messages:
   ```python
   for body, parent_ctx in self.parent_service.consume(QUEUE):
       with tracer.start_as_current_span(
           "my_plugin.handle_message",
           context=parent_ctx,
       ):
           process(body)
   ```

### The `@trace_plugin_method` Decorator

The `@trace_plugin_method` decorator wraps a regular (non-generator) method in a span:

```python
from courier.tracing import trace_plugin_method

class MyPlugin:
    @trace_plugin_method("my_plugin.process", attributes={"key": "value"})
    def process(self, data):
        return transform(data)
```

**Behavior**:
- Creates a span named `"my_plugin.process"` with the given attributes.
- Returns the original function's result unchanged.
- Preserves `__name__`, `__qualname__`, and `__doc__` on the wrapper.
- Sets `__wrapped__` for introspection.

**Generator Exclusion Rule**:
The decorator **raises `TypeError` at decoration time** if applied to a generator function:

```python
# THIS RAISES TypeError:
@trace_plugin_method("my_plugin.stream")
def my_generator(self):
    yield 1
```

This is intentional: generator functions are suspended at each `yield`, and the span's `__exit__` would close the span on the first `yield` rather than on generator exhaustion. Generator methods must use inline `start_as_current_span`:

```python
# Correct: inline span for generator
def my_generator(self):
    tracer = get_tracer(__name__)
    with tracer.start_as_current_span("my_plugin.stream"):
        yield 1
        yield 2
```

The reason this works inline but not via a decorator is that the inline `with` block wraps the entire generator body, while a decorator would wrap only the function call (which returns immediately with the generator object).

---

## Quick Reference

### Environment Variables

```bash
COURIER_TRACING_ENABLED=true          # Enable tracing (default)
COURIER_TRACING_ENABLED=false         # Disable tracing
OTEL_TRACES_EXPORTER=none             # Alternative disable (SDK convention)
COURIER_TRACING_ENDPOINT=http://...   # OTLP collector URL
OTEL_EXPORTER_OTLP_ENDPOINT=http://... # OTLP collector URL (takes precedence)
COURIER_TRACING_SERVICE_NAME=courier  # Service name in traces
COURIER_TRACING_SAMPLE_RATE=0.5       # 50% sampling
```

### Span Name Quick Index

| Span                             | Plugin       | Purpose                       |
|----------------------------------|--------------|-------------------------------|
| `data_monitor.process_file`      | DataMonitor  | File detection + processing   |
| `data_monitor.add_metadata`      | DataMonitor  | Metadata enrichment           |
| `data_monitor.emit_file`         | DataMonitor  | File publication              |
| `job_builder.build_job`          | JobBuilder   | Incoming file consumption     |
| `job_builder.process_job_group`  | JobBuilder   | Per-group file processing     |
| `job_builder.emit_job`           | JobBuilder   | Fan-out job emission          |
| `job_builder.emit_one`           | JobBuilder   | Single target publish         |
| `dispatcher.dispatch_job`        | Dispatcher   | Job consumption + execution   |
| `dispatcher.execute_job`         | Dispatcher   | Plugin execution logic        |
| `dispatcher.emit_execution_log`  | Dispatcher   | Log publication               |
| `metadata_router.route_file`     | Router       | Route-first-match             |

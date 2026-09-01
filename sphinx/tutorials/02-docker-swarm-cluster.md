# Tutorial 2: Distributed Docker Deployment

**Level:** Intermediate | **Time:** 30 minutes

In this tutorial, you'll deploy a Courier pipeline across multiple Docker
containers — each running a subset of plugins — connected by a shared
RabbitMQ broker. You'll also set up Prometheus, Grafana, and Jaeger for
full observability.

## Learning Objectives

By the end of this tutorial, you will:

- Split a Courier config across multiple containers using `--only`
- Deploy RabbitMQ as the shared message broker
- Configure Prometheus metrics ports per container
- Generate and provision a Grafana dashboard
- Enable distributed tracing with Jaeger
- Verify end-to-end file processing across containers

## Prerequisites

- Courier installed ({doc}`../getting-started/installation`)
- Docker and Docker Compose installed
- Completion of {doc}`../tutorials/01-simple-file-watcher` (recommended)
- Basic familiarity with YAML and Docker concepts

## Step 1: Architecture Overview

In a single-machine deployment, all plugins run in one process. In a
clustered deployment, you split them across containers:

```
                   ┌──────────────────────┐
                   │      RabbitMQ         │
                   │   (message broker)    │
                   └──────┬───────┬────────┘
                          │       │
            ┌─────────────┘       └─────────────┐
            ▼                                   ▼
   ┌─────────────────┐                 ┌─────────────────┐
   │  data-monitor    │                 │  builder +       │
   │  (--only watch-  │                 │  dispatcher      │
   │   files)         │                 │  (--only create- │
   │                  │                 │   jobs,process-  │
   │  Prom :8001      │                 │   files)         │
   └─────────────────┘                 │  Prom :8002      │
                                       └─────────────────┘

     Observability stack (optional):
     ┌──────────┐  ┌─────────┐  ┌────────┐
     │Prometheus│  │ Grafana │  │ Jaeger │
     └──────────┘  └─────────┘  └────────┘
```

> **Note:** This tutorial uses `docker compose` for simplicity. For
> Docker Swarm mode, use `docker stack deploy -c docker-compose.yml
> courier` instead and add `deploy:` keys to service definitions.

One configuration file defines the entire pipeline. Each container runs
only its assigned plugins via the `--only` flag, using the YAML keys from
`spec.run` as identifiers. Plugins communicate through RabbitMQ, which
decouples the containers: you can start, stop, and scale them independently.

## Step 2: Project Setup

Create a directory for this tutorial:

```
mkdir ~/tutorial02-docker-swarm
cd ~/tutorial02-docker-swarm
```

Create the required subdirectories:

```
mkdir -p data/incoming
mkdir -p grafana/provisioning/dashboards
mkdir -p grafana/provisioning/datasources
mkdir -p prometheus
```

Create the service configuration at `config.yaml`:

```
apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: tutorial-02-docker-swarm
  namespace: tutorial02
  description: Distributed pipeline across Docker containers for tutorial 02.
  docstring: |
    This service demonstrates a multi-container Courier deployment
    using Docker Compose. The data monitor runs in one container,
    while the job builder and dispatcher share a second container.

spec:
  heartbeat_interval: 30

  broker:
    host: rabbitmq
    port: 5672
    username: admin
    password: admin_test

  run:
    # Data monitor — watches for files
    - watch-files:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: /data/incoming
          metadata-tools:
            - goes18_abi

    # Job builder — one file = one job
    - create-jobs:
        kind: job_builder
        name: DummyJobBuilder
        config: null

    # Dispatcher — echo file details
    - process-files:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            #!/bin/bash
            echo "=========================================="
            echo "File detected: {file}"
            echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "=========================================="
```

Key differences from {doc}`01-simple-file-watcher`:

- The broker `host` is `rabbitmq` (the Docker Compose service name)
  instead of `localhost`.
- The data monitor's `path` is `/data/incoming` (an absolute path inside
  the container volume).
- The dispatcher run key is `process-files` (the YAML identifier used
  with `--only`).

## Step 3: RabbitMQ Service

Create `docker-compose.yml` with a RabbitMQ service first:

```
# docker-compose.yml
services:
  rabbitmq:
    image: rabbitmq:management
    hostname: rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: admin_test
```

**What the ports do:**

| Port | Purpose |
|------|---------|
| `5672` | AMQP protocol — Courier plugins use this to publish and consume messages |
| `15672` | Management UI — browse exchanges, queues, and message rates at <http://localhost:15672> |

Credentials must match those in `config.yaml`. The `hostname` setting
(`rabbitmq`) is the name Courier uses to reach the broker inside the
Docker network.

## Step 4: Data Monitor Container

Add a service for the data monitor. This container runs only the file
watcher:

```
  courier-watcher:
    image: dgshanee/courier:latest
    command:
      - "courier"
      - "run"
      - "/config/service.yaml"
      - "--only"
      - "watch-files"
    environment:
      COURIER_PROMETHEUS_PORT: "8001"
    volumes:
      - ./config.yaml:/config/service.yaml:ro
      - ./data:/data
    ports:
      - "8001:8001"
    depends_on:
      - rabbitmq
```

**What's happening here:**

- `--only watch-files` tells Courier to start only the plugin listed
  under the YAML key `watch-files` in `spec.run`. All other plugins are
  skipped.
- `COURIER_PROMETHEUS_PORT=8001` overrides the default Prometheus port
  (8000). Each container needs a unique port so Prometheus can scrape
  both.
- The config file is mounted read-only at `/config/service.yaml`. The
  data directory is mounted at `/data` (matching the `path` setting in
  the data monitor config).
- Port `8001` is exposed so Prometheus can scrape metrics from this
  container.

> The `--only` flag tells Courier to start only the listed pipeline steps using YAML key identifiers from `spec.run` — not plugin class names. For full semantics including duplicate dispatcher warnings, see {doc}`../getting-started/configuration` (Distributed Deployment with `--only`).

## Step 5: Builder and Dispatcher Container

Add a second Courier container that runs the job builder and dispatcher
together:

```
  courier-processor:
    image: dgshanee/courier:latest
    command:
      - "courier"
      - "run"
      - "/config/service.yaml"
      - "--only"
      - "create-jobs,process-files"
    environment:
      COURIER_PROMETHEUS_PORT: "8002"
    volumes:
      - ./config.yaml:/config/service.yaml:ro
      - ./data:/data
    ports:
      - "8002:8002"
    depends_on:
      - rabbitmq
```

This container runs two plugins (`create-jobs` and `process-files`) in a
single process. They share one Prometheus endpoint on port 8002.

**Why split this way?** The data monitor (file watcher) is I/O-bound — it
polls the filesystem — while the builder and dispatcher are CPU-bound
(processing and executing). Splitting them lets you scale each role
independently. In a production swarm, you might run three watcher
replicas and five processor replicas.

## Step 6: Prometheus and Grafana

Add observability services to `docker-compose.yml`.

### Prometheus

Create `prometheus/prometheus.yml`:

```
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "courier-watcher"
    static_configs:
      - targets: ["courier-watcher:8001"]

  - job_name: "courier-processor"
    static_configs:
      - targets: ["courier-processor:8002"]
```

Add the Prometheus service to `docker-compose.yml`:

```
  prometheus:
    image: prom/prometheus
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports:
      - "9090:9090"
    depends_on:
      - courier-watcher
      - courier-processor
```

### Grafana Dashboard Generation

The `courier dashboard` command reads your config and generates Grafana
dashboard JSON tailored to your pipeline:

Run this on your **host machine** (not inside a container), in the same
directory as your config:

```
pip install data-courier[grafana]
courier dashboard config.yaml -o grafana/provisioning/dashboards/courier.json
```

This produces a dashboard with panels for service health, data monitor
throughput, job builder metrics, and dispatcher execution stats — only
the panels relevant to your configured plugins.

Create a Grafana datasource provisioning file at
`grafana/provisioning/datasources/prometheus.yml`:

```
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

Add the Grafana service to `docker-compose.yml`:

```
  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
    depends_on:
      - prometheus
```

Grafana will auto-provision the Prometheus datasource and the Courier
dashboard on startup. Access the dashboard at <http://localhost:3000>.

### Complete Compose File So Far

Your `docker-compose.yml` should now contain:

```
# docker-compose.yml
services:
  rabbitmq:
    image: rabbitmq:management
    hostname: rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: admin_test

  courier-watcher:
    image: dgshanee/courier:latest
    command:
      - "courier"
      - "run"
      - "/config/service.yaml"
      - "--only"
      - "watch-files"
    environment:
      COURIER_PROMETHEUS_PORT: "8001"
    volumes:
      - ./config.yaml:/config/service.yaml:ro
      - ./data:/data
    ports:
      - "8001:8001"
    depends_on:
      - rabbitmq

  courier-processor:
    image: dgshanee/courier:latest
    command:
      - "courier"
      - "run"
      - "/config/service.yaml"
      - "--only"
      - "create-jobs,process-files"
    environment:
      COURIER_PROMETHEUS_PORT: "8002"
    volumes:
      - ./config.yaml:/config/service.yaml:ro
      - ./data:/data
    ports:
      - "8002:8002"
    depends_on:
      - rabbitmq

  prometheus:
    image: prom/prometheus
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports:
      - "9090:9090"
    depends_on:
      - courier-watcher
      - courier-processor

  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
    depends_on:
      - prometheus
```

## Step 7: Distributed Tracing

Courier instruments its plugin pipeline with OpenTelemetry tracing.
Courier enables traces by default. They follow each file from detection to
execution across all containers. For details, see
{doc}`../operations/tracing`.

Add a Jaeger all-in-one service to `docker-compose.yml`:

```
  jaeger:
    image: jaegertracing/all-in-one:1.68.0
    ports:
      - "16686:16686"
      - "4318:4318"
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
```

| Port | Purpose |
|------|---------|
| `16686` | Jaeger UI — search and visualize traces |
| `4318` | OTLP HTTP receiver — Courier sends spans here |

Add the tracing environment variable to **both** Courier containers.
Append to each container's `environment` block:

```
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://jaeger:4318/v1/traces"
```

Courier sends spans via OTLP HTTP to the Jaeger collector. Each file
traversal produces a single distributed trace linked by `courier.correlation_id`,
with parent-child span relationships propagated across broker messages.
For span naming conventions, sampling configuration, and production query
workflows, see {doc}`../operations/tracing`.

After restarting the stack, the Jaeger UI is available at
<http://localhost:16686>. Select the `courier` service to see traces with
spans from both containers linked by a shared `trace_id`.

### Complete Compose File

At this point your full `docker-compose.yml` should look like this:

```
# docker-compose.yml
services:
  rabbitmq:
    image: rabbitmq:management
    hostname: rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: admin_test

  courier-watcher:
    image: dgshanee/courier:latest
    command:
      - "courier"
      - "run"
      - "/config/service.yaml"
      - "--only"
      - "watch-files"
    environment:
      COURIER_PROMETHEUS_PORT: "8001"
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://jaeger:4318/v1/traces"
    volumes:
      - ./config.yaml:/config/service.yaml:ro
      - ./data:/data
    ports:
      - "8001:8001"
    depends_on:
      - rabbitmq

  courier-processor:
    image: dgshanee/courier:latest
    command:
      - "courier"
      - "run"
      - "/config/service.yaml"
      - "--only"
      - "create-jobs,process-files"
    environment:
      COURIER_PROMETHEUS_PORT: "8002"
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://jaeger:4318/v1/traces"
    volumes:
      - ./config.yaml:/config/service.yaml:ro
      - ./data:/data
    ports:
      - "8002:8002"
    depends_on:
      - rabbitmq

  prometheus:
    image: prom/prometheus
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports:
      - "9090:9090"
    depends_on:
      - courier-watcher
      - courier-processor

  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
    depends_on:
      - prometheus

  jaeger:
    image: jaegertracing/all-in-one:1.68.0
    ports:
      - "16686:16686"
      - "4318:4318"
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
```

## Step 8: Deploy and Verify

### Start the Stack

From the tutorial directory, bring everything up:

```
docker compose up -d
```

Docker Compose pulls images (if needed) and starts all six services:
RabbitMQ, two Courier containers, Prometheus, Grafana, and Jaeger.

> **Note:** The `docker compose up` command demonstrated here runs in
> Compose mode, which ignores `deploy.replicas` and other Swarm-specific
> settings. To run as a true Swarm stack with replica support, use
> `docker stack deploy -c docker-compose.yml courier`.

### Check Service Status

```
docker compose ps
```

Expected output shows all services with `Up` status:

```
NAME                    STATUS
tutorial02-docker-swarm-rabbitmq-1            Up
tutorial02-docker-swarm-courier-watcher-1     Up
tutorial02-docker-swarm-courier-processor-1   Up
tutorial02-docker-swarm-prometheus-1          Up
tutorial02-docker-swarm-grafana-1             Up
tutorial02-docker-swarm-jaeger-1              Up
```

Names may vary slightly depending on your directory name. If any service
is restarting, check its logs:

```
docker compose logs courier-watcher
docker compose logs courier-processor
```

### Verify the Data Monitor

Look for the watcher's startup logs:

```
docker compose logs courier-watcher
```

You should see:

```
[Service: tutorial-02-docker-swarm] Starting Service tutorial-02-docker-swarm
[Manager: PrometheusManager] Starting Prometheus server on port 8001
[Manager: RabbitMQManager] Successfully connected to RabbitMQ
[Plugin: file_system_poller_watchdog] Starting to watch directory: /data/incoming
[Service: tutorial-02-docker-swarm] Service tutorial-02-docker-swarm started successfully
```

Repeat for the processor container — it should show port 8002 and both
plugins starting.

### Browse the UIs

| Service | URL | What to Check |
|---------|-----|---------------|
| RabbitMQ | <http://localhost:15672> | Login `admin`/`admin_test`. Look for the fanout exchange and auto-created queues under the Exchanges and Queues tabs. |
| Prometheus | <http://localhost:9090/targets> | Both Courier targets should show State `UP`. |
| Grafana | <http://localhost:3000> | The Courier dashboard auto-loads. If not visible, browse Dashboards and select "Courier Service". |
| Jaeger | <http://localhost:16686> | Select service `courier` from the dropdown. Traces appear once files are processed. |

### Test File Processing

Create a test file in the shared data directory:

```
touch data/incoming/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc
```

The watcher container detects it, extracts metadata, and publishes it to
RabbitMQ. The processor container's job builder receives it, creates a
job, and the dispatcher executes the echo script.

Check the processor logs:

```
docker compose logs courier-processor
```

You should see:

```
[Plugin: DummyJobBuilder] Received file from file queue
[Plugin: DummyJobBuilder] Job job_... is ready; emitting
[Plugin: serial_bash] Executing job
==========================================
File detected: /data/incoming/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000...
Timestamp: 2026-06-12 ...
==========================================
```

### Verify the Trace

Open Jaeger at <http://localhost:16686>, select the `courier` service,
and click **Find Traces**. You should see a trace containing spans from
both containers:

- `data_monitor.process_file` (from `courier-watcher`)
- `job_builder.build_job` (from `courier-processor`)
- `dispatcher.dispatch_job` (from `courier-processor`)
- `dispatcher.execute_job` (from `courier-processor`)

Expand the trace to see span durations and attributes, including
`courier.correlation_id` and `courier.file.path`.

A typical trace will appear in the Jaeger UI as:

```
┌─────────────────────────────────────────────────────────────────┐
│ courier: data_monitor.process_file         3.45s                 │
├─────────────────────────────────────────────────────────────────┤
│  ├─ data_monitor.add_metadata              0.12s                 │
│  ├─ data_monitor.emit_file                 0.89s                 │
│  ├─ file.found                              event                │
│  └─ file.emitted                            event                │
├─────────────────────────────────────────────────────────────────┤
│ courier: job_builder.build_job             1.23s                 │
├─────────────────────────────────────────────────────────────────┤
│  ├─ job_builder.process_job_group          0.45s                 │
│  ├─ job_builder.emit_job                   0.31s                 │
│  │   └─ job_builder.emit_one               0.28s                 │
│  ├─ job.ready                               event                │
│  └─ job.emitted                             event                │
├─────────────────────────────────────────────────────────────────┤
│ courier: dispatcher.dispatch_job          28.10s                 │
├─────────────────────────────────────────────────────────────────┤
│  ├─ dispatcher.execute_job                27.80s                 │
│  │   └─ job.executed                        event                │
│  └─ dispatcher.emit_execution_log          0.05s                 │
├─────────────────────────────────────────────────────────────────┤
│ Attributes:                                                      │
│  courier.correlation_id: a1b2c3d4-...                          │
│  courier.file.path: /data/incoming/OR_ABI-...                   │
│  plugin.name: file_system_poller_watchdog                       │
│  courier.execution_log.return_code: 0                           │
└─────────────────────────────────────────────────────────────────┘
```

The spans are ordered chronologically and linked by parent-child
relationships. The `data_monitor.*` spans originate from
`courier-watcher`, while `job_builder.*` and `dispatcher.*` spans
originate from `courier-processor` — all part of the same distributed
trace identified by a shared `trace_id`.

### Clean Shutdown

```
docker compose down
```

This stops and removes all containers while preserving your config files
and data directory.

## Common Issues

```{include} ../includes/common-troubleshooting.md
```

**Grafana dashboard not showing:**

- Run `courier dashboard config.yaml` locally to verify the dashboard
  JSON is valid.
- Ensure `pip install data-courier[grafana]` completed successfully.
- Check that `grafana/provisioning/dashboards/courier.json` exists and
  contains valid JSON.

**No traces in Jaeger:**

- Verify `OTEL_EXPORTER_OTLP_ENDPOINT` is set on both Courier containers.
- Check the endpoint is `http://jaeger:4318/v1/traces` (not localhost).
- Jaeger's OTLP receiver must be enabled — the
  `COLLECTOR_OTLP_ENABLED=true` environment variable does this.

**Port conflicts:**

- If port 3000, 9090, 8001, 8002, or 16686 is already in use, change the
  host-side port in `docker-compose.yml` (e.g., `"8003:8001"`). Update
  Prometheus targets to match.

## What You Learned

You've completed all the learning objectives listed at the start of this tutorial. You can now split a Courier config across Docker containers using `--only`, deploy RabbitMQ as a shared broker, assign unique Prometheus ports per container, generate and provision Grafana dashboards, and enable OpenTelemetry distributed tracing across containers.

## Next Steps

- {doc}`../operations/high-availability` — Run multiple processor replicas
  with shared state
- {doc}`../operations/tracing` — Deep dive into span hierarchy and audit
  workflows

## Challenge Exercises

1. **Scale the processor.** Add `deploy: replicas: 2` to the
   `courier-processor` service. Note: this requires deploying with
   `docker stack deploy -c docker-compose.yml courier` (not `docker
   compose up`, which ignores `deploy.replicas`). Observe how both
   replicas consume from the same RabbitMQ queues.
1. **Add a second data monitor.** Add another YAML key under `spec.run`
   (e.g., `watch-backup`) pointing to a different directory. Spin up a
   third Courier container with `--only watch-backup`.
1. **Enable state sync.** Add a Redis service and a `state_sync` block to
   the `create-jobs` builder config. Run two processor replicas and
   verify only one dispatches each job (see
   {doc}`../operations/high-availability`).
1. **Adjust the sampling rate.** Set
   `COURIER_TRACING_SAMPLE_RATE=0.25` on both Courier containers, process
   20 files, and verify roughly 5 complete traces appear in Jaeger.
1. **Split Prometheus per kind.** Use `courier dashboard config.yaml
   --split-by kind -o grafana/provisioning/dashboards/` to generate
   separate dashboards for the data monitor and dispatcher.

## Complete Code

The complete configuration and compose file are available in the tutorial
repository:

[tutorial02-docker-swarm/](<https://github.com/biosafetylvl5/courier/tree/main/examples/tutorials/02-docker-swarm>)

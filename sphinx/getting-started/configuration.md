# Configuration Reference

This guide covers the service configuration file format and all
broker transport options available in Courier.

## Overview

Courier services are configured through YAML (or JSON) files that
describe **what** the service does and **how** it connects to
infrastructure. A single file contains:

1. **Document metadata** -- apiVersion, kind, and a `metadata` block (name, namespace, description)
1. **Service spec** -- heartbeat, broker, and pipeline steps

Run `courier validate <file>` to check a file before starting the
service.

## Minimal Working Example

You don't need an external broker to get started. Omit the `broker`
section entirely and Courier uses the in-memory transport:

```yaml
apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: my-service
  namespace: default
  description: A minimal Courier service.

spec:
  run:
    - watch:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: /data/incoming

    - build:
        kind: job_builder
        name: DummyJobBuilder

    - dispatch:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            echo "Processing {{ files[0].file }}"
```

To connect to a real broker instead, add connection details. When
`host` is present, Courier infers the transport as AMQP:

```yaml
  broker:
    host: localhost
    port: 5672
    username: guest
    password: guest
```

## Document Metadata

The top-level fields identify the configuration document. The
`apiVersion` must follow the `<group>/v<N>[alphaN|betaN]` format
(e.g. `runcourier.dev/v1alpha1`).

```yaml
apiVersion: runcourier.dev/v1alpha1  # Required. CRD-style API version.
kind: Service                      # Required. Must be a non-empty string.
metadata:
  name: my-service                 # Required. DNS subdomain name (lowercase, digits, hyphens).
  namespace: production            # Optional. Groups queues and metrics.
  description: Short summary.      # Required. One-line description.
  docstring: |                     # Optional. Long-form documentation.
    Multi-line explanation of what
    this service does and why.
  labels:                          # Optional. Key/value pairs for selection.
    app.kubernetes.io/part-of: geoips
  annotations: {}                  # Optional. Non-identifying metadata.
```

The `name` and `namespace` fields must be valid DNS subdomain names:
lowercase letters, digits, and hyphens only (max 63 chars, no
leading/trailing hyphens). All required string fields must be
non-empty after trimming whitespace.

## Service Spec

Everything under `spec:` controls runtime behavior.

```yaml
spec:
  heartbeat_interval: 30   # Optional. Seconds between heartbeats. Default: 30.
  broker: { ... }          # Optional. Defaults to in-memory. See "Broker Configuration" below.
  run: [ ... ]             # Required. At least one pipeline step.
```

### `heartbeat_interval`

How often the service publishes health metrics, in seconds. Defaults
to `30` when omitted.

(run-section)=
### `run`

An ordered list of pipeline steps. Each step is a mapping from an
identifier (unique within the file) to a plugin definition:

```yaml
run:
  - my_step_name:            # Unique identifier for this step.
      kind: data_monitor     # Plugin kind: data_monitor, job_builder, or dispatcher.
      name: plugin_name      # Registered plugin name.
      config: { ... }        # Optional. Plugin-specific configuration. Defaults to null when omitted.
```

Step identifiers must be unique. Duplicate identifiers cause a
validation error.

## Broker Configuration

The `broker` section tells the service how to connect to its message
broker. Courier uses [Kombu](https://docs.celeryq.dev/projects/kombu/)
under the hood, so **any Kombu transport** is supported.

There are four configuration styles, selected by the `transport` field.
When `transport` is omitted, the default depends on context:

- If `host` is present, Courier infers AMQP (backward compatibility).
- Otherwise, Courier uses the in-memory transport (no external broker needed).

### AMQP

For RabbitMQ and other AMQP brokers. This is the most common
production setup.

```yaml
broker:
  transport: amqp             # Inferred when `host` is present.
  host: rabbitmq.example.com # Required. Hostname or IP.
  port: 5672                 # Default: 5672.
  username: admin            # Required.
  password: secret           # Required.
  vhost: /                   # Default: "/". AMQP virtual host.
  ssl: false                 # Default: false. Set true for amqps://.
  max_retries: 5             # Default: 5. Connection retry attempts (>= 0).
```

The resulting Kombu URL is:

```
amqp://admin:secret@rabbitmq.example.com:5672/
```

With `ssl: true` and `vhost: production`:

```
amqps://admin:secret@rabbitmq.example.com:5672/production
```

#### AMQP field reference

| Field         | Type    | Required | Default  | Description                      |
| ------------- | ------- | -------- | -------- | -------------------------------- |
| `transport`   | string  | No       | `"amqp"` | Inferred when `host` is present. |
| `host`        | string  | Yes      | --       | Broker hostname or IP address.   |
| `port`        | integer | No       | `5672`   | TCP port (1--65535).             |
| `username`    | string  | Yes      | --       | Authentication username.         |
| `password`    | string  | Yes      | --       | Authentication password.         |
| `vhost`       | string  | No       | `"/"`    | AMQP virtual host.               |
| `ssl`         | boolean | No       | `false`  | Use TLS (`amqps://`).            |
| `max_retries` | integer | No       | `5`      | Max connection retry attempts.   |

### Redis

For Redis-backed message queues.

```yaml
broker:
  transport: redis
  host: redis.example.com   # Default: "localhost".
  port: 6379                # Default: 6379.
  password: secret           # Default: "" (no auth).
  db: 0                     # Default: 0. Redis database index.
  ssl: false                # Default: false. Set true for rediss://.
  max_retries: 5            # Default: 5.
```

The resulting Kombu URL is:

```
redis://:secret@redis.example.com:6379/0
```

When `password` is empty the URL omits the auth segment:

```
redis://redis.example.com:6379/0
```

#### Redis field reference

| Field         | Type    | Required | Default       | Description                    |
| ------------- | ------- | -------- | ------------- | ------------------------------ |
| `transport`   | string  | Yes      | --            | Must be `"redis"`.             |
| `host`        | string  | No       | `"localhost"` | Redis server hostname.         |
| `port`        | integer | No       | `6379`        | TCP port (1--65535).           |
| `password`    | string  | No       | `""`          | AUTH password (empty to skip). |
| `db`          | integer | No       | `0`           | Database index (>= 0).         |
| `ssl`         | boolean | No       | `false`       | Use TLS (`rediss://`).         |
| `max_retries` | integer | No       | `5`           | Max connection retry attempts. |

### In-Memory (default)

A testing transport that passes messages between threads without any
external broker. This is the default when the `broker` section is
omitted entirely, or when `transport` and `host` are both absent.

```yaml
broker:
  transport: memory
  max_retries: 5            # Default: 5.
```

The resulting Kombu URL is:

```
memory://
```

No other fields are accepted.

### URL (generic passthrough)

For **any other Kombu transport** -- Amazon SQS, Kafka, MongoDB,
SQLAlchemy, Filesystem, Azure Service Bus, Google Cloud Pub/Sub,
Consul, etcd, Zookeeper, and more. Provide the full Kombu connection
URL directly.

```yaml
broker:
  transport: url
  url: "sqs://"
  max_retries: 10
```

Courier passes the URL to Kombu unchanged.

#### Examples for common transports

**Amazon SQS:**

```yaml
broker:
  transport: url
  url: "sqs://"
  max_retries: 10
```

SQS credentials are typically provided through environment variables
or IAM roles rather than the URL.

**Confluent Kafka:**

```yaml
broker:
  transport: url
  url: "confluentkafka://localhost:9092"
```

**MongoDB:**

```yaml
broker:
  transport: url
  url: "mongodb://user:password@mongo.example.com:27017/kombu_default"
```

**SQLAlchemy (PostgreSQL):**

```yaml
broker:
  transport: url
  url: "sqla+postgresql://user:password@db.example.com:5432/mydb"
```

**Filesystem (no external service):**

```yaml
broker:
  transport: url
  url: "filesystem://"
```

Filesystem transport requires Kombu `transport_options` to be set
at the application level (`data_folder_in`, `data_folder_out`).

**Azure Service Bus:**

```yaml
broker:
  transport: url
  url: "azureservicebus://policy_name:policy_key@my-namespace"
```

**Google Cloud Pub/Sub:**

```yaml
broker:
  transport: url
  url: "gcpubsub://projects/my-gcp-project"
```

#### URL field reference

| Field         | Type    | Required | Default | Description                    |
| ------------- | ------- | -------- | ------- | ------------------------------ |
| `transport`   | string  | Yes      | --      | Must be `"url"`.               |
| `url`         | string  | Yes      | --      | Full Kombu connection URL.     |
| `max_retries` | integer | No       | `5`     | Max connection retry attempts. |

## Distributed Deployment with `--only`

```{note}
This section covers operational deployment across containers. For the
configuration format reference, see {ref}`run-section` above.
```

The `--only` flag on `courier run` lets you split a single service
configuration across multiple containers. Each container runs a subset
of the pipeline steps defined in `spec.run[]`, sharing the same broker
and configuration file.

This is the foundation of Docker clustering: one `config.yaml` deployed
to several containers, each responsible for a different part of the
pipeline.

### Syntax

```
courier run <config-file> --only <id1,id2,...>
```

The flag accepts a comma-separated list of step identifiers. Courier starts only steps whose YAML keys match one of the listed
identifiers and skips the rest. Order in the list does not matter -- steps run in
their original `spec.run[]` order.

### Example: Two-Container Split

Using the identifiers from the minimal configuration above (`watch`,
`build`, `dispatch`):

```
# Container 1: data monitor only
courier run service.yaml --only watch

# Container 2: job builder + dispatcher
courier run service.yaml --only build,dispatch
```

Both containers read the same `service.yaml` and connect to the same
broker. Container 1 watches for new data and publishes file events.
Container 2 picks up those events, builds jobs, and dispatches them.

### Important Notes

**Identifiers, not plugin names.** The values passed to `--only` are
the YAML keys under `spec.run[]` -- the short, unique names like
`watch` or `dispatch`. Do not use plugin class names such as
`file_system_poller_watchdog` or `serial_bash`.

**Only runnable kinds may appear in `spec.run`.** Those are
`data_monitor`, `job_builder`, and `dispatcher`. Metadata configs are
not pipeline steps -- a data monitor names them in its
`metadata-tools` list. Any other `kind` is rejected at startup rather
than skipped, so a typo cannot produce a service that runs and
processes nothing.

**Avoid duplicate dispatchers.** A dispatcher consumes jobs from a
shared queue. Running the same dispatcher identifier in two containers
creates a split-brain scenario where both compete for the same messages.
Each dispatcher identifier should appear in exactly one `--only` list
across your deployment.

**Empty `--only` runs everything.** Omitting the flag, or passing an
empty value (`--only ""`), starts all steps defined in `spec.run[]`.

## Complete Example

The following shows a production-ready configuration with AMQP and TLS.
For in-memory and Redis broker blocks, see the field reference tables
above.

### Production AMQP with TLS

```yaml
apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: goes18-processor
  namespace: production
  description: Production GOES-18 data processing with TLS.

spec:
  broker:
    transport: amqp
    host: rabbitmq.prod.internal
    port: 5671
    username: svc_goes18
    password: "${RABBITMQ_PASSWORD}"
    vhost: /geoips
    ssl: true
    max_retries: 10

  run:
    - watch:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: /data/goes18/incoming

    - build:
        kind: job_builder
        name: DummyJobBuilder

    - dispatch:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            run_geoips.sh {{ files[0].file }}
```

## Validation

Always validate configuration files before deploying:

```bash
courier validate my_service.yaml
```

Common validation errors:

| Error                                        | Cause                          | Fix                                     |
| -------------------------------------------- | ------------------------------ | --------------------------------------- |
| `Field 'host' must be a non-empty string`    | Missing or blank `host`        | Add `host: your-broker-hostname`        |
| `Unable to extract tag using discriminator`  | Invalid `transport` value      | Use `amqp`, `redis`, `memory`, or `url` |
| `Extra inputs are not permitted`             | Unknown field in broker config | Remove the unrecognized field           |
| `Duplicate run step identifiers`             | Two steps share the same name  | Rename one of the step identifiers      |
| `Input should be greater than or equal to 0` | Negative `max_retries` or `db` | Use a non-negative integer              |

## Configuration Precedence

At startup, Courier resolves configuration from multiple
sources in ascending priority:

```
defaults  <  YAML file  <  environment variables  <  CLI flags
```

For example, you can override `max_retries: 5` in the YAML by setting the `BROKER_MAX_RETRIES` environment variable.

## Jinja2 Template Context

The ``serial_bash`` and ``parallel_bash`` dispatchers use
[Jinja2](https://jinja.palletsprojects.com/) for script templates,
replacing the legacy ``{file}`` placeholder. Every template has access
to ``files`` (list of file dicts), ``job`` (metadata), and ``config``
(convenience alias for ``job.config``). All standard Jinja2 features --
variables, filters, conditionals, and loops -- are available. Courier catches syntax
errors at config load time, and undefined variables render
as empty strings (``DebugUndefined``) rather than raising errors.

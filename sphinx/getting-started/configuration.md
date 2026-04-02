# Configuration Reference

This guide covers the service configuration file format and all
broker transport options available in Lazy Lemon.

## Overview

Lazy Lemon services are configured through YAML (or JSON) files that
describe **what** the service does and **how** it connects to
infrastructure. A single file contains:

1. **Document metadata** -- name, version, description
1. **Service spec** -- namespace, heartbeat, broker, and pipeline steps

Run `lazylemon validate <file>` to check a file before starting the
service.

## Minimal Working Example

No external broker is required to get started. Omit the `broker`
section entirely and Lazy Lemon uses the in-memory transport:

```yaml
apiVersion: lazylemon/driver/v1
kind: Service
name: my-service
description: A minimal Lazy Lemon service.

spec:
  namespace: default

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
            echo "Processing {file}"
```

To connect to a real broker instead, add connection details. When
`host` is present the transport is inferred as AMQP:

```yaml
  broker:
    host: localhost
    port: 5672
    username: guest
    password: guest
```

## Document Metadata

The top-level fields identify the configuration document.

```yaml
apiVersion: lazylemon/driver/v1   # Required. API version string.
kind: Service                      # Required. Must be a non-empty string.
name: my-service                   # Required. Unique service name.
description: Short summary.        # Required. One-line description.
docstring: |                       # Optional. Long-form documentation.
  Multi-line explanation of what
  this service does and why.
```

All required string fields must be non-empty after whitespace is
trimmed.

## Service Spec

Everything under `spec:` controls runtime behavior.

```yaml
spec:
  namespace: production    # Required. Groups queues and metrics.
  heartbeat_interval: 30   # Optional. Seconds between heartbeats. Default: 30.
  broker: { ... }          # Optional. Defaults to in-memory. See "Broker Configuration" below.
  run: [ ... ]             # Required. At least one pipeline step.
```

### `namespace`

A string that prefixes all queue names on the broker. Use it to
isolate multiple services sharing the same broker (e.g. `staging`
vs `production`).

### `heartbeat_interval`

How often the service publishes health metrics, in seconds. Defaults
to `30` when omitted.

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
broker. Lazy Lemon uses [Kombu](https://docs.celeryq.dev/projects/kombu/)
under the hood, so **any Kombu transport** is supported.

There are four configuration styles, selected by the `transport` field.
When `transport` is omitted, the default depends on context:

- If `host` is present, AMQP is inferred (backward compatibility).
- Otherwise, the in-memory transport is used (no external broker needed).

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

The URL is passed to Kombu unchanged.

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

## Complete Examples

### In-memory for local development

The simplest configuration — no external services required:

```yaml
apiVersion: lazylemon/driver/v1
kind: Service
name: test-pipeline
description: In-memory broker for local dev and tests.

spec:
  namespace: test

  run:
    - watch:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: /tmp/test-data

    - build:
        kind: job_builder
        name: DummyJobBuilder

    - dispatch:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            echo "Test: {file}"
```

### Redis for simple deployments

```yaml
apiVersion: lazylemon/driver/v1
kind: Service
name: himawari-watcher
description: Himawari-9 watcher backed by Redis.

spec:
  namespace: himawari

  broker:
    transport: redis
    host: redis.local
    port: 6379
    password: redis_secret
    db: 1
    ssl: false

  run:
    - watch:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: /data/himawari9/incoming

    - build:
        kind: job_builder
        name: DummyJobBuilder

    - dispatch:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            echo "Received {file}"
```

### Production AMQP with TLS

```yaml
apiVersion: lazylemon/driver/v1
kind: Service
name: goes18-processor
description: Production GOES-18 data processing with TLS.

spec:
  namespace: production

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
            run_geoips.sh {file}
```

## Validation

Always validate configuration files before deploying:

```bash
lazylemon validate my_service.yaml
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

When the service starts, configuration is resolved from multiple
sources in ascending priority:

```
defaults  <  YAML file  <  environment variables  <  CLI flags
```

For example, `max_retries: 5` in the YAML can be overridden by
setting the `BROKER_MAX_RETRIES` environment variable.

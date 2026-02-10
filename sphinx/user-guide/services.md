# Services

This guide covers service lifecycle, configuration, management, and
operations.

## What is a Service?

A **Service** is a running instance of GeoIPS Driver with a configured
plugin workload. Each service:

-   Has a unique namespace
-   Has an associated namespace
-   Runs one or more plugin pipelines
-   Connects to a message broker for message passing
-   Exposes Prometheus metrics
-   Can run independently, alongside other services or as part of a networked cluster

## Service Lifecycle

Services progress through these states:

    INIT → STARTING → RUNNING → STOPPING → STOPPED
            │            │          │
            ▼            ▼          ▼
         FAILED ←─── FAILED ←─── FAILED

**State transitions:**

1.  **INIT**: Service configuration loaded and validated
2.  **STARTING**: Managers and plugins initializing
3.  **RUNNING**: Normal operation, processing data
4.  **STOPPING**: Graceful shutdown in progress
5.  **STOPPED**: Service has exited cleanly
6.  **FAILED**: Irrecoverable error occurred

## Service Configuration

### Basic Structure

Every service is defined by a YAML configuration file. For example:

    apiVersion: geoips_driver/v1
    kind: Service
    name: my-service
    description: Service description.

    spec:
      service_namespace: production
      heartbeat_interval: 30
      rabbitmq:
        host: localhost
        port: 5672
        username: user
        password: pass
      run:
        - monitor:
            kind: data_monitor
            name: plugin_name
            config: {}

### Required Fields

-   `apiVersion`: API version (currently `geoips_driver/v1`)
-   `kind`: Must be `Service` (other `kind`s are implemented in GeoIPS, GeoIPS-RT *only* implements `Service`s)
-   `name`: Unique service identifier set by the user
-   `description`: Human-readable description
-   `spec.service_namespace`: Namespace for isolation or bundling of related services
-   `spec.rabbitmq`: RabbitMQ connection details
-   `spec.run`: List of plugins to run

### Optional Fields

-   `docstring`: Multi-line documentation
-   `spec.heartbeat_interval`: Health check interval (seconds)

## Starting a Service

Services can be started via the CLI or from within Python.

### Command Line

    Start service
    =============
    geoips-driver run config.yaml

    With custom log level
    =====================
    geoips-driver run config.yaml --log-level DEBUG

    With environment variables
    ==========================
    RABBITMQ_PASSWORD=secret geoips-driver run config.yaml

### Python API

    from pathlib import Path
    from geoips_driver.service import Service

    Create service
    ==============
    service = Service(Path("config.yaml"))

    Start service (blocking)
    ========================
    service.start()

    Or run in background
    ====================
    import threading
    thread = threading.Thread(target=service.start)
    thread.start()

<!--### Docker

    docker run -d \
      --name geoips-driver \
      -v $(pwd)/config.yaml:/config/config.yaml \
      -v $(pwd)/data:/data \
      -e RABBITMQ_PASSWORD=secret \
      ghcr.io/your-org/geoips-driver:latest \
      geoips-driver run /config/config.yaml


### Kubernetes

    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: geoips-driver
    spec:
      replicas: 2
      template:
        spec:
          containers:
            - name: geoips-driver
              image: ghcr.io/your-org/geoips-driver:latest
              command:
                - geoips-driver
                - run
                - /config/service-config.yaml
              volumeMounts:
                - name: config
                  mountPath: /config
          volumes:
            - name: config
              configMap:
                name: geoips-driver-config
-->
## Stopping a Service

### Graceful Shutdown

Send SIGINT (Ctrl+C) or SIGTERM:

    Ctrl+C in foreground
    ====================
    ^C

    Kill process with SIGTERM
    =========================
    kill <pid>

<!--
    Docker
    ======
    docker stop geoips-driver

    Kubernetes
    ==========
    kubectl delete pod <pod-name>
-->

**Shutdown sequence:**

1.  Service receives signal
2.  Stops accepting new files
3.  Finishes processing active jobs
4.  Closes RabbitMQ connections
5.  Stops Prometheus server
6.  Exits cleanly

### Forceful Shutdown

Send SIGKILL (not recommended):

    kill -9 <pid>

This does not allow graceful cleanup and may leave:

-   Unacknowledged messages in queues
-   Incomplete jobs
-   Open file handles

## Service Health Monitoring

### Health Checks

Services expose health status via metrics:

    Check service health
    ====================
    curl http://localhost:8000/metrics | grep service_health

    service_health 1.0 = healthy
    ============================
    service_health 0.0 = unhealthy
    ==============================

**Health criteria:**

-   All managers operational
-   At least one plugin running
-   RabbitMQ connection healthy
-   No critical level errors

<!--### Readiness Checks

For Kubernetes readiness probes:

    readinessProbe:
      httpGet:
        path: /metrics
        port: 8000
      initialDelaySeconds: 10
      periodSeconds: 5

### Liveness Checks

For Kubernetes liveness probes:

    livenessProbe:
      httpGet:
        path: /metrics
        port: 8000
      initialDelaySeconds: 30
      periodSeconds: 10
      failureThreshold: 3
-->

### Heartbeat

Services send periodic heartbeats:

    Heartbeat updates this metric
    =============================
    service_last_heartbeat_timestamp

    Alert if no heartbeat for 2 minutes
    ===================================
    time() - service_last_heartbeat_timestamp > 120

## Service Namespaces

### Purpose

Namespaces provide isolation between services:

-   Separate queue names
-   Independent processing
-   Environment isolation (dev, test, prod)
-   Multi-tenant support

### Namespace Naming

**Recommended patterns:**

    By environment
    ==============
    service_namespace: production
    service_namespace: staging
    service_namespace: development

    By platform
    ===========
    service_namespace: goes18_processing
    service_namespace: himawari9_processing

    By purpose
    ==========
    service_namespace: realtime_conus
    service_namespace: archive_reprocessing

### Queue Names

Queues are automatically prefixed with namespace:

    {namespace}-FilesFoundQueue
    {namespace}-JobReadyQueue

    For example: 
    production-FilesFoundQueue
    development-FilesFoundQueue

### Multiple Services

Run multiple services with different namespaces:

    service1.yaml
    =============
    spec:
      service_namespace: goes18_east

    service2.yaml
    =============
    spec:
      service_namespace: goes18_west

Both can run simultaneously without conflict.

## Environment Variables

Configuration can reference environment variables:

    rabbitmq:
      host: ${RABBITMQ_HOST:-localhost}
      port: ${RABBITMQ_PORT:-5672}
      username: ${RABBITMQ_USER}
      password: ${RABBITMQ_PASSWORD}

**Syntax:**

-   `${VAR}` - Required variable
-   `${VAR:-default}` - Optional with default

**Setting variables:**

    Shell
    =====
    export RABBITMQ_PASSWORD=secret
    geoips-driver run config.yaml

    Docker
    ======
    docker run -e RABBITMQ_PASSWORD=secret ...

    Kubernetes
    ==========
    env:
      - name: RABBITMQ_PASSWORD
        valueFrom:
          secretKeyRef:
            name: rabbitmq-secret
            key: password

## Logging

### Log Levels

    TRACE = 5     # Very detailed
    DEBUG = 10    # Debug information
    INFO = 20     # Normal operations
    WARNING = 30  # Warning messages
    ERROR = 40    # Error messages
    CRITICAL = 50 # Critical failures

Set log level:

    geoips-driver run config.yaml --log-level DEBUG

### Log Format

Structured logging with context:

    [2024-01-15 12:00:00] [INFO] [Service: goes18-processor] Starting service
    [2024-01-15 12:00:01] [INFO] [Plugin: file_system_poller_watchdog] Watching /data
    [2024-01-15 12:00:05] [INFO] [Plugin: file_system_poller_watchdog] Found file: test.nc

### Log Destinations

**Console (stdout):**

Default for all services.

**File:**

    import logging

    logging.basicConfig(
        filename='/var/log/geoips-driver/service.log',
        level=logging.INFO
    )

**Loki (centralized):**

    Enable Loki integration
    =======================
    export LOKI_URL=http://loki:3100/loki/api/v1/push
    export LOKI_ENABLED=true

## Metrics

### Prometheus Endpoint

Metrics exposed on `http://localhost:8000/metrics`

**Service metrics:**

    service_health
    service_uptime_seconds
    service_restarts_total

**Plugin metrics:**

    files_processed_total{plugin,status}
    jobs_built_total{plugin,status}
    jobs_processed_total{plugin,status}

**Performance metrics:**

    dispatcher_job_execution_duration_seconds

See :doc:`../reference/metrics-reference` for complete list.

### Custom Metrics

Add custom metrics in plugins:

    from prometheus_client import Counter

    class MyPlugin(BasePlugin):
        def __init__(self):
            self.custom_counter = Counter(
                'my_custom_metric',
                'Description',
                ['label1', 'label2']
            )

        def process(self):
            self.custom_counter.labels(
                label1='value1',
                label2='value2'
            ).inc()

## Service Management

### Restart Strategies

**Automatic restart (systemd):**

    [Unit]
    Description=GeoIPS Driver Service
    After=network.target rabbitmq.service

    [Service]
    Type=simple
    User=geoips
    WorkingDirectory=/opt/geoips-driver
    ExecStart=/usr/local/bin/geoips-driver run /etc/geoips-driver/config.yaml
    Restart=always
    RestartSec=10

    [Install]
    WantedBy=multi-user.target

**Automatic restart (Kubernetes):**

    spec:
      template:
        spec:
          restartPolicy: Always

### Configuration Updates

**Without restart:**

Not currently supported - configuration is loaded once at startup.

**With restart:**

1.  Update configuration file
2.  Validate configuration
3.  Restart service

<!-- -->

    Validate
    ========
    geoips-driver validate config.yaml

    Restart
    =======
    systemctl restart geoips-driver

    Or Kubernetes
    =============
    kubectl rollout restart deployment/geoips-driver

### Rolling Updates

Zero-downtime updates in Kubernetes:

    spec:
      strategy:
        type: RollingUpdate
        rollingUpdate:
          maxSurge: 1
          maxUnavailable: 0

## Troubleshooting

### Service Won't Start

**Check configuration:**

    geoips-driver validate config.yaml

**Check logs:**

    View startup logs
    =================
    journalctl -u geoips-driver -n 50

    Or direct output
    ================
    geoips-driver run config.yaml

**Common issues:**

-   RabbitMQ connection failed
-   Invalid plugin name
-   Missing environment variables
-   Port already in use (8000)

### Service Crashes

**Check logs before crash:**

    journalctl -u geoips-driver --since "10 minutes ago"

**Check metrics:**

    curl http://localhost:8000/metrics | grep error

**Common causes:**

-   Out of memory
-   Disk full
-   Plugin exception
-   RabbitMQ disconnection

### No Files Being Processed

**Check data monitor:**

    curl http://localhost:8000/metrics | grep files_processed

**Check file permissions:**

    ls -la /data/incoming

**Check metadata matching:**

-   Filenames must match metadata config patterns
-   Check logs for "No matching config" messages

## Best Practices

1.  **Use descriptive service names**

    `goes18-fulldisk-processor` not `service1`

2.  **Set appropriate heartbeat intervals**

    15-30 seconds for production

3.  **Use namespaces for isolation**

    Separate dev, test, prod

4.  **Configure resource limits**

    Especially in Kubernetes

5.  **Monitor service health**

    Set up alerts in Prometheus

6.  **Log at appropriate levels**

    INFO for production, DEBUG for troubleshooting

7.  **Test configuration before deployment**

    Use `geoips-driver validate`

8.  **Document service purpose**

    Use `description` and `docstring` fields

## Next Steps

-   `` `plugins ``\` - Plugin configuration and usage
-   `` `monitoring ``\` - Comprehensive monitoring setup
-   `` `troubleshooting ``\` - Troubleshooting guide
-   `` `deployment ``\` - Production deployment patterns

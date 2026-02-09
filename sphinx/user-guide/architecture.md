# Architecture Overview

This guide provides a comprehensive overview of GeoIPS Driver's
architecture, explaining how components interact to enable near
real-time satellite data processing.

## High-Level Architecture

GeoIPS Driver follows a **plugin-based pipeline architecture** with
message passing between components:

    ┌──────────────────────────────────────────────────────────────┐
    │                     GeoIPS Driver Service                    │
    │                                                              │
    │  ┌────────────┐    ┌────────────┐    ┌─────────────┐       │
    │  │   Data     │───▶│    Job     │───▶│ Dispatcher  │       │
    │  │  Monitor   │    │  Builder   │    │             │       │
    │  └────────────┘    └────────────┘    └─────────────┘       │
    │        │                  │                   │             │
    │        └──────────────────┴───────────────────┘             │
    │                    RabbitMQ Queues                          │
    │                                                              │
    │  ┌───────────────────────────────────────────────────────┐  │
    │  │              Management Layer                         │  │
    │  │                                                       │  │
    │  │  Plugin Manager  │  RabbitMQ Manager  │  Prometheus  │  │
    │  └───────────────────────────────────────────────────────┘  │
    └──────────────────────────────────────────────────────────────┘
               │                    │                    │
               ▼                    ▼                    ▼
        File System          Message Broker        Metrics Store

## Core Components

### Service

The **Service** is the top-level component that:

-   Manages the lifecycle of all plugins
-   Coordinates message passing via RabbitMQ
-   Provides health checking and monitoring
-   Handles graceful startup and shutdown

**Key responsibilities:**

-   Initialize and start all plugins
-   Monitor plugin health and restart on failure
-   Expose Prometheus metrics endpoint
-   Handle signals (SIGTERM, SIGINT) for graceful shutdown

### Data Monitors

**Data Monitors** watch for new satellite data files and emit them to
the pipeline.

**Interface:**

    class DataMonitorBasePlugin:
        def find_file(self) -> Iterator[File]:
            """Continuously yield newly discovered files."""

**Built-in implementations:**

-   `file_system_poller_watchdog` - Uses Python watchdog for filesystem
    events

**Output:** Emits `File` objects to `FilesFoundQueue`

### Job Builders

**Job Builders** group related files into processing jobs.

**Interface:**

    class JobBuilder:
        def __init__(self, service, config):
            self.job_groups = [...]  # List of JobGroup instances

    class JobGroup:
        def file_is_relevant(self, file: File) -> bool:
            """Check if file should be processed."""

        def get_job_ids_from_file(self, file: File) -> list[str]:
            """Generate job IDs for grouping."""

        def check_ready(self) -> list[Job]:
            """Return jobs that are ready to process."""

**Built-in implementations:**

-   `DummyJobBuilder` - One file per job (testing only)

**Input:** Consumes `File` objects from `FilesFoundQueue`

**Output:** Emits `Job` objects to `JobReadyQueue`

### Dispatchers

**Dispatchers** execute processing jobs (GeoIPS workflows, bash scripts,
etc.).

**Interface:**

    class Dispatcher:
        def get_execution_log(self, job: Job) -> list[ExecutionLog]:
            """Execute job and return execution logs."""

**Built-in implementations:**

-   `serial_bash` - Execute bash scripts serially

**Input:** Consumes `Job` objects from `JobReadyQueue`

**Output:** Emits `ExecutionLog` objects with results

## Message Flow

**Complete data flow:**

1.  **File Discovery**
    -   Data Monitor discovers new file
    -   Extracts metadata (platform, sensor, timestamp, sector)
    -   Creates `File` object
    -   Publishes to `FilesFoundQueue`
2.  **Job Building**
    -   Job Builder consumes from `FilesFoundQueue`
    -   Determines job ID from file metadata
    -   Adds file to appropriate job group
    -   Checks if job is ready (all files received or timeout)
    -   Publishes ready `Job` to `JobReadyQueue`
3.  **Job Execution**
    -   Dispatcher consumes from `JobReadyQueue`
    -   Executes processing (calls GeoIPS, runs script, etc.)
    -   Captures stdout, stderr, return code
    -   Creates `ExecutionLog` with results
    -   Publishes log (optional, for audit trail)
4.  **Metrics & Monitoring**
    -   Each step updates Prometheus metrics
    -   Health checks run periodically
    -   Logs sent to stdout or Loki

**Example with GOES-18 Full-Disk:**

    Files arrive (16 channels):
    Channel 1 → File → FilesFoundQueue
    Channel 2 → File → FilesFoundQueue
    ...
    Channel 16 → File → FilesFoundQueue
                 ↓
    Job Builder groups by scan time:
    All 16 → Job(id="goes18_fulldisk_20240115120000")
                 ↓
    Job → JobReadyQueue
                 ↓
    Dispatcher executes:
    geoips run single_source [16 files] ...
                 ↓
    ExecutionLog (success/failure)

## Plugin System

**Plugin types:**

1.  **Module-based plugins** (Python code)
    -   Data Monitors
    -   Job Builders
    -   Dispatchers
    -   Defined in Python modules with `interface` and `name` attributes
2.  **YAML-based plugins** (Configuration)
    -   Data Monitor Configs (metadata extraction patterns)
    -   Service Configs (service definitions)
    -   Defined in YAML files

**Plugin discovery:**

    Discover all data monitors
    ==========================
    from geoips_driver.interfaces import data_monitors

    List available plugins
    ======================
    print(data_monitors.plugins)

    Get specific plugin
    ===================
    plugin = data_monitors.get_plugin('file_system_poller_watchdog')

    Instantiate
    ===========
    instance = plugin(service, config)

**Plugin lifecycle:**

    STOPPED → STARTING → RUNNING → STOPPING → STOPPED
                 ↓          ↓           ↓
               FAILED ←────┘           ↓
                 ↑                     ↓
                 └─────────────────────┘
                    (restart on failure)

## Management Layer

### Plugin Manager

Manages plugin lifecycle:

-   Starts/stops plugins in correct order
-   Monitors plugin health
-   Restarts failed plugins (up to max attempts)
-   Tracks plugin state

### RabbitMQ Manager

Manages message broker connections:

-   Establishes connection to RabbitMQ
-   Declares queues and exchanges
-   Handles connection failures and reconnection
-   Provides publish/consume interfaces

### Prometheus Manager

Manages metrics collection:

-   Starts Prometheus HTTP server
-   Registers custom metrics
-   Updates service-level metrics (uptime, health)

## Data Types

### File

Represents a satellite data file with metadata:

    @dataclass
    class File:
        file: Path                    # File path
        hostname: str                 # Where file is located
        platform: str | None          # e.g., "goes18"
        sensor: str | None            # e.g., "abi"
        level: str | None             # e.g., "L1B"
        sector: str | None            # e.g., "Full-Disk"
        num_expected: int | None      # Expected files per scan
        timestamp: datetime | None    # Observation time

### Job

Represents a processing job with one or more files:

    @dataclass
    class Job:
        name: str                # Job type name
        identifier: str          # Unique job ID
        config: dict            # Job configuration
        files: set[FrozenFile]  # Files in this job
        last_modified: float    # Last update time
        timeout: int            # Timeout in seconds

        def ready(self) -> bool:
            """Check if job is ready to process."""

        def is_timeout(self) -> bool:
            """Check if job has timed out."""

### ExecutionLog

Records job execution results:

    @dataclass
    class ExecutionLog:
        return_code: int    # Exit code
        stdout: str         # Standard output
        stderr: str         # Standard error
        hostname: str       # Execution host

## Configuration

**Service configuration structure:**

    apiVersion: geoips_driver/v1
    kind: Service
    name: service-name
    description: Human-readable description.

    spec:
      service_namespace: namespace      # For queue isolation
      heartbeat_interval: 30            # Seconds

      rabbitmq:
        host: localhost
        port: 5672
        username: user
        password: password

      run:                              # Plugin pipeline
        - step1:
            kind: data_monitor          # Plugin type
            name: plugin_name           # Plugin implementation
            config:                     # Plugin-specific config
              key: value

**Namespacing:**

Each service operates in a namespace, which isolates its queues:

    Namespace: production
    - production-FilesFoundQueue
    - production-JobReadyQueue

    Namespace: testing
    - testing-FilesFoundQueue
    - testing-JobReadyQueue

Multiple services can run concurrently with different namespaces.

## Concurrency Model

**Threading:**

-   Each plugin runs in its own thread
-   Data monitors run continuously in background thread
-   Job builders check for ready jobs periodically
-   Dispatchers process jobs serially or in parallel

**Process model:**

-   Single service = single Python process
-   Multiple plugins = multiple threads within process
-   Scale horizontally by running multiple service instances

**Queue-based coordination:**

-   Plugins communicate via RabbitMQ queues
-   Decouples producers from consumers
-   Enables distributed processing (multiple services consuming same
    queue)

## Scalability Patterns

### Horizontal Scaling

Run multiple service instances consuming from the same queues:

    ┌─────────────┐      ┌─────────────────┐
    │ Service 1   │─────▶│  FilesFoundQueue│
    └─────────────┘      └─────────────────┘
                                │
    ┌─────────────┐            ├───────────┐
    │ Service 2   │────────────┘           │
    └─────────────┘                        ▼
                               ┌─────────────────┐
    ┌─────────────┐            │  JobReadyQueue  │
    │ Service 3   │────────────┘                 │
    └─────────────┘                              ▼
                                         [Dispatchers]

**Benefits:**

-   Increased throughput
-   Fault tolerance (if one service fails, others continue)
-   Load distribution

### Vertical Scaling

Optimize single service performance:

-   Increase plugin parallelism
-   Use faster storage (NVMe SSD)
-   Optimize metadata extraction
-   Reduce logging overhead

### Partitioning

Separate services by function:

-   Service A: Monitor GOES-18 Full-Disk
-   Service B: Monitor GOES-18 CONUS
-   Service C: Monitor GOES-16

Each service processes a specific data stream.

## Error Handling

**Plugin restart policy:**

-   On plugin failure: Log error, mark as FAILED
-   Wait `plugin_restart_delay` seconds
-   Attempt restart (up to `plugin_max_restarts` times)
-   If all retries exhausted: Plugin remains FAILED, service continues

**Message acknowledgment:**

-   Messages acknowledged after successful processing
-   On failure: Message requeued for retry
-   Dead letter queues for persistent failures

**Graceful degradation:**

-   If data monitor fails: Stop finding new files, existing pipeline
    continues
-   If job builder fails: Files queue up, processing continues when
    recovered
-   If dispatcher fails: Jobs queue up, retry on recovery

## Monitoring & Observability

**Prometheus metrics:**

-   Service-level: health, uptime
-   Plugin-level: files processed, jobs built, jobs executed
-   Queue-level: depth, throughput
-   System-level: memory, CPU (via node\_exporter)

**Logging:**

-   Structured logging with context (service, plugin, job ID)
-   Configurable log levels (TRACE, DEBUG, INFO, WARN, ERROR)
-   Optional Loki integration for centralized logging

**Health checks:**

-   Service health endpoint: `/health`
-   Metrics endpoint: `/metrics`
-   Kubernetes readiness/liveness probes

## Deployment Patterns

### Standalone

Single service on single host:

    [Host]
    ├── GeoIPS Driver Service
    ├── RabbitMQ
    └── Prometheus

**Use case:** Development, testing, low-volume processing

### Docker

Containerized deployment:

    docker-compose up

**Use case:** Isolated environments, easy deployment

### Kubernetes

Cloud-native deployment with scaling:

    [Kubernetes Cluster]
    ├── GeoIPS Driver Deployment (2-10 replicas)
    ├── RabbitMQ StatefulSet
    ├── Prometheus Operator
    └── Persistent Volumes

**Use case:** Production, high availability, auto-scaling

## Design Principles

1.  **Separation of Concerns**
    -   Data discovery ≠ Job building ≠ Execution
    -   Each plugin has single responsibility
2.  **Loose Coupling**
    -   Plugins communicate via queues, not direct calls
    -   Easy to swap implementations
3.  **Extensibility**
    -   Plugin architecture enables custom behavior
    -   No core code changes needed
4.  **Fault Tolerance**
    -   Plugin failures don't crash service
    -   Automatic restarts
    -   Message persistence
5.  **Observability**
    -   Comprehensive metrics at every layer
    -   Structured logging
    -   Health checks
6.  **Declarative Configuration**
    -   Services defined in YAML
    -   Version controlled
    -   Reproducible

## Performance Characteristics

**Throughput:**

-   Single service: ~100-1000 files/minute (depends on processing)
-   Horizontal scaling: Linear increase with services
-   Bottleneck typically: GeoIPS processing time, not driver overhead

**Latency:**

-   File discovery: &lt;1 second (watchdog events)
-   Metadata extraction: ~10ms per file
-   Job building: ~100ms (depends on group size)
-   Queue latency: &lt;100ms (local RabbitMQ)

**Resource usage:**

-   Memory: ~200MB base + plugin overhead
-   CPU: Minimal when idle, spikes during processing
-   Disk I/O: Depends on data volume

## See Also

-   `` `services ``\` - Service management guide
-   `` `plugins ``\` - Plugin development guide
-   `` `configuration ``\` - Configuration reference
-   :doc:`../developer-guide/architecture-deep-dive` - Detailed
    architecture

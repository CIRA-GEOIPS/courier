# Core Concepts

This guide explains the fundamental concepts behind GeoIPS Driver's
architecture and how components work together.

## The Big Picture

GeoIPS Driver orchestrates near real-time satellite data processing
through a :plugin-based pipeline architecture: .. code-block:: text

> ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ │ Data
> Monitor │─────▶│ Job Builder │─────▶│ Dispatcher │ │ │ │ │ │ │ │ •
> Watch files │ │ • Group files │ │ • Execute │ │ • Extract meta │ │ •
> Build specs │ │ • GeoIPS calls │ │ • Emit to queue │ │ • Emit when │ │
> • Bash scripts │ │ │ │ ready │ │ • Custom code │ └─────────────────┘
> └─────────────────┘ └─────────────────┘ │ │ │
> └────────────────────────┴────────────────────────┘ RabbitMQ Message
> Queues

Each stage is a **plugin** that can be customized or replaced.

## Services

A **Service** is a running instance of GeoIPS Driver with a configured
plugin pipeline.

**Key characteristics:**

-   Defined by a YAML configuration file
-   Runs as a long-lived process
-   Manages plugin lifecycle (start, stop, restart)
-   Provides monitoring and health checks
-   Isolated by namespace

**Example:**

    apiVersion: geoips_driver/v1
    kind: Service
    name: goes18-processor
    spec:
      service_namespace: production
      run:
        - monitor: { kind: data_monitor, ... }
        - build: { kind: job_builder, ... }
        - dispatch: { kind: dispatcher, ... }

**Multiple services can run concurrently**, each in its own namespace.

## Plugins

**Plugins** are the building blocks of a service. Each plugin implements
a specific function in the processing pipeline.

### Plugin Types

There are three main plugin types:

1.  **Data Monitors** - Watch for new data and extract metadata
2.  **Job Builders** - Group files into processing jobs
3.  **Dispatchers** - Execute jobs (GeoIPS workflows, scripts, etc.)

Each plugin type has a specific role and interface.

### Plugin Lifecycle

Plugins follow this lifecycle:

    STOPPED → STARTING → RUNNING ⟲ (health checks)
                 ↓           ↓
               FAILED ← STOPPING → STOPPED

-   **STARTING**: Plugin initialization
-   **RUNNING**: Normal operation with periodic health checks
-   **FAILED**: Plugin encountered an error
-   **STOPPING**: Graceful shutdown in progress
-   **STOPPED**: Plugin not running

If a plugin fails, GeoIPS Driver automatically attempts to restart it
(up to a configured maximum).

## Data Monitors

**Data Monitors** watch for new satellite data files.

**Responsibilities:**

1.  Monitor a data source (filesystem, S3, etc.)
2.  Detect new files
3.  Extract metadata (platform, sensor, time, sector)
4.  Emit File objects to the message queue

### Built-in Monitors

-   `file_system_poller_watchdog` - Watch local/mounted filesystems

**Example configuration:**

    - monitor:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: /data/goes18
          metadata-tools:
            - goes18_abi

**What it does:**

1.  Watches `/data/goes18` for new files
2.  When a file appears, matches it against `goes18_abi` patterns
3.  Extracts: platform=goes18, sensor=abi, sector, timestamp
4.  Creates a File object with this metadata
5.  Emits File to `FilesFoundQueue`

### Metadata Extraction

Metadata is extracted using **metadata configuration plugins**:

    goes18_abi metadata config
    ==========================
    file-metadata:
      goes18_abi_l1b:
        platform: goes18
        sensor: abi
        level: L1B
        date: '.*s(?P<YYYY>\d{4})(?P<JJJ>\d{3})(?P<HH>\d{2})(?P<NN>\d{2}).*'
        match:
          - '.*M6C[01][1-6].*s\d{4}\d{3}\d{2}\d{2}.*'

      full-disk:
        sector: Full-Disk
        num_expected: 16
        match:
          - '.*RadF.*'

**Matching process:**

1.  Filename tested against each `match` pattern (regex)
2.  If match found, metadata fields applied to File
3.  Date components extracted from `date` regex
4.  Multiple configs can layer metadata

See :doc:`../user-guide/metadata-matching` for details.

## Job Builders

**Job Builders** group related files into processing jobs.

**Responsibilities:**

1.  Consume File objects from `FilesFoundQueue`
2.  Determine if file is relevant
3.  Group files by some criteria (time, sector, etc.)
4.  Decide when a job is "ready" to process
5.  Emit Job objects to `JobReadyQueue`

### Built-in Builders

-   `DummyJobBuilder` - Creates one job per file (simple testing)

**Example:**

    - build:
        kind: job_builder
        name: DummyJobBuilder
        config: null

**What it does:**

1.  Receives File from queue
2.  Creates a new Job with that single file
3.  Immediately marks Job as ready (no grouping)
4.  Emits Job to `JobReadyQueue`

### Job Grouping Logic

A more sophisticated job builder might group files by scan time:

    Files arrive:
    - OR_ABI-L1b-RadF-M6C01_G18_s20240151200000...nc  (12:00, Chan 1)
    - OR_ABI-L1b-RadF-M6C02_G18_s20240151200000...nc  (12:00, Chan 2)
    ...
    - OR_ABI-L1b-RadF-M6C16_G18_s20240151200000...nc  (12:00, Chan 16)

    Job Builder creates:
    Job {
      id: "goes18_fulldisk_20240151200000",
      files: [Chan01, Chan02, ..., Chan16],
      ready: true  (all 16 channels received)
    }

This ensures GeoIPS has all required channels for processing.

## Dispatchers

**Dispatchers** execute processing jobs.

**Responsibilities:**

1.  Consume Job objects from `JobReadyQueue`
2.  Execute the job (call GeoIPS, run script, etc.)
3.  Handle errors and retries
4.  Emit ExecutionLog objects with results

### Built-in Dispatchers

-   `serial_bash` - Execute bash scripts serially

**Example:**

    - dispatch:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            geoips run single_source {file} \
              --reader_name abi_netcdf \
              --product_name Infrared-Gray \
              --output_formatter clean_fname \
              --filename_formatter geoips_fname

**What it does:**

1.  Receives Job from queue
2.  For each file in the job:
    1.  Substitutes `{file}` with actual path
    2.  Executes the bash script
    3.  Captures stdout, stderr, and return code
3.  Emits ExecutionLog with results

### Template Variables

Dispatchers support template variables in configuration:

-   `{file}` - Path to input file
-   `{platform}` - Platform name (e.g., goes18)
-   `{sensor}` - Sensor name (e.g., abi)
-   `{timestamp}` - File timestamp

**Example:**

    Organize outputs by date and platform
    =====================================
    geoips run single_source {file} \
      --output_dir /products/{platform}/{timestamp:%Y%m%d}

## Message Queues

GeoIPS Driver uses **RabbitMQ** for communication between plugins.

### Queue Architecture

    Data Monitor ──▶ FilesFoundQueue ──▶ Job Builder ──▶ JobReadyQueue ──▶ Dispatcher

Each queue is:

-   **Durable**: Survives RabbitMQ restarts
-   **Namespaced**: Isolated by service\_namespace
-   **Persistent**: Messages written to disk

**Example queue names:**

    production-FilesFoundQueue
    production-JobReadyQueue
    testing-FilesFoundQueue
    testing-JobReadyQueue

### Message Format

Messages are JSON-serialized objects:

**File message:**

    {
      "file": "/data/goes18/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc",
      "hostname": "localhost",
      "platform": "goes18",
      "sensor": "abi",
      "level": "L1B",
      "sector": "Full-Disk",
      "num_expected": 16,
      "datetime": "2024-01-15T12:00:00"
    }

**Job message:**

    {
      "name": "DummyJob",
      "identifier": "job_12345",
      "config": null,
      "files": [
        "{...File JSON...}"
      ],
      "last_modified": 1705320000.0,
      "timeout": 86400
    }

## Health and Monitoring

GeoIPS Driver includes comprehensive monitoring capabilities.

### Health Checks

Each component reports health status:

-   **Service**: Overall health (all managers healthy)
-   **Managers**: Prometheus, RabbitMQ, Plugin Manager
-   **Plugins**: Individual plugin health

**Health check interval** is configurable per service.

### Prometheus Metrics

Exposed on `http://localhost:8000/metrics`:

**Service-level:**

-   `service_uptime_seconds` - Service uptime
-   `service_health` - Overall health (1=healthy, 0=unhealthy)

**Plugin-level:**

-   `files_processed_total` - Files processed by data monitors
-   `jobs_built_total` - Jobs created by job builders
-   `jobs_processed_total` - Jobs executed by dispatchers

**RabbitMQ:**

-   `rabbitmq_connections_total` - Connection attempts
-   `rabbitmq_messages_sent_total` - Messages sent
-   `rabbitmq_messages_received_total` - Messages received

See :doc:`../reference/metrics-reference` for complete list.

### Logging

GeoIPS Driver uses structured logging with optional Loki integration.

**Log levels:**

-   `TRACE` (5) - Very detailed debug information
-   `DEBUG` (10) - Debug information
-   `INFO` (20) - Normal operational messages
-   `WARNING` (30) - Warning messages
-   `ERROR` (40) - Error messages
-   `CRITICAL` (50) - Critical failures

**Log context:**

Every log message includes source information:

    [Service: goes18-processor] Starting service
    [Manager: PluginManager] Registered plugin: file_system_poller_watchdog
    [Plugin: file_system_poller_watchdog] Found file: ...

## Data Flow Example

Let's trace a file through the complete pipeline:

**Step 1: File Appears**

    User copies file:
    /data/goes18/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000...nc

**Step 2: Data Monitor Detects**

    file_system_poller_watchdog:

    1. Watchdog library detects new file

    2. Matches against goes18_abi patterns

    3. Extracts metadata:
       - platform: goes18
       - sensor: abi
       - sector: Full-Disk
       - timestamp: 2024-01-15 12:00:00

    4. Creates File object

    5. Emits to FilesFoundQueue

**Step 3: Job Builder Groups**

    DummyJobBuilder:

    1. Consumes File from FilesFoundQueue

    2. Creates Job with single file

    3. Marks Job as ready

    4. Emits to JobReadyQueue

**Step 4: Dispatcher Executes**

    serial_bash:

    1. Consumes Job from JobReadyQueue

    2. Substitutes {file} in bash_script

    3. Executes script:
       geoips run single_source /data/goes18/OR_ABI-L1b-RadF...

    4. Captures output and return code

    5. Emits ExecutionLog

**Step 5: Metrics Updated**

    Prometheus metrics incremented:
    - files_processed_total{status="success"} +1
    - jobs_built_total{status="ready"} +1
    - jobs_processed_total{status="success"} +1

## Namespaces and Isolation

**Namespaces** provide isolation between services.

**Benefits:**

-   Multiple services can run concurrently
-   Different environments (dev, test, prod) on same infrastructure
-   Queue name collisions prevented
-   Independent monitoring

**Example:**

    Service 1: Production
    =====================
    name: goes18-production
    spec:
      service_namespace: production
      # Queues: production-FilesFoundQueue, production-JobReadyQueue

    Service 2: Testing
    ==================
    name: goes18-testing
    spec:
      service_namespace: testing
      # Queues: testing-FilesFoundQueue, testing-JobReadyQueue

Both services can monitor the same files but process them independently.

## Error Handling and Restarts

GeoIPS Driver automatically handles plugin failures:

**Restart Policy:**

1.  Plugin fails (exception, health check failure, thread death)
2.  Plugin state changes to FAILED
3.  Wait `plugin_restart_delay` seconds (default: 5)
4.  Attempt restart (up to `plugin_max_restart_attempts`, default: 3)
5.  If all retries exhausted, plugin remains FAILED

**Configuration:**

    In ServiceConfig (environment variables or code)
    ================================================
    PLUGIN_RESTART_DELAY=5           # Wait 5 seconds before restart
    PLUGIN_MAX_RESTARTS=3            # Maximum 3 restart attempts
    PLUGIN_HEALTH_CHECK_INTERVAL=2  # Check health every 2 seconds

**Metrics:**

-   `plugin_restarts_total` - Count of restart attempts
-   `plugin_state` - Current plugin state (enum)

## Next Steps

Now that you understand the concepts:

**Try the tutorials:**

-   :doc:`../tutorials/01-simple-file-watcher` - Build a basic file
    watcher
-   :doc:`../tutorials/03-custom-job-builder` - Create custom grouping
    logic
-   :doc:`../tutorials/05-geoips-workflow-dispatcher` - Integrate with
    GeoIPS

**Dive deeper:**

-   :doc:`../user-guide/architecture` - Detailed architecture guide
-   :doc:`../user-guide/plugins` - All built-in plugins
-   :doc:`../user-guide/metadata-matching` - Metadata extraction guide
-   :doc:`../developer-guide/plugin-development` - Build your own
    plugins

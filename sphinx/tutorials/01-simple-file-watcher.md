# Tutorial 1: Simple File Watcher

**Level:** Beginner | **Time:** 15 minutes

In this tutorial, you'll create a basic file watcher service that
monitors a directory for GOES-18 ABI data files and logs when they
appear.

## Learning Objectives

By the end of this tutorial, you will:

- Create a service configuration from scratch
- Configure the file system poller data monitor
- Test file detection with GOES-18 data
- Understand basic metadata extraction
- Monitor service health with Prometheus

## Prerequisites

- Lazy Lemon installed ({doc}\[../getting-started/installation\`)
- RabbitMQ running on localhost
- Basic familiarity with YAML
- A sample GOES-18 ABI file (or ability to create a test file)

## Step 1: Project Setup

Create a directory for this tutorial:

```
mkdir ~/tutorial01-file-watcher
cd ~/tutorial01-file-watcher

Create directories for data
===========================
mkdir -p data/incoming
mkdir -p data/processed
```

## Step 2: Create Test Data

If you don't have real GOES-18 files, create a test file with the
correct naming pattern:

```
Create a dummy file with GOES-18 naming convention
==================================================
touch data/incoming/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc
```

**Understanding the filename:**

```
OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc
│  │   │   │    │    │  │               │                │
│  │   │   │    │    │  └─ Start: 2024 day 015, 12:00:00
│  │   │   │    │    └─ Satellite: GOES-18
│  │   │   │    └─ Channel: 01 (Mode 6)
│  │   │   └─ Scan type: RadF (Full-Disk)
│  │   └─ Level: L1b
│  └─ Instrument: ABI
└─ Operational/Realtime
```

## Step 3: Write Service Configuration

Create `watcher.yaml`:

```
apiVersion: lazylemon/v1
kind: Service
name: tutorial-01-file-watcher
description: Basic GOES-18 file monitoring service for tutorial 01.

docstring: |
  This service demonstrates basic file watching capabilities.
  It monitors a directory for GOES-18 ABI Full-Disk files and
  extracts metadata automatically.

spec:
  service_namespace: tutorial01
  heartbeat_interval: 30

  rabbitmq:
    host: localhost
    port: 5672
    username: admin
    password: admin_test

  run:
    # Monitor for files
    - watch_files:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: ./data/incoming
          metadata-tools:
            - goes18_abi

    # Simple job builder (1 file = 1 job)
    - create_jobs:
        kind: job_builder
        name: DummyJobBuilder
        config: null

    # Log processing
    - log_files:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            #!/bin/bash
            echo "=========================================="
            echo "File detected: {file}"
            echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "=========================================="

            # Optional: Move to processed directory
            # mv {file} ./data/processed/
```

Let's break down this configuration:

**Data Monitor Configuration:**

```
- watch_files:
    kind: data_monitor
    name: file_system_poller_watchdog
    config:
      path: ./data/incoming           # Directory to watch
      metadata-tools:
        - goes18_abi                  # Use GOES-18 metadata patterns
```

This configures the watchdog-based file monitor to:

- Watch `./data/incoming` (relative to where you run the service)
- Use the `goes18_abi` metadata configuration to extract:
  - platform: goes18
  - sensor: abi
  - level: L1B
  - sector: Full-Disk (from filename pattern RadF)
  - timestamp: 2024-01-15 12:00:00

**Job Builder:**

```
- create_jobs:
    kind: job_builder
    name: DummyJobBuilder
    config: null
```

For this simple tutorial, each file becomes its own job immediately.

**Dispatcher:**

```
- log_files:
    kind: dispatcher
    name: serial_bash
    config:
      bash_script: |
        echo "File detected: {file}"
```

The `{file}` template variable is replaced with the actual file path.

## Step 4: Validate Configuration

Before running, validate the configuration:

```
lazylemon validate watcher.yaml
```

Expected output:

```
✅ Config valid
```

If you see errors, check your YAML syntax and indentation.

## Step 5: Start the Service

Start the service in the foreground:

```
lazylemon run watcher.yaml
```

You should see startup logs:

```
[Service: tutorial-01-file-watcher] Starting Service tutorial-01-file-watcher
[Manager: PrometheusManager] Starting Prometheus server on port 8000
[Manager: RabbitMQManager] Successfully connected to RabbitMQ
[Manager: PluginManager] Registered plugin: file_system_poller_watchdog v0.0.0
[Manager: PluginManager] Registered plugin: DummyJobBuilder v-1
[Manager: PluginManager] Registered plugin: serial_bash v-1
[Plugin: file_system_poller_watchdog] Starting to watch directory: ./data/incoming
[Service: tutorial-01-file-watcher] Service tutorial-01-file-watcher started successfully
```

The service is now running and watching for files!

## Step 6: Test File Detection

In another terminal, copy a file to the watched directory:

```
cd ~/tutorial01-file-watcher

Copy the test file
==================
cp data/incoming/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc \
   data/incoming/test_$(date +%s).nc
```

**Note:** The watchdog library only detects *new* files. If you want to
test again, you must create a file with a different name.

In the service logs, you'll see:

```
[Plugin: file_system_poller_watchdog] Found file: File(
  file=./data/incoming/test_1705320123.nc,
  hostname=localhost,
  platform=goes18,
  sensor=abi,
  level=L1B,
  sector=Full-Disk,
  num_expected=16,
  timestamp=2024-01-15 12:00:00
)
[Plugin: DummyJobBuilder] Received file from file queue
[Plugin: DummyJobBuilder] Job job_test_1705320123 is ready; emitting
[Plugin: serial_bash] Executing job
==========================================
File detected: ./data/incoming/test_1705320123.nc
Timestamp: 2024-01-15 12:01:03
==========================================
```

Success! The file was detected, metadata was extracted, and the
dispatcher executed.

## Step 7: Examine Metadata Extraction

Notice how the service automatically extracted:

- **platform**: goes18 (from G18 in filename)
- **sensor**: abi (from OR_ABI in filename)
- **level**: L1B (from L1b in filename)
- **sector**: Full-Disk (from RadF in filename)
- **num_expected**: 16 (GOES ABI has 16 channels per full-disk scan)
- **timestamp**: 2024-01-15 12:00:00 (from s20240151200000 in
  filename)

This metadata is available to downstream job builders and dispatchers.

## Step 8: Monitor with Prometheus

Open <http://localhost:8000/metrics> in your browser.

Look for these metrics:

```
Service health
==============
service_health 1.0

Files processed
===============
files_processed_file_system_poller_watchdog{status="success"} 1.0

Jobs built
==========
job_builder_jobs_built_total{status="ready",job_builder_name="DummyJobBuilder"} 1.0

Jobs executed
=============
dispatcher_jobs_processed_total{status="success",dispatcher_name="serial_bash"} 1.0
```

These metrics update in real-time as files are processed.

## Step 9: Experiment with Multiple Files

Test with multiple files:

```
Create multiple test files
==========================
for i in {1..5}; do
  touch data/incoming/OR_ABI-L1b-RadF-M6C$(printf %02d $i)_G18_s20240151200000_e20240151209310_c20240151209360_${i}.nc
  sleep 1  # Wait 1 second between files
done
```

Watch the logs as each file is detected and processed.

Check Prometheus metrics again - counters should have incremented:

```
files_processed_file_system_poller_watchdog{status="success"} 6.0
```

## Step 10: Clean Shutdown

Stop the service gracefully with `Ctrl+C`:

```
^C[Service: tutorial-01-file-watcher] Received keyboard interrupt
[Service: tutorial-01-file-watcher] Cleaning up resources...
[Manager: PluginManager] Plugin stopped: file_system_poller_watchdog
[Manager: PluginManager] Plugin stopped: DummyJobBuilder
[Manager: PluginManager] Plugin stopped: serial_bash
[Manager: PluginManager] Plugin manager stopped
[Manager: RabbitMQManager] RabbitMQ connection closed
[Service: tutorial-01-file-watcher] Service tutorial-01-file-watcher stopped
```

## Common Issues

**Files not detected:**

- Watchdog only detects files created/copied *after* service starts
- Check file permissions: `ls -la data/incoming`
- Verify path in config matches actual directory
- On some filesystems, use `mv` instead of `cp` for more reliable
  detection

**Metadata not extracted:**

- Check filename matches GOES-18 pattern
- View available metadata configs:
  `from lazylemon.interfaces import data_monitor_configs; print(data_monitor_configs.plugins)`
- See :doc:`../user-guide/metadata-matching` for pattern details

**RabbitMQ connection failed:**

- Verify RabbitMQ is running: `docker ps | grep rabbitmq`
- Check credentials match your RabbitMQ setup
- Try <http://localhost:15672> to access RabbitMQ management UI

## What You Learned

✅ How to create a service configuration ✅ How to configure the file
system poller ✅ How metadata extraction works ✅ How to validate
configurations ✅ How to monitor services with Prometheus ✅ How the
processing pipeline flows

## Next Steps

- `` `02-adding-metadata ``\` - Configure custom metadata patterns
- `` `03-custom-job-builder ``\` - Group files by sector or time
- `` `04-bash-dispatcher ``\` - Create more sophisticated bash scripts
- `` `07-monitoring-with-prometheus ``\` - Set up Grafana dashboards

## Challenge Exercises

1. **Modify the bash script** to copy processed files to
   `data/processed/` instead of just logging them
1. **Add a second data monitor** watching a different directory (e.g.,
   `data/backup`)
1. **Change the heartbeat interval** to 10 seconds and observe in
   Prometheus
1. **Create a metadata configuration** for a different satellite (if
   you have the data)

## Complete Code

The complete configuration is available in the tutorial repository:

\`tutorial01-file-watcher/watcher.yaml
\](<https://github.com/biosafetylvl5/lazylemon/tree/main/examples/tutorials/01-file-watcher>)

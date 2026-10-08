# Tutorial 1: Simple File Watcher

**Level:** Beginner | **Time:** 15 minutes

> **Prerequisite:** This tutorial expands on the {doc}`../getting-started/quick-start`. Complete the quick start first for the basic file watcher setup.

In this tutorial, you'll create a basic file watcher service that
monitors a directory for GOES-18 ABI data files and logs when they
appear.

## Learning Objectives

By the end of this tutorial, you will:

- Create a service configuration from scratch
- Configure the file system poller data monitor
- Test file detection with GOES-18 data
- Extract metadata from GOES-18 filenames automatically
- Monitor service health with Prometheus

## Prerequisites

- Courier installed ({doc}`../getting-started/installation`)
- RabbitMQ running on localhost (or delete the `broker` block from the config
  below to use the built-in in-memory broker)
- Basic familiarity with YAML
- A sample GOES-18 ABI file (or ability to create a test file)

## Step 1: Project Setup

Create a directory for this tutorial:

```
mkdir ~/tutorial01-file-watcher
cd ~/tutorial01-file-watcher
```

Create directories for data:

```
mkdir -p data/incoming
mkdir -p data/processed
```

## Step 2: Create Test Data

If you don't have real GOES-18 files, create a test file with the
correct naming pattern:

```
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
apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: tutorial-01-file-watcher
  namespace: tutorial01
  description: Basic GOES-18 file monitoring service for tutorial 01.
  docstring: |
    This service demonstrates basic file watching capabilities.
    It monitors a directory for GOES-18 ABI Full-Disk files and
    extracts metadata automatically.

spec:
  broker:
    host: localhost
    port: 5672
    username: admin
    password: admin_test

  run:
    # Monitor for files
    - watch-files:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: ./data/incoming
          metadata-tools:
            - goes18_abi

    # Simple job builder (1 file = 1 job)
    - create-jobs:
        kind: job_builder
        name: DummyJobBuilder
        config:
          targets:
            - log-files
          payload:
            log-payload:
              kind: payload
              name: bash_payload
              config:
                script: |
                  #!/bin/bash
                  echo "=========================================="
                  echo "File detected: {{ files[0].file }}"
                  echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
                  echo "=========================================="

                  # Optional: Move to processed directory
                  # mv {{ files[0].file }} ./data/processed/

    # Run each job's payload and log its output
    - log-files:
        kind: dispatcher
        name: local_dispatcher
        config:
          log_to_logger: true
```

> **Template syntax:** This tutorial uses `{{ files[0].file }}`. The script
> is the job builder's `payload`: the builder renders it for each job and the
> dispatcher runs it. For the full template context, see
> {doc}`../api-reference/payloads`.

## Step 4: Validate Configuration

Before running, validate the configuration:

```
courier validate watcher.yaml
```

Expected output:

```
watcher.yaml is valid.
  3 pipeline steps: 1 data monitor, 1 job builder, 1 dispatcher
  create-jobs runs payload log-payload (bash_payload)
  broker: amqp

Run it:  courier run watcher.yaml
```

If you see errors, check your YAML syntax and indentation.

## Step 5: Start the Service

Start the service in the foreground:

```
courier run watcher.yaml
```

You should see startup logs like these (timestamps and log levels trimmed):

```
[Module: tracing] OpenTelemetry tracing disabled
[Manager: PluginManager] Registered plugin: watch-files (class=file_system_poller_watchdog v0.0.0)
[Manager: PluginManager] Registered plugin: create-jobs (class=DummyJobBuilder v-1)
[Manager: PluginManager] Registered plugin: log-files (class=local_dispatcher v-1)
[Service: tutorial-01-file-watcher] Starting Service tutorial-01-file-watcher
[Manager: PrometheusManager] Starting Prometheus server on port 8000
[Plugin: file_system_poller_watchdog] Starting to watch directory: data/incoming
[Manager: PluginManager] Plugin started successfully: DummyJobBuilder
[Manager: PluginManager] Plugin started successfully: local_dispatcher
[Manager: PluginManager] Plugin started successfully: file_system_poller_watchdog
[Service: tutorial-01-file-watcher] Service tutorial-01-file-watcher started successfully
```

The service is now running and watching for files!

## Step 6: Test File Detection

In another terminal, copy a file to the watched directory:

```
cd ~/tutorial01-file-watcher
```

Copy the test file to a new name that still follows the GOES-18 pattern (here,
channel 02):

```
cp data/incoming/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc \
   data/incoming/OR_ABI-L1b-RadF-M6C02_G18_s20240151200000_e20240151209310_c20240151209360.nc
```

```{include} ../includes/watchdog-new-files-only.md
```

In the service logs, you'll see lines like these:

```
[Plugin: file_system_poller_watchdog] Found file: {"data": null, "file": "/home/user/tutorial01-file-watcher/data/incoming/OR_ABI-L1b-RadF-M6C02_G18_s20240151200000_e20240151209310_c20240151209360.nc", "hostname": "localhost", "source": "goes18", "instrument": "abi", "processing_stage": "l1b", "domain": "FULL-DISK", "metadata": {}, "num_expected": 16, "timestamp": "2024-01-15T12:00:00+00:00"}
[Plugin: DummyJobBuilder] Job /home/user/tutorial01-file-watcher/data/incoming/OR_ABI-L1b-RadF-M6C02_G18_...nc is ready; emitting
[Plugin: DummyJobBuilder] Emitted job /home/user/tutorial01-file-watcher/data/incoming/OR_ABI-L1b-RadF-M6C02_G18_...nc to targets ['log-files']
[Plugin: bash_payload] [job: /home/user/tutorial01-file-watcher/data/incoming/OR_ABI-...nc] [stdout] ==========================================
[Plugin: bash_payload] [job: /home/user/tutorial01-file-watcher/data/incoming/OR_ABI-...nc] [stdout] File detected: /home/user/tutorial01-file-watcher/data/incoming/OR_ABI-L1b-RadF-M6C02_G18_...nc
[Plugin: bash_payload] [job: /home/user/tutorial01-file-watcher/data/incoming/OR_ABI-...nc] [stdout] Timestamp: 2024-01-15 12:01:03
[Plugin: bash_payload] [job: /home/user/tutorial01-file-watcher/data/incoming/OR_ABI-...nc] [stdout] ==========================================
```

Success! The file was detected, metadata was extracted, and the
dispatcher ran the payload. The `[stdout]` lines appear because the
dispatcher sets `log_to_logger: true`; without it, a job that succeeds leaves
no log line of its own, and one whose script fails is logged at ERROR.

## Step 7: Examine Metadata Extraction

Notice how the service automatically extracted:

- **source**: goes18 (from G18 in filename)
- **instrument**: abi (from OR_ABI in filename)
- **processing_stage**: l1b (from L1b in filename)
- **domain**: FULL-DISK (from RadF in filename)
- **num_expected**: 16 (GOES ABI has 16 channels per full-disk scan)
- **timestamp**: 2024-01-15 12:00:00 UTC (from s20240151200000 in
  filename)

This metadata is available to downstream job builders, and to the payload
template as `files[0].source`, `files[0].timestamp` and so on.

## Step 8: Monitor with Prometheus

Open <http://localhost:8000/metrics> in your browser.

Look for these metrics (labels trimmed):

### Service health

Set on every heartbeat (every 30 seconds by default). It reads `0.0` until
the first heartbeat, so wait 30 seconds after startup before checking it:

```
courier_service_health 1.0
```

### Files processed

```
courier_data_monitor_files_processed_total{monitor_identifier="watch-files",monitor_name="file_system_poller_watchdog",status="success"} 1.0
```

### Jobs built

```
courier_job_builder_jobs_built_total{job_builder_identifier="create-jobs",job_builder_name="DummyJobBuilder",status="ready"} 1.0
```

### Jobs executed

```
courier_dispatcher_jobs_processed_total{dispatcher_identifier="log-files",dispatcher_name="local_dispatcher",status="success"} 1.0
courier_payload_jobs_processed_total{payload_identifier="log-payload",payload_name="bash_payload",status="success"} 1.0
```

The dispatcher counts every job it executed; the payload counter's `status`
reflects the script's exit code.

These metrics update in real-time as files are processed.

## Step 9: Experiment with Multiple Files

Test with multiple files:

**Note:** The filenames below use a simplified pattern. Real GOES-18 files follow the naming convention shown in Step 2.

```
# Uses explicit list instead of {1..5} for portability across shells
for i in 1 2 3 4 5; do
  touch data/incoming/OR_ABI-L1b-RadF-M6C$(printf %02d $i)_G18_s20240151200000_e20240151209310_c20240151209360_${i}.nc
  sleep 1  # Wait 1 second between files
done
```

Watch the logs as each file is detected and processed.

Check Prometheus metrics again - counters should have incremented:

```
courier_data_monitor_files_processed_total{monitor_identifier="watch-files",monitor_name="file_system_poller_watchdog",status="success"} 6.0
```

## Step 10: Clean Shutdown

Stop the service gracefully with `Ctrl+C`:

```
^C[Module: signals] Received signal 2, requesting graceful shutdown...
[Manager: PluginManager] Plugin stopped: file_system_poller_watchdog
[Manager: PluginManager] Plugin stopped: DummyJobBuilder
[Manager: PluginManager] Plugin stopped: local_dispatcher
[Manager: PluginManager] Plugin manager stopped
[Manager: PrometheusManager] Prometheus manager stopped
[Service: tutorial-01-file-watcher] Service tutorial-01-file-watcher stopped
```

## Common Issues

```{include} ../includes/watchdog-new-files-only.md
```

```{include} ../includes/common-troubleshooting.md
```

**Metadata not extracted:**

- Check filename matches GOES-18 pattern
- View available metadata configs: `courier plugins list`
- See the `goes18_abi` metadata config in
  `src/courier/plugins/data_monitor_configs/goes18_abi.py` for pattern details.

## What You Learned

You've completed all the learning objectives listed at the start of this tutorial. You can now:

- Create service configurations from scratch
- Configure the file system poller data monitor
- Extract metadata from GOES-18 filenames
- Validate configurations and monitor services with Prometheus

## Next Steps

- {doc}`02-docker-swarm-cluster` — Deploy across multiple Docker containers

## Challenge Exercises

1. **Modify the payload script** to copy processed files to
   `data/processed/` instead of just logging them
1. **Add a second data monitor** watching a different directory (e.g.,
   `data/backup`)
1. **Change the heartbeat interval** to 10 seconds
   (`spec.service_config.heartbeat_interval: 10`) and observe in Prometheus
1. **Create a metadata configuration** for a different satellite (if
   you have the data)

## Complete Code

The complete configuration is the `watcher.yaml` from Step 3.

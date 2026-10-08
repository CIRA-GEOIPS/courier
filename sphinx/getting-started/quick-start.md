# Quick Start

Get your first Courier service running. This guide
walks you through setting up a file watcher for GOES-18 ABI data.

## What We'll Build

A service that:

1. Watches a directory for new GOES-18 ABI Level 1B files
1. Extracts metadata (platform, sensor, sector, timestamp)
1. Groups files into processing jobs
1. Executes a simple processing script for each job

## Prerequisites

- Courier installed ({doc}`installation`)
- A directory with GOES-18 ABI files (or the ability to copy them
  there)

## Step 1: Prepare Your Data Directory

Create a directory for incoming GOES-18 data:

```
mkdir -p ~/goes18_data/incoming
mkdir -p ~/goes18_data/processed
```

**Example GOES-18 ABI filenames:**

```
OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc
OR_ABI-L1b-RadF-M6C02_G18_s20240151200000_e20240151209310_c20240151209360.nc
...
OR_ABI-L1b-RadF-M6C16_G18_s20240151200000_e20240151209310_c20240151209360.nc
```

These are Full-Disk (RadF) Level 1B files for channels 1-16.

## Step 2: Create Service Configuration

Create a file `goes18_watcher.yaml`:

```yaml
apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: goes18-file-watcher
  namespace: goes18-quickstart
  description: Monitor for GOES-18 ABI Full-Disk data and process it.

spec:
  broker:
    host: localhost
    port: 5672
    username: admin
    password: admin_test

  run:
    - watch-files:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: ~/goes18_data/incoming
          metadata-tools:
            - goes18_abi  # Use built-in GOES-18 metadata extractor

    - group-files:
        kind: job_builder
        name: DummyJobBuilder
        config:
          targets:
            - process-data
          payload:
            process-payload:
              kind: payload
              name: bash_payload
              config:
                script: |
                  #!/bin/bash
                  echo "=========================================="
                  echo "Processing file: {{ files[0].file }}"
                  echo "Timestamp: $(date)"
                  echo "=========================================="

                  # Move processed file
                  mv {{ files[0].file }} ~/goes18_data/processed/
                  echo "Processing complete!"

    - process-data:
        kind: dispatcher
        name: local_dispatcher
        config:
          log_to_logger: true  # Log the script's output line by line
```

> **Note:** The `run` pipeline above has three stages: `watch-files` (data monitor), `group-files` (job builder), and `process-data` (dispatcher). The script the dispatcher runs is the job builder's `payload`: the builder fills in `{{ files[0].file }}` for each job and the dispatcher executes the result. The `~` in file paths expands to your home directory on both Linux and macOS.

> **Note:** The `broker` section above expects RabbitMQ on `localhost:5672`. For single-process testing without RabbitMQ, omit the entire `broker` section to use the built-in in-memory broker.

## Step 3: Start the Service

Run the service:

```
courier run goes18_watcher.yaml
```

> The above command works whether you installed with `uv pip`, `pip`, or an editable development install — the `courier` CLI is registered as a console script entry point in both cases.

You should see output like this (timestamps and log levels trimmed):

```
[Module: tracing] OpenTelemetry tracing disabled
[Manager: PluginManager] Registered plugin: watch-files (class=file_system_poller_watchdog v0.0.0)
[Manager: PluginManager] Registered plugin: group-files (class=DummyJobBuilder v-1)
[Manager: PluginManager] Registered plugin: process-data (class=local_dispatcher v-1)
[Service: goes18-file-watcher] Starting Service goes18-file-watcher
[Manager: PrometheusManager] Starting Prometheus server on port 8000
[Plugin: file_system_poller_watchdog] Starting to watch directory: /home/user/goes18_data/incoming
[Service: goes18-file-watcher] Service goes18-file-watcher started successfully
```

## Step 4: Test with Data

Copy a GOES-18 file to the watched directory:

```
cp /path/to/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000*.nc ~/goes18_data/incoming/
```

Watch the service logs. You should see lines like these (file names
shortened):

```
[Plugin: file_system_poller_watchdog] Found file: {"data": null, "file": "/home/user/goes18_data/incoming/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_...nc", "hostname": "localhost", "source": "goes18", "instrument": "abi", "processing_stage": "l1b", "domain": "FULL-DISK", "metadata": {}, "num_expected": 16, "timestamp": "2024-01-15T12:00:00+00:00"}
[Plugin: DummyJobBuilder] Job /home/user/goes18_data/incoming/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_...nc is ready; emitting
[Plugin: DummyJobBuilder] Emitted job /home/user/goes18_data/incoming/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_...nc to targets ['process-data']
[Plugin: bash_payload] [job: /home/user/goes18_data/incoming/OR_ABI-...nc] [stdout] ==========================================
[Plugin: bash_payload] [job: /home/user/goes18_data/incoming/OR_ABI-...nc] [stdout] Processing file: /home/user/goes18_data/incoming/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_...nc
[Plugin: bash_payload] [job: /home/user/goes18_data/incoming/OR_ABI-...nc] [stdout] Timestamp: Mon Jan 15 12:05:30 UTC 2024
[Plugin: bash_payload] [job: /home/user/goes18_data/incoming/OR_ABI-...nc] [stdout] ==========================================
[Plugin: bash_payload] [job: /home/user/goes18_data/incoming/OR_ABI-...nc] [stdout] Processing complete!
```

The `[stdout]` lines are the script's output, logged by the payload because
the dispatcher sets `log_to_logger: true`. Without it, a job that succeeds
leaves no log line of its own; a job whose script fails is always logged at
ERROR with its return code.

Courier logs at INFO by default. For more detail, pass a global log level
before the subcommand, e.g. `courier --log-level DEBUG run goes18_watcher.yaml`.
Tracing is off by default; see {doc}`../operations/tracing` to enable it.

The file will be moved to the `processed` directory.

## Step 5: Monitor with Prometheus

Courier automatically exposes Prometheus metrics on port 8000.

Open `http://localhost:8000/metrics` in your browser. After processing a file, you should see counters for files processed, jobs built, and jobs executed. For a complete breakdown of each metric, see {doc}`../tutorials/01-simple-file-watcher` (Step 8: Monitor with Prometheus).

## Step 6: Stop the Service

Press `Ctrl+C` to gracefully stop the service:

```
^C
```

The service shuts down gracefully. For the full shutdown log output, see {doc}`../tutorials/01-simple-file-watcher` (Step 10).

## Next Steps

Congratulations! You've created your first Courier service. Now you can:

- {doc}`configuration` — Understand YAML configuration in depth
- {doc}`../concepts/index` — Learn about services, plugins, and queues
- {doc}`../tutorials/01-simple-file-watcher` — Step-by-step tutorial
- {doc}`../tutorials/02-docker-swarm-cluster` — Deploy across Docker containers

## Common Issues

```{include} ../includes/watchdog-new-files-only.md
```

```{include} ../includes/common-troubleshooting.md
```

## Need Help?

- Review logs in the service output for details.
- Ask questions in [GitHub Discussions](https://github.com/biosafetylvl5/courier/discussions)

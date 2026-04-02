# Quick Start

Get your first Lazy Lemon service running in 5 minutes! This guide
walks you through setting up a file watcher for GOES-18 ABI data.

## What We'll Build

A service that:

1. Watches a directory for new GOES-18 ABI Level 1B files
1. Extracts metadata (platform, sensor, sector, timestamp)
1. Groups files into processing jobs
1. Executes a simple processing script for each job

## Prerequisites

- Lazy Lemon installed ({doc}\[installation\`)
- RabbitMQ running on localhost:5672
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

```
apiVersion: lazylemon/v1
kind: Service
name: goes18-file-watcher
description: Monitor for GOES-18 ABI Full-Disk data and process it.

spec:
  namespace: goes18_quickstart
  heartbeat_interval: 30  # Send heartbeat every 30 seconds

  rabbitmq:
    host: localhost
    port: 5672
    username: admin
    password: admin_password

  run:
    # Step 1: Monitor for files
    - watch_files:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: /home/user/goes18_data/incoming
          metadata-tools:
            - goes18_abi  # Use built-in GOES-18 metadata extractor

    # Step 2: Build jobs from files
    - group_files:
        kind: job_builder
        name: DummyJobBuilder
        config: null

    # Step 3: Process jobs
    - process_data:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            #!/bin/bash
            echo "=========================================="
            echo "Processing file: {file}"
            echo "Timestamp: $(date)"
            echo "=========================================="

            # Move processed file
            mv {file} /home/user/goes18_data/processed/

            echo "Processing complete!"
```

Note

Replace `/home/user` with your actual home directory path.

## Step 3: Understanding the Configuration

Let's break down what each section does:

**Metadata Section:**

```
apiVersion: lazylemon/v1  # API version
kind: Service                  # This is a service configuration
name: goes18-file-watcher     # Unique service name
```

**Service Spec:**

```
namespace: goes18_quickstart  # Namespace for isolation
heartbeat_interval: 30                # Health check frequency
```

**RabbitMQ Connection:**

```
rabbitmq:
  host: localhost      # RabbitMQ server
  port: 5672          # Default AMQP port
  username: admin     # Authentication
  password: admin_password
```

**Processing Pipeline:**

The `run` section defines three plugins that form a processing pipeline:

1. **watch_files** (Data Monitor)
   - Watches `/home/user/goes18_data/incoming`
   - Uses `goes18_abi` metadata configuration
   - Automatically extracts: platform=goes18, sensor=abi, sector,
     timestamp
1. **group_files** (Job Builder)
   - Groups related files into jobs
   - `DummyJobBuilder` creates one job per file (simple for
     quickstart)
1. **process_data** (Dispatcher)
   - Executes the bash script for each job
   - `{file}` is replaced with the actual file path

## Step 4: Start the Service

Run your service:

```
If using pip installation
=========================
lazylemon run goes18_watcher.yaml

If using poetry
===============
poetry run python -m lazylemon.dummy_cli run goes18_watcher.yaml
```

You should see output like:

```
[Service: goes18-file-watcher] Starting Service goes18-file-watcher
[Service: goes18-file-watcher] Starting Prometheus server on port 8000
[Manager: PluginManager] Registered plugin: file_system_poller_watchdog v0.0.0
[Manager: PluginManager] Registered plugin: DummyJobBuilder v-1
[Manager: PluginManager] Registered plugin: serial_bash v-1
[Plugin: file_system_poller_watchdog] Starting to watch directory: /home/user/goes18_data/incoming
[Service: goes18-file-watcher] Service goes18-file-watcher started successfully
```

## Step 5: Test with Data

Copy a GOES-18 file to the watched directory:

```
Copy a test file
================
cp /path/to/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000*.nc ~/goes18_data/incoming/
```

Watch the service logs. You should see:

```
[Plugin: file_system_poller_watchdog] Found file: File(file=/home/user/goes18_data/incoming/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc, platform=goes18, sensor=abi, sector=Full-Disk, timestamp=2024-01-15 12:00:00)
[Plugin: DummyJobBuilder] Received file from file queue
[Plugin: serial_bash] Executing job
==========================================
Processing file: /home/user/goes18_data/incoming/OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc
Timestamp: Mon Jan 15 12:05:30 UTC 2024
==========================================
Processing complete!
```

The file will be moved to the `processed` directory.

## Step 6: Monitor with Prometheus

Lazy Lemon automatically exposes Prometheus metrics on port 8000.

View metrics in your browser:

```
http://localhost:8000
```

You'll see metrics like:

```
HELP files_processed_file_system_poller_watchdog Total number of files processed
================================================================================
TYPE files_processed_file_system_poller_watchdog counter
========================================================
files_processed_file_system_poller_watchdog{status="success"} 1.0

HELP service_health Overall service health status
=================================================
TYPE service_health gauge
=========================
service_health 1.0

HELP service_uptime_seconds Service uptime in seconds
=====================================================
TYPE service_uptime_seconds gauge
=================================
service_uptime_seconds 127.5
```

## Step 7: Stop the Service

Press `Ctrl+C` to gracefully stop the service:

```
^C[Service: goes18-file-watcher] Received keyboard interrupt
[Service: goes18-file-watcher] Cleaning up resources...
[Manager: PluginManager] Plugin manager stopped
[Manager: RabbitMQManager] RabbitMQ connection closed
[Manager: PrometheusManager] Prometheus manager stopped
[Service: goes18-file-watcher] Service goes18-file-watcher stopped
```

## What's Next?

Congratulations! You've created your first Lazy Lemon service. Now
you can:

**Learn More:**

- `` `configuration-basics ``\` - Understand YAML configuration in
  depth
- `` `concepts ``\` - Learn about services, plugins, and queues
- :doc:`../tutorials/02-adding-metadata` - Configure advanced metadata
  extraction

**Build On This:**

- :doc:`../tutorials/03-custom-job-builder` - Group files by sector or
  time
- :doc:`../tutorials/05-geoips-workflow-dispatcher` - Call real GeoIPS
  workflows
- :doc:`../tutorials/06-multi-satellite-monitor` - Watch multiple
  satellites

**Deploy to Production:**

- :doc:`../tutorials/07-monitoring-with-prometheus` - Set up Grafana
  dashboards
- :doc:`../tutorials/08-production-deployment` - Deploy to Kubernetes
- :doc:`../user-guide/deployment` - Production deployment guide

## Common Issues

**Service won't start:**

- Check RabbitMQ is running: `docker ps | grep rabbitmq`
- Verify configuration syntax:
  `poetry run python -m lazylemon.dummy_cli validate goes18_watcher.yaml`

**Files not being detected:**

- Check file permissions: `ls -la ~/goes18_data/incoming`
- Verify path in configuration matches actual directory
- Check filename patterns match GOES-18 naming convention

**No output when copying files:**

- Watchdog monitors for *new* files created/moved into the directory
- Files already present when service starts are not detected
- Try copying a file while the service is running

## Need Help?

- Check :doc:`../user-guide/troubleshooting` for common issues
- Review logs in the service output
- Ask questions in \`GitHub Discussions
  \](<https://github.com/biosafetylvl5/lazylemon/discussions>)

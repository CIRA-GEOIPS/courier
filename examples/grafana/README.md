# Courier Grafana Dashboard

Programmatic Grafana dashboard generated with [grafanalib](https://github.com/weaveworks/grafanalib).

Covers all 46 Prometheus metrics exposed by the Courier service, organized into:

- **Service Overview** -- health, uptime, heartbeat, total files
- **Data Monitors** -- file processing rate, status breakdown, scan age, scan duration, poll errors, connection status, consumer lag, last emitted file age
- **Job Builders** -- files received, jobs built, active groups, discards, duration percentiles, files per job, timeout emissions
- **Dispatchers** -- jobs processed, success ratio, active jobs, execution duration, logs emitted, queue wait latency, parallel workers active
- **Plugin Manager** -- health, status table, restarts
- **Broker** -- connection status, connection attempts, messages sent, messages received
- **State Sync / HA** -- pushes, applies, emit claims, sync errors (collapsed by default)
- **Pipeline Summary** -- end-to-end throughput funnel
- **Job Builder — Metadata Router** -- route matches, unmatched files
- **Dispatcher — SLURM** -- pending jobs, submissions (collapsed by default)
- **Dispatcher — HTTP** -- response codes, request duration percentiles (collapsed by default)

## Install

```bash
pip install -e .[grafana]
```

## Generate

```bash
python examples/grafana/courier_dashboard.py > dashboard.json
```

## Import into Grafana

**Via UI:**

1. Open Grafana -> Dashboards -> Import
1. Upload `dashboard.json`
1. Select your Prometheus datasource

**Via provisioning:**

1. Copy `dashboard.json` to your Grafana provisioning dashboards directory
1. Wrap the JSON if required by your provisioning config:
   ```json
   {"dashboard": <contents of dashboard.json>, "overwrite": true}
   ```

## Template Variables

The dashboard includes dropdown filters for:

- Data Source (Prometheus instance)
- Data Monitor name
- Job Builder name
- Dispatcher name
- Plugin name
- Sync Builder name (HA only)
- Queue (for RabbitMQ metrics)
- Error Type (for data monitor poll errors)
- Topic (for data monitor consumer lag)
- Route Name (for metadata router)
- HTTP Status Code (for HTTP dispatcher)
- Dispatcher Identifier (for routing throughput, dispatch latency, queue depth, and dedupe skips)

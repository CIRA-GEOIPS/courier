# Courier Grafana Dashboard

Programmatic Grafana dashboard generated with [grafanalib](https://github.com/weaveworks/grafanalib).

Covers all 20 Prometheus metrics exposed by the Courier service, organized into:

- **Service Overview** -- health, uptime, heartbeat, total files
- **Data Monitors** -- file processing rate, status breakdown, scan age
- **Job Builders** -- files received, jobs built, active groups, discards, duration percentiles
- **Dispatchers** -- jobs processed, success ratio, active jobs, execution duration, logs emitted
- **Plugin Manager** -- health, status table, restarts
- **State Sync / HA** -- pushes, applies, emit claims (collapsed by default)
- **Pipeline Summary** -- end-to-end throughput funnel

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

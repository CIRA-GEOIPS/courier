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
- **Job Builder -- Metadata Router** -- route matches, unmatched files
- **Dispatcher -- SLURM** -- pending jobs, submissions (collapsed by default)
- **Dispatcher -- HTTP** -- response codes, request duration percentiles (collapsed by default)

## CLI Command (Recommended)

The `courier dashboard` command reads your Courier service configuration and generates tailored Grafana dashboard JSON -- automatically including only the panels and template variables relevant to the plugins defined in your config.

### Installation

```bash
pip install courier[grafana]
```

### Basic Usage

Generate a dashboard from your service config and write it to a file:

```bash
courier dashboard config.yaml -o dashboard.json
```

If no config path is given, the command looks for `courier.yaml` or `courier.yml` in the current directory. With no `--output` flag, the JSON is printed to stdout:

```bash
courier dashboard > dashboard.json
```

### Generation Modes

By default, a single unified dashboard is produced covering all configured plugins. Use `--split-by` to generate separate dashboards per plugin kind or per plugin instance:

```bash
# One dashboard per plugin kind (data_monitor, job_builder, dispatcher)
courier dashboard config.yaml --split-by kind -o ./dashboards/

# One dashboard per plugin instance
courier dashboard config.yaml --split-by plugin -o ./dashboards/
```

When splitting, `--output` must point to a directory (or be omitted for stdout, where dashboards are concatenated).

### Cluster / Sub-Section Support

For multi-node deployments, you can filter the dashboard to a subset of plugins:

```bash
# Only specific plugin identifiers
courier dashboard config.yaml --run-identifiers my-dm,my-dp

# Only specific plugin kinds
courier dashboard config.yaml --run-kinds data_monitor,dispatcher
```

`--run-identifiers` accepts comma-separated plugin identifiers. `--run-kinds` accepts comma-separated plugin kind names (`data_monitor`, `job_builder`, `dispatcher`).

### Live Plugin Detection

Use `--live` to auto-detect active plugins from a running Courier instance's Prometheus endpoint. The command queries the metrics endpoint, identifies which plugins are currently emitting metrics, and generates the dashboard for only those plugins:

```bash
courier dashboard config.yaml --live
```

By default it connects to `localhost:8000`. Override the host and port with `--prom-host` and `--prom-port`:

```bash
courier dashboard config.yaml --live --prom-host 10.0.0.5 --prom-port 9090
```

If the endpoint is unreachable, the command falls back to all plugins defined in the config and prints a warning.

### Panel Selection

Limit the generated dashboard to Prometheus metric panels only or Tempo trace search panels only:

```bash
# Metrics only (skip TraceQL panels)
courier dashboard config.yaml --only-metrics -o dashboard.json

# Traces only (skip Prometheus panels)
courier dashboard config.yaml --only-traces -o dashboard.json
```

### All Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--output`, `-o` | path | stdout | Output file (`.json`) or directory (for split mode). Stdout if omitted. |
| `--split-by` | string | `unified` | Split strategy: `kind` (one per plugin kind) or `plugin` (one per instance). |
| `--run-identifiers` | string | — | Comma-separated plugin identifiers to filter (cluster sub-section). |
| `--run-kinds` | string | — | Comma-separated plugin kinds to filter: `data_monitor`, `job_builder`, `dispatcher`. |
| `--live` | bool | `false` | Auto-detect active plugins from a running Courier Prometheus endpoint. |
| `--prom-host` | string | `localhost` | Prometheus metrics host (used with `--live`). |
| `--prom-port` | int | `8000` | Prometheus metrics port (used with `--live`). |
| `--only-metrics` | bool | `false` | Only generate Prometheus metric panels (skip TraceQL). |
| `--only-traces` | bool | `false` | Only generate TraceQL trace search panels (skip Prometheus). |
| `--name` | string | — | Override the dashboard title. |
| `--uid` | string | — | Override the dashboard UID. |
| `--datasource` | string | `Prometheus` | Prometheus datasource name or UID in Grafana. |
| `--traces-datasource` | string | `Tempo` | Tempo datasource name or UID in Grafana. |
| `--indent` | int | `2` | JSON indentation level for the output. |

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

## Alternative: Script-based Generation

The original script-based approach uses `examples/grafana/courier_dashboard.py` to produce a static dashboard covering all metrics regardless of your actual configuration. This method is still available but the CLI command is recommended for most use cases.

### Install

```bash
pip install -e .[grafana]
```

### Generate

```bash
python examples/grafana/courier_dashboard.py > dashboard.json
```

The script always produces a single, unified dashboard with all possible panels and template variables -- it does not read your service config, so panels will appear even for plugin kinds you are not running. Use the CLI command if you want a dashboard tailored to your actual pipeline configuration.

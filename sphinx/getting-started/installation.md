# Installation

This guide covers installing Courier in different environments.

## Requirements

**System Requirements:**

- Python 3.11, 3.12, or 3.13
- 4GB RAM minimum (8GB recommended)
- Linux, macOS, or Windows (with WSL2)

**Python Dependencies:**

Courier depends on:

- Kombu (AMQP messaging — RabbitMQ is optional for production)
- Prometheus client
- Pydantic for configuration validation
- Rich for enhanced logging

All dependencies are automatically installed.

## Installation Methods

### Using uv pip (Recommended for Users)

Install the latest stable release from PyPI:

```
uv pip install data-courier
```

> **Note:** The distribution is named `data-courier` on PyPI (the name
> `courier` was already taken), but it installs and imports as `courier`.
> The CLI command is `courier`.

Optional extras: `data-courier[doc]` for documentation tools, `data-courier[test]` for testing, `data-courier[doc,lint,test]` for development.

### Plugin Extras

A handful of plugins need a third-party package that courier does not install by
default. All of them are *listed* on a plain install — `courier plugins list`
shows every plugin without importing any of them — but naming one in a config
without its extra fails with the install command you need:

```text
cron_glob requires the cron extra: pip install data-courier[cron]
```

| Extra | Provides | Needed by |
| --- | --- | --- |
| `data-courier[cron]` | `croniter` | `cron_glob` data monitor |
| `data-courier[s3]` | `boto3`, `botocore` | `s3_poller` data monitor |
| `data-courier[sftp]` | `paramiko` | `sftp_poller` data monitor |
| `data-courier[kafka]` | `kafka-python` | `kafka_consumer` data monitor |
| `data-courier[http]` | `httpx` | `http_dispatcher` |
| `data-courier[ha]` | `redis` | Multi-instance state sync |
| `data-courier[grafana]` | `grafanalib` | `courier dashboard` generation |
| `data-courier[viz]` | `textual`, `httpx` | `courier viz` terminal UI |

`data-courier[all-monitors]` and `data-courier[all-dispatchers]` install every plugin
dependency for their side of the pipeline.

The `file_system_poller_watchdog` monitor — the one used in
{doc}`quick-start` and most examples — needs no extra; `watchdog` is a core
dependency precisely because it backs the default.

### Using Nix (Recommended for Developers)

For development work:

```
git clone https://github.com/biosafetylvl5/courier.git
cd courier
nix-shell -p uv python312
uv pip install -e ".[doc,lint,test]"
```

### Using Docker

Pull the image:

```
docker pull ghcr.io/biosafetylvl5/courier:latest
```

For a full production deployment with RabbitMQ, Prometheus, Grafana, and Jaeger, see
{doc}`../tutorials/02-docker-swarm-cluster`.

## Setting Up Dependencies

### Prometheus (Optional)

For monitoring and metrics collection, Courier exposes a Prometheus endpoint on port 8000 by default. To set up a full Prometheus + Grafana observability stack, see the {doc}`../tutorials/02-docker-swarm-cluster` tutorial, which includes a complete `docker-compose.yml` with Prometheus, Grafana, and Jaeger.

### Grafana Loki (Optional)

For centralized logging:

```
docker run -d \
  --name loki \
  -p 3100:3100 \
  grafana/loki:latest
```

> **Note:** `latest` pulls the most recent image. Pin to a specific version tag for production.

> **Note:** Courier sends logs to Loki using the `python-logging-loki` package, which is installed by default.

## Verification

Verify your installation:

**Check version:**

```
python -c "import courier; print(courier.__version__)"
```

**List available plugins:**

```
courier plugins list
```

You should see output without errors.

## Development Setup

For development and contributing, install as described above, then set up pre-commit hooks and run the tests:

```
uv run pre-commit install
uv run pytest
uv run ruff check .
uv run mypy src
```

See {doc}`../contribute/code-style` for full conventions.

### Using Dev Containers (VS Code)

The repository includes a dev container configuration:

1. Install Docker Desktop and VS Code with Remote-Containers extension
1. Clone the repository and open in VS Code
1. When prompted, click "Reopen in Container"
1. Everything is pre-configured and ready to use

## Troubleshooting

**Import Error: No module named 'courier'**

Make sure you've activated your virtual environment:

uv:

```
uv venv
source .venv/bin/activate
```

pip:

```
source venv/bin/activate
```

**Permission Denied Errors**

On Linux, you may need to add your user to the docker group:

```
sudo usermod -aG docker $USER
```

Log out and back in for changes to take effect.

**Python Version Issues**

Courier requires Python 3.11+. Check your version:

```
python --version
```

Use pyenv or conda to install a compatible version if needed.

## Next Steps

- {doc}`quick-start` — Create your first file watcher service
- {doc}`configuration` — Learn about YAML configuration
- {doc}`../concepts/index` — Understand core concepts

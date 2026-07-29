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
uv pip install runcourier
```

> **Note:** The Python package is named `runcourier` but is installed and imported as `courier`. The CLI command is `courier`.

Optional extras: `runcourier[doc]` for documentation tools, `runcourier[test]` for testing, `runcourier[doc,lint,test]` for development.

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

# Installation

This guide covers installing Courier in different environments.

## Requirements

**System Requirements:**

- Python 3.11, 3.12, or 3.13
- RabbitMQ 3.x or later (required for multi-container deployments; the in-memory broker is sufficient for single-process testing)
- 4GB RAM minimum (8GB recommended)
- Linux, macOS, or Windows (with WSL2)

**Python Dependencies:**

Courier depends on:

- RabbitMQ (Kombu)
- Prometheus client
- Pydantic for configuration validation
- Rich for enhanced logging

All dependencies are automatically installed.

## Installation Methods

### Using pip (Recommended for Users)

Install the latest stable release from PyPI:

```
pip install courier
```

> **Note:** The Python package is named `runcourier` but is installed and imported as `courier`. The CLI command is `courier`.

Optional extras: `courier[doc]` for documentation tools, `courier[test]` for testing, `courier[doc,lint,test]` for development.

### Using Poetry (Recommended for Developers)

For development work:

```
git clone https://github.com/biosafetylvl5/courier.git
cd courier
poetry install --all-extras
poetry shell
```

### Using Docker

Pull the image:
```
docker pull ghcr.io/biosafetylvl5/courier:latest
```

For a full deployment with RabbitMQ, Prometheus, Grafana, and Jaeger, see
{doc}`../tutorials/02-docker-swarm-cluster`.

For quick testing, start RabbitMQ locally:
```
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin_test \
  rabbitmq:management
```

## Setting Up Dependencies

### RabbitMQ

Courier uses RabbitMQ for inter-plugin communication in production. For development and testing, the built-in in-memory broker works without external services.

> **Note:** For Docker users, see the previous section.

**Using Package Manager:**

Ubuntu/Debian:

```
sudo apt-get update
sudo apt-get install rabbitmq-server
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server
```

macOS:

```
brew install rabbitmq
brew services start rabbitmq
```

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

**Verify RabbitMQ is running:**
```
docker ps | grep rabbitmq
```

**List available plugins:**
```
python -c "from courier.interfaces import data_monitor_configs; print(data_monitor_configs.get_plugins())"
```

You should see output without errors.

## Development Setup

For development and contributing, install with Poetry as described above, then set up pre-commit hooks and run the tests:

```
poetry run pre-commit install
poetry run pytest
poetry run ruff check .
poetry run mypy src
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

Poetry:
```
poetry shell
```

venv:
```
source venv/bin/activate
```

**RabbitMQ Connection Failed**

Check that RabbitMQ is running:

Docker:
```
docker ps | grep rabbitmq
```

System service:
```
sudo systemctl status rabbitmq-server
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

# Installation

This guide covers installing Lazy Lemon in different environments.

## Requirements

**System Requirements:**

-   Python 3.11, 3.12, or 3.13
-   RabbitMQ 3.x or later (for message passing)
-   4GB RAM minimum (8GB recommended)
-   Linux, macOS, or Windows (with WSL2)

**Python Dependencies:**

Lazy Lemon depends on:

-   GeoIPS &gt;= 1.16.1
-   RabbitMQ (pika)
-   Prometheus client
-   Pydantic for configuration validation
-   Rich for enhanced logging

All dependencies are automatically installed.

## Installation Methods

### Using pip (Recommended for Users)

Install the latest stable release from PyPI:

    pip install geoips_driver

Or install with optional dependencies:

    With documentation tools
    ========================
    pip install geoips_driver[doc]

    With development tools
    ======================
    pip install geoips_driver[dev,lint,test]

    With all extras
    ===============
    pip install geoips_driver[doc,lint,test]

### Using Poetry (Recommended for Developers)

For development work:

    Clone the repository
    ====================
    git clone https://github.com/biosafetylvl5/geoips_driver.git
    cd geoips_driver

    Install with poetry
    ===================
    poetry install --all-extras

    Activate the virtual environment
    ================================
    poetry shell

### Using Docker

Run Lazy Lemon in a container:

    Pull the image
    ==============
    docker pull ghcr.io/biosafetylvl5/geoips_driver:latest

    Run with docker compose (recommended)
    =====================================
    docker compose up

See :doc:`../user-guide/deployment` for complete Docker deployment
guide.

### From Source

Install the latest development version:

    git clone https://github.com/biosafetylvl5/geoips_driver.git
    cd geoips_driver
    pip install -e .

## Setting Up Dependencies

### RabbitMQ

Lazy Lemon requires RabbitMQ for message passing between components.

**Using Docker (Easiest):**

    docker run -d \
      --name rabbitmq \
      -p 5672:5672 \
      -p 15672:15672 \
      -e RABBITMQ_DEFAULT_USER=admin \
      -e RABBITMQ_DEFAULT_PASS=admin_password \
      rabbitmq:3-management

Access the management UI at <http://localhost:15672>

**Using Package Manager:**

Ubuntu/Debian:

    sudo apt-get update
    sudo apt-get install rabbitmq-server
    sudo systemctl enable rabbitmq-server
    sudo systemctl start rabbitmq-server

macOS:

    brew install rabbitmq
    brew services start rabbitmq

### Prometheus (Optional)

For monitoring and metrics collection:

    docker run -d \
      --name prometheus \
      -p 9090:9090 \
      -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
      prom/prometheus

See :doc:`../tutorials/07-monitoring-with-prometheus` for setup guide.

### Grafana Loki (Optional)

For centralized logging:

    docker run -d \
      --name loki \
      -p 3100:3100 \
      grafana/loki:latest

## Verification

Verify your installation:

    Check version
    =============
    python -c "import geoips_driver; print(geoips_driver.__version__)"

    Verify RabbitMQ connection
    ==========================
    python -c "import pika; pika.BlockingConnection(pika.URLParameters('amqp://admin:admin_password@localhost:5672/'))"

    List available plugins
    ======================
    python -c "from geoips_driver.interfaces import data_monitors; print(data_monitors.plugins)"

You should see output without errors.

## Development Setup

For development and contributing:

    Clone and install
    =================
    git clone https://github.com/biosafetylvl5/geoips_driver.git
    cd geoips_driver
    poetry install --all-extras

    Install pre-commit hooks
    ========================
    poetry run pre-commit install

    Run tests to verify
    ===================
    poetry run pytest

    Run linters
    ===========
    poetry run ruff check .
    poetry run mypy src

See :doc:`../developer-guide/contributing` for more details.

### Using Dev Containers (VS Code)

The repository includes a dev container configuration:

1.  Install Docker Desktop and VS Code with Remote-Containers extension
2.  Clone the repository and open in VS Code
3.  When prompted, click "Reopen in Container"
4.  Everything is pre-configured and ready to use

## Troubleshooting

**Import Error: No module named 'geoips\_driver'**

Make sure you've activated your virtual environment:

    Poetry
    ======
    poetry shell

    venv
    ====
    source venv/bin/activate

**RabbitMQ Connection Failed**

Check that RabbitMQ is running:

    Docker
    ======
    docker ps | grep rabbitmq

    System service
    ==============
    sudo systemctl status rabbitmq-server

**Permission Denied Errors**

On Linux, you may need to add your user to the docker group:

    sudo usermod -aG docker $USER
    Log out and back in for changes to take effect
    ==============================================

**Python Version Issues**

Lazy Lemon requires Python 3.11+. Check your version:

    python --version

Use pyenv or conda to install a compatible version if needed.

## Next Steps

-   `` `quick-start ``\` - Create your first file watcher service
-   `` `configuration-basics ``\` - Learn about YAML configuration
-   `` `concepts ``\` - Understand core concepts

# Courier

![Tests](.github/badges/tests-badge.svg)
![Coverage](.github/badges/coverage-badge.svg)
![Mypy](.github/badges/mypy-badge.svg)
![Ruff](.github/badges/ruff-badge.svg)
![Install](.github/badges/install-badge.svg)
![CSpell](.github/badges/cspell-badge.svg)

Courier is a plugin-based, event-driven orchestration framework for building data processing pipelines. It watches for incoming data, groups it into jobs, and dispatches those jobs to processing workflows. It scales from a single laptop to a distributed cluster without changing your pipeline code.

While general-purpose, Courier ships with extra tooling for geolocation data — satellite instrument configs, metadata extraction, and integration with [GeoIPS](https://github.com/NRLMMD-GEOIPS/geoips).

## Design Philosophy

**Plugin-based.** Data monitors, job builders, and dispatchers are all plugins that conform to a simple protocol. Swap a filesystem watcher for a RabbitMQ consumer, or a serial dispatcher for a SLURM submitter, without touching the rest of your pipeline.

**Event-driven.** Plugins communicate through message queues and do not share state[^1]. When a monitor detects a file, it emits an event. A job builder consumes that event, groups files, and emits a job. A dispatcher picks up the job and runs it. Each stage is decoupled and independently scalable and duplicatable.

**Distributable.** The broker backend (AMQP, Redis, in-memory or many others) determines your deployment topology. Run everything in one process for development, or spread plugins across machines or even networks for production.

**Observable by default.** Every plugin exposes health checks and Prometheus metrics. Structured logs carry correlation IDs from file arrival to final product. Optional Loki and Grafana integration for centralized monitoring.

## How It Works

Courier runs a central `Service` that coordinates three stages of plugins through a message broker:

```
[Data Monitor] → detects new files, emits events
       ↓ (broker queue)
[Job Builder]  → groups files into complete jobs
       ↓ (broker queue)
[Dispatcher]   → executes the processing workflow
```

Each plugin runs in its own thread with independent health monitoring and automatic restart on failure. Configuration is validated at startup with Pydantic — not halfway through a run.

## Quick Start

```bash
pip install data-courier
```

### Running the service

Point Courier at a YAML config and start it:

```bash
courier run --config my_config.yaml
```

Validate your config before running:

```bash
courier validate --config my_config.yaml
```

### Writing a data monitor

Subclass `DataMonitorBasePlugin` and implement `find_file` as a generator that yields `File` objects. The base class handles threading, metadata enrichment, and queue emission for you.

```python
from collections.abc import Generator
from pathlib import Path

from courier.interfaces.module_based.data_monitors import DataMonitorBasePlugin
from courier.service import Service
from courier.types.file import File

interface = "data_monitors"
family = "standard"
name = "my_monitor"


class MyMonitor(DataMonitorBasePlugin):
    name = "my-monitor"
    version = "1.0.0"

    def __init__(self, service: Service, config: dict) -> None:
        super().__init__(service, config)
        self.watch_dir = Path(config["path"])

    def find_file(self) -> Generator[File, None, None]:
        # Yield File objects as they appear — the base class
        # handles metadata, emission, and metrics automatically.
        for path in self.watch_dir.iterdir():
            if path.is_file():
                yield File(file=path, hostname="localhost")
```

## Satellite Data Support

Courier ships with YAML configs for common satellite instruments including GOES-16/18/19 ABI, Himawari-9 AHI, GK-2A AMI, and Meteosat SEVIRI. These configs define file-matching patterns, expected file counts per scan, and metadata extraction rules — so Courier knows when a complete observation has arrived and how to label it.

## Development

```bash
pip install -e .[doc,lint,test]
pre-commit install
pre-commit run --all-files
```

Python 3.11–3.14. Strict mypy.

## License

See [LICENSE](LICENSE) for details.

[^1]: Except for rare edge cases for high availability deployments on clusters.

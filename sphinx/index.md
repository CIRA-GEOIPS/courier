# Courier Documentation

[![Tests](https://github.com/biosafetylvl5/courier/workflows/Tests/badge.svg)](https://github.com/biosafetylvl5/courier)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org)

**Courier** (`courier` CLI, Python package `data-courier`) is a plugin-based
mid-level orchestration framework. It watches for data files, groups them
into processing jobs, and dispatches them to workflows — in real time, on
a schedule, or for historical reprocessing. It excels at geolocated data
processing with built-in metadata extraction for common satellite filename
conventions.

## Who Should Use Courier

Courier is designed for:

- **Data center operators** managing automated data workflows
- **Researchers** requiring timely processing of observations
- **Developers** building custom real-time data processing systems
- **GeoIPS users** who need near real-time processing capabilities for geolocated data

```{toctree}
:maxdepth: 2
:caption: Getting Started

getting-started/installation
getting-started/quick-start
getting-started/init
getting-started/configuration
```

```{toctree}
:maxdepth: 2
:caption: Tutorials

tutorials/01-simple-file-watcher
tutorials/02-docker-swarm-cluster
```

```{toctree}
:maxdepth: 2
:caption: Operations

operations/high-availability
operations/tracing
```

```{toctree}
:maxdepth: 2
:caption: Concepts

concepts/index
```

```{toctree}
:maxdepth: 2
:caption: Contribute

contribute/code-style
contribute/writing-a-plugin
```

```{toctree}
:maxdepth: 2
:caption: API Reference

api-reference/index
```

## Indices and tables

- {ref}`genindex`
- {ref}`modindex`
- {ref}`search`

[GitHub Repository](https://github.com/biosafetylvl5/courier)

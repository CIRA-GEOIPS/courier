# Lazy Lemon Documentation

[![Tests](https://github.com/biosafetylvl5/courier/workflows/Tests/badge.svg)](https://github.com/biosafetylvl5/courier)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org)

Welcome to Lazy Lemon's documentation! Lazy Lemon is a framework
for running software jobs.
It is designed for real-time processing, batch processing, or historical reprocessing.

It is general purpose, but has functionality that makes it especially useful for
geolocated data processing.

## Quick Links

- {doc}`getting-started/installation` - Get Lazy Lemon installed
- {doc}`getting-started/quick-start` - Your first file watcher in 5 minutes
- {doc}`tutorials/01-simple-file-watcher` - Step-by-step tutorials

## What is Lazy Lemon?

Lazy Lemon is a plugin-based mid-level orchestration framework that enables turning
"lemons" (random data processing software) into .... whatever you want!
It watches for new data files, groups them into processing jobs, and dispatches them to
workflows or custom scripts.

It also serves as a layer of abstraction between general software and
higher level orchestration software like Kubernetes, Docker Swarm and others.

## Who Should Use This?

Lazy Lemon is designed for:

- **Data center operators** managing automated data workflows
- **Researchers** requiring timely processing of observations
- **Developers** building custom real-time data processing systems
- **GeoIPS users** who need near real-time processing capabilities for geolocated data

```{toctree}
:maxdepth: 2
:caption: Getting Started

getting-started/installation
getting-started/init
getting-started/quick-start
getting-started/configuration
```

```{toctree}
:maxdepth: 2
:caption: Tutorials

tutorials/01-simple-file-watcher
```

```{toctree}
:maxdepth: 2
:caption: Operations

operations/high-availability
```

```{toctree}
:maxdepth: 2
:caption: Contribute

contribute/code-style
concepts/index
```

```{toctree}
:maxdepth: 2
:caption: API Reference

api-reference/service
api-reference/plugins
api-reference/types
api-reference/utils
api-reference/interfaces
```

## Indices and tables

- {ref}`genindex`
- {ref}`modindex`
- {ref}`search`

[GitHub Repository](https://github.com/biosafetylvl5/courier)

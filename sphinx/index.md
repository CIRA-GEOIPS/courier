# GeoIPS Driver Documentation

!`Tests <https://github.com/biosafetylvl5/geoips_driver/workflows/Tests/badge.svg>`\_
!`Python 3.11+ <https://img.shields.io/badge/python-3.11+-blue.svg>`\_

Welcome to GeoIPS Driver's documentation! GeoIPS Driver extends the
GeoIPS framework with near real-time processing capabilities, enabling
automated satellite data processing workflows as data arrives.

## Quick Links

-   `` `getting-started/installation ``\` - Get GeoIPS Driver installed
-   `` `getting-started/quick-start ``\` - Your first file watcher in 5
    minutes
-   `` `tutorials/01-simple-file-watcher ``\` - Step-by-step tutorials
-   `` `user-guide/architecture ``\` - Complete usage guide
-   `` `developer-guide/plugin-development ``\` - Plugin development

## What is GeoIPS Driver?

GeoIPS Driver is a plugin-based orchestration framework that enables
near real-time satellite data processing using GeoIPS. It monitors for
new data files, groups them into processing jobs, and dispatches them to
GeoIPS workflows or custom scripts.

**Key capabilities:**

-   **Automated file monitoring** - Watch directories for new satellite
    data
-   **Intelligent job building** - Group related files for batch
    processing
-   **Flexible dispatching** - Execute bash scripts, GeoIPS workflows,
    or custom code
-   **Production-ready** - Prometheus monitoring, Loki logging,
    automatic restarts
-   **Extensible** - Plugin architecture for custom behavior

<!-- -->

    Example: Watch for GOES-18 data and process it
    ==============================================
    apiVersion: geoips_driver/v1
    kind: Service
    name: goes18-realtime
    spec:
      run:
        - monitor:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: /data/goes18
              metadata-tools: [goes18_abi]
        - build:
            kind: job_builder  
            name: DummyJobBuilder
        - process:
            kind: dispatcher
            name: serial_bash
            config:
              bash_script: |
                geoips run single_source {file}

## Who Should Use This?

GeoIPS Driver is designed for:

-   **GeoIPS users** who need near real-time processing capabilities
-   **Data center operators** managing automated satellite data
    workflows
-   **Researchers** requiring timely processing of satellite
    observations
-   **Developers** building custom real-time data processing systems

You should be familiar with:

-   What GeoIPS is and how it processes satellite data
-   Satellite data file formats (NetCDF, HDF5, etc.)
-   Basic Python programming
-   Docker or Kubernetes (for deployment)

getting-started/installation getting-started/quick-start
getting-started/configuration-basics getting-started/concepts

tutorials/01-simple-file-watcher tutorials/02-adding-metadata
tutorials/03-custom-job-builder tutorials/04-bash-dispatcher
tutorials/05-geoips-workflow-dispatcher
tutorials/06-multi-satellite-monitor
tutorials/07-monitoring-with-prometheus
tutorials/08-production-deployment tutorials/09-error-handling
tutorials/10-testing-plugins

user-guide/architecture user-guide/services user-guide/plugins
user-guide/configuration user-guide/metadata-matching
user-guide/monitoring user-guide/deployment user-guide/troubleshooting

developer-guide/architecture-deep-dive
developer-guide/plugin-development developer-guide/testing
developer-guide/contributing developer-guide/code-style
developer-guide/extending-interfaces

api-reference/service api-reference/plugins api-reference/types
api-reference/utils api-reference/interfaces

reference/configuration-schema reference/plugin-catalog
reference/metrics-reference reference/queue-reference reference/faq

## Indices and tables

-   `` `genindex ``\`
-   `` `modindex ``\`
-   `` `search ``\`

`GitHub Repository <https://github.com/biosafetylvl5/geoips_driver>`\_

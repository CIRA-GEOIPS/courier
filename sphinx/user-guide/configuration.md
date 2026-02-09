# Configuration Reference

Complete reference for GeoIPS Driver configuration files.

## Configuration File Structure

Every GeoIPS Driver service configuration follows this structure:

    apiVersion: geoips_driver/v1
    kind: Service
    name: service-name
    description: Brief description.
    docstring: |
      Optional multi-line
      documentation.

    spec:
      service_namespace: namespace
      heartbeat_interval: 30
      rabbitmq:
        host: localhost
        port: 5672
        username: admin
        password: password
      run:
        - step_name:
            kind: plugin_type
            name: plugin_name
            config:
              key: value

## Metadata Fields

### apiVersion

Type  
String

Required  
Yes

Default  
None

Valid Values  
`geoips_driver/v1`

Specifies the configuration API version.

    apiVersion: geoips_driver/v1

### kind

Type  
String

Required  
Yes

Default  
None

Valid Values  
`Service`

Defines the configuration type.

    kind: Service

### name

Type  
String

Required  
Yes

Default  
None

Pattern  
`^`a-z0-9 &lt;\[-a-z0-9\]\*\[a-z0-9\]&gt;`_?$`

Unique identifier for the service.

Rules  
-   Must start with lowercase letter or digit

-   Can contain lowercase letters, digits, and hyphens
-   Must end with lowercase letter or digit
-   Maximum length: 63 characters

<!-- -->

    name: goes18-full-disk-processor

### description

Type  
String

Required  
Yes

Default  
None

Short human-readable description (single sentence preferred).

    description: Processes GOES-18 full-disk imagery for weather products.

### docstring

Type  
String (multi-line)

Required  
No

Default  
None

Detailed documentation about the service.

    docstring: |
      This service monitors GOES-18 ABI Full-Disk data,
      groups files by scan time, and generates imagery products.

      Products include:
      - Infrared imagery
      - Visible imagery
      - RGB composites

## Service Specification

### service\_namespace

Type  
String

Required  
Yes

Default  
None

Namespace for isolating service resources (queues, metrics labels).

    spec:
      service_namespace: production

Examples  
-   `production`, `staging`, `development`

-   `goes18_conus`, `himawari_fulldisk`
-   `realtime`, `archive_reprocessing`

### heartbeat\_interval

Type  
Integer

Required  
No

Default  
30

Units  
Seconds

Min  
1

Max  
3600

Frequency for health check heartbeats.

    spec:
      heartbeat_interval: 30

Recommendations  
-   Development: 10-30 seconds

-   Production: 30-60 seconds
-   Critical services: 10 seconds

### RabbitMQ Configuration

Connection parameters for message broker.

    spec:
      rabbitmq:
        host: rabbitmq.example.com
        port: 5672
        username: geoips_user
        password: ${RABBITMQ_PASSWORD}

host  
**Type**: String

Required  
Yes

Default  
None

Hostname or IP address of RabbitMQ server.

port  
**Type**: Integer

Required  
No

Default  
5672

Valid Values  
1-65535

AMQP port (5672 for non-TLS, 5671 for TLS).

username  
**Type**: String

Required  
Yes

Default  
None

Authentication username.

password  
**Type**: String

Required  
Yes

Default  
None

Authentication password. Use environment variables for security.

## Plugin Pipeline Configuration

The `run` section defines the processing pipeline as an ordered list of
plugins.

### Structure

    spec:
      run:
        - step_identifier:
            kind: plugin_type
            name: plugin_implementation
            config:
              parameter: value

step\_identifier  
**Type**: String

Required  
Yes

Pattern  
`^[a-z][a-z0-9_]*$`

Unique name for this pipeline step.

kind  
**Type**: String

Required  
Yes

Valid Values  
`data_monitor`, `job_builder`, `dispatcher`

Plugin type/interface.

name  
**Type**: String

Required  
Yes

Specific plugin implementation name.

config  
**Type**: Object or null

Required  
Yes

Plugin-specific configuration (can be `null`).

### Data Monitor Configuration

    - monitor_goes18:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: /data/goes18/incoming
          metadata-tools:
            - goes18_abi
            - goes16_abi

Common parameters  
**path**:

Type  
String

Required  
Yes (for file\_system\_poller\_watchdog)

Directory path to monitor for files.

metadata-tools  
**Type**: List of strings

Required  
Yes

List of metadata configuration names to apply.

### Job Builder Configuration

    - build_jobs:
        kind: job_builder
        name: ChannelGroupBuilder
        config:
          timeout_seconds: 600
          platform: goes18

Common parameters  
**timeout\_seconds**:

Type  
Integer

Required  
No

Default  
Varies by plugin

Units  
Seconds

Maximum time to wait for job completion.

### Dispatcher Configuration

Bash Script Dispatcher  

> -   process:  
>     kind: dispatcher name: serial\_bash config: bash\_script: |
>     \#!/bin/bash set -e echo "Processing {file}" geoips run
>     single\_source {file}

bash\_script  
**Type**: String (multi-line)

Required  
Yes

Bash script to execute for each job. Template variables available:

-   `{file}` - File path(s)
-   `{platform}` - Platform name
-   `{sensor}` - Sensor name
-   `{sector}` - Sector name
-   `{timestamp}` - Timestamp

## Environment Variables

### Syntax

    ${VARIABLE_NAME}           # Required variable
    ${VARIABLE_NAME:-default}  # Optional with default value

### Examples

    rabbitmq:
      host: ${RABBITMQ_HOST:-localhost}
      port: ${RABBITMQ_PORT:-5672}
      username: ${RABBITMQ_USER}
      password: ${RABBITMQ_PASSWORD}

### Setting Variables

Shell  

> export RABBITMQ\_PASSWORD=secret geoips-driver run config.yaml

Docker  

> docker run -e RABBITMQ\_PASSWORD=secret ...

Kubernetes  

> env:  
> -   name: RABBITMQ\_PASSWORD valueFrom: secretKeyRef: name:
>     rabbitmq-secret key: password

## Complete Examples

### Minimal Configuration

    apiVersion: geoips_driver/v1
    kind: Service
    name: minimal-service
    description: Minimal configuration example.

    spec:
      service_namespace: test
      rabbitmq:
        host: localhost
        port: 5672
        username: admin
        password: admin
      run:
        - monitor:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: /tmp/data
              metadata-tools: [goes18_abi]
        - build:
            kind: job_builder
            name: DummyJobBuilder
            config: null
        - process:
            kind: dispatcher
            name: serial_bash
            config:
              bash_script: "echo {file}"

### Production Configuration

    apiVersion: geoips_driver/v1
    kind: Service
    name: goes18-production
    description: Production GOES-18 Full-Disk processor.

    docstring: |
      Processes GOES-18 ABI Full-Disk Level 1B data in near real-time.
      Generates multiple imagery products for weather forecasting.

    spec:
      service_namespace: goes18_production
      heartbeat_interval: 30

      rabbitmq:
        host: ${RABBITMQ_HOST}
        port: ${RABBITMQ_PORT:-5672}
        username: ${RABBITMQ_USER}
        password: ${RABBITMQ_PASSWORD}

      run:
        - monitor_fulldisk:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: /data/goes18/fulldisk
              metadata-tools:
                - goes18_abi

        - group_by_scan:
            kind: job_builder
            name: ChannelGroupBuilder
            config:
              timeout_seconds: 600

        - generate_products:
            kind: dispatcher
            name: geoips_workflow
            config:
              products:
                - Infrared-Gray
                - True-Color
              output_dir: /products/goes18

## Validation

### Command Line

    Validate configuration
    ======================
    geoips-driver validate config.yaml

    Successful validation
    =====================
    ✅ Config valid

    Failed validation
    =================
    ❌ Invalid config: Field 'name' is required

### Python API

    from pathlib import Path
    from geoips_driver.pydantic.service import ServiceConfig

    Load and validate
    =================
    config = ServiceConfig.from_yaml(Path("config.yaml"))

## Best Practices

1.  **Use environment variables for secrets**

    Never commit passwords in configuration files.

2.  **Validate before deployment**

    Always run `geoips-driver validate` before deploying.

3.  **Use meaningful names**

    Service and step names should be descriptive.

4.  **Document your configuration**

    Use `description` and `docstring` fields.

5.  **Version control your configs**

    Store configurations in git for change tracking.

6.  **Use appropriate namespaces**

    Separate environments with different namespaces.

7.  **Set resource limits**

    Especially important in Kubernetes deployments.

## See Also

-   `` `services ``\` - Service lifecycle and management
-   :doc:`../getting-started/configuration-basics` - Configuration
    tutorial
-   :doc:`../reference/configuration-schema` - Complete schema reference

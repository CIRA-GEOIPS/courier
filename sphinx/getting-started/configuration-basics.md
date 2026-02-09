# Configuration Basics

This guide explains GeoIPS Driver's YAML configuration format and how to
structure service configurations.

## Configuration File Structure

Every GeoIPS Driver service is defined by a YAML configuration file with
this structure:

    apiVersion: geoips_driver/v1
    kind: Service
    name: service-name
    description: Human-readable description.
    docstring: |
      Optional multi-line documentation
      about this service configuration.

    spec:
      service_namespace: namespace
      heartbeat_interval: 30
      rabbitmq:
        host: localhost
        port: 5672
        username: admin
        password: admin_password
      run:
        - plugin1:
            kind: plugin_kind
            name: plugin_name
            config:
              # plugin-specific configuration

Let's examine each section in detail.

## Metadata Fields

### apiVersion

Specifies the configuration schema version:

    apiVersion: geoips_driver/v1

Currently, only `geoips_driver/v1` is supported.

### kind

Defines the type of configuration. For services, this is always:

    kind: Service

### name

Unique identifier for your service. Must be unique across your
deployment.

**Rules:**

-   Alphanumeric and hyphens only
-   Must start with a letter
-   Used in Prometheus metrics and logs

<!-- -->

    name: goes18-full-disk-processor

### description

Short human-readable description (required):

    description: Processes GOES-18 full-disk imagery for NWS products.

**Style guidelines:**

-   Start with a capital letter
-   End with a period
-   One sentence preferred

### docstring (Optional)

Multi-line documentation for the service:

    docstring: |
      This service monitors for GOES-18 ABI Full-Disk Level 1B data,
      groups files by scan time, and generates Infrared and Visible
      imagery products for dissemination to NWS.

      Processing includes:
      - Remapping to Mercator projection
      - Quality control and cloud masking
      - Product generation for 16 ABI channels

## Service Specification (spec)

The `spec` section contains the core service configuration.

### service\_namespace

Namespace for isolating this service's resources:

    spec:
      service_namespace: goes18_production

**Purpose:**

-   Prevents queue name collisions
-   Enables multiple services running concurrently
-   Groups related services

**Examples:**

-   `production` - Production environment
-   `testing` - Test environment
-   `goes18_conus` - GOES-18 CONUS processing
-   `himawari_fulldisk` - Himawari full-disk processing

### heartbeat\_interval

Frequency (in seconds) for sending health check metrics:

    spec:
      heartbeat_interval: 30

**Recommendations:**

-   Development: 10-30 seconds
-   Production: 30-60 seconds
-   Critical services: 10 seconds
-   Low-priority: 60-300 seconds

### RabbitMQ Configuration

Connection details for the message broker:

    spec:
      rabbitmq:
        host: localhost       # RabbitMQ hostname/IP
        port: 5672           # AMQP port (default 5672)
        username: admin      # Authentication username
        password: admin_pass # Authentication password

**Security best practices:**

-   Use environment variables for credentials
-   Enable TLS for production (port 5671)
-   Create dedicated users per service
-   Use strong passwords

**Example with environment variables:**

    rabbitmq:
      host: ${RABBITMQ_HOST:-localhost}
      port: ${RABBITMQ_PORT:-5672}
      username: ${RABBITMQ_USER:-admin}
      password: ${RABBITMQ_PASS:-password}

## Plugin Pipeline (run)

The `run` section defines the processing pipeline as a list of plugins:

    spec:
      run:
        - step_name:
            kind: plugin_type
            name: plugin_implementation
            config:
              # plugin configuration

### Step Structure

Each step has:

1.  **Identifier** (`step_name`): Unique name for this step
2.  **kind**: Plugin type (data\_monitor, job\_builder, dispatcher)
3.  **name**: Specific plugin implementation
4.  **config**: Plugin-specific configuration (can be `null`)

**Example:**

    run:
      - watch_for_files:           # Step identifier
          kind: data_monitor        # Plugin type
          name: file_system_poller_watchdog  # Implementation
          config:                   # Plugin config
            path: /data/goes18
            metadata-tools:
              - goes18_abi

### Data Monitor Configuration

Monitors for new data files:

    - monitor_goes18:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: /data/satellite/goes18     # Directory to watch
          metadata-tools:                   # Metadata extractors
            - goes18_abi                    # GOES-18 ABI patterns
            - goes16_abi                    # Also match GOES-16 (optional)

**Common configurations:**

-   `path`: Directory to monitor (required)
-   `metadata-tools`: List of metadata configuration names

### Job Builder Configuration

Groups files into processing jobs:

    - build_jobs:
        kind: job_builder
        name: DummyJobBuilder
        config: null  # No configuration needed for DummyJobBuilder

**Configuration depends on the specific job builder plugin.**

### Dispatcher Configuration

Executes processing jobs:

**Bash Script Dispatcher:**

    - execute:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            #!/bin/bash
            set -e  # Exit on error

            echo "Processing {file}"

            # Your processing logic here
            geoips run single_source {file} \
              --reader_name abi_netcdf \
              --product_name Infrared-Gray

**Template Variables:**

-   `{file}` - Path to the file being processed
-   More variables may be available depending on the dispatcher

## Complete Example

Here's a complete, production-ready configuration:

    apiVersion: geoips_driver/v1
    kind: Service
    name: goes18-conus-processor
    description: Process GOES-18 CONUS scans for rapid refresh products.

    docstring: |
      Monitors /data/goes18/conus for new GOES-18 ABI CONUS Level 1B files.
      Groups files by scan time (16 channels per scan) and executes GeoIPS
      workflows to generate Infrared, Visible, and RGB composite products.

      Products are remapped to Mercator projection and delivered to
      /products/goes18/conus for dissemination.

    spec:
      service_namespace: goes18_conus_production
      heartbeat_interval: 30

      rabbitmq:
        host: rabbitmq.example.com
        port: 5672
        username: goes18_service
        password: ${RABBITMQ_PASSWORD}

      run:
        # Monitor for CONUS files
        - watch_conus:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: /data/goes18/conus
              metadata-tools:
                - goes18_abi

        # Group 16 channels per scan
        - group_by_scan:
            kind: job_builder
            name: SectorTimeJobBuilder  # Custom plugin
            config:
              sector: CONUS
              num_files_per_job: 16
              timeout_seconds: 300

        # Execute GeoIPS workflow
        - process_scan:
            kind: dispatcher
            name: geoips_workflow
            config:
              workflow: conus_rapid_refresh
              output_dir: /products/goes18/conus
              cleanup_inputs: true

## Configuration Validation

Validate your configuration before running:

    Using the dummy CLI
    ===================
    poetry run python -m geoips_driver.dummy_cli validate config.yaml

    Or with the full CLI (when available)
    =====================================
    geoips-driver validate config.yaml

**Successful validation:**

    ✅ Config valid

**Failed validation:**

    ❌ Invalid config: Field 'heartbeat_interval' must be a positive integer

    At line 8:
      heartbeat_interval: -5

## Common Configuration Patterns

### Multiple Data Monitors

Watch multiple directories or satellites:

    run:
      - watch_goes18:
          kind: data_monitor
          name: file_system_poller_watchdog
          config:
            path: /data/goes18
            metadata-tools: [goes18_abi]

      - watch_goes16:
          kind: data_monitor
          name: file_system_poller_watchdog
          config:
            path: /data/goes16
            metadata-tools: [goes16_abi]

      - build_jobs:
          kind: job_builder
          name: DummyJobBuilder
          config: null

      - process:
          kind: dispatcher
          name: serial_bash
          config:
            bash_script: |
              echo "Processing {file}"

### Test Configuration

Minimal configuration for testing:

    apiVersion: geoips_driver/v1
    kind: Service
    name: test-service
    description: Test configuration.

    spec:
      service_namespace: test
      heartbeat_interval: 10
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
              path: /tmp/test_data
              metadata-tools: [goes18_abi]
        - build:
            kind: job_builder
            name: DummyJobBuilder
            config: null
        - execute:
            kind: dispatcher
            name: serial_bash
            config:
              bash_script: "echo {file}"

## Best Practices

1.  **Use descriptive names**

    \`\`\`yaml

    name: goes18-fulldisk-ir-processor \# Good name: service-1 \# Bad

<!-- -->

    2. **Document your service**

       Include a docstring explaining what the service does


    3. **Keep configuration DRY**

       Use environment variables for repeated values


    4. **Validate before deploying**

       Always validate configuration files before running in production


    5. **Version your configs**

       Store configurations in version control (git)


    6. **Use appropriate heartbeat intervals**

       Balance monitoring granularity with system load


    7. **Secure credentials**

       Never commit passwords; use environment variables or secrets management

    Next Steps
    ----------


    * :doc:`concepts` - Understand services, plugins, and queues

    * :doc:`../tutorials/02-adding-metadata` - Configure metadata extraction

    * :doc:`../user-guide/configuration` - Complete configuration reference

    * :doc:`../reference/configuration-schema` - Full schema documentation

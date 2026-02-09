# Tutorial 6: Multi-Satellite Monitoring

**Level:** Intermediate | **Time:** 30 minutes

Learn how to configure GeoIPS Driver to monitor and process data from
multiple satellite platforms simultaneously. This tutorial demonstrates
watching GOES-18, GOES-16, and Himawari-9 data in a single service.

## Learning Objectives

By the end of this tutorial, you will:

-   Configure multiple data monitors in one service
-   Create metadata configs for different satellites
-   Handle mixed satellite data streams
-   Route satellite-specific processing
-   Monitor multi-satellite operations
-   Organize outputs by satellite and time

## Prerequisites

-   Completed `` `01-simple-file-watcher ``<span class="title-ref">
    through
    :doc:</span><span class="title-ref">03-custom-job-builder</span>\`
-   Familiarity with multiple satellite data formats
-   Understanding of metadata extraction patterns
-   Access to data from multiple satellites (or ability to create test
    files)

## Multi-Satellite Architecture

**Challenge:** Different satellites have different:

-   File naming conventions
-   Channel counts and wavelengths
-   Scan patterns and timing
-   Spatial coverage

**Solution:** Use multiple data monitors with satellite-specific
metadata configs.

    ┌──────────────────┐
    │  Data Monitor 1  │──▶ GOES-18 files ──┐
    │   (GOES-18)      │                    │
    └──────────────────┘                    │
                                            ▼
    ┌──────────────────┐              ┌──────────┐
    │  Data Monitor 2  │──▶ GOES-16 ─▶│ Job      │
    │   (GOES-16)      │     files     │ Builder  │
    └──────────────────┘              └──────────┘
                                            │
    ┌──────────────────┐                    ▼
    │  Data Monitor 3  │──▶ Himawari ──┐
    │   (Himawari-9)   │     files      │
    └──────────────────┘                │
                                        │
                                   ┌────────────┐
                                   │ Dispatcher │
                                   └────────────┘

## Step 1: Create Multi-Satellite Metadata Configs

First, ensure metadata configs exist for each satellite.

**GOES-18** (already created in Tutorial 2):

`plugins/yaml/data_monitor_configs/goes18_abi.yaml`

**GOES-16** (similar to GOES-18):

`plugins/yaml/data_monitor_configs/goes16_abi.yaml`:

    apiVersion: geoips_driver/v1
    name: goes16_abi
    interface: data_monitor_configs
    family: standard
    description: Metadata for GOES-16 ABI L1B data files.

    spec:
      file-metadata:
        goes16_abi_l1b:
          platform: goes16
          sensor: abi
          level: L1B
          date: 's(?P<YYYY>\d{4})(?P<JJJ>\d{3})(?P<HH>\d{2})(?P<NN>\d{2})\d{2}'
          match:
            - 'OR_ABI-L1b-Rad.-M6C[01][1-6]_G16_s\d{4}\d{3}\d{6}_e\d{4}\d{3}\d{6}_c\d{4}\d{3}\d{6}\.nc'

        full-disk:
          sector: Full-Disk
          num_expected: 16
          match:
            - '.*RadF.*M6C[01][1-6].*'

        conus:
          sector: CONUS
          num_expected: 16
          match:
            - '.*RadC.*M6C[01][1-6].*'

**Himawari-9 AHI:**

`plugins/yaml/data_monitor_configs/himawari9_ahi.yaml`:

    apiVersion: geoips_driver/v1
    name: himawari9_ahi
    interface: data_monitor_configs
    family: standard
    description: Metadata for Himawari-9 AHI data files.

    spec:
      file-metadata:
        himawari9_ahi_l1b:
          platform: himawari9
          sensor: ahi
          level: L1B
          # Himawari naming: HS_H09_20240115_1200_B01_FLDK_R20_S0110.DAT
          date: 'HS_H09_(?P<YYYY>\d{4})(?P<MM>\d{2})(?P<DD>\d{2})_(?P<HH>\d{2})(?P<NN>\d{2})'
          match:
            - 'HS_H09_\d{8}_\d{4}_B[01][0-9]_FLDK_.*'

        full-disk:
          sector: Full-Disk
          num_expected: 16
          match:
            - '.*_FLDK_.*'

        japan-area:
          sector: Japan
          num_expected: 16
          match:
            - '.*_JP\d{2}_.*'

    [``
    Step 2: Create Basic Multi-Satellite Service
    --------------------------------------------

    ``tutorial06-multi-sat/config_basic.yaml``:

    ```yaml

    apiVersion: geoips_driver/v1
    kind: Service
    name: multi-satellite-monitor
    description: Monitor GOES-18, GOES-16, and Himawari-9 simultaneously.

    spec:
      service_namespace: multi_sat
      heartbeat_interval: 30

      rabbitmq:
        host: localhost
        port: 5672
        username: admin
        password: admin_test

      run:
        # GOES-18 Full-Disk monitor
        - watch_goes18:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: ./data/goes18
              metadata-tools:
                - goes18_abi

        # GOES-16 CONUS monitor
        - watch_goes16:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: ./data/goes16
              metadata-tools:
                - goes16_abi

        # Himawari-9 Full-Disk monitor
        - watch_himawari:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: ./data/himawari9
              metadata-tools:
                - himawari9_ahi

        # Single job builder handles all satellites
        - build_jobs:
            kind: job_builder
            name: DummyJobBuilder
            config: null

        # Dispatcher routes by platform
        - process:
            kind: dispatcher
            name: serial_bash
            config:
              bash_script: |
                #!/bin/bash
                echo "Processing file from: {platform}"
                echo "File: {file}"

This configuration:

-   Watches three separate directories
-   Uses appropriate metadata configs for each
-   Processes all satellites through the same pipeline

## Step 3: Create Satellite-Specific Job Builder

Group files separately for each satellite:

`plugins/multi_sat_job_builder.py`:

    """Multi-satellite job builder with platform-specific grouping."""

    from typing import Any
    from geoips_driver.interfaces.module_based.job_builders import (
        Job,
        JobBuilder,
        JobGroup,
    )
    from geoips_driver.interfaces.module_based.service import Service
    from geoips_driver.types.file import File, FrozenFile

    interface = "job_builders"
    family = "standard"
    name = "multi_satellite_job_builder"


    class SatelliteJobGroup(JobGroup):
        """Job group for a specific satellite platform."""

        def __init__(self, platform: str, config: dict[str, Any]) -> None:
            super().__init__(f"{platform}_jobs", config)
            self.platform = platform.lower()
            self.timeout = config.get("timeout_seconds", 300)

        def file_is_relevant(self, file: File | FrozenFile) -> bool:
            """Only process files from this platform."""
            return (
                file.platform is not None
                and file.platform.lower() == self.platform
            )

        def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
            """Create job ID: {platform}_{sector}_{timestamp}."""
            if not all([file.platform, file.sector, file.timestamp]):
                return []

            platform = file.platform.lower()
            sector = file.sector.lower().replace(" ", "-")
            timestamp = file.timestamp.strftime("%Y%j%H%M%S")

            return [f"{platform}_{sector}_{timestamp}"]


    class MultiSatelliteJobBuilder(JobBuilder):
        """Builds jobs for multiple satellites independently."""

        name = "MultiSatelliteJobBuilder"
        version = "1.0.0"

        def __init__(self, service: Service, config: dict[str, Any]) -> None:
            super().__init__(service, config)

            # Create job group for each satellite
            satellites = config.get("satellites", ["goes18", "goes16", "himawari9"])

            self.job_groups = [
                SatelliteJobGroup(sat, config)
                for sat in satellites
            ]


    def call() -> None:
        """Raise error if called directly."""
        raise NotImplementedError("You cannot call this plugin directly.")

Use in configuration:

    - build_jobs:
        kind: job_builder
        name: MultiSatelliteJobBuilder
        config:
          satellites:
            - goes18
            - goes16
            - himawari9
          timeout_seconds: 600

## Step 4: Create Satellite-Specific Dispatcher

Route processing based on satellite platform:

`scripts/multi_sat_dispatcher.sh`:

    #!/bin/bash

    set -e
    set -u

    Configuration
    =============
    OUTPUT_BASE="./products"
    LOG_DIR="./logs"

    Logging
    =======
    log() {
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/processing.log"
    }

    Detect platform from filename
    =============================
    detect_platform() {
        local file="$1"

        if [[ "${file}" =~ _G18_ ]]; then
            echo "goes18"
        elif [[ "${file}" =~ _G16_ ]]; then
            echo "goes16"
        elif [[ "${file}" =~ HS_H09 ]]; then
            echo "himawari9"
        else
            echo "unknown"
        fi
    }

    Process GOES-18
    ===============
    process_goes18() {
        local files="$1"
        local output_dir="${OUTPUT_BASE}/goes18/$(date +%Y/%m/%d)"
        mkdir -p "${output_dir}"

        log "Processing GOES-18 files"

        geoips run single_source ${files} \
            --reader_name abi_netcdf \
            --product_name True-Color \
            --output_formatter imagery_clean \
            --filename_formatter geoips_fname \
            --output_dir "${output_dir}"
    }

    Process GOES-16
    ===============
    process_goes16() {
        local files="$1"
        local output_dir="${OUTPUT_BASE}/goes16/$(date +%Y/%m/%d)"
        mkdir -p "${output_dir}"

        log "Processing GOES-16 files"

        geoips run single_source ${files} \
            --reader_name abi_netcdf \
            --product_name Visible-Gray \
            --output_formatter imagery_clean \
            --filename_formatter geoips_fname \
            --output_dir "${output_dir}"
    }

    Process Himawari-9
    ==================
    process_himawari9() {
        local files="$1"
        local output_dir="${OUTPUT_BASE}/himawari9/$(date +%Y/%m/%d)"
        mkdir -p "${output_dir}"

        log "Processing Himawari-9 files"

        geoips run single_source ${files} \
            --reader_name ahi_hsd \
            --product_name Infrared-Gray \
            --output_formatter imagery_clean \
            --filename_formatter geoips_fname \
            --output_dir "${output_dir}"
    }

    Main processing
    ===============
    main() {
        local files="$1"
        local first_file=$(echo "${files}" | awk '{print $1}')
        local platform=$(detect_platform "${first_file}")

        log "==========================================="
        log "Multi-Satellite Dispatcher"
        log "Platform: ${platform}"
        log "Files: ${files}"
        log "==========================================="

        case "${platform}" in
            goes18)
                process_goes18 "${files}"
                ;;
            goes16)
                process_goes16 "${files}"
                ;;
            himawari9)
                process_himawari9 "${files}"
                ;;
            *)
                log "ERROR: Unknown platform: ${platform}"
                exit 1
                ;;
        esac

        log "Processing complete for ${platform}"
    }

    main "{file}"

## Step 5: Organize Output by Satellite

Create a structured output directory:

    products/
    ├── goes18/
    │   └── 2024/
    │       └── 01/
    │           └── 15/
    │               ├── true_color/
    │               └── infrared/
    ├── goes16/
    │   └── 2024/
    │       └── 01/
    │           └── 15/
    │               └── visible/
    └── himawari9/
        └── 2024/
            └── 01/
                └── 15/
                    └── infrared/

## Step 6: Create Test Data Structure

    mkdir -p tutorial06-multi-sat/{data/{goes18,goes16,himawari9},products,logs,scripts}
    cd tutorial06-multi-sat

    Create test GOES-18 files
    =========================
    cd data/goes18
    for i in $(seq -f "%02g" 1 16); do
        touch "OR_ABI-L1b-RadF-M6C${i}_G18_s20240151200000_e20240151209310_c20240151209360.nc"
    done

    Create test GOES-16 files
    =========================
    cd ../goes16
    for i in $(seq -f "%02g" 1 16); do
        touch "OR_ABI-L1b-RadC-M6C${i}_G16_s20240151200000_e20240151209310_c20240151209360.nc"
    done

    Create test Himawari-9 files
    ============================
    cd ../himawari9
    for i in $(seq -f "%02g" 1 16); do
        touch "HS_H09_20240115_1200_B${i}_FLDK_R20_S0110.DAT"
    done

## Step 7: Monitor Multi-Satellite Operations

Create custom metrics for each satellite:

`plugins/multi_sat_metrics.py`:

    """Dispatcher with per-satellite metrics."""

    from prometheus_client import Counter
    from geoips_driver.interfaces.module_based.dispatchers import Dispatcher

    interface = "dispatchers"
    family = "standard"
    name = "multi_sat_metrics"


    class MultiSatMetricsDispatcher(Dispatcher):
        """Track metrics per satellite platform."""

        name = "multi_sat_metrics"
        version = "1.0.0"

        def __init__(self, service, config):
            super().__init__(service, config)

            # Per-satellite metrics
            self.files_by_platform = Counter(
                'files_processed_by_platform_total',
                'Files processed per platform',
                ['platform', 'sector']
            )

            self.products_by_platform = Counter(
                'products_generated_by_platform_total',
                'Products generated per platform',
                ['platform', 'product']
            )

        def get_execution_log(self, job):
            # Extract platform from job
            first_file = next(iter(job.files))
            platform = first_file.platform or "unknown"
            sector = first_file.sector or "unknown"

            # Increment metrics
            self.files_by_platform.labels(
                platform=platform,
                sector=sector
            ).inc(len(job.files))

            # Process job
            # ... (implementation) ...

            return logs

Prometheus queries:

    Files per platform
    ==================
    sum by (platform) (rate(files_processed_by_platform_total[5m]))

    Products by platform and type
    =============================
    sum by (platform, product) (products_generated_by_platform_total)

## Step 8: Handle Different Channel Counts

Different satellites have different channel counts:

-   GOES ABI: 16 channels
-   Himawari AHI: 16 channels
-   Meteosat SEVIRI: 12 channels

Create a flexible job builder:

    class FlexibleChannelJobGroup(JobGroup):
        """Handle different channel counts per satellite."""

        # Expected channels per platform
        PLATFORM_CHANNELS = {
            "goes18": 16,
            "goes16": 16,
            "himawari9": 16,
            "meteosat9": 12,
            "meteosat10": 12,
        }

        def ready(self, job: Job) -> bool:
            """Check if all expected channels received."""
            if not job.files:
                return False

            first_file = next(iter(job.files))
            platform = first_file.platform.lower()

            # Get expected count for this platform
            expected = self.PLATFORM_CHANNELS.get(platform, 16)

            return len(job.files) >= expected

## Step 9: Cross-Platform Product Generation

Generate comparison products from multiple satellites:

    Compare GOES-18 vs GOES-16 coverage
    ===================================
    compare_platforms() {
        local goes18_files="$1"
        local goes16_files="$2"

        log "Generating cross-platform comparison"

        # Generate side-by-side comparison
        geoips compare_platforms \
            --platform1 goes18 \
            --files1 "${goes18_files}" \
            --platform2 goes16 \
            --files2 "${goes16_files}" \
            --product_name Infrared-Gray \
            --output_dir ./products/comparisons
    }

## Step 10: Complete Production Configuration

`tutorial06-multi-sat/config_production.yaml`:

    apiVersion: geoips_driver/v1
    kind: Service
    name: multi-satellite-production
    description: Production multi-satellite monitoring and processing.

    docstring: |
      Monitors and processes data from multiple geostationary satellites:
      - GOES-18 (West): Full-Disk
      - GOES-16 (East): CONUS
      - Himawari-9 (JMA): Full-Disk

      Routes each platform to appropriate GeoIPS workflows with
      platform-specific product configurations.

    spec:
      service_namespace: multi_sat_production
      heartbeat_interval: 30

      rabbitmq:
        host: rabbitmq.example.com
        port: 5672
        username: multi_sat_processor
        password: ${RABBITMQ_PASSWORD}

      run:
        # GOES-18 Full-Disk (West CONUS, Pacific)
        - watch_goes18_fd:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: /data/goes18/fulldisk
              metadata-tools: [goes18_abi]

        # GOES-16 CONUS (East CONUS, Atlantic)
        - watch_goes16_conus:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: /data/goes16/conus
              metadata-tools: [goes16_abi]

        # Himawari-9 Full-Disk (Asia-Pacific)
        - watch_himawari9_fd:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: /data/himawari9/fulldisk
              metadata-tools: [himawari9_ahi]

        # Platform-specific job building
        - build_jobs:
            kind: job_builder
            name: MultiSatelliteJobBuilder
            config:
              satellites: [goes18, goes16, himawari9]
              timeout_seconds: 600

        # Process with platform awareness
        - process:
            kind: dispatcher
            name: multi_sat_metrics
            config:
              script_path: ./scripts/multi_sat_dispatcher.sh

## Testing Multi-Satellite Setup

    Start service
    =============
    export PYTHONPATH=./plugins:$PYTHONPATH
    geoips-driver run config_production.yaml

    In separate terminals, add files for each satellite
    ===================================================
    Terminal 2: GOES-18
    ===================
    cp test_data/goes18/* data/goes18/

    Terminal 3: GOES-16
    ===================
    cp test_data/goes16/* data/goes16/

    Terminal 4: Himawari-9
    ======================
    cp test_data/himawari9/* data/himawari9/

    Monitor processing
    ==================
    tail -f logs/processing.log

Check Prometheus metrics:

    curl http://localhost:8000/metrics | grep platform

## Common Challenges

**Time synchronization:**

Different satellites may have different file arrival patterns. Use
appropriate timeouts per platform.

**Storage management:**

Multiple satellites generate more data. Implement cleanup:

    Clean old products (older than 7 days)
    ======================================
    find products/ -type f -mtime +7 -delete

**Processing priority:**

If resources are limited, prioritize certain satellites:

    class PriorityJobGroup(JobGroup):
        PLATFORM_PRIORITY = {
            "goes18": 1,  # Highest
            "goes16": 2,
            "himawari9": 3,
        }

## What You Learned

✅ Multi-satellite monitoring configuration ✅ Platform-specific
metadata extraction ✅ Satellite-specific job building ✅ Routing by
platform in dispatchers ✅ Per-platform metrics and monitoring ✅
Organizing multi-satellite outputs ✅ Cross-platform comparisons

## Next Steps

-   `` `04-bash-dispatcher ``\` - Advanced dispatcher patterns
-   `` `07-monitoring-with-prometheus ``\` - Monitor multi-satellite ops
-   `` `09-error-handling ``\` - Handle platform-specific failures

## Challenge Exercises

1.  **Add Meteosat SEVIRI** - Include European satellites
2.  **Implement data fusion** - Combine multiple satellites
3.  **Create regional mosaics** - Stitch satellite coverage
4.  **Add latency tracking** - Monitor time from obs to product

## Complete Code

\`tutorial06-multi-satellite/
\](<https://github.com/biosafetylvl5/geoips_driver/tree/main/examples/tutorials/06-multi-satellite>)

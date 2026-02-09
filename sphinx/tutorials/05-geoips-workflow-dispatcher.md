# Tutorial 5: GeoIPS Workflow Integration

**Level:** Intermediate | **Time:** 35 minutes

Learn how to integrate GeoIPS Driver with GeoIPS workflows for automated
satellite imagery product generation. This tutorial shows real-world
integration between the near real-time driver and GeoIPS processing
workflows.

## Learning Objectives

By the end of this tutorial, you will:

-   Understand GeoIPS workflow structure
-   Create dispatchers that call GeoIPS workflows
-   Handle multi-file product generation
-   Manage GeoIPS output products
-   Implement error handling for GeoIPS failures
-   Monitor processing performance

## Prerequisites

-   Completed `` `01-simple-file-watcher ``<span class="title-ref">
    through
    :doc:</span><span class="title-ref">04-bash-dispatcher</span>\`
-   GeoIPS installed and configured
-   Familiarity with GeoIPS command-line interface
-   Understanding of GOES-18 ABI products

## Understanding GeoIPS Workflows

**GeoIPS** processes satellite data through workflows that:

1.  **Read** data files with a reader plugin
2.  **Apply** algorithms to generate products
3.  **Format** outputs for dissemination
4.  **Write** products to disk

**Common GOES-18 workflows:**

-   Single channel products (Infrared, Visible)
-   RGB composites (True Color, Day Cloud Phase)
-   Derived products (Cloud Top Height, Fire Detection)

**GeoIPS CLI:**

    geoips run single_source <files> \
        --reader_name abi_netcdf \
        --product_name Infrared-Gray \
        --output_formatter imagery_clean \
        --filename_formatter geoips_fname

    [``
    Step 1: Basic GeoIPS Integration
    --------------------------------

    Create a simple dispatcher that calls GeoIPS:

    ``tutorial05-geoips/config_basic.yaml``:

    ```yaml

    apiVersion: geoips_driver/v1
    kind: Service
    name: tutorial-05-geoips-basic
    description: Basic GeoIPS workflow integration.

    spec:
      service_namespace: tutorial05_basic
      heartbeat_interval: 30

      rabbitmq:
        host: localhost
        port: 5672
        username: admin
        password: admin_test

      run:
        - monitor:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: ./data/incoming
              metadata-tools:
                - goes18_abi

        - group:
            kind: job_builder
            name: DummyJobBuilder
            config: null

        - geoips_process:
            kind: dispatcher
            name: serial_bash
            config:
              bash_script: |
                #!/bin/bash
                set -e

                # Output directory
                OUTPUT_DIR="./products/$(date +%Y%m%d)"
                mkdir -p "${OUTPUT_DIR}"

                echo "Processing file: {file}"
                echo "Output directory: ${OUTPUT_DIR}"

                # Call GeoIPS
                geoips run single_source {file} \
                  --reader_name abi_netcdf \
                  --product_name Infrared-Gray \
                  --output_formatter imagery_clean \
                  --filename_formatter geoips_fname \
                  --sector_list global \
                  --minimum_coverage 0 \
                  --output_dir "${OUTPUT_DIR}"

                echo "GeoIPS processing complete"

**This processes each file individually** - good for single-channel
products.

## Step 2: Multi-File Product Generation

For RGB composites, we need multiple channels. Use a custom job builder:

First, create the job builder (reusing from Tutorial 3):

`plugins/channel_group_builder.py`:

    """Groups GOES-18 files by scan time for multi-channel products."""

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
    name = "channel_group_builder"


    class ChannelGroupJob(Job):
        """Job for a complete scan with all channels."""

        def ready(self) -> bool:
            """Ready when all expected channels received."""
            if not self.files:
                return False

            first_file = next(iter(self.files))
            expected = first_file.num_expected
            return len(self.files) >= expected


    class ChannelGroupJobGroup(JobGroup):
        """Groups files by sector and scan time."""

        def __init__(self, config: dict[str, Any]) -> None:
            super().__init__("ChannelGroupJob", config)
            self.job = ChannelGroupJob

        def file_is_relevant(self, file: File | FrozenFile) -> bool:
            """Check if file is GOES-18."""
            return file.platform == "goes18"

        def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
            """Create job ID from metadata."""
            if not all([file.platform, file.sector, file.timestamp]):
                return []

            platform = file.platform.lower()
            sector = file.sector.lower().replace(" ", "-")
            timestamp = file.timestamp.strftime("%Y%j%H%M%S")

            return [f"{platform}_{sector}_{timestamp}"]


    class ChannelGroupBuilder(JobBuilder):
        """Groups GOES-18 files by scan time."""

        name = "ChannelGroupBuilder"
        version = "1.0.0"

        def __init__(self, service: Service, config: dict[str, Any]) -> None:
            super().__init__(service, config)
            self.job_groups = [ChannelGroupJobGroup(config)]


    def call() -> None:
        """Raise error if called directly."""
        raise NotImplementedError("You cannot call this plugin directly.")

Now create the multi-product configuration:

`tutorial05-geoips/config_multi.yaml`:

    apiVersion: geoips_driver/v1
    kind: Service
    name: tutorial-05-geoips-multi
    description: Multi-channel RGB composite generation with GeoIPS.

    spec:
      service_namespace: tutorial05_multi
      heartbeat_interval: 30

      rabbitmq:
        host: localhost
        port: 5672
        username: admin
        password: admin_test

      run:
        - monitor:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: ./data/incoming
              metadata-tools:
                - goes18_abi

        - group:
            kind: job_builder
            name: ChannelGroupBuilder
            config:
              timeout_seconds: 300

        - geoips_rgb:
            kind: dispatcher
            name: serial_bash
            config:
              bash_script: |
                #!/bin/bash
                set -e

                # Configuration
                OUTPUT_DIR="./products/rgb/$(date +%Y%m%d)"
                mkdir -p "${OUTPUT_DIR}"

                # Log job info
                echo "==========================================="
                echo "Multi-channel RGB Product Generation"
                echo "==========================================="
                echo "Files to process:"
                echo "{file}"
                echo ""

                # Generate True Color RGB
                echo "Generating True Color RGB..."
                geoips run single_source {file} \
                  --reader_name abi_netcdf \
                  --product_name True-Color \
                  --output_formatter imagery_clean \
                  --filename_formatter geoips_fname \
                  --sector_list global \
                  --minimum_coverage 0 \
                  --output_dir "${OUTPUT_DIR}/true_color"

                # Generate Day Cloud Phase RGB
                echo "Generating Day Cloud Phase RGB..."
                geoips run single_source {file} \
                  --reader_name abi_netcdf \
                  --product_name Day-Cloud-Phase-Distinction \
                  --output_formatter imagery_clean \
                  --filename_formatter geoips_fname \
                  --sector_list global \
                  --minimum_coverage 0 \
                  --output_dir "${OUTPUT_DIR}/cloud_phase"

                echo "RGB products generated successfully"

## Step 3: Create Production-Ready Dispatcher

For production use, create a more robust dispatcher:

`scripts/geoips_dispatcher.sh`:

    #!/bin/bash

    Production GeoIPS Dispatcher
    ============================
    Handles multiple products with error recovery and logging
    =========================================================

    set -e
    set -u
    set -o pipefail

    Configuration from environment or defaults
    ==========================================
    OUTPUT_BASE="${GEOIPS_OUTPUT_DIR:-./products}"
    LOG_DIR="${GEOIPS_LOG_DIR:-./logs}"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)

    Create directories
    ==================
    mkdir -p "${LOG_DIR}" "${OUTPUT_BASE}"

    Logging
    =======
    LOG_FILE="${LOG_DIR}/geoips_${TIMESTAMP}.log"
    exec 1> >(tee -a "${LOG_FILE}")
    exec 2>&1

    log() {
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    }

    error() {
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
    }

    Product configuration
    =====================
    declare -A PRODUCTS=(
        ["Infrared-Gray"]="ir_gray"
        ["Visible-Gray"]="visible"
        ["True-Color"]="true_color"
        ["Day-Cloud-Phase-Distinction"]="cloud_phase"
    )

    Process single product
    ======================
    process_product() {
        local product_name="$1"
        local output_subdir="$2"
        local files="$3"

        local output_dir="${OUTPUT_BASE}/${output_subdir}"
        mkdir -p "${output_dir}"

        log "Processing product: ${product_name}"
        log "Output directory: ${output_dir}"

        # GeoIPS command
        geoips run single_source ${files} \
            --reader_name abi_netcdf \
            --product_name "${product_name}" \
            --output_formatter imagery_clean \
            --filename_formatter geoips_fname \
            --sector_list global \
            --minimum_coverage 0 \
            --output_dir "${output_dir}" \
            --logging_level info

        local exit_code=$?

        if [[ ${exit_code} -eq 0 ]]; then
            log "Product ${product_name} generated successfully"
            return 0
        else
            error "Product ${product_name} failed with exit code ${exit_code}"
            return ${exit_code}
        fi
    }

    Main processing
    ===============
    main() {
        local files="$1"

        log "================================================"
        log "GeoIPS Dispatcher - Production Processing"
        log "================================================"
        log "Input files: ${files}"

        # Process each configured product
        local success_count=0
        local fail_count=0

        for product in "${!PRODUCTS[@]}"; do
            local subdir="${PRODUCTS[$product]}"

            if process_product "${product}" "${subdir}" "${files}"; then
                ((success_count++))
            else
                ((fail_count++))
                # Continue processing other products even if one fails
            fi
        done

        log "================================================"
        log "Processing Summary"
        log "Successful: ${success_count}"
        log "Failed: ${fail_count}"
        log "================================================"

        # Exit with error if any products failed
        if [[ ${fail_count} -gt 0 ]]; then
            error "Some products failed to generate"
            exit 1
        fi

        log "All products generated successfully"
        return 0
    }

    Execute
    =======
    main "{file}"

Use in configuration:

    - geoips_multi_product:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            #!/bin/bash
            GEOIPS_OUTPUT_DIR="./products/$(date +%Y/%m/%d)"
            GEOIPS_LOG_DIR="./logs"
            export GEOIPS_OUTPUT_DIR GEOIPS_LOG_DIR

            bash ./scripts/geoips_dispatcher.sh "{file}"

## Step 4: Sector-Specific Processing

Process different sectors with different products:

    Detect sector from metadata
    ===========================
    detect_sector() {
        local file="$1"

        # Extract sector from filename
        if [[ "${file}" =~ RadF ]]; then
            echo "full-disk"
        elif [[ "${file}" =~ RadC ]]; then
            echo "conus"
        elif [[ "${file}" =~ RadM1 ]]; then
            echo "meso1"
        elif [[ "${file}" =~ RadM2 ]]; then
            echo "meso2"
        else
            echo "unknown"
        fi
    }

    Sector-specific product lists
    =============================
    process_by_sector() {
        local files="$1"
        local first_file=$(echo "${files}" | awk '{print $1}')
        local sector=$(detect_sector "${first_file}")

        log "Detected sector: ${sector}"

        case "${sector}" in
            full-disk)
                # Full-Disk: Generate all products
                process_product "True-Color" "fulldisk/true_color" "${files}"
                process_product "Infrared-Gray" "fulldisk/ir" "${files}"
                ;;
            conus)
                # CONUS: Faster refresh, limited products
                process_product "Visible-Gray" "conus/visible" "${files}"
                process_product "Infrared-Gray" "conus/ir" "${files}"
                ;;
            meso*)
                # Mesoscale: High temporal resolution
                process_product "Infrared-Gray" "meso/${sector}/ir" "${files}"
                ;;
            *)
                error "Unknown sector: ${sector}"
                return 1
                ;;
        esac
    }

## Step 5: Parallel Product Generation

Generate multiple products in parallel for faster processing:

    Parallel processing with job control
    ====================================
    process_products_parallel() {
        local files="$1"
        local max_parallel=4
        local pids=()

        log "Processing products in parallel (max ${max_parallel})"

        # Start background jobs
        for product in "${!PRODUCTS[@]}"; do
            local subdir="${PRODUCTS[$product]}"

            # Wait if we've hit max parallel
            while [[ ${#pids[@]} -ge ${max_parallel} ]]; do
                # Check for completed jobs
                for i in "${!pids[@]}"; do
                    if ! kill -0 "${pids[$i]}" 2>/dev/null; then
                        wait "${pids[$i]}"
                        unset 'pids[$i]'
                    fi
                done
                sleep 1
            done

            # Start new job
            process_product "${product}" "${subdir}" "${files}" &
            pids+=($!)
        done

        # Wait for all jobs to complete
        for pid in "${pids[@]}"; do
            wait "${pid}"
        done

        log "All parallel processing complete"
    }

**Warning:** Monitor system resources when processing in parallel!

## Step 6: Error Handling and Recovery

Implement comprehensive error handling:

    Retry mechanism for transient failures
    ======================================
    retry_geoips() {
        local max_attempts=3
        local attempt=0
        local delay=10

        while [[ ${attempt} -lt ${max_attempts} ]]; do
            ((attempt++))
            log "Attempt ${attempt}/${max_attempts}"

            if geoips run single_source "$@"; then
                log "GeoIPS succeeded on attempt ${attempt}"
                return 0
            fi

            local exit_code=$?
            error "GeoIPS failed with exit code ${exit_code}"

            if [[ ${attempt} -lt ${max_attempts} ]]; then
                log "Retrying in ${delay} seconds..."
                sleep ${delay}
                delay=$((delay * 2))  # Exponential backoff
            fi
        done

        error "GeoIPS failed after ${max_attempts} attempts"
        return 1
    }

    Cleanup on failure
    ==================
    cleanup_partial_products() {
        local output_dir="$1"

        log "Cleaning up partial products in ${output_dir}"

        # Remove files modified in last 5 minutes (likely from failed run)
        find "${output_dir}" -type f -mmin -5 -delete
    }

    Error trap
    ==========
    handle_error() {
        local line_no="$1"
        error "Script failed at line ${line_no}"
        cleanup_partial_products "${OUTPUT_BASE}"
        exit 1
    }

    trap 'handle_error ${LINENO}' ERR

## Step 7: Product Verification

Verify products were generated correctly:

    verify_product() {
        local output_dir="$1"
        local product_name="$2"

        log "Verifying product: ${product_name}"

        # Check if output directory has files
        local file_count=$(find "${output_dir}" -type f -name "*.png" | wc -l)

        if [[ ${file_count} -eq 0 ]]; then
            error "No products generated for ${product_name}"
            return 1
        fi

        log "Found ${file_count} product files for ${product_name}"

        # Check file sizes (products should be > 1KB)
        local small_files=$(find "${output_dir}" -type f -size -1k | wc -l)

        if [[ ${small_files} -gt 0 ]]; then
            error "Found ${small_files} suspiciously small files"
            return 1
        fi

        log "Product verification passed for ${product_name}"
        return 0
    }

## Step 8: Monitoring and Metrics

Add custom metrics for GeoIPS processing:

Create `plugins/geoips_metrics_dispatcher.py`:

    """GeoIPS dispatcher with detailed metrics."""

    import subprocess
    import time
    from typing import Any

    from prometheus_client import Counter, Histogram

    from geoips_driver.interfaces.module_based.dispatchers import (
        Dispatcher,
        ExecutionLog,
    )
    from geoips_driver.interfaces.module_based.job_builders import Job
    from geoips_driver.interfaces.module_based.service import Service

    interface = "dispatchers"
    family = "standard"
    name = "geoips_metrics"


    class GeoIPSMetricsDispatcher(Dispatcher):
        """Dispatcher with GeoIPS-specific metrics."""

        name = "geoips_metrics"
        version = "1.0.0"

        def __init__(self, service: Service, config: dict[str, Any]) -> None:
            super().__init__(service, config)

            # GeoIPS-specific metrics
            self.products_generated = Counter(
                "geoips_products_generated_total",
                "Total GeoIPS products generated",
                ["product_name", "sector"],
            )

            self.processing_duration = Histogram(
                "geoips_processing_duration_seconds",
                "GeoIPS processing duration",
                ["product_name"],
                buckets=(10, 30, 60, 120, 300, 600),
            )

        def get_execution_log(self, job: Job) -> list[ExecutionLog]:
            """Execute GeoIPS with metrics tracking."""
            start_time = time.time()

            # Extract metadata
            first_file = next(iter(job.files))
            sector = first_file.sector or "unknown"

            # Build GeoIPS command
            file_paths = " ".join(str(f.file) for f in job.files)
            products = self.config.get("products", ["Infrared-Gray"])

            logs = []

            for product in products:
                product_start = time.time()

                cmd = [
                    "geoips", "run", "single_source", file_paths,
                    "--reader_name", "abi_netcdf",
                    "--product_name", product,
                    "--output_formatter", "imagery_clean",
                    "--filename_formatter", "geoips_fname",
                    "--sector_list", "global",
                    "--minimum_coverage", "0",
                ]

                result = subprocess.run(cmd, capture_output=True, text=True)

                if result.returncode == 0:
                    self.products_generated.labels(
                        product_name=product,
                        sector=sector,
                    ).inc()

                duration = time.time() - product_start
                self.processing_duration.labels(product_name=product).observe(duration)

                logs.append(
                    ExecutionLog(
                        return_code=result.returncode,
                        stdout=result.stdout,
                        stderr=result.stderr,
                        hostname="localhost",
                    )
                )

            return logs

## Step 9: Complete Production Example

`tutorial05-geoips/config_production.yaml`:

    apiVersion: geoips_driver/v1
    kind: Service
    name: goes18-geoips-production
    description: Production GOES-18 product generation with GeoIPS.

    docstring: |
      Production-ready service for GOES-18 Full-Disk product generation.
      Processes complete scans and generates multiple RGB and single-channel products.

    spec:
      service_namespace: goes18_production
      heartbeat_interval: 60

      rabbitmq:
        host: rabbitmq.example.com
        port: 5672
        username: goes18_processor
        password: ${RABBITMQ_PASSWORD}

      run:
        - monitor:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: /data/goes18/fulldisk
              metadata-tools:
                - goes18_abi

        - group:
            kind: job_builder
            name: ChannelGroupBuilder
            config:
              timeout_seconds: 600

        - process:
            kind: dispatcher
            name: geoips_metrics
            config:
              products:
                - True-Color
                - Infrared-Gray
                - Visible-Gray
                - Day-Cloud-Phase-Distinction

## Testing

**Test with sample data:**

    Download sample GOES-18 data
    ============================
    wget https://example.com/goes18_sample_fulldisk.tar.gz
    tar xzf goes18_sample_fulldisk.tar.gz -C data/incoming/

    Start service
    =============
    PYTHONPATH=./plugins geoips-driver run config_production.yaml

**Monitor processing:**

    Watch logs
    ==========
    tail -f logs/geoips_*.log

    Check Prometheus metrics
    ========================
    curl http://localhost:8000/metrics | grep geoips

**Verify products:**

    List generated products
    =======================
    find products/ -name "*.png" -newer products/.baseline

    Check product sizes
    ===================
    du -sh products/*

## What You Learned

✅ GeoIPS workflow integration ✅ Multi-file product generation ✅
Sector-specific processing ✅ Error handling and retry logic ✅ Product
verification ✅ Performance monitoring ✅ Production deployment patterns

## Next Steps

-   `` `06-multi-satellite-monitor ``\` - Process multiple satellites
-   `` `07-monitoring-with-prometheus ``\` - Set up Grafana dashboards
-   `` `08-production-deployment ``\` - Deploy to Kubernetes
-   :doc:`../user-guide/deployment` - Production best practices

## Challenge Exercises

1.  **Add quality control** - Reject products with insufficient coverage
2.  **Implement product archiving** - Move products to long-term storage
3.  **Create notification system** - Alert when products are ready
4.  **Add derived products** - Cloud top height, fire detection, etc.

## Complete Code

\`tutorial05-geoips/
\](<https://github.com/biosafetylvl5/geoips_driver/tree/main/examples/tutorials/05-geoips>)

# Tutorial 4: Creating a Bash Script Dispatcher

**Level:** Intermediate | **Time:** 25 minutes

Learn how to create a custom bash script dispatcher that processes
GOES-18 data with sophisticated error handling, logging, and file
organization.

## Learning Objectives

By the end of this tutorial, you will:

-   Understand dispatcher architecture and responsibilities
-   Create custom bash script dispatchers
-   Use template variables effectively
-   Implement error handling and logging
-   Organize output files by date and platform
-   Handle execution logs

## Prerequisites

-   Completed `` `01-simple-file-watcher ``<span class="title-ref">
    through
    :doc:</span><span class="title-ref">03-custom-job-builder</span>\`
-   Familiarity with bash scripting
-   GeoIPS installed and configured
-   Understanding of GOES-18 data structure

## Understanding Dispatchers

A **Dispatcher** executes processing jobs. It:

1.  Consumes Job objects from `JobReadyQueue`
2.  Executes processing (bash scripts, Python code, GeoIPS workflows)
3.  Captures execution results (stdout, stderr, return code)
4.  Emits ExecutionLog objects with results

**Built-in dispatcher:**

-   `serial_bash` - Executes bash scripts serially, one job at a time

## Step 1: Simple Bash Dispatcher

Let's start with a basic dispatcher configuration:

    - process:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            #!/bin/bash
            echo "Processing: {file}"

**Template variables available:**

-   `{file}` - File path (or paths for multi-file jobs)
-   Additional variables depend on File metadata

## Step 2: Create Advanced Processing Script

Create a more sophisticated script with error handling:

Create `tutorial04-dispatcher/scripts/process_goes18.sh`:

    #!/bin/bash

    Advanced GOES-18 processing script
    ==================================
    Usage: Called by GeoIPS Driver dispatcher with template variables
    =================================================================

    set -e  # Exit on error
    set -u  # Exit on undefined variable
    set -o pipefail  # Exit on pipe failure

    Configuration
    =============
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    LOG_DIR="${SCRIPT_DIR}/../logs"
    OUTPUT_DIR="${SCRIPT_DIR}/../products"

    Create directories
    ==================
    mkdir -p "${LOG_DIR}" "${OUTPUT_DIR}"

    Logging function
    ================
    log() {
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_DIR}/processing.log"
    }

    error() {
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "${LOG_DIR}/processing.log" >&2
    }

    Main processing function
    ========================
    process_file() {
        local filepath="$1"
        local filename=$(basename "${filepath}")

        log "=========================================="
        log "Processing GOES-18 file"
        log "File: ${filename}"
        log "Full path: ${filepath}"
        log "=========================================="

        # Extract metadata from filename
        # OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc
        local platform=$(echo "${filename}" | grep -oP 'G\d+' || echo "unknown")
        local scan_time=$(echo "${filename}" | grep -oP 's\K\d{14}' || echo "unknown")
        local channel=$(echo "${filename}" | grep -oP 'C\K\d{2}' || echo "unknown")

        log "Platform: ${platform}"
        log "Scan time: ${scan_time}"
        log "Channel: ${channel}"

        # Organize output by date
        local year=${scan_time:0:4}
        local doy=${scan_time:4:3}
        local output_subdir="${OUTPUT_DIR}/${platform}/${year}/${doy}"
        mkdir -p "${output_subdir}"

        log "Output directory: ${output_subdir}"

        # Simulate processing (replace with actual GeoIPS call)
        log "Starting processing..."
        sleep 1

        # Example: Copy to output directory
        cp "${filepath}" "${output_subdir}/"

        log "Processing complete!"
        log "Output saved to: ${output_subdir}/${filename}"

        return 0
    }

    Error handling
    ==============
    trap 'error "Script failed at line $LINENO"' ERR

    Main execution
    ==============
    main() {
        local file_path="$1"

        if [[ -z "${file_path}" ]]; then
            error "No file path provided"
            exit 1
        fi

        if [[ ! -f "${file_path}" ]]; then
            error "File not found: ${file_path}"
            exit 1
        fi

        process_file "${file_path}"
    }

    Run main function with provided file path
    =========================================
    main "{file}"

Key features:

-   **Error handling**: `set -e`, trap, validation
-   **Logging**: Timestamped logs to file and stdout
-   **Organization**: Outputs organized by platform/year/day
-   **Metadata extraction**: Parse filename for metadata
-   **Reusable**: Functions for different processing steps

## Step 3: Configure Service with Custom Script

Create `tutorial04-dispatcher/config.yaml`:

    apiVersion: geoips_driver/v1
    kind: Service
    name: tutorial-04-bash-dispatcher
    description: Advanced bash script dispatcher for GOES-18 processing.

    spec:
      service_namespace: tutorial04
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

        - process:
            kind: dispatcher
            name: serial_bash
            config:
              bash_script: |
                #!/bin/bash

                # Get absolute path to processing script
                SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
                PROCESS_SCRIPT="${SCRIPT_DIR}/scripts/process_goes18.sh"

                # Check script exists
                if [[ ! -f "${PROCESS_SCRIPT}" ]]; then
                    echo "ERROR: Processing script not found: ${PROCESS_SCRIPT}"
                    exit 1
                fi

                # Execute processing script
                bash "${PROCESS_SCRIPT}" "{file}"

## Step 4: Create Directory Structure

    mkdir -p tutorial04-dispatcher/{data/incoming,logs,products,scripts}
    cd tutorial04-dispatcher

    Copy the script we created
    ==========================
    (Script content from Step 2 goes in scripts/process_goes18.sh)
    ==============================================================
    chmod +x scripts/process_goes18.sh

## Step 5: Test the Dispatcher

Create test data:

    cd data/incoming
    touch OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc

Start the service:

    cd ~/tutorial04-dispatcher
    geoips-driver run config.yaml

Check the logs:

    [Plugin: serial_bash] Executing job
    [2024-01-15 12:05:30] ==========================================
    [2024-01-15 12:05:30] Processing GOES-18 file
    [2024-01-15 12:05:30] File: OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc
    [2024-01-15 12:05:30] ==========================================
    [2024-01-15 12:05:30] Platform: G18
    [2024-01-15 12:05:30] Scan time: 20240151200000
    [2024-01-15 12:05:30] Channel: 01
    [2024-01-15 12:05:30] Output directory: ./products/G18/2024/015
    [2024-01-15 12:05:30] Starting processing...
    [2024-01-15 12:05:31] Processing complete!

Check output:

    tree products/
    products/
    =========
    └── G18
    =======
    └── 2024
    ========
    └── 015
    =======
    └── OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc
    ================================================================================

## Step 6: Handle Multiple Files (Full Scan)

Modify the script to handle multiple files from a complete scan:

    Updated main function for multiple files
    ========================================
    main() {
        local file_paths="$1"

        # Split space-separated file paths
        IFS=' ' read -ra files <<< "${file_paths}"

        log "Processing ${#files[@]} files"

        for file_path in "${files[@]}"; do
            if [[ -f "${file_path}" ]]; then
                process_file "${file_path}"
            else
                error "File not found: ${file_path}"
            fi
        done

        log "All files processed successfully"
    }

When using a job builder that groups files, `{file}` contains all file
paths separated by spaces.

## Step 7: Add Progress Reporting

Report progress back through logging:

    process_file() {
        local filepath="$1"
        local current="$2"
        local total="$3"

        log "=========================================="
        log "Processing file ${current}/${total}"
        log "File: $(basename "${filepath}")"
        log "=========================================="

        # ... rest of processing
    }

    main() {
        local file_paths="$1"
        IFS=' ' read -ra files <<< "${file_paths}"

        local total=${#files[@]}
        local current=0

        for file_path in "${files[@]}"; do
            ((current++))
            process_file "${file_path}" "${current}" "${total}"
        done
    }

    [``
    Step 8: Integrate with GeoIPS
    -----------------------------

    Replace the simulation with actual GeoIPS processing:

    ```bash

    process_file() {
        local filepath="$1"
        local filename=$(basename "${filepath}")

        log "Starting GeoIPS processing for ${filename}"

        # Call GeoIPS
        geoips run single_source "${filepath}" \
            --reader_name abi_netcdf \
            --product_name Infrared-Gray \
            --output_formatter imagery_clean \
            --filename_formatter geoips_fname \
            --sector_list global \
            --minimum_coverage 0 \
            --output_dir "${output_subdir}" \
            2>&1 | tee -a "${LOG_DIR}/geoips_${scan_time}.log"

        local exit_code=${PIPESTATUS[0]}

        if [[ ${exit_code} -eq 0 ]]; then
            log "GeoIPS processing successful"
            return 0
        else
            error "GeoIPS processing failed with exit code ${exit_code}"
            return ${exit_code}
        fi
    }

## Step 9: Advanced Error Handling

Add retry logic and error recovery:

    Retry function
    ==============
    retry() {
        local max_attempts=3
        local timeout=30
        local attempt=0
        local exit_code=0

        while [[ ${attempt} -lt ${max_attempts} ]]; do
            ((attempt++))
            log "Attempt ${attempt}/${max_attempts}..."

            "$@"
            exit_code=$?

            if [[ ${exit_code} -eq 0 ]]; then
                log "Command succeeded on attempt ${attempt}"
                return 0
            fi

            if [[ ${attempt} -lt ${max_attempts} ]]; then
                log "Command failed with exit code ${exit_code}, retrying in ${timeout}s..."
                sleep ${timeout}
            fi
        done

        error "Command failed after ${max_attempts} attempts"
        return ${exit_code}
    }

    Use retry for critical operations
    =================================
    retry geoips run single_source "${filepath}" ...

## Step 10: Create Custom Dispatcher Plugin

For even more control, create a custom Python dispatcher plugin:

Create `plugins/advanced_bash_dispatcher.py`:

    """Advanced bash dispatcher with enhanced features."""

    import subprocess
    import tempfile
    from pathlib import Path
    from typing import Any

    from geoips_driver.interfaces.module_based.dispatchers import (
        Dispatcher,
        ExecutionLog,
    )
    from geoips_driver.interfaces.module_based.job_builders import Job
    from geoips_driver.interfaces.module_based.service import Service

    interface = "dispatchers"
    family = "standard"
    name = "advanced_bash"


    class AdvancedBashDispatcher(Dispatcher):
        """Enhanced bash dispatcher with template support and error handling."""

        name = "advanced_bash"
        version = "1.0.0"

        def __init__(self, service: Service, config: dict[str, Any]) -> None:
            super().__init__(service, config)
            self.script_template = config["bash_script"]
            self.timeout = config.get("timeout_seconds", 3600)
            self.retry_attempts = config.get("retry_attempts", 1)

        def get_execution_log(self, job: Job) -> list[ExecutionLog]:
            """Execute bash script with retry logic."""
            logs = []

            for attempt in range(self.retry_attempts):
                self._logger.info(
                    f"Executing job {job.identifier} "
                    f"(attempt {attempt + 1}/{self.retry_attempts})"
                )

                # Create temporary script file
                script_content = self._render_template(job)

                with tempfile.NamedTemporaryFile(
                    mode="w",
                    suffix=".sh",
                    delete=False,
                ) as f:
                    f.write(script_content)
                    script_path = f.name

                try:
                    Path(script_path).chmod(0o755)

                    result = subprocess.run(
                        ["/bin/bash", script_path],
                        capture_output=True,
                        text=True,
                        timeout=self.timeout,
                    )

                    log = ExecutionLog(
                        return_code=result.returncode,
                        stdout=result.stdout,
                        stderr=result.stderr,
                        hostname="localhost",
                    )

                    logs.append(log)

                    if result.returncode == 0:
                        self._logger.info(f"Job {job.identifier} completed successfully")
                        break
                    else:
                        self._logger.warning(
                            f"Job {job.identifier} failed with code {result.returncode}"
                        )

                except subprocess.TimeoutExpired:
                    self._logger.error(f"Job {job.identifier} timed out")
                    logs.append(
                        ExecutionLog(
                            return_code=-1,
                            stdout="",
                            stderr="Execution timed out",
                            hostname="localhost",
                        )
                    )
                finally:
                    Path(script_path).unlink(missing_ok=True)

            return logs

        def _render_template(self, job: Job) -> str:
            """Render script template with job data."""
            # Get file paths
            file_paths = " ".join(str(f.file) for f in job.files)

            # Get metadata from first file
            first_file = next(iter(job.files))

            # Template substitutions
            replacements = {
                "{file}": file_paths,
                "{platform}": first_file.platform or "unknown",
                "{sensor}": first_file.sensor or "unknown",
                "{sector}": first_file.sector or "unknown",
                "{timestamp}": (
                    first_file.timestamp.strftime("%Y%m%d%H%M%S")
                    if first_file.timestamp
                    else "unknown"
                ),
                "{job_id}": job.identifier,
            }

            script = self.script_template
            for key, value in replacements.items():
                script = script.replace(key, value)

            return script


    def call() -> None:
        """Raise error if called directly."""
        raise NotImplementedError("You cannot call this plugin directly.")

Use in configuration:

    - process:
        kind: dispatcher
        name: advanced_bash
        config:
          timeout_seconds: 1800  # 30 minutes
          retry_attempts: 3
          bash_script: |
            #!/bin/bash
            echo "Platform: {platform}"
            echo "Sector: {sector}"
            echo "Timestamp: {timestamp}"
            echo "Job ID: {job_id}"

            # Process files
            for file in {file}; do
                echo "Processing: $file"
            done

## Testing and Debugging

**Test scripts independently:**

    Test script with dummy file
    ===========================
    bash scripts/process_goes18.sh "test_file.nc"

**Check execution logs:**

    Service logs
    ============
    tail -f logs/processing.log

    GeoIPS logs
    ===========
    tail -f logs/geoips_*.log

**Monitor metrics:**

    Check Prometheus metrics
    ========================
    curl http://localhost:8000/metrics | grep dispatcher

## Common Patterns

**Parallel processing:**

    Process files in parallel (be careful with resources!)
    ======================================================
    for file in {file}; do
        process_file "$file" &
    done
    wait

**Conditional processing:**

    Only process certain channels
    =============================
    if [[ "${channel}" =~ ^(01|02|03)$ ]]; then
        log "Processing visible channel ${channel}"
        process_visible "${filepath}"
    else
        log "Processing IR channel ${channel}"
        process_infrared "${filepath}"
    fi

**Email notifications:**

    Send email on completion/failure
    ================================
    send_notification() {
        local status="$1"
        local message="$2"

        echo "${message}" | mail -s "GOES-18 Processing ${status}" admin@example.com
    }

    trap 'send_notification "FAILED" "Processing failed at line $LINENO"' ERR

## What You Learned

✅ How to create sophisticated bash scripts for dispatchers ✅ Template
variable usage and substitution ✅ Error handling and retry logic ✅
File organization strategies ✅ GeoIPS integration ✅ Custom dispatcher
plugin development ✅ Logging and monitoring

## Next Steps

-   `` `05-geoips-workflow-dispatcher ``\` - Full GeoIPS workflow
    integration
-   `` `09-error-handling ``\` - Advanced error handling patterns
-   :doc:`../developer-guide/plugin-development` - Create Python
    dispatchers

## Challenge Exercises

1.  **Add quality control** - Check file sizes before processing
2.  **Implement checkpointing** - Resume interrupted processing
3.  **Create output manifests** - Generate JSON files listing products
4.  **Add notification hooks** - Webhook or email on completion

## Complete Code

\`tutorial04-dispatcher/
\](<https://github.com/biosafetylvl5/geoips_driver/tree/main/examples/tutorials/04-bash-dispatcher>)

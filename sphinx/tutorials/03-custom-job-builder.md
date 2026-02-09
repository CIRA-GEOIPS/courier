# Tutorial 3: Creating a Custom Job Builder

**Level:** Intermediate | **Time:** 30 minutes

Learn how to create a custom job builder plugin that groups GOES-18
files by sector and scan time for batch processing.

## Learning Objectives

By the end of this tutorial, you will:

-   Understand job builder architecture
-   Create a custom job grouping strategy
-   Group files by metadata (sector + timestamp)
-   Implement "ready" logic for complete scans
-   Handle timeouts for partial scans

## Prerequisites

-   Completed
    {doc}\[01-simple-file-watcher`and :doc:`02-adding-metadata\`
-   Python programming experience
-   Understanding of classes and inheritance
-   Familiarity with GOES-18 scan structure (16 channels per scan)

## Understanding Job Builders

A **Job Builder** determines how files are grouped into processing jobs.

**Purpose:**

For GOES-18 Full-Disk, a complete scan consists of 16 channel files:

    OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_...nc  (Channel 1)
    OR_ABI-L1b-RadF-M6C02_G18_s20240151200000_...nc  (Channel 2)
    ...
    OR_ABI-L1b-RadF-M6C16_G18_s20240151200000_...nc  (Channel 16)

We want to:

1.  **Group** these 16 files together
2.  **Wait** until all 16 arrive
3.  **Emit** a single job containing all 16 files
4.  **Timeout** if some files don't arrive

**Default behavior (DummyJobBuilder):**

-   1 file = 1 job
-   No grouping
-   Good for testing, bad for real processing

## Step 1: Understanding the JobBuilder Base Class

Job builders inherit from `JobBuilder`:

    from geoips_driver.interfaces.module_based.job_builders import JobBuilder, JobGroup

    class MyJobBuilder(JobBuilder):
        name = "MyJobBuilder"
        version = "1.0.0"

        def __init__(self, service, config):
            super().__init__(service, config)
            # Create job groups
            self.job_groups = [MyJobGroup(config)]

**Key concepts:**

-   **JobBuilder**: Manages multiple JobGroup instances
-   **JobGroup**: Groups files and creates jobs
-   **Job**: A collection of files ready for processing

## Step 2: Design the Grouping Strategy

For GOES-18, we'll group by:

1.  **Sector** (Full-Disk, CONUS, Mesoscale 1, Mesoscale 2)
2.  **Scan timestamp** (all files from same scan time)

**Job ID format:**

    goes18_full-disk_20240151200000
    │      │          │
    │      │          └─ Scan start time
    │      └─ Sector
    └─ Platform

**Example:**

All files with `s20240151200000` (Jan 15, 2024, 12:00:00) and `RadF`
(Full-Disk) go into job `goes18_full-disk_20240151200000`.

## Step 3: Create the Plugin Directory

    mkdir -p tutorial03-job-builder/plugins
    cd tutorial03-job-builder

Create the plugin file structure:

    tutorial03-job-builder/
    ├── plugins/
    │   └── sector_time_job_builder.py  # Our custom plugin
    ├── data/
    │   └── incoming/
    └── config.yaml

## Step 4: Implement the JobGroup

Create `plugins/sector_time_job_builder.py`:

    """Sector-Time Job Builder Plugin.

    Groups GOES-18 files by sector and scan time.
    """

    from typing import Any
    from geoips_driver.interfaces.module_based.job_builders import (
        Job,
        JobBuilder,
        JobGroup,
    )
    from geoips_driver.interfaces.module_based.service import Service
    from geoips_driver.types.file import File, FrozenFile

    Plugin metadata
    ===============
    interface = "job_builders"
    family = "standard"
    name = "sector_time_job_builder"


    class SectorTimeJob(Job):
        """Job for a complete satellite scan."""

        def ready(self) -> bool:
            """Job is ready when all expected files have arrived."""
            # Get expected number from first file's metadata
            if not self.files:
                return False

            first_file = next(iter(self.files))
            expected = first_file.num_expected

            # Ready when we have all expected files
            return len(self.files) >= expected


    class SectorTimeJobGroup(JobGroup):
        """Groups files by sector and scan time."""

        def __init__(self, config: dict[str, Any]) -> None:
            super().__init__("SectorTimeJob", config)
            self.job = SectorTimeJob

            # Configuration
            self.timeout = config.get("timeout_seconds", 300)  # 5 min default
            self.required_platform = config.get("platform", "goes18")

        def file_is_relevant(self, file: File | FrozenFile) -> bool:
            """Check if file should be processed by this job group."""
            # Only process files for our configured platform
            return (
                file.platform is not None
                and file.platform.lower() == self.required_platform
            )

        def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
            """Generate job ID from file metadata.

            Format: {platform}_{sector}_{timestamp}
            Example: goes18_full-disk_20240151200000
            """
            if not all([file.platform, file.sector, file.timestamp]):
                # Can't create job ID without required metadata
                return []

            # Create standardized job ID
            platform = file.platform.lower()
            sector = file.sector.lower().replace(" ", "-")
            timestamp = file.timestamp.strftime("%Y%j%H%M%S")

            job_id = f"{platform}_{sector}_{timestamp}"
            return [job_id]


    class SectorTimeJobBuilder(JobBuilder):
        """Job builder that groups files by sector and scan time."""

        name = "SectorTimeJobBuilder"
        version = "1.0.0"

        def __init__(self, service: Service, config: dict[str, Any]) -> None:
            super().__init__(service, config)

            # Create job group for GOES-18
            self.job_groups = [
                SectorTimeJobGroup({
                    "platform": "goes18",
                    "timeout_seconds": config.get("timeout_seconds", 300),
                })
            ]


    def call() -> None:
        """Raise error if called directly."""
        raise NotImplementedError("You cannot call this plugin directly.")

Let's break down the key methods:

**file\_is\_relevant():**

    def file_is_relevant(self, file: File | FrozenFile) -> bool:
        """Only process GOES-18 files."""
        return file.platform == "goes18"

This filters which files this job group processes.

**get\_job\_ids\_from\_file():**

    def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
        """Create job ID from metadata."""
        platform = file.platform.lower()      # "goes18"
        sector = file.sector.lower()          # "full-disk"
        timestamp = file.timestamp.strftime("%Y%j%H%M%S")  # "20240151200000"

        return [f"{platform}_{sector}_{timestamp}"]

This creates a unique ID for each scan.

**ready():**

    def ready(self) -> bool:
        """Job ready when all files received."""
        first_file = next(iter(self.files))
        expected = first_file.num_expected  # 16 for Full-Disk
        return len(self.files) >= expected

This determines when to emit the job to the dispatcher.

## Step 5: Register the Plugin

For the plugin to be discoverable, it must be:

1.  In the Python module path
2.  Have the correct `interface` and `name` attributes

For development, add to `PYTHONPATH`:

    export PYTHONPATH="$PWD/plugins:$PYTHONPATH"

Or install as a package (advanced):

    pyproject.toml
    ==============
    [tool.poetry.plugins."geoips_driver.plugin_packages"]
    "tutorial_plugins" = "plugins"

## Step 6: Create Service Configuration

Create `config.yaml`:

    apiVersion: geoips_driver/v1
    kind: Service
    name: tutorial-03-job-builder
    description: Test custom sector-time job builder.

    spec:
      service_namespace: tutorial03
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

        - group_by_sector_time:
            kind: job_builder
            name: SectorTimeJobBuilder  # Our custom plugin!
            config:
              timeout_seconds: 300  # Wait up to 5 minutes for all files

        - process:
            kind: dispatcher
            name: serial_bash
            config:
              bash_script: |
                #!/bin/bash
                echo "=========================================="
                echo "Processing complete GOES-18 scan"
                echo "Job: $0"
                echo "Number of files: $(echo {file} | wc -w)"
                echo "=========================================="

                # In real usage, would call:
                # geoips run single_source {file} ...

## Step 7: Create Test Data

Create 16 files for a complete Full-Disk scan:

    mkdir -p data/incoming
    cd data/incoming

    Create all 16 channels for same scan time
    =========================================
    for i in $(seq -f "%02g" 1 16); do
        touch "OR_ABI-L1b-RadF-M6C${i}_G18_s20240151200000_e20240151209310_c20240151209360.nc"
    done

    Verify
    ======
    ls -1

You should see 16 files, all with `s20240151200000` (same scan time).

## Step 8: Run and Test

Start the service:

    cd ~/tutorial03-job-builder
    export PYTHONPATH="$PWD/plugins:$PYTHONPATH"
    geoips-driver run config.yaml

Watch the logs carefully:

    [Plugin: file_system_poller_watchdog] Found file: ...C01...
    [Plugin: SectorTimeJobBuilder] File added to job goes18_full-disk_20240151200000
    [Plugin: file_system_poller_watchdog] Found file: ...C02...
    [Plugin: SectorTimeJobBuilder] File added to job goes18_full-disk_20240151200000
    ...
    [Plugin: SectorTimeJobBuilder] File added to job goes18_full-disk_20240151200000
    [Plugin: SectorTimeJobBuilder] Job goes18_full-disk_20240151200000 is ready; emitting
    [Plugin: serial_bash] Processing complete GOES-18 scan
    Number of files: 16

The job builder:

1.  Received 16 files
2.  Grouped them into one job
3.  Waited until all 16 arrived
4.  Emitted the complete job

## Step 9: Test Incomplete Scans

What happens if only some files arrive?

    cd data/incoming

    Create only 10 files (incomplete scan)
    ======================================
    for i in $(seq -f "%02g" 1 10); do
        touch "OR_ABI-L1b-RadF-M6C${i}_G18_s20240151210000_e20240151219310_c20240151219360.nc"
    done

Watch the logs:

    [Plugin: SectorTimeJobBuilder] File added to job goes18_full-disk_20240151210000
    ... (9 more files)
    [Plugin: SectorTimeJobBuilder] Job goes18_full-disk_20240151210000 not ready (10/16 files)

After 5 minutes (timeout), the job is discarded:

    [Plugin: SectorTimeJobBuilder] Discarding old job goes18_full-disk_20240151210000

## Step 10: Add Logging and Metrics

Enhance the plugin with better logging:

    class SectorTimeJobGroup(JobGroup):
        def add_file(self, file: File | FrozenFile) -> bool:
            if not self.file_is_relevant(file):
                return False

            job_ids = self.get_job_ids_from_file(file)
            for job_id in job_ids:
                if job_id in self.jobs:
                    self.jobs[job_id].add_file(file)
                    job = self.jobs[job_id]

                    # Log progress
                    progress = f"{len(job.files)}/{job.files.num_expected}"
                    self._logger.info(f"Job {job_id}: {progress} files")

                else:
                    # Create new job
                    self.jobs[job_id] = self.job(self.name, job_id, self.config)
                    self.jobs[job_id].add_file(file)
                    self._logger.info(f"Created new job: {job_id}")

            return True

## Advanced: Multiple Sectors

Handle multiple sectors simultaneously:

    class SectorTimeJobBuilder(JobBuilder):
        def __init__(self, service: Service, config: dict[str, Any]) -> None:
            super().__init__(service, config)

            # Create job groups for different sectors
            self.job_groups = [
                SectorTimeJobGroup({
                    "name": "goes18_fulldisk",
                    "platform": "goes18",
                    "sector": "Full-Disk",
                    "timeout_seconds": 300,
                }),
                SectorTimeJobGroup({
                    "name": "goes18_conus",
                    "platform": "goes18",
                    "sector": "CONUS",
                    "timeout_seconds": 180,  # CONUS scans faster
                }),
            ]

Each job group handles its own sector.

## Advanced: Priority Jobs

Process certain sectors with higher priority:

    class PriorityJob(Job):
        def __init__(self, name, identifier, config, priority=0):
            super().__init__(name, identifier, config)
            self.priority = priority

        def __lt__(self, other):
            """For priority queue sorting."""
            return self.priority > other.priority  # Higher priority first

## Testing Your Plugin

Create unit tests:

    test_sector_time_job_builder.py
    ===============================
    import pytest
    from datetime import datetime
    from pathlib import Path
    from types.file import File
    from plugins.sector_time_job_builder import SectorTimeJobGroup

    def test_job_id_creation():
        """Test job ID generation."""
        config = {"platform": "goes18", "timeout_seconds": 300}
        group = SectorTimeJobGroup(config)

        file = File(
            file=Path("test.nc"),
            platform="goes18",
            sector="Full-Disk",
            timestamp=datetime(2024, 1, 15, 12, 0, 0),
            num_expected=16,
        )

        job_ids = group.get_job_ids_from_file(file)
        assert job_ids == ["goes18_full-disk_20240151200000"]

    def test_job_ready_logic():
        """Test job ready when all files received."""
        # ... test implementation

Run tests:

    pytest test_sector_time_job_builder.py

## Common Issues

**Plugin not found:**

-   Check `PYTHONPATH` includes plugin directory
-   Verify `interface = "job_builders"` in plugin
-   Check `name` matches config

**Jobs never become ready:**

-   Check `num_expected` is set correctly in metadata
-   Verify `ready()` logic
-   Look for typos in job ID generation

**Jobs timeout too quickly:**

-   Increase `timeout_seconds` in config
-   Check system time synchronization
-   Verify files arrive within timeout window

## What You Learned

✅ How job builders group files ✅ How to implement custom grouping
logic ✅ How to use metadata for intelligent grouping ✅ How to
implement "ready" conditions ✅ How to handle timeouts ✅ How to test
custom plugins

## Next Steps

-   `` `04-bash-dispatcher ``\` - Process complete scans with bash
-   `` `05-geoips-workflow-dispatcher ``\` - Integrate with GeoIPS
    workflows
-   `` `10-testing-plugins ``\` - Comprehensive plugin testing
-   :doc:`../developer-guide/plugin-development` - Advanced plugin
    development

## Challenge Exercises

1.  **Add sector filtering** - Only process specific sectors (e.g., only
    Full-Disk)
2.  **Implement early emission** - Emit job after 14/16 files with
    timeout
3.  **Add quality checks** - Reject files that are too small or
    corrupted
4.  **Multi-satellite support** - Group files from GOES-16 and GOES-18
    separately

## Complete Code

Full plugin and examples:

\`tutorial03-job-builder/
\](<https://github.com/biosafetylvl5/geoips_driver/tree/main/examples/tutorials/03-job-builder>)

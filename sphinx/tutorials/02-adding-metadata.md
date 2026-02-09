# Tutorial 2: Adding Metadata Configuration

**Level:** Beginner | **Time:** 20 minutes

Learn how to create custom metadata configurations for extracting
information from satellite data filenames. In this tutorial, you'll
configure metadata patterns for GOES-18 CONUS (Continental US) scans.

## Learning Objectives

By the end of this tutorial, you will:

-   Understand metadata configuration structure
-   Create custom filename matching patterns
-   Extract date/time from filenames using regex
-   Configure sector-specific metadata
-   Test metadata extraction

## Prerequisites

-   Completed `` `01-simple-file-watcher ``\`
-   Understanding of regular expressions (basic)
-   Familiarity with GOES-18 filename conventions

## Understanding Metadata Configs

Metadata configs are YAML files that define:

1.  **Filename patterns** - Regex to match specific files
2.  **Metadata fields** - What to extract (platform, sensor, sector,
    etc.)
3.  **Date patterns** - How to parse timestamps from filenames

They're stored as plugins in
`src/geoips_driver/plugins/yaml/data_monitor_configs/`

## Step 1: Examine Built-in Config

Let's look at the built-in GOES-18 configuration:

    cat src/geoips_driver/plugins/yaml/data_monitor_configs/goes18_abi.yaml

Key structure:

    apiVersion: geoips_driver/v1
    name: goes18_abi
    interface: data_monitor_configs
    family: standard
    description: Information necessary to add metadata to GOES-18 ABI L1B data files.

    spec:
      file-metadata:
        goes18_abi_l1b:                    # Base metadata
          platform: goes18
          sensor: abi
          level: L1B
          date: 's(?P<YYYY>\d{4})(?P<JJJ>\d{3})(?P<HH>\d{2})(?P<NN>\d{2})'
          match:
            - '.*M6C[01][1-6].*s\d{4}\d{3}\d{2}\d{2}.*'

        full-disk:                          # Sector-specific
          sector: Full-Disk
          num_expected: 16
          match:
            - '.*RadF.*M6C[01][1-6].*'

        conus:
          sector: CONUS
          num_expected: 16
          match:
            - '.*RadC.*M6C[01][1-6].*'

**How it works:**

1.  File tested against all `match` patterns
2.  If matched, metadata fields applied
3.  Multiple entries can match - metadata is layered

## Step 2: Create CONUS-Specific Config

Let's create a dedicated config for GOES-18 CONUS scans.

Create
`src/geoips_driver/plugins/yaml/data_monitor_configs/goes18_conus_custom.yaml`:

    apiVersion: geoips_driver/v1
    name: goes18_conus_custom
    interface: data_monitor_configs
    family: standard
    description: Custom metadata configuration for GOES-18 ABI CONUS scans.

    docstring: |
      Extracts metadata from GOES-18 ABI CONUS Level 1B filenames.
      Handles Mode 6 operations with all 16 channels.

      Example filename:
      OR_ABI-L1b-RadC-M6C01_G18_s20240151200000_e20240151209143_c20240151209198.nc

    spec:
      file-metadata:
        # Base configuration for all GOES-18 CONUS files
        goes18_conus_base:
          platform: goes18
          sensor: abi
          level: L1B
          sector: CONUS
          num_expected: 16

          # Extract date/time from filename
          # s20240151200000 = start: year 2024, day 015, time 12:00:00
          date: 's(?P<YYYY>\d{4})(?P<JJJ>\d{3})(?P<HH>\d{2})(?P<NN>\d{2})\d{2}'

          # Match CONUS files (RadC) in Mode 6, channels 1-16
          match:
            - 'OR_ABI-L1b-RadC-M6C[01][1-6]_G18_s\d{4}\d{3}\d{6}_e\d{4}\d{3}\d{6}_c\d{4}\d{3}\d{6}\.nc'

        # Channel-specific metadata (optional)
        channel_visible:
          # Channels 1-6 are visible/near-infrared
          match:
            - 'OR_ABI-L1b-RadC-M6C0[1-6]_G18_.*'

        channel_infrared:
          # Channels 7-16 are infrared
          match:
            - 'OR_ABI-L1b-RadC-M6C(0[7-9]|1[0-6])_G18_.*'

Let's break down the date regex:

    s(?P<YYYY>\d{4})(?P<JJJ>\d{3})(?P<HH>\d{2})(?P<NN>\d{2})\d{2}
    │ │           │ │          │ │         │ │         │
    │ └─ Year    │ └─ Day     │ └─ Hour   │ └─ Minute│
    │   (2024)   │    (015)   │    (12)   │    (00)  └─ Seconds (ignored)
    └─ Literal 's' in filename

**Named groups** (`?P<NAME>`) are required for date extraction:

-   `YYYY` - 4-digit year
-   `JJJ` - 3-digit day of year (Julian day)
-   `HH` - 2-digit hour
-   `NN` - 2-digit minute
-   `MM` and `DD` - Can use instead of `JJJ` for month/day

## Step 3: Understanding Match Patterns

Match patterns use Python regex. Common patterns:

**Literal matches:**

    match:
      - 'RadC'  # Matches CONUS (RadC = CONUS scan)

**Character classes:**

    match:
      - 'M6C[01][1-6]'  # M6C01, M6C02, ..., M6C16
      #      │   │
      #      │   └─ Second digit: 1-6
      #      └─ First digit: 0 or 1

**Quantifiers:**

    match:
      - '\d{4}'     # Exactly 4 digits
      - '\d{3}'     # Exactly 3 digits
      - '.*'        # Any characters, any length

**Anchors:**

    match:
      - '^OR_ABI'   # Starts with OR_ABI
      - '\.nc$'     # Ends with .nc

## Step 4: Create Test Service

Create `tutorial02-metadata/watcher.yaml`:

    apiVersion: geoips_driver/v1
    kind: Service
    name: tutorial-02-metadata
    description: Test custom GOES-18 CONUS metadata extraction.

    spec:
      service_namespace: tutorial02
      heartbeat_interval: 30
      rabbitmq:
        host: localhost
        port: 5672
        username: admin
        password: admin_test

      run:
        - watch:
            kind: data_monitor
            name: file_system_poller_watchdog
            config:
              path: ./data/conus
              metadata-tools:
                - goes18_conus_custom  # Use our custom config!

        - build:
            kind: job_builder
            name: DummyJobBuilder
            config: null

        - process:
            kind: dispatcher
            name: serial_bash
            config:
              bash_script: |
                echo "File: {file}"
                echo "Platform: {platform}"
                echo "Sensor: {sensor}"
                echo "Sector: {sector}"
                echo "Timestamp: {timestamp}"

**Note:** Template variables like `{platform}` may not be available in
all dispatchers. This is illustrative - check your dispatcher's
documentation.

## Step 5: Create Test Files

    mkdir -p tutorial02-metadata/data/conus
    cd tutorial02-metadata/data/conus

    Create test files with CONUS naming pattern
    ===========================================
    touch OR_ABI-L1b-RadC-M6C01_G18_s20240151200000_e20240151209143_c20240151209198.nc
    touch OR_ABI-L1b-RadC-M6C02_G18_s20240151200000_e20240151209143_c20240151209198.nc
    touch OR_ABI-L1b-RadC-M6C07_G18_s20240151200000_e20240151209143_c20240151209198.nc

    Also create a Full-Disk file (should NOT match our CONUS config)
    ================================================================
    touch OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_e20240151209310_c20240151209360.nc

## Step 6: Test Metadata Extraction

We can test metadata extraction without running the full service:

    test_metadata.py
    ================
    from pathlib import Path
    from geoips_driver.types.file import File
    from geoips_driver.utils.metadata import apply_metadata_from_configs
    from geoips_driver.pydantic.data_monitor_configs import DataMonitorConfig
    from geoips_driver.interfaces import data_monitor_configs

    Load our custom config
    ======================
    config_data = data_monitor_configs.get_plugin('goes18_conus_custom')
    config = DataMonitorConfig(**config_data)

    Test CONUS file
    ===============
    conus_file = File(file=Path('data/conus/OR_ABI-L1b-RadC-M6C01_G18_s20240151200000_e20240151209143_c20240151209198.nc'))
    result = apply_metadata_from_configs([config], conus_file, require_match=True)

    print(f"Platform: {result.platform}")
    print(f"Sensor: {result.sensor}")
    print(f"Sector: {result.sector}")
    print(f"Timestamp: {result.timestamp}")
    print(f"Num Expected: {result.num_expected}")

Expected output:

    Platform: goes18
    Sensor: abi
    Sector: CONUS
    Timestamp: 2024-01-15 12:00:00
    Num Expected: 16

## Step 7: Run Full Service

    cd tutorial02-metadata
    geoips-driver run watcher.yaml

Then trigger file detection:

    In another terminal
    ===================
    cp data/conus/OR_ABI-L1b-RadC-M6C01_G18_s20240151200000_e20240151209143_c20240151209198.nc \
       data/conus/test_$(date +%s).nc

Check logs for extracted metadata!

## Step 8: Handle Multiple Sectors

You can layer multiple metadata configs:

    - watch:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: ./data/mixed
          metadata-tools:
            - goes18_abi         # Handles Full-Disk
            - goes18_conus_custom  # Handles CONUS

Files will match the appropriate config based on patterns.

## Advanced: Manual Date Components

Sometimes filenames don't include all date components. You can manually
specify them:

    spec:
      file-metadata:
        partial_date:
          platform: goes18
          sensor: abi
          YYYY: "2024"         # Manually set year
          MM: "01"             # Manually set month
          date: 'day_(?P<DD>\d{2})'  # Extract day from filename
          match:
            - 'day_\d{2}\.nc'

    [``
    This allows matching simpler filenames like ``day_15.nc``.

    Common Patterns Reference
    -------------------------

    **GOES-18 ABI:**

    ```yaml

    Full-Disk
    =========
    match: ['.*RadF.*M6C[01][1-6].*']

    CONUS
    =====
    match: ['.*RadC.*M6C[01][1-6].*']

    Mesoscale 1
    ===========
    match: ['.*RadM1.*M6C[01][1-6].*']

    Mesoscale 2
    ===========
    match: ['.*RadM2.*M6C[01][1-6].*']

**Himawari-9 AHI:**

    Full-Disk
    =========
    match: ['HS_H09_\d{8}_\d{4}.*_FLDK_.*_S[01][0-9]10.*']

    Japan Area
    ==========
    match: ['HS_H09_\d{8}_\d{4}.*_JP0[1-4]_.*']

**Meteosat SEVIRI:**

    Full-Disk
    =========
    match: ['H-000-MSG[23]__-MSG[23]________-.*-\d{12}-.*']

## Troubleshooting

**NoMatchError: No matching config entries found**

-   Check filename exactly matches pattern
-   Test regex at <https://regex101.com>
-   Verify config name is in `metadata-tools` list

**Metadata not extracted (all None)**

-   Check if `date` regex named groups are correct (YYYY, JJJ, HH, NN)
-   Verify match pattern captures your files
-   Look for typos in field names (platform vs Platform)

**Wrong sector extracted:**

-   More specific patterns should come after general ones
-   Check pattern order in `match` list
-   Verify filename actually contains sector indicator

## What You Learned

✅ How metadata configs are structured ✅ How to write filename match
patterns with regex ✅ How to extract dates from filenames ✅ How to
create sector-specific metadata ✅ How to test metadata extraction ✅
How to layer multiple configs

## Next Steps

-   `` `03-custom-job-builder ``\` - Use metadata to group files
    intelligently
-   `` `06-multi-satellite-monitor ``\` - Create configs for multiple
    satellites
-   :doc:`../user-guide/metadata-matching` - Complete metadata matching
    guide

## Challenge Exercises

1.  **Create a metadata config** for GOES-18 Mesoscale sectors (RadM1,
    RadM2)
2.  **Add channel number extraction** to identify which ABI channel a
    file contains
3.  **Create a config for a different satellite** (Himawari, Meteosat,
    etc.)
4.  **Handle multiple modes** - GOES ABI has Mode 3, Mode 4, and Mode 6

## Complete Example Files

Full example available in the tutorial repository:

\`tutorial02-metadata/
\](<https://github.com/biosafetylvl5/geoips_driver/tree/main/examples/tutorials/02-metadata>)

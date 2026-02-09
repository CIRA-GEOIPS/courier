# Tutorial 10: Testing Custom Plugins

**Level:** Advanced | **Time:** 40 minutes

Learn how to write comprehensive tests for custom GeoIPS Driver plugins.
Implement unit tests, integration tests, and end-to-end tests to ensure
plugin reliability and correctness.

## Learning Objectives

By the end of this tutorial, you will:

-   Write unit tests for custom plugins
-   Create integration tests for plugin pipelines
-   Implement end-to-end service tests
-   Use pytest fixtures effectively
-   Mock external dependencies
-   Test error conditions
-   Set up continuous integration

## Prerequisites

-   Completed
    {doc}\[01-simple-file-watcher`through :doc:`09-error-handling\`
-   Python testing experience (pytest)
-   Understanding of mocking and fixtures
-   Familiarity with custom plugin development

## Testing Strategy

**Test pyramid:**

    /\
    /  \
    / E2E \         End-to-End (few, slow)
    /______\
    /        \
    /Integration\      Integration (some, medium)
    /____________\
    /              \
    /   Unit Tests   \    Unit (many, fast)
    /__________________\

**Testing layers:**

1.  **Unit tests**: Individual plugin methods
2.  **Integration tests**: Plugin interactions
3.  **End-to-end tests**: Complete service workflows

## Step 1: Project Structure

Organize tests:

    tutorial10-testing/
    ├── plugins/
    │   ├── __init__.py
    │   ├── custom_monitor.py
    │   ├── custom_builder.py
    │   └── custom_dispatcher.py
    ├── tests/
    │   ├── __init__.py
    │   ├── conftest.py           # Shared fixtures
    │   ├── unit/
    │   │   ├── test_monitor.py
    │   │   ├── test_builder.py
    │   │   └── test_dispatcher.py
    │   ├── integration/
    │   │   ├── test_pipeline.py
    │   │   └── test_workflows.py
    │   └── e2e/
    │       └── test_service.py
    ├── pyproject.toml
    └── pytest.ini

## Step 2: Configure pytest

`pytest.ini`:

    [pytest]
    testpaths = tests
    python_files = test_*.py
    python_classes = Test*
    python_functions = test_*

    Show extra test summary
    =======================
    addopts = 
        -v
        --tb=short
        --strict-markers
        --cov=plugins
        --cov-report=term-missing
        --cov-report=html

    Markers for categorizing tests
    ==============================
    markers =
        unit: Unit tests
        integration: Integration tests
        e2e: End-to-end tests
        slow: Slow running tests
        requires_rabbitmq: Tests requiring RabbitMQ

`pyproject.toml`:

    [tool.pytest.ini_options]
    minversion = "7.0"
    testpaths = ["tests"]

    [tool.coverage.run]
    source = ["plugins"]
    omit = ["*/tests/*"]

    [tool.coverage.report]
    exclude_lines = [
        "pragma: no cover",
        "def __repr__",
        "raise NotImplementedError",
        "if __name__ == .__main__.:",
        "if TYPE_CHECKING:",
    ]

## Step 3: Create Test Fixtures

`tests/conftest.py`:

    """Shared pytest fixtures."""

    import tempfile
    from pathlib import Path
    from datetime import datetime

    import pytest

    from geoips_driver.types.file import File
    from geoips_driver.interfaces.module_based.service import Service


    @pytest.fixture
    def temp_dir():
        """Create temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)


    @pytest.fixture
    def sample_goes18_file(temp_dir):
        """Create sample GOES-18 file."""
        filename = (
            "OR_ABI-L1b-RadF-M6C01_G18_s20240151200000_"
            "e20240151209310_c20240151209360.nc"
        )
        filepath = temp_dir / filename
        filepath.touch()
        return filepath


    @pytest.fixture
    def sample_file_object(sample_goes18_file):
        """Create File object with metadata."""
        return File(
            file=sample_goes18_file,
            hostname="localhost",
            platform="goes18",
            sensor="abi",
            level="L1B",
            sector="Full-Disk",
            num_expected=16,
            timestamp=datetime(2024, 1, 15, 12, 0, 0),
        )


    @pytest.fixture
    def complete_scan_files(temp_dir):
        """Create complete GOES-18 scan (16 channels)."""
        files = []
        for i in range(1, 17):
            filename = (
                f"OR_ABI-L1b-RadF-M6C{i:02d}_G18_s20240151200000_"
                f"e20240151209310_c20240151209360.nc"
            )
            filepath = temp_dir / filename
            filepath.touch()

            file_obj = File(
                file=filepath,
                hostname="localhost",
                platform="goes18",
                sensor="abi",
                sector="Full-Disk",
                num_expected=16,
                timestamp=datetime(2024, 1, 15, 12, 0, 0),
            )
            files.append(file_obj)

        return files


    @pytest.fixture
    def mock_service(mocker):
        """Mock service instance."""
        service = mocker.Mock(spec=Service)
        service.name = "test-service"
        service.namespace = "test"
        return service


    @pytest.fixture
    def mock_rabbitmq(mocker):
        """Mock RabbitMQ connection."""
        connection = mocker.Mock()
        channel = mocker.Mock()
        connection.channel.return_value = channel
        return connection, channel

## Step 4: Unit Test Data Monitor

`tests/unit/test_monitor.py`:

    """Unit tests for custom data monitor."""

    import pytest
    from pathlib import Path
    from datetime import datetime

    from plugins.custom_monitor import CustomFileMonitor
    from geoips_driver.types.file import File


    @pytest.mark.unit
    class TestCustomFileMonitor:
        """Test custom file monitor plugin."""

        def test_initialization(self, mock_service):
            """Test monitor initializes correctly."""
            config = {
                "path": "/test/path",
                "metadata-tools": ["goes18_abi"],
            }

            monitor = CustomFileMonitor(mock_service, config)

            assert monitor.config == config
            assert monitor.service == mock_service

        def test_file_discovery(self, mock_service, temp_dir, sample_goes18_file):
            """Test file discovery in directory."""
            config = {
                "path": str(temp_dir),
                "metadata-tools": ["goes18_abi"],
            }

            monitor = CustomFileMonitor(mock_service, config)

            # Get files (may need to call once or iterate)
            files = list(monitor.find_file())

            assert len(files) > 0
            assert any(f.file == sample_goes18_file for f in files)

        def test_metadata_extraction(self, mock_service, sample_goes18_file):
            """Test metadata is extracted correctly."""
            config = {
                "path": str(sample_goes18_file.parent),
                "metadata-tools": ["goes18_abi"],
            }

            monitor = CustomFileMonitor(mock_service, config)

            # Process file
            file_obj = monitor._process_file(sample_goes18_file)

            assert file_obj.platform == "goes18"
            assert file_obj.sensor == "abi"
            assert file_obj.sector == "Full-Disk"

        def test_handles_missing_directory(self, mock_service):
            """Test monitor handles missing directory."""
            config = {
                "path": "/nonexistent/path",
                "metadata-tools": ["goes18_abi"],
            }

            monitor = CustomFileMonitor(mock_service, config)

            # Should not raise, but log error
            files = list(monitor.find_file())
            assert len(files) == 0

        def test_skip_invalid_files(self, mock_service, temp_dir):
            """Test monitor skips invalid files."""
            # Create invalid file
            invalid_file = temp_dir / "invalid.txt"
            invalid_file.touch()

            config = {
                "path": str(temp_dir),
                "metadata-tools": ["goes18_abi"],
            }

            monitor = CustomFileMonitor(mock_service, config)
            files = list(monitor.find_file())

            # Should skip invalid file
            assert all(f.file.suffix == ".nc" for f in files)

## Step 5: Unit Test Job Builder

`tests/unit/test_builder.py`:

    """Unit tests for custom job builder."""

    import pytest
    from datetime import datetime

    from plugins.custom_builder import (
        CustomJobBuilder,
        CustomJobGroup,
        CustomJob,
    )


    @pytest.mark.unit
    class TestCustomJobBuilder:
        """Test custom job builder plugin."""

        def test_initialization(self, mock_service):
            """Test job builder initializes."""
            config = {"timeout_seconds": 300}

            builder = CustomJobBuilder(mock_service, config)

            assert builder.service == mock_service
            assert len(builder.job_groups) > 0

        def test_file_relevance(self, mock_service, sample_file_object):
            """Test file relevance checking."""
            config = {"timeout_seconds": 300}
            job_group = CustomJobGroup(config)

            # GOES-18 file should be relevant
            assert job_group.file_is_relevant(sample_file_object)

            # Change platform
            sample_file_object.platform = "goes16"
            # May or may not be relevant depending on config

        def test_job_id_generation(self, mock_service, sample_file_object):
            """Test job ID generation from file metadata."""
            config = {"timeout_seconds": 300}
            job_group = CustomJobGroup(config)

            job_ids = job_group.get_job_ids_from_file(sample_file_object)

            assert len(job_ids) > 0
            assert "goes18" in job_ids[0]
            assert "full-disk" in job_ids[0].lower()

        def test_job_ready_complete_scan(self, mock_service, complete_scan_files):
            """Test job becomes ready with complete scan."""
            config = {"timeout_seconds": 300}
            job_group = CustomJobGroup(config)

            # Add all files
            for file_obj in complete_scan_files:
                job_group.add_file(file_obj)

            # Get the job
            job_id = list(job_group.jobs.keys())[0]
            job = job_group.jobs[job_id]

            # Should be ready
            assert job.ready()
            assert len(job.files) == 16

        def test_job_not_ready_incomplete(self, mock_service, complete_scan_files):
            """Test job not ready with incomplete scan."""
            config = {"timeout_seconds": 300}
            job_group = CustomJobGroup(config)

            # Add only 10 files
            for file_obj in complete_scan_files[:10]:
                job_group.add_file(file_obj)

            job_id = list(job_group.jobs.keys())[0]
            job = job_group.jobs[job_id]

            # Should not be ready
            assert not job.ready()
            assert len(job.files) == 10

        def test_job_timeout(self, mock_service, sample_file_object, mocker):
            """Test job timeout behavior."""
            config = {"timeout_seconds": 1}  # 1 second timeout
            job_group = CustomJobGroup(config)

            # Add one file
            job_group.add_file(sample_file_object)

            # Wait for timeout
            import time
            time.sleep(2)

            # Check timeouts
            timed_out = job_group.check_timeouts()

            assert len(timed_out) > 0

## Step 6: Unit Test Dispatcher

`tests/unit/test_dispatcher.py`:

    """Unit tests for custom dispatcher."""

    import pytest
    from pathlib import Path

    from plugins.custom_dispatcher import CustomDispatcher
    from geoips_driver.interfaces.module_based.job_builders import Job
    from geoips_driver.types.execution_log import ExecutionLog


    @pytest.mark.unit
    class TestCustomDispatcher:
        """Test custom dispatcher plugin."""

        def test_initialization(self, mock_service):
            """Test dispatcher initializes."""
            config = {
                "bash_script": "echo 'test'",
                "timeout_seconds": 60,
            }

            dispatcher = CustomDispatcher(mock_service, config)

            assert dispatcher.service == mock_service
            assert dispatcher.config == config

        def test_template_substitution(self, mock_service, sample_file_object):
            """Test template variable substitution."""
            config = {
                "bash_script": "echo {file}",
            }

            dispatcher = CustomDispatcher(mock_service, config)

            # Create job
            job = Job("test_job", "job_001", {})
            job.add_file(sample_file_object)

            # Render template
            script = dispatcher._render_template(job)

            assert str(sample_file_object.file) in script
            assert "{file}" not in script

        def test_successful_execution(self, mock_service, sample_file_object):
            """Test successful job execution."""
            config = {
                "bash_script": "exit 0",
            }

            dispatcher = CustomDispatcher(mock_service, config)

            job = Job("test_job", "job_001", {})
            job.add_file(sample_file_object)

            logs = dispatcher.get_execution_log(job)

            assert len(logs) > 0
            assert logs[0].return_code == 0

        def test_failed_execution(self, mock_service, sample_file_object):
            """Test failed job execution."""
            config = {
                "bash_script": "exit 1",
            }

            dispatcher = CustomDispatcher(mock_service, config)

            job = Job("test_job", "job_001", {})
            job.add_file(sample_file_object)

            logs = dispatcher.get_execution_log(job)

            assert len(logs) > 0
            assert logs[0].return_code != 0

        def test_timeout_handling(self, mock_service, sample_file_object):
            """Test command timeout."""
            config = {
                "bash_script": "sleep 10",
                "timeout_seconds": 1,
            }

            dispatcher = CustomDispatcher(mock_service, config)

            job = Job("test_job", "job_001", {})
            job.add_file(sample_file_object)

            logs = dispatcher.get_execution_log(job)

            # Should timeout
            assert any("timeout" in log.stderr.lower() for log in logs)

## Step 7: Integration Tests

`tests/integration/test_pipeline.py`:

    """Integration tests for plugin pipeline."""

    import pytest
    from pathlib import Path

    from plugins.custom_monitor import CustomFileMonitor
    from plugins.custom_builder import CustomJobBuilder
    from plugins.custom_dispatcher import CustomDispatcher


    @pytest.mark.integration
    class TestPluginPipeline:
        """Test complete plugin pipeline."""

        def test_monitor_to_builder(
            self,
            mock_service,
            temp_dir,
            complete_scan_files,
        ):
            """Test data flows from monitor to builder."""
            # Create test files
            for file_obj in complete_scan_files:
                file_obj.file.touch()

            # Initialize monitor
            monitor_config = {
                "path": str(temp_dir),
                "metadata-tools": ["goes18_abi"],
            }
            monitor = CustomFileMonitor(mock_service, monitor_config)

            # Initialize builder
            builder_config = {"timeout_seconds": 300}
            builder = CustomJobBuilder(mock_service, builder_config)

            # Monitor finds files
            files = list(monitor.find_file())

            # Builder processes files
            for file_obj in files:
                for job_group in builder.job_groups:
                    job_group.add_file(file_obj)

            # Should create job
            assert len(builder.job_groups[0].jobs) > 0

            # Job should be ready
            job = list(builder.job_groups[0].jobs.values())[0]
            assert job.ready()

        def test_builder_to_dispatcher(
            self,
            mock_service,
            complete_scan_files,
        ):
            """Test jobs flow from builder to dispatcher."""
            # Initialize builder
            builder_config = {"timeout_seconds": 300}
            builder = CustomJobBuilder(mock_service, builder_config)

            # Add files to builder
            for file_obj in complete_scan_files:
                builder.job_groups[0].add_file(file_obj)

            # Get ready job
            job = list(builder.job_groups[0].jobs.values())[0]
            assert job.ready()

            # Initialize dispatcher
            dispatcher_config = {
                "bash_script": "echo {file}",
            }
            dispatcher = CustomDispatcher(mock_service, dispatcher_config)

            # Execute job
            logs = dispatcher.get_execution_log(job)

            assert len(logs) > 0
            assert logs[0].return_code == 0

## Step 8: End-to-End Tests

`tests/e2e/test_service.py`:

    """End-to-end service tests."""

    import pytest
    import time
    import subprocess
    from pathlib import Path


    @pytest.mark.e2e
    @pytest.mark.slow
    @pytest.mark.requires_rabbitmq
    class TestCompleteService:
        """Test complete service operation."""

        def test_service_startup_shutdown(self, temp_dir):
            """Test service starts and stops cleanly."""
            # Create config file
            config = temp_dir / "service.yaml"
            config.write_text("""
    apiVersion: geoips_driver/v1
    kind: Service
    name: test-service
    spec:
        service_namespace: test
        heartbeat_interval: 30
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
                    path: {}
                    metadata-tools: [goes18_abi]
    """.format(temp_dir))

            # Start service
            proc = subprocess.Popen(
                ["geoips-driver", "run", str(config)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # Wait for startup
            time.sleep(5)

            # Check process is running
            assert proc.poll() is None

            # Stop service
            proc.terminate()
            proc.wait(timeout=10)

            assert proc.returncode in [0, -15]  # Clean exit or SIGTERM

        def test_file_processing_workflow(self, temp_dir, complete_scan_files):
            """Test complete file processing workflow."""
            # Setup directories
            incoming = temp_dir / "incoming"
            products = temp_dir / "products"
            incoming.mkdir()
            products.mkdir()

            # Create config
            config = temp_dir / "service.yaml"
            # ... (create full service config)

            # Start service
            proc = subprocess.Popen(["geoips-driver", "run", str(config)])
            time.sleep(5)

            try:
                # Copy files to incoming
                for file_obj in complete_scan_files:
                    dest = incoming / file_obj.file.name
                    file_obj.file.rename(dest)

                # Wait for processing
                time.sleep(30)

                # Check products were created
                assert len(list(products.glob("*"))) > 0

            finally:
                proc.terminate()
                proc.wait()

## Step 9: Parameterized Tests

Test with multiple inputs:

    @pytest.mark.parametrize("platform,expected_channels", [
        ("goes18", 16),
        ("goes16", 16),
        ("himawari9", 16),
        ("meteosat9", 12),
    ])
    def test_platform_channels(platform, expected_channels, mock_service):
        """Test different platforms have correct channel counts."""
        file_obj = File(
            file=Path(f"test_{platform}.nc"),
            platform=platform,
            num_expected=expected_channels,
        )

        config = {"timeout_seconds": 300}
        job_group = CustomJobGroup(config)

        # Add files
        for i in range(expected_channels):
            job_group.add_file(file_obj)

        job = list(job_group.jobs.values())[0]
        assert job.ready()

## Step 10: Running Tests

    Run all tests
    =============
    pytest

    Run specific test file
    ======================
    pytest tests/unit/test_monitor.py

    Run specific test
    =================
    pytest tests/unit/test_monitor.py::TestCustomFileMonitor::test_initialization

    Run by marker
    =============
    pytest -m unit
    pytest -m integration
    pytest -m e2e

    Run with coverage
    =================
    pytest --cov=plugins --cov-report=html

    Run in parallel
    ===============
    pytest -n auto

    Verbose output
    ==============
    pytest -v -s

## Continuous Integration

`.github/workflows/tests.yml`:

    name: Tests

    on: [push, pull_request]

    jobs:
      test:
        runs-on: ubuntu-latest

        services:
          rabbitmq:
            image: rabbitmq:3-management
            ports:
              - 5672:5672
              - 15672:15672

        steps:
          - uses: actions/checkout@v3

          - name: Set up Python
            uses: actions/setup-python@v4
            with:
              python-version: '3.11'

          - name: Install dependencies
            run: |
              pip install poetry
              poetry install

          - name: Run unit tests
            run: poetry run pytest -m unit --cov=plugins

          - name: Run integration tests
            run: poetry run pytest -m integration

          - name: Upload coverage
            uses: codecov/codecov-action@v3

## What You Learned

✅ Writing unit tests for plugins ✅ Creating integration tests ✅
End-to-end testing strategies ✅ Using pytest fixtures ✅ Mocking
dependencies ✅ Parameterized testing ✅ CI/CD integration

## Next Steps

-   :doc:`../developer-guide/testing` - Advanced testing strategies
-   :doc:`../developer-guide/contributing` - Contributing guidelines

## Challenge Exercises

1.  **Property-based testing** - Use Hypothesis for generators
2.  **Performance testing** - Benchmark plugin throughput
3.  **Load testing** - Test with thousands of files
4.  **Mutation testing** - Use mutmut to verify test quality

## Complete Code

\`tutorial10-testing/
\](<https://github.com/biosafetylvl5/geoips_driver/tree/main/examples/tutorials/10-testing>)

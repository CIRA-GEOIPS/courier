"""End-to-end integration tests for the Courier pipeline.

Tests verify that files flow through the complete pipeline:
DataMonitor -> FILE_FOUND_QUEUE -> JobBuilder -> JOB_READY_QUEUE -> Dispatcher

No external services required -- uses kombu ``memory://`` transport.
Run with: ``pytest tests/test_integration.py -m integration -v``
"""

import contextlib
import threading
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import prometheus_client
import pytest

from courier.config import ServiceConfig
from courier.plugins.classes.data_monitors.cron_glob import CronGlob
from courier.plugins.classes.data_monitors.file_system_poller_watchdog import (
    FileSystemPoller,
)
from courier.plugins.classes.dispatchers.serial_bash import SerialBashDispatcher
from courier.plugins.classes.job_builders.dummy_job_builder import DummyJobBuilder
from courier.service import Service

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _poll_for_file(path: Path, timeout: float = 30.0) -> bool:
    """Block until *path* exists on disk or *timeout* seconds elapse.

    Parameters
    ----------
    path : Path
        File path to wait for.
    timeout : float
        Maximum seconds to wait.

    Returns
    -------
    bool
        ``True`` if the file appeared before the deadline.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            return True
        time.sleep(0.5)
    return False


def _poll_for_content(
    path: Path,
    expected: list[str],
    timeout: float = 30.0,
) -> bool:
    """Block until *path* contains every string in *expected*.

    Parameters
    ----------
    path : Path
        File path to read.
    expected : list[str]
        Strings that must all appear in the file content.
    timeout : float
        Maximum seconds to wait.

    Returns
    -------
    bool
        ``True`` if all strings were found before the deadline.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            content = path.read_text()
            if all(s in content for s in expected):
                return True
        time.sleep(0.5)
    return False


def _wait_for_healthy(service: Service, timeout: float = 15.0) -> bool:
    """Wait until the service reports all managers healthy.

    Parameters
    ----------
    service : Service
        Running service instance.
    timeout : float
        Maximum seconds to wait.

    Returns
    -------
    bool
        ``True`` if the service became healthy before the deadline.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if service._health_check():
                return True
        except Exception:  # noqa: S110
            pass
        time.sleep(0.5)
    return False


def _shutdown_service(service: Service, thread: threading.Thread) -> None:
    """Trigger graceful shutdown and join the service thread.

    Parameters
    ----------
    service : Service
        Running service instance.
    thread : threading.Thread
        Thread running ``service.start()``.
    """
    service._signal_handler._shutdown_requested = True
    service._signal_handler.stop_event.set()
    for info in service._plugin_manager._plugins.values():
        if hasattr(info.plugin, "_stop_event"):
            info.plugin._stop_event.set()
    thread.join(timeout=30)


def _make_service_config() -> ServiceConfig:
    """Create a ``ServiceConfig`` using the in-memory kombu transport.

    Returns
    -------
    ServiceConfig
        Config with ``memory://`` broker and a unique namespace.
    """
    return ServiceConfig(
        broker_url="memory://",
        prometheus_port=0,
        heartbeat_interval=2,
        plugin_health_check_interval=1,
        namespace=f"test-{uuid.uuid4().hex[:8]}",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _prometheus_cleanup(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Prevent Prometheus HTTP server and clean up dynamic metrics."""
    monkeypatch.setattr(
        prometheus_client,
        "start_http_server",
        lambda *_args, **_kwargs: None,
    )
    yield
    # Remove metrics created in manager/plugin __init__ methods so that
    # subsequent tests can re-register them without ValueError.
    to_remove: set[Any] = set()
    for name, collector in list(
        prometheus_client.REGISTRY._names_to_collectors.items(),
    ):
        if name.startswith(("broker_", "last_file_emitted")):
            to_remove.add(collector)
    for collector in to_remove:
        with contextlib.suppress(Exception):
            prometheus_client.REGISTRY.unregister(collector)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_cron_glob_single_file_end_to_end(tmp_path: Path) -> None:
    """CronGlob detects a pre-existing file and the pipeline dispatches it.

    Pipeline: CronGlob (run_on_start) -> DummyJobBuilder -> SerialBash (cp).
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    test_file = input_dir / "sample.nc"
    test_file.write_text("sensor data payload")

    service = Service(_make_service_config())
    service.register_plugin(
        CronGlob,
        {
            "path": str(input_dir),
            "glob_pattern": "*.nc",
            "cron_expression": "* * * * *",
            "run_on_start": True,
            "ignore_existing": False,
            "hostname": "test-host",
        },
    )
    service.register_plugin(DummyJobBuilder, {})
    service.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "cp {file} " + str(output_dir) + "/"},
        identifier="runner",
    )

    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()

    try:
        assert _wait_for_healthy(service), "Service did not become healthy"

        output_file = output_dir / "sample.nc"
        assert _poll_for_file(output_file), (
            f"Pipeline did not produce {output_file}"
        )
        assert output_file.read_text() == "sensor data payload"
    finally:
        _shutdown_service(service, thread)


@pytest.mark.integration
def test_watchdog_detects_new_files_end_to_end(tmp_path: Path) -> None:
    """FileSystemPoller detects files created after startup.

    Pipeline: FileSystemPoller -> DummyJobBuilder -> SerialBash (echo >> log).
    """
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    processed_log = output_dir / "processed.log"

    service = Service(_make_service_config())
    service.register_plugin(FileSystemPoller, {"path": str(watch_dir)})
    service.register_plugin(DummyJobBuilder, {})
    service.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "echo {file} >> " + str(processed_log)},
        identifier="runner",
    )

    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()

    try:
        assert _wait_for_healthy(service), "Service did not become healthy"

        filenames = ["alpha.dat", "bravo.dat", "charlie.dat"]
        for name in filenames:
            (watch_dir / name).write_text(f"content of {name}")
            time.sleep(1.0)

        assert _poll_for_content(processed_log, filenames, timeout=30), (
            f"Not all files appeared in {processed_log}"
        )
    finally:
        _shutdown_service(service, thread)


@pytest.mark.integration
@pytest.mark.slow
def test_cron_glob_ignore_existing_processes_only_new(tmp_path: Path) -> None:
    """CronGlob with ignore_existing=True skips pre-existing files.

    Pre-existing files are seeded into the seen-set at startup.  A new
    file added after startup is detected on the next cron tick and
    dispatched.  Pre-existing files must not appear in output.

    Pipeline: CronGlob (ignore_existing) -> DummyJobBuilder -> SerialBash (cp).
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Pre-existing files -- should NOT be dispatched
    (input_dir / "old_a.nc").write_text("old a")
    (input_dir / "old_b.nc").write_text("old b")

    service = Service(_make_service_config())
    service.register_plugin(
        CronGlob,
        {
            "path": str(input_dir),
            "glob_pattern": "*.nc",
            "cron_expression": "* * * * *",
            "run_on_start": True,
            "ignore_existing": True,
            "hostname": "test-host",
        },
    )
    service.register_plugin(DummyJobBuilder, {})
    service.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "cp {file} " + str(output_dir) + "/"},
        identifier="runner",
    )

    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()

    try:
        assert _wait_for_healthy(service), "Service did not become healthy"

        # Wait for startup scan to complete, then add a new file
        time.sleep(2)
        new_file = input_dir / "new_data.nc"
        new_file.write_text("fresh data")

        # Wait for the next cron tick to detect the new file (up to ~65s)
        output_new = output_dir / "new_data.nc"
        assert _poll_for_file(output_new, timeout=90), (
            "New file was not dispatched within timeout"
        )
        assert output_new.read_text() == "fresh data"

        # Give extra time to confirm old files never show up
        time.sleep(3)
        assert not (output_dir / "old_a.nc").exists(), (
            "Pre-existing file old_a.nc should not have been dispatched"
        )
        assert not (output_dir / "old_b.nc").exists(), (
            "Pre-existing file old_b.nc should not have been dispatched"
        )
    finally:
        _shutdown_service(service, thread)

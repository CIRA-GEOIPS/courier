"""Extended integration tests for the Courier pipeline.

Tests verify routing, deduplication, lifecycle, namespace isolation,
plugin monitoring, and advanced routing patterns.
"""

import concurrent.futures
import contextlib
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import prometheus_client
import pytest

from courier.config import ServiceConfig
from courier.constants import (
    DISPATCHER_QUEUE,
    FILE_FOUND_EXCHANGE,
    PluginRunState,
    job_ready_queue_for,
)
from courier.errors import UnknownTargetError
from courier.managers.plugin_manager import PluginStateInfo
from courier.plugins.classes.data_monitors.cron_glob import CronGlob
from courier.plugins.classes.dispatchers.serial_bash import SerialBashDispatcher
from courier.plugins.classes.job_builders.dummy_job_builder import DummyJobBuilder
from courier.plugins.classes.job_builders.filter_and_group import (
    FilterAndGroupJobBuilder,
)
from courier.plugins.classes.job_builders.metadata_router import (
    MetadataRouterBuilder,
)
from courier.service import Service
from courier.types.execution_log import ExecutionLog
from courier.types.file import File
from courier.types.job import Job

# ---------------------------------------------------------------------------
# Helpers (reused pattern from test_integration.py)
# ---------------------------------------------------------------------------


def _poll_for_file(path: Path, timeout: float = 30.0) -> bool:
    """Block until *path* exists on disk or *timeout* seconds elapse."""
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
    """Block until *path* contains every string in *expected*."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            content = path.read_text()
            if all(s in content for s in expected):
                return True
        time.sleep(0.5)
    return False


def _wait_for_healthy(service: Service, timeout: float = 15.0) -> bool:
    """Wait until the service reports all managers healthy."""
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
    """Trigger graceful shutdown and join the service thread."""
    service._signal_handler._shutdown_requested = True
    service._signal_handler.stop_event.set()
    for info in service._plugin_manager.get_plugins().values():
        if hasattr(info.plugin, "_stop_event"):
            info.plugin._stop_event.set()
    thread.join(timeout=30)


def _make_service_config() -> ServiceConfig:
    """Create a ``ServiceConfig`` using the in-memory kombu transport."""
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
def test_multi_builder_multi_dispatcher_explicit_routing(
    tmp_path: Path,
) -> None:
    """Two builders route exclusively to separate dispatchers.

    Builder_A targets only ``runner_a``, Builder_B targets only ``runner_b``.
    A single file is manually published to FILE_FOUND_EXCHANGE after healthy —
    both builders consume it via fanout and each dispatches to its target.
    The test confirms that each dispatcher's output directory receives a copy.
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_a = tmp_path / "output_a"
    output_a.mkdir()
    output_b = tmp_path / "output_b"
    output_b.mkdir()

    test_file = input_dir / "data.txt"
    test_file.write_text("explicit routing payload")

    service = Service(_make_service_config())
    service.register_plugin(DummyJobBuilder, {}, identifier="builder_a")
    service.register_plugin(DummyJobBuilder, {}, identifier="builder_b")
    service.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "cp {{ files[0].file }} " + str(output_a) + "/"},
        identifier="runner_a",
    )
    service.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "cp {{ files[0].file }} " + str(output_b) + "/"},
        identifier="runner_b",
    )

    service.configure_routing(
        dispatcher_identifiers=["runner_a", "runner_b"],
        builder_targets={
            "builder_a": ("runner_a",),
            "builder_b": ("runner_b",),
        },
        allow_implicit_target=False,
    )

    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()

    try:
        assert _wait_for_healthy(service), "Service did not become healthy"

        service.emit(
            queue=FILE_FOUND_EXCHANGE,
            message=str(File(file=test_file, hostname="test")),
        )

        copied_a = output_a / "data.txt"
        copied_b = output_b / "data.txt"
        assert _poll_for_file(copied_a), f"runner_a did not produce {copied_a}"
        assert _poll_for_file(copied_b), f"runner_b did not produce {copied_b}"
        assert copied_a.read_text() == "explicit routing payload"
        assert copied_b.read_text() == "explicit routing payload"
    finally:
        _shutdown_service(service, thread)


@pytest.mark.integration
def test_single_builder_fan_out_to_multiple_dispatchers(
    tmp_path: Path,
) -> None:
    """One builder targeting two dispatchers fans out every job to both.

    A single file is manually published to FILE_FOUND_EXCHANGE after healthy.
    The single builder fans the job out to both ``runner_a`` and ``runner_b``,
    so the file appears in two output directories.
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_a = tmp_path / "out_a"
    output_a.mkdir()
    output_b = tmp_path / "out_b"
    output_b.mkdir()

    test_file = input_dir / "fanout.dat"
    test_file.write_text("fan-out payload")

    service = Service(_make_service_config())
    service.register_plugin(DummyJobBuilder, {}, identifier="fanout_builder")
    service.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "cp {{ files[0].file }} " + str(output_a) + "/"},
        identifier="runner_a",
    )
    service.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "cp {{ files[0].file }} " + str(output_b) + "/"},
        identifier="runner_b",
    )

    service.configure_routing(
        dispatcher_identifiers=["runner_a", "runner_b"],
        builder_targets={
            "fanout_builder": ("runner_a", "runner_b"),
        },
        allow_implicit_target=False,
    )

    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()

    try:
        assert _wait_for_healthy(service), "Service did not become healthy"

        service.emit(
            queue=FILE_FOUND_EXCHANGE,
            message=str(File(file=test_file, hostname="test")),
        )

        file_a = output_a / "fanout.dat"
        file_b = output_b / "fanout.dat"
        assert _poll_for_file(file_a), f"Fan-out to runner_a failed: {file_a}"
        assert _poll_for_file(file_b), f"Fan-out to runner_b failed: {file_b}"
        assert file_a.read_text() == "fan-out payload"
        assert file_b.read_text() == "fan-out payload"
    finally:
        _shutdown_service(service, thread)


@pytest.mark.integration
def test_preflight_rejects_unknown_target() -> None:
    """Preflight raises UnknownTargetError when a builder references a
    dispatcher that was never registered.
    """
    service = Service(_make_service_config())
    service.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "echo ok"},
        identifier="runner",
    )
    service.register_plugin(DummyJobBuilder, {}, identifier="bad_builder")
    service.configure_routing(
        dispatcher_identifiers=["runner"],
        builder_targets={
            "bad_builder": ("nonexistent",),
        },
        allow_implicit_target=False,
    )

    with pytest.raises(UnknownTargetError) as exc_info:
        service.preflight_check()

    assert "nonexistent" in str(exc_info.value)
    assert "bad_builder" in str(exc_info.value)
    assert "runner" in str(exc_info.value)


@pytest.mark.integration
def test_filter_and_group_with_files_per_job(tmp_path: Path) -> None:
    """FilterAndGroupJobBuilder groups files into jobs, with overflow.

    Three files in the same 3600-second time bucket with ``files_per_job=2``
    ``min_files=1``, and ``window_timeout_seconds=2``.  The first two files
    create one ready job (fast path); the third file triggers an overflow
    job that becomes ready via the reaper (dropout path).

    All three filenames appear in the output.  Because SerialBashDispatcher
    passes only the first file of a job to the bash script, the script
    echoes every ``.dat`` file in the watch directory on each invocation
    so that multi-file job output is not lost.
    """
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    processed_log = output_dir / "processed.log"

    service = Service(_make_service_config())
    service.register_plugin(
        FilterAndGroupJobBuilder,
        {
            "files_per_job": 2,
            "min_files": 1,
            "window_timeout_seconds": 2,
            "time_grouping": {
                "seconds": 3600,
                "start": "2020-01-01 00:00:00",
            },
        },
        identifier="grouper",
    )
    service.register_plugin(
        SerialBashDispatcher,
        {
            "bash_script": (
                "{% for f in files %}"
                "echo {{ f.file }} >> "
                + str(processed_log)
                + ";"
                "{% endfor %}"
            ),
        },
        identifier="runner",
    )
    service.configure_routing(
        dispatcher_identifiers=["runner"],
        builder_targets={"grouper": ("runner",)},
        allow_implicit_target=False,
    )

    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()

    try:
        assert _wait_for_healthy(service), "Service did not become healthy"

        filenames = ["alpha.dat", "bravo.dat", "charlie.dat"]
        for i, name in enumerate(filenames):
            file_path = watch_dir / name
            file_path.write_text(f"content-{name}")
            f = File(
                file=file_path,
                hostname="test",
                timestamp=datetime(2020, 1, 1, 0, 0, i * 15),
            )
            service.emit(queue=FILE_FOUND_EXCHANGE, message=str(f))
            time.sleep(0.2)

        # All three files appear: first 2 in one job (fast path),
        # third in overflow job (dropout / reaper path).
        assert _poll_for_content(
            processed_log,
            filenames,
            timeout=15,
        ), f"Expected files not found in {processed_log}"
    finally:
        _shutdown_service(service, thread)


@pytest.mark.integration
def test_dispatcher_dedupe_lru_prevents_reprocessing(
    tmp_path: Path,
) -> None:
    """Dispatchers LRU deduplication prevents processing the same job twice.

    After the service is healthy, the same Job JSON is published to the
    dispatcher's incoming queue twice. The second occurrence is skipped
    by ``_recently_seen``, so the output side effect appears only once.
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    processed_log = output_dir / "dedupe.log"

    # Seed a file that the CronGlob will pick up so the full pipeline
    # routes a real job through — this makes the service healthy and
    # gives the dispatcher a genuine first job to process.
    seed_file = input_dir / "seed.nc"
    seed_file.write_text("seed")

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
        {"bash_script": "echo {{ files[0].file }} >> " + str(processed_log)},
        identifier="runner",
    )

    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()

    try:
        assert _wait_for_healthy(service), "Service did not become healthy"

        # Wait for the pipeline to process the seed file so the log is populated.
        assert _poll_for_content(processed_log, ["seed.nc"], timeout=15), (
            f"Seed file not processed: {processed_log}"
        )

        # Count lines after seed processing.
        line_count_before = len(processed_log.read_text().splitlines())

        # Build a synthetic Job referencing a real file on disk.
        dup_file = input_dir / "dup_synthetic.txt"
        dup_file.write_text("duplicate test")
        synthetic_file = File(file=dup_file, hostname="test").freeze()
        synthetic_job = Job(
            name="dedupe-job",
            identifier="dedupe-id-001",
            config={},
            files=[synthetic_file],
        )
        job_json = str(synthetic_job)

        # Publish the identical Job JSON twice.
        target_queue = job_ready_queue_for("runner")
        service.emit(queue=target_queue, message=job_json)
        service.emit(queue=target_queue, message=job_json)

        # Allow time for both messages to be consumed.
        time.sleep(3)

        line_count_after = len(processed_log.read_text().splitlines())
        # Only one additional line should appear — the duplicate is skipped.
        assert line_count_after == line_count_before + 1, (
            f"Expected +1 line after dedupe, got "
            f"{line_count_after} (was {line_count_before})"
        )
    finally:
        _shutdown_service(service, thread)


@pytest.mark.integration
def test_execution_log_flows_back(tmp_path: Path) -> None:
    """Every dispatched job produces an ExecutionLog on DISPATCHER_QUEUE.

    After one end-to-end file flows through the pipeline, consume the
    DISPATCHER_QUEUE and verify an ExecutionLog message with the expected
    fields is present.
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    test_file = input_dir / "report.nc"
    test_file.write_text("execution log test")

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
        {"bash_script": "cp {{ files[0].file }} " + str(output_dir) + "/"},
        identifier="runner",
    )

    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()

    try:
        assert _wait_for_healthy(service), "Service did not become healthy"

        copied = output_dir / "report.nc"
        assert _poll_for_file(copied), f"Pipeline did not produce {copied}"

        # Drain a single ExecutionLog from the dispatcher queue.
        # Use a thread with timeout so the test never blocks indefinitely.
        def _consume_one() -> ExecutionLog | None:
            for raw_message in service.consume(DISPATCHER_QUEUE):
                return ExecutionLog.from_string(raw_message)
            return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_consume_one)
            log_entry = future.result(timeout=15)

        assert log_entry is not None, (
            "No ExecutionLog received from DISPATCHER_QUEUE within timeout"
        )
        assert log_entry.return_code is not None, (
            "ExecutionLog must have a return_code"
        )
        assert isinstance(log_entry.return_code, int)
    finally:
        _shutdown_service(service, thread)


@pytest.mark.integration
def test_service_startup_health_graceful_shutdown(tmp_path: Path) -> None:
    """Full lifecycle: register plugins, start, become healthy, and shut down.

    Verifies the service thread exits cleanly after a shutdown request.
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    test_file = input_dir / "lifecycle.nc"
    test_file.write_text("lifecycle")

    service = Service(_make_service_config())
    service.register_plugin(DummyJobBuilder, {})
    service.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "cp {{ files[0].file }} " + str(output_dir) + "/"},
        identifier="runner",
    )

    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()

    try:
        assert _wait_for_healthy(service), "Service did not become healthy"

        # Emit file manually to avoid fanout-exchange race with CronGlob.
        service.emit(
            queue=FILE_FOUND_EXCHANGE,
            message=str(File(file=test_file, hostname="test-host")),
        )

        # Confirm the job flowed through.
        copied = output_dir / "lifecycle.nc"
        assert _poll_for_file(copied), (
            f"Pipeline did not produce {copied}"
        )
        assert copied.read_text() == "lifecycle"
    finally:
        _shutdown_service(service, thread)

    # Thread should exit without error (no exception leaked).
    assert not thread.is_alive(), "Service thread did not exit after shutdown"


@pytest.mark.integration
def test_implicit_routing_auto_wires_sole_dispatcher(
    tmp_path: Path,
) -> None:
    """When no explicit routing is configured, the sole dispatcher is
    auto-wired to every builder by preflight.

    No call to ``configure_routing`` — preflight discovers the single
    dispatcher and routes the builder to it automatically.
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    test_file = input_dir / "implicit.nc"
    test_file.write_text("implicit routing")

    service = Service(_make_service_config())
    service.register_plugin(DummyJobBuilder, {})
    service.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "cp {{ files[0].file }} " + str(output_dir) + "/"},
        identifier="runner",
    )
    # NOTE: configure_routing is deliberately NOT called.

    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()

    try:
        assert _wait_for_healthy(service), "Service did not become healthy"

        # Manually publish to avoid CronGlob race (fanout exchange does not buffer).
        service.emit(
            queue=FILE_FOUND_EXCHANGE,
            message=str(File(file=test_file, hostname="test-host")),
        )

        output_file = output_dir / "implicit.nc"
        assert _poll_for_file(output_file), (
            f"Implicit routing did not produce {output_file}"
        )
        assert output_file.read_text() == "implicit routing"
    finally:
        _shutdown_service(service, thread)


@pytest.mark.integration
def test_namespace_isolation_between_services(tmp_path: Path) -> None:
    """Two services with different namespaces do not interfere.

    Service A (``ns-a``) and Service B (``ns-b``) share the same
    ``memory://`` transport but their queue names are namespace-prefixed.
    Publishing a file to Service A's FILE_FOUND_EXCHANGE does not cause
    Service B to process it.
    """
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_a = tmp_path / "output_a"
    output_a.mkdir()
    output_b = tmp_path / "output_b"
    output_b.mkdir()

    config_a = ServiceConfig(
        broker_url="memory://",
        prometheus_port=0,
        heartbeat_interval=2,
        plugin_health_check_interval=1,
        namespace="ns-a",
    )
    config_b = ServiceConfig(
        broker_url="memory://",
        prometheus_port=0,
        heartbeat_interval=2,
        plugin_health_check_interval=1,
        namespace="ns-b",
    )

    service_a = Service(config_a)
    service_b = Service(config_b)

    # NOTE: CronGlob deliberately NOT registered --- both services would
    # scan the same input_dir, creating a timing race that obscures the
    # namespace-isolation signal. Files are published manually below.
    for svc in (service_a, service_b):
        svc.register_plugin(DummyJobBuilder, {})

    service_a.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "cp {{ files[0].file }} " + str(output_a) + "/"},
        identifier="runner",
    )
    service_b.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "cp {{ files[0].file }} " + str(output_b) + "/"},
        identifier="runner",
    )

    # Verify queue names are namespace-prefixed.
    qa = service_a._broker_manager.get_queue_name(FILE_FOUND_EXCHANGE)
    qb = service_b._broker_manager.get_queue_name(FILE_FOUND_EXCHANGE)
    assert qa.startswith("ns-a-"), f"Unexpected queue name for ns-a: {qa}"
    assert qb.startswith("ns-b-"), f"Unexpected queue name for ns-b: {qb}"
    assert qa != qb, "Namespaced queue names must differ"

    thread_a = threading.Thread(target=service_a.start, daemon=True)
    thread_b = threading.Thread(target=service_b.start, daemon=True)
    thread_a.start()
    thread_b.start()

    try:
        assert _wait_for_healthy(service_a), "Service A did not become healthy"
        assert _wait_for_healthy(service_b), "Service B did not become healthy"

        # Publish a file ONLY to Service A's FILE_FOUND_EXCHANGE.
        namespace_file = input_dir / "ns_only.nc"
        namespace_file.write_text("namespace isolation test")
        service_a.emit(
            queue=FILE_FOUND_EXCHANGE,
            message=str(File(file=namespace_file, hostname="test")),
        )

        # Service A should process the file; Service B should not.
        assert _poll_for_file(output_a / "ns_only.nc", timeout=15), (
            "Service A did not process namespace-only file"
        )

        # Give Service B time to accidentally process the file (should not).
        time.sleep(3)
        assert not (output_b / "ns_only.nc").exists(), (
            "Service B should NOT have processed file from Service A's queue"
        )
    finally:
        _shutdown_service(service_a, thread_a)
        _shutdown_service(service_b, thread_b)


@pytest.mark.integration
def test_plugin_monitoring_detects_dead_thread(tmp_path: Path) -> None:
    """Plugin monitor transitions a plugin to FAILED when its thread dies.

    A CronGlob subclass whose ``find_file()`` generator exits immediately
    causes the plugin thread to finish. The monitor detects the dead thread,
    marks the plugin FAILED, and attempts a restart.
    """

    class DeadOnArrivalCronGlob(CronGlob):
        """CronGlob whose find_file generator exits immediately."""

        name = "dead_cron_glob"

        def find_file(self):
            """Empty generator — exits immediately, killing the thread."""
            if False:  # pragma: no cover
                yield

    watch_dir = tmp_path / "doa_input"
    watch_dir.mkdir()
    output_dir = tmp_path / "doa_output"
    output_dir.mkdir()

    service = Service(
        ServiceConfig(
            broker_url="memory://",
            prometheus_port=0,
            heartbeat_interval=2,
            plugin_health_check_interval=1,
            plugin_restart_delay=0,
            plugin_max_restart_attempts=3,
            namespace=f"test-doa-{uuid.uuid4().hex[:8]}",
        ),
    )
    service.register_plugin(
        DeadOnArrivalCronGlob,
        {
            "path": str(watch_dir),
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
        {"bash_script": "cp {{ files[0].file }} " + str(output_dir) + "/"},
        identifier="runner",
    )

    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()

    try:
        # The plugin thread dies immediately, but the service heartbeat
        # loop still reports healthy (other managers are fine).
        assert _wait_for_healthy(service), "Service did not become healthy"

        # Wait for plugin monitor to detect the dead thread.
        # Use restart_count as the stable signal: with fast restart
        # (plugin_restart_delay=0), FAILED can be missed between polls.
        deadline = time.time() + 15
        restart_count = 0
        while time.time() < deadline:
            plugins = service._plugin_manager.get_plugins()
            for _key, info in plugins.items():
                if info.restart_count > 0:
                    restart_count = info.restart_count
                    break
            if restart_count > 0:
                break
            time.sleep(0.5)

        assert restart_count > 0, (
            f"Plugin monitor did not detect dead thread; restart_count={restart_count}"
        )

        # After observing restart, give it time to settle and verify
        # the plugin is not in a STOPPED state (should be RUNNING or FAILED).
        time.sleep(2)
        plugins = service._plugin_manager.get_plugins()
        for _key, info in plugins.items():
            if info.restart_count > 0:
                assert info.state != PluginRunState.STOPPED, (
                    f"Plugin should not be STOPPED after restart attempts; "
                    f"state={info.state}"
                )
                break
    finally:
        _shutdown_service(service, thread)


@pytest.mark.integration
def test_metadata_router_routes_by_source(tmp_path: Path) -> None:
    """MetadataRouterBuilder routes files to dispatchers by source attribute.

    Two routes: ``sat_a`` → ``proc_a`` and ``sat_b`` → ``proc_b``.
    Files published manually with matching ``source`` metadata are processed
    only by the targeted dispatcher.
    """
    output_a = tmp_path / "out_a"
    output_a.mkdir()
    output_b = tmp_path / "out_b"
    output_b.mkdir()

    a_log = output_a / "processed.log"
    b_log = output_b / "processed.log"

    service = Service(_make_service_config())
    service.register_plugin(
        MetadataRouterBuilder,
        {
            "routes": [
                {
                    "name": "sat-a-route",
                    "filters": {"source": "sat_a"},
                    "files_per_job": 1,
                    "targets": ["proc_a"],
                },
                {
                    "name": "sat-b-route",
                    "filters": {"source": "sat_b"},
                    "files_per_job": 1,
                    "targets": ["proc_b"],
                },
            ],
        },
    )
    service.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "echo {{ files[0].file }} >> " + str(a_log)},
        identifier="proc_a",
    )
    service.register_plugin(
        SerialBashDispatcher,
        {"bash_script": "echo {{ files[0].file }} >> " + str(b_log)},
        identifier="proc_b",
    )

    service.configure_routing(
        dispatcher_identifiers=["proc_a", "proc_b"],
        builder_targets={
            "metadata_router": ("proc_a", "proc_b"),
        },
        allow_implicit_target=False,
    )

    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()

    try:
        assert _wait_for_healthy(service), "Service did not become healthy"

        # Create files on disk so SerialBashDispatcher can process them.
        file_a = tmp_path / "sat_a_data.dat"
        file_a.write_text("sat-a-data")
        file_b = tmp_path / "sat_b_data.dat"
        file_b.write_text("sat-b-data")

        # Publish File with source="sat_a"
        service.emit(
            queue=FILE_FOUND_EXCHANGE,
            message=str(File(file=file_a, hostname="test", source="sat_a")),
        )
        # Publish File with source="sat_b"
        service.emit(
            queue=FILE_FOUND_EXCHANGE,
            message=str(File(file=file_b, hostname="test", source="sat_b")),
        )

        # proc_a should only process sat_a files.
        assert _poll_for_content(a_log, [str(file_a)], timeout=15), (
            f"Route sat_a → proc_a did not produce expected log entry in {a_log}"
        )
        assert str(file_b) not in a_log.read_text(), (
            "proc_a should NOT process sat_b file"
        )

        # proc_b should only process sat_b files.
        assert _poll_for_content(b_log, [str(file_b)], timeout=15), (
            f"Route sat_b → proc_b did not produce expected log entry in {b_log}"
        )
        assert str(file_a) not in b_log.read_text(), (
            "proc_b should NOT process sat_a file"
        )
    finally:
        _shutdown_service(service, thread)

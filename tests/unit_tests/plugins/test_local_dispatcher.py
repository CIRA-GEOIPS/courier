"""Behavioural tests for the local dispatcher and its payload execution.

These run real subprocesses through ``LocalDispatcher`` -- hydrate, render,
write the script, execute, clean up -- so the dispatcher config
(``timeout_seconds``, the logging flags, ``output_files``/``scan_stderr``)
is exercised where it takes effect: in the payload the dispatcher hydrates.
"""

from __future__ import annotations

import contextlib
import logging
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from courier.constants import FILE_FOUND_EXCHANGE
from courier.metrics import COURIER_CUSTOM_GAUGE
from courier.plugins.dispatchers.local_dispatcher import (
    LocalDispatcher,
    _ingest_courier_metrics,
)
from courier.errors import CourierError
from courier.plugins.payloads.bash_payload import BashPayload
from courier.plugins.payloads.python_payload import PythonPayload
from courier.types.file import File
from courier.types.job import Job

if TYPE_CHECKING:
    from collections.abc import Iterator

    from courier.interfaces.payloads import Payload
    from courier.types.execution_log import ExecutionLog

#: Upper bound for anything that should finish "promptly" -- well above the
#: one-second timeouts used below, far below the scripts' own sleeps.
_PROMPT_SECONDS = 10.0


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


@pytest.fixture
def private_tmpdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point ``TMPDIR`` at an empty directory, as a deployment would.

    ``tempfile`` caches the temp directory on first use, so the cache is
    cleared too; monkeypatch restores both afterwards.
    """
    directory = tmp_path / "tmpdir"
    directory.mkdir()
    monkeypatch.setenv("TMPDIR", str(directory))
    monkeypatch.setattr(tempfile, "tempdir", None)
    return directory


def _wire(
    service: MagicMock,
    payload_config: dict,
    *,
    identifier: str = "job-1",
    files: list[Path] | None = None,
    payload_cls: type[Payload] = BashPayload,
) -> Job:
    """Return a job carrying a payload (bash by default), as a dispatcher gets it."""
    payload = payload_cls(service, payload_config, "p1")
    job = Job(
        "n",
        identifier,
        {},
        files=[File(file=f).freeze() for f in files or []],
    )
    job.targets = ("ld",)
    job.payload = payload.to_job_spec(job)
    return Job.from_string(str(job))


def _run(
    service: MagicMock,
    script: str,
    dispatcher_config: dict | None = None,
    **payload_config: object,
) -> tuple[list[ExecutionLog], LocalDispatcher]:
    """Execute an inline bash *script* through a LocalDispatcher."""
    dispatcher = LocalDispatcher(service, dispatcher_config or {}, identifier="ld")
    logs = dispatcher.get_execution_log(
        _wire(service, {"script": script, **payload_config}),
    )
    return logs, dispatcher


@contextlib.contextmanager
def _captured(logger_name: str) -> Iterator[list[logging.LogRecord]]:
    """Collect the records *logger_name* emits, whatever its current setup.

    Courier loggers do not propagate, so ``caplog`` cannot see them, and their
    level depends on whichever config configured them first.  Attaching a
    handler directly and forcing the level keeps the capture independent of
    test order.
    """
    records: list[logging.LogRecord] = []

    class _Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger(logger_name)
    handler = _Collector(level=logging.DEBUG)
    previous_level, previous_disabled = logger.level, logger.disabled
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.disabled = False
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
        logger.disabled = previous_disabled


def _gone(pid: int) -> bool:
    """Return True once *pid* no longer exists (polling briefly)."""
    deadline = time.monotonic() + _PROMPT_SECONDS
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


class TestExecution:
    def test_binary_payload_executes(self, service: MagicMock, tmp_path: Path) -> None:
        source = tmp_path / "in.txt"
        source.write_text("payload")
        output_dir = tmp_path / "out"
        output_dir.mkdir()
        wire = _wire(
            service,
            {
                "binary": "cp",
                "suffix_args": ["{{ files[0].file }}", str(output_dir)],
            },
            files=[source],
        )

        dispatcher = LocalDispatcher(service, {}, identifier="ld")
        logs = dispatcher.get_execution_log(wire)

        assert logs[0].return_code == 0
        assert (output_dir / "in.txt").exists()

    def test_deferred_dispatcher_values_resolve(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        template = tmp_path / "t.sh"
        template.write_text('echo "disp={{ dispatcher.identifier }}"')
        wire = _wire(service, {"file": template})

        dispatcher = LocalDispatcher(service, {}, identifier="ld")
        logs = dispatcher.get_execution_log(wire)

        assert logs[0].return_code == 0
        assert "disp=ld" in logs[0].stdout

    def test_untrusted_data_is_literal(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        template = tmp_path / "t.sh"
        template.write_text("echo {{ files[0].file }}")
        wire = _wire(service, {"file": template}, files=[Path("/data/{{ 6*7 }}.nc")])

        dispatcher = LocalDispatcher(service, {}, identifier="ld")
        logs = dispatcher.get_execution_log(wire)

        assert "{{ 6*7 }}" in logs[0].stdout
        assert "42" not in logs[0].stdout

    def test_dispatcher_config_values_resolve_in_pass_two(
        self,
        service: MagicMock,
    ) -> None:
        """Nested dispatcher values keep their full access path to pass two."""
        logs, _ = _run(
            service,
            'echo "t={{ dispatcher.config.timeout_seconds }} '
            'name={{ dispatcher.name }}"',
            {"timeout_seconds": 42},
        )

        assert logs[0].return_code == 0
        assert "t=42.0 name=local_dispatcher" in logs[0].stdout

    def test_value_this_dispatcher_does_not_define_fails_the_job(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        """``output_dir`` exists only on dispatchers that provide one.

        On a local dispatcher the job must fail loudly, never run with the
        name rendered as an empty string, and leave no script behind.
        """
        dispatcher = LocalDispatcher(service, {}, identifier="ld")
        wire = _wire(service, {"script": 'rm -rf "{{ output_dir }}/"*'})

        with pytest.raises(CourierError, match="output_dir"):
            dispatcher.get_execution_log(wire)

        assert list(private_tmpdir.iterdir()) == []

    def test_non_zero_exit_is_reported_as_an_error(
        self,
        service: MagicMock,
    ) -> None:
        """A failed script must leave an ERROR, not only a return code."""
        with _captured("courier.plugin.local_dispatcher") as records:
            logs, _ = _run(service, "exit 3")

        assert logs[0].return_code == 3  # noqa: PLR2004
        errors = [r.getMessage() for r in records if r.levelno == logging.ERROR]
        assert any("return code(s) [3]" in message for message in errors)


class TestPythonPayload:
    """``python_payload`` scripts run through the dispatcher's own interpreter."""

    def test_inline_script_runs(self, service: MagicMock) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="ld")
        wire = _wire(
            service,
            {
                "script": "import sys\nprint('ran', sys.argv[0].endswith('.py'))",
                "default_binary": sys.executable,
            },
            payload_cls=PythonPayload,
        )

        logs = dispatcher.get_execution_log(wire)

        assert logs[0].return_code == 0, logs[0].stderr
        assert "ran True" in logs[0].stdout

    def test_template_file_runs_with_its_own_suffix(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        template = tmp_path / "job.py"
        template.write_text("print('job {{ job.identifier }}')")
        dispatcher = LocalDispatcher(service, {}, identifier="ld")
        wire = _wire(
            service,
            {"file": template, "default_binary": sys.executable},
            payload_cls=PythonPayload,
        )

        logs = dispatcher.get_execution_log(wire)

        assert logs[0].return_code == 0, logs[0].stderr
        assert "job job-1" in logs[0].stdout

    def test_toolchain_is_probed_with_the_payload_interpreter(
        self,
        service: MagicMock,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="ld")
        wire = _wire(
            service,
            {
                "script": "print('ok')",
                "default_binary": sys.executable,
                "toolchain": ["courier-test-no-such-tool"],
            },
            payload_cls=PythonPayload,
        )

        with pytest.raises(CourierError, match="courier-test-no-such-tool"):
            dispatcher.get_execution_log(wire)


class TestTimeout:
    def test_timeout_kills_the_process_tree(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        """``timeout_seconds`` reaches the payload and kills the whole group.

        Output produced before the timeout is kept, and the grandchild the
        script started is gone too: killing only the shell used to leave the
        real workload running.
        """
        child_pid_file = tmp_path / "child.pid"
        script = f"echo started\nsleep 30 &\necho $! > {child_pid_file}\nwait\n"

        start = time.monotonic()
        logs, _ = _run(service, script, {"timeout_seconds": 1})
        elapsed = time.monotonic() - start

        (log,) = logs
        assert log.return_code == -1
        assert "started" in log.stdout
        assert "timed out after 1.0s" in log.stderr
        assert elapsed < _PROMPT_SECONDS
        assert _gone(int(child_pid_file.read_text())), "grandchild still running"


class TestLogging:
    def test_log_to_file_writes_the_job_log(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        log_dir = tmp_path / "logs"
        logs, _ = _run(
            service,
            "echo hello\necho oops >&2\n",
            {"log_to_file": True, "log_dir": str(log_dir)},
        )

        (log,) = logs
        assert log.return_code == 0
        assert log.log_file_path is not None
        written = Path(log.log_file_path)
        assert written.parent == log_dir
        assert written.name.startswith("dispatch_job-1_")
        content = written.read_text()
        assert "[stdout] hello" in content
        assert "[stderr] oops" in content

    def test_log_to_file_with_a_toolchain_does_not_crash(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        """The toolchain probe has no job log file, and must not need one.

        This combination once raised ValueError from the probe on every job,
        which took the whole service down.  The probe must not write a log
        file of its own either: only the job's log lands in ``log_dir``.
        """
        log_dir = tmp_path / "logs"
        logs, _ = _run(
            service,
            "echo ran",
            {"log_to_file": True, "log_dir": str(log_dir)},
            toolchain=["sh"],
        )

        assert logs[0].return_code == 0
        assert "ran" in logs[0].stdout
        assert len(list(log_dir.iterdir())) == 1

    def test_log_to_logger_streams_prefixed_lines(
        self,
        service: MagicMock,
    ) -> None:
        with _captured("courier.plugin.bash_payload") as records:
            logs, _ = _run(
                service,
                "echo hello\necho oops >&2\n",
                {"log_to_logger": True},
            )

        assert logs[0].return_code == 0
        lines = [(r.levelno, r.getMessage()) for r in records]
        assert any(
            level == logging.DEBUG and "[job: job-1] [stdout] hello" in message
            for level, message in lines
        )
        assert any(
            level == logging.WARNING and "[job: job-1] [stderr] oops" in message
            for level, message in lines
        )

    def test_log_only_errors_discards_stdout(self, service: MagicMock) -> None:
        logs, _ = _run(
            service,
            "echo out\necho err >&2\n",
            {"log_only_errors": True},
        )

        (log,) = logs
        assert log.return_code == 0
        assert log.stdout == ""
        assert log.stderr == "err\n"


class TestCourierMetric:
    def test_wellformed_metric_is_recorded(self) -> None:
        _ingest_courier_metrics("COURIER_METRIC: files_written 42", "ld")

        gauge = COURIER_CUSTOM_GAUGE.labels(
            dispatcher_identifier="ld",
            metric_name="files_written",
        )
        assert gauge._value.get() == 42.0  # noqa: PLR2004

    def test_malformed_metric_does_not_raise(self) -> None:
        _ingest_courier_metrics("COURIER_METRIC: malformed_metric 1.2.3", "ld")

        gauge = COURIER_CUSTOM_GAUGE.labels(
            dispatcher_identifier="ld",
            metric_name="malformed_metric",
        )
        assert gauge._value.get() == 0.0

    @pytest.mark.parametrize(
        "line",
        [
            "COURIER_METRIC: warned_a abc",
            "COURIER_METRIC: warned_b 1.2.3",
            "COURIER_METRIC: warned_c nan",
            "COURIER_METRIC:",
        ],
    )
    def test_every_malformed_metric_line_is_warned_about(self, line: str) -> None:
        with _captured("courier.module.local_dispatcher") as records:
            _ingest_courier_metrics(line, "ld-warn")

        assert [r.levelno for r in records] == [logging.WARNING]
        assert "malformed COURIER_METRIC line" in records[0].getMessage()

    def test_execution_ingests_metrics(self, service: MagicMock) -> None:
        _run(service, "echo 'COURIER_METRIC: jobs_done 7'")

        gauge = COURIER_CUSTOM_GAUGE.labels(
            dispatcher_identifier="ld",
            metric_name="jobs_done",
        )
        assert gauge._value.get() == 7.0  # noqa: PLR2004

    def test_conduit_through_the_consume_loop(self, service: MagicMock) -> None:
        """A malformed metric line is ignored; the job still succeeds."""
        dispatcher = LocalDispatcher(service, {}, identifier="ld-conduit")
        script = (
            "echo 'COURIER_METRIC: conduit_widgets 3'\n"
            "echo 'COURIER_METRIC: conduit_broken 1.2.3'\n"
        )
        wire = _wire(service, {"script": script})
        labels = {
            "status": "success",
            "dispatcher_name": dispatcher.name,
            "dispatcher_identifier": "ld-conduit",
        }
        before = (
            REGISTRY.get_sample_value("courier_dispatcher_jobs_processed_total", labels)
            or 0.0
        )

        def _consume(*_args: object, **_kwargs: object) -> Iterator[tuple[str, None]]:
            yield str(wire), None
            dispatcher._stop_event.set()

        service.consume.side_effect = _consume
        dispatcher.handle_incoming_jobs()

        gauge = COURIER_CUSTOM_GAUGE.labels(
            dispatcher_identifier="ld-conduit",
            metric_name="conduit_widgets",
        )
        assert gauge._value.get() == 3.0  # noqa: PLR2004
        after = REGISTRY.get_sample_value(
            "courier_dispatcher_jobs_processed_total",
            labels,
        )
        assert after == before + 1


def _consume(
    service: MagicMock,
    script: str,
    dispatcher_config: dict | None = None,
) -> None:
    """Run an inline bash *script* the way the consumer loop does.

    Output files are re-emitted by ``_run_job``, next to the execution-log
    publish, not by ``get_execution_log``.
    """
    dispatcher = LocalDispatcher(service, dispatcher_config or {}, identifier="ld")
    job = _wire(service, {"script": script})
    dispatcher._run_job(job, str(job), dispatcher._dedupe_key(job))


def _emitted_files(service: MagicMock) -> list[str]:
    return [
        str(File.from_string(call.kwargs["message"]).file)
        for call in service.emit.call_args_list
        if call.kwargs.get("queue") == FILE_FOUND_EXCHANGE
    ]


class TestOutputFiles:
    def test_output_file_is_re_emitted(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        produced = tmp_path / "product.nc"
        _consume(
            service,
            f'echo "{produced}"',
            {"output_files": [{"pattern": r"(?P<file>/.*\.nc)"}]},
        )

        assert _emitted_files(service) == [str(produced)]

    def test_no_output_files_means_no_scan(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        _consume(service, f'echo "{tmp_path / "x.nc"}"')

        assert _emitted_files(service) == []

    @pytest.mark.parametrize(("scan_stderr", "expected"), [(True, 1), (False, 0)])
    def test_scan_stderr_controls_stderr_discovery(
        self,
        service: MagicMock,
        tmp_path: Path,
        scan_stderr: bool,
        expected: int,
    ) -> None:
        produced = tmp_path / "on-stderr.nc"
        _consume(
            service,
            f'echo "{produced}" >&2',
            {
                "output_files": [{"pattern": r"(?P<file>/.*\.nc)"}],
                "scan_stderr": scan_stderr,
            },
        )

        assert len(_emitted_files(service)) == expected

    def test_get_execution_log_itself_publishes_nothing(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        _run(
            service,
            f'echo "{tmp_path / "product.nc"}"',
            {"output_files": [{"pattern": r"(?P<file>/.*\.nc)"}]},
        )

        assert _emitted_files(service) == []


class TestTempScripts:
    def test_script_lives_in_tmpdir_and_is_removed(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        """TMPDIR is honoured (read-only roots, small /tmp) and nothing leaks."""
        logs, _ = _run(service, 'echo "$0"')

        assert logs[0].return_code == 0
        assert Path(logs[0].stdout.strip()).parent == private_tmpdir
        assert list(private_tmpdir.iterdir()) == []

    def test_failed_run_leaves_nothing_behind(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        logs, _ = _run(service, "exit 7")

        assert logs[0].return_code == 7  # noqa: PLR2004
        assert list(private_tmpdir.iterdir()) == []

    @pytest.mark.parametrize("name", ["tmp{{7*7}}", "tmp{%x", "tmp{#x"])
    def test_script_path_is_never_rendered_as_a_template(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        name: str,
    ) -> None:
        """Only argument templates are rendered; the TMPDIR path stays literal."""
        directory = tmp_path / name
        directory.mkdir()
        monkeypatch.setenv("TMPDIR", str(directory))
        monkeypatch.setattr(tempfile, "tempdir", None)

        logs, _ = _run(
            service,
            'echo "$0 $1"',
            suffix_args=["{{ files | length }}"],
        )

        assert logs[0].return_code == 0, logs[0].stderr
        script, argument = logs[0].stdout.split()
        assert Path(script).parent == directory
        assert argument == "0"

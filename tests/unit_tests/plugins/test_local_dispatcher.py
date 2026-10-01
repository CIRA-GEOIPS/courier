"""Behavioural tests for the local dispatcher and its payload execution.

These run real subprocesses through ``LocalDispatcher`` -- hydrate, render,
write the script, execute, clean up -- so the dispatcher config
(``timeout_seconds``, the logging flags, ``output_files``/``scan_stderr``)
is exercised where it takes effect: in the payload the dispatcher hydrates.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from prometheus_client import REGISTRY

from courier.constants import FILE_FOUND_EXCHANGE
from courier.errors import CourierError
from courier.metrics import COURIER_CUSTOM_GAUGE
from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
from courier.plugins.payloads.python_payload import PythonPayload
from courier.types.file import File
from tests._helpers import (
    captured_records,
    consume,
    file_job,
    run_locally,
    wire_job,
)

if TYPE_CHECKING:
    from unittest.mock import MagicMock

#: Upper bound for anything that should finish "promptly" -- well above the
#: one-second timeouts used below, far below the scripts' own sleeps.
_PROMPT_SECONDS = 10.0


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

        logs = run_locally(
            service,
            {"binary": "cp", "suffix_args": ["{{ files[0].file }}", str(output_dir)]},
            job=file_job(source),
        )

        assert logs[0].return_code == 0
        assert (output_dir / "in.txt").exists()

    def test_untrusted_data_is_literal(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        template = tmp_path / "t.sh"
        template.write_text("echo {{ files[0].file }}")

        logs = run_locally(
            service,
            {"file": template},
            job=file_job("/data/{{ 6*7 }}.nc"),
        )

        assert "{{ 6*7 }}" in logs[0].stdout
        assert "42" not in logs[0].stdout

    def test_dispatcher_values_resolve_on_the_dispatcher(
        self,
        service: MagicMock,
    ) -> None:
        logs = run_locally(
            service,
            {
                "script": 'echo "id={{ dispatcher.identifier }} '
                "name={{ dispatcher.name }} "
                't={{ dispatcher.config.timeout_seconds }}"',
            },
            {"timeout_seconds": 42},
        )

        assert logs[0].return_code == 0
        assert "id=ld name=local_dispatcher t=42.0" in logs[0].stdout

    def test_value_this_dispatcher_does_not_define_fails_the_job(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        """``output_dir`` exists only on dispatchers that provide one.

        On a local dispatcher the job must fail loudly, never run with the
        name rendered as an empty string, and leave no script behind.
        """
        with pytest.raises(CourierError, match="output_dir"):
            run_locally(service, {"script": 'rm -rf "{{ output_dir }}/"*'})

        assert list(private_tmpdir.iterdir()) == []

    def test_non_zero_exit_is_reported_as_an_error(
        self,
        service: MagicMock,
    ) -> None:
        """A failed script must leave an ERROR, not only a return code."""
        with captured_records("courier.plugin.local_dispatcher") as records:
            logs = run_locally(service, {"script": "exit 3"})

        assert logs[0].return_code == 3  # noqa: PLR2004
        errors = [r.getMessage() for r in records if r.levelno == logging.ERROR]
        assert any("return code(s) [3]" in message for message in errors)

    def test_toolchain_is_probed_with_the_payload_interpreter(
        self,
        service: MagicMock,
    ) -> None:
        with pytest.raises(CourierError, match="courier-test-no-such-tool"):
            run_locally(
                service,
                {
                    "script": "print('ok')",
                    "default_binary": sys.executable,
                    "toolchain": ["courier-test-no-such-tool"],
                },
                payload_cls=PythonPayload,
            )


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
        (log,) = run_locally(service, {"script": script}, {"timeout_seconds": 1})
        elapsed = time.monotonic() - start

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
        (log,) = run_locally(
            service,
            {"script": "echo hello\necho oops >&2\n"},
            {"log_to_file": True, "log_dir": str(log_dir)},
        )

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
        logs = run_locally(
            service,
            {"script": "echo ran", "toolchain": ["sh"]},
            {"log_to_file": True, "log_dir": str(log_dir)},
        )

        assert logs[0].return_code == 0
        assert "ran" in logs[0].stdout
        assert len(list(log_dir.iterdir())) == 1

    def test_log_to_logger_streams_prefixed_lines(
        self,
        service: MagicMock,
    ) -> None:
        with captured_records("courier.plugin.bash_payload") as records:
            logs = run_locally(
                service,
                {"script": "echo hello\necho oops >&2\n"},
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
        (log,) = run_locally(
            service,
            {"script": "echo out\necho err >&2\n"},
            {"log_only_errors": True},
        )

        assert log.return_code == 0
        assert log.stdout == ""
        assert log.stderr == "err\n"


class TestCourierMetric:
    def test_execution_ingests_metrics(self, service: MagicMock) -> None:
        run_locally(service, {"script": "echo 'COURIER_METRIC: jobs_done 7'"})

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
        labels = {
            "status": "success",
            "dispatcher_name": dispatcher.name,
            "dispatcher_identifier": "ld-conduit",
        }
        before = (
            REGISTRY.get_sample_value("courier_dispatcher_jobs_processed_total", labels)
            or 0.0
        )

        consume(dispatcher, wire_job(service, {"script": script}))

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


def _emitted_files(
    service: MagicMock,
    script: str,
    dispatcher_config: dict | None = None,
) -> list[str]:
    """Run an inline bash *script* through the consume loop; return its files."""
    dispatcher = LocalDispatcher(service, dispatcher_config or {}, identifier="ld")
    consume(dispatcher, wire_job(service, {"script": script}))
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

        emitted = _emitted_files(
            service,
            f'echo "{produced}"',
            {"output_files": [{"pattern": r"(?P<file>/.*\.nc)"}]},
        )

        assert emitted == [str(produced)]

    def test_no_output_files_means_no_scan(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        assert _emitted_files(service, f'echo "{tmp_path / "x.nc"}"') == []

    @pytest.mark.parametrize(("scan_stderr", "expected"), [(True, 1), (False, 0)])
    def test_scan_stderr_controls_stderr_discovery(
        self,
        service: MagicMock,
        tmp_path: Path,
        scan_stderr: bool,
        expected: int,
    ) -> None:
        emitted = _emitted_files(
            service,
            f'echo "{tmp_path / "on-stderr.nc"}" >&2',
            {
                "output_files": [{"pattern": r"(?P<file>/.*\.nc)"}],
                "scan_stderr": scan_stderr,
            },
        )

        assert len(emitted) == expected


class TestTempScripts:
    def test_failed_run_leaves_nothing_behind(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        logs = run_locally(service, {"script": "exit 7"})

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

        logs = run_locally(
            service,
            {"script": 'echo "$0 $1"', "suffix_args": ["{{ files | length }}"]},
        )

        assert logs[0].return_code == 0, logs[0].stderr
        script, argument = logs[0].stdout.split()
        assert Path(script).parent == directory
        assert argument == "1"

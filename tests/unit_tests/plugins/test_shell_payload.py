import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY
from pydantic import ValidationError

from courier.errors import CourierError, UnexecutableJobError
from courier.interfaces.payloads import DispatcherGroupConfig
from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
from courier.plugins.payloads.shell_payload import (
    RUN_ARGV_SCRIPT,
    ShellPayload,
    ShellPayloadConfig,
)
from courier.types.execution_log import ExecutionLog
from courier.types.file import File
from courier.types.job import Job


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


@pytest.fixture
def config(tmp_path) -> dict:
    file = tmp_path / "demo.sh"
    file.write_text('#!/bin/sh\n\necho "hello world! file: {{ files[0].file }}"')
    return {"file": file}


def _job(identifier: str = "job-1") -> Job:
    return Job("n", identifier, {}, files=[File(file=Path("/d/a.nc")).freeze()])


def _base_config() -> DispatcherGroupConfig:
    return DispatcherGroupConfig.model_validate({})


class TestConstruction:
    def test_payload_construction_with_config(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = ShellPayload(service, config, "dummy-payload")

        assert payload.config.file == Path(config["file"])
        assert payload.generate_calling_method() == ["sh"]

    def test_payload_construction_with_invalid_file_path(
        self, service: MagicMock
    ) -> None:
        with pytest.raises(ValidationError):
            ShellPayload(service, {"file": "invalid_file"}, "dummy-payload")


class TestPayloadWorkflow:
    def test_get_payload_from_binary(self, service, config):
        job = _job()

        config["binary"] = "file"
        config["prefix_args"] = ["-b"]
        payload = ShellPayload(service, config, "dummy-payload")
        payload.base_config = _base_config()
        result = payload.get_payload_from_job(
            ["sh", "-c", f"file -b {payload.config.file}"], job
        )

        assert len(result) > 0
        assert result[0].return_code == 0
        assert result[0].stdout
        assert "POSIX" in result[0].stdout


class TestCommandRendering:
    def test_script_file_is_the_command_by_default(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = ShellPayload(service, config, "dummy-payload")

        assert payload.generate_calling_method() == ["sh"]
        assert payload.declare_command() == [str(payload.config.file)]

    def test_rendered_script_runs_with_job_file_context(
        self, service: MagicMock, config: dict, tmp_path: Path
    ) -> None:
        """Render the template, write it, then run it against the job's files."""
        payload = ShellPayload(service, config, "dummy-payload")
        payload.base_config = _base_config()
        job = _job()

        rendered = payload.render_script(job, Path(config["file"]).read_text())
        script = tmp_path / "rendered.sh"
        payload.write_script(rendered, script)
        command = payload.generate_calling_method() + payload.declare_command(script)

        result = payload.get_payload_from_job(command, job)

        assert result[0].return_code == 0
        assert "hello world! file: /d/a.nc" in result[0].stdout

    def test_binary_prefix_and_suffix_are_separate_argv_entries(
        self, service: MagicMock, config: dict
    ) -> None:
        """Binary mode never joins its parts into one shell string."""
        config["binary"] = "file"
        config["prefix_args"] = ["-b"]
        config["suffix_args"] = ["--mime"]
        payload = ShellPayload(service, config, "dummy-payload")
        payload.base_config = _base_config()

        command = payload.generate_calling_method() + payload.declare_command()
        result = payload.get_payload_from_job(command, _job())

        assert command == [
            "sh",
            "-c",
            RUN_ARGV_SCRIPT,
            "sh",
            "file",
            "-b",
            str(payload.config.file),
            "--mime",
        ]
        assert result[0].return_code == 0
        assert "text/" in result[0].stdout

    def test_dropping_the_binary_argv_fails_loudly(
        self, service: MagicMock, config: dict
    ) -> None:
        """A caller that keeps only the first entry must not succeed silently."""
        config["binary"] = "echo"
        payload = ShellPayload(service, config, "dummy-payload")
        payload.base_config = _base_config()

        command = [*payload.generate_calling_method(), payload.declare_command()[0]]
        result = payload.get_payload_from_job(command, _job())

        assert result[0].return_code != 0
        assert "no command to run" in result[0].stderr

    def test_prefix_and_suffix_without_binary_stay_separate_argv(
        self, service: MagicMock, config: dict
    ) -> None:
        """Without a binary the args are argv entries, not a shell string.

        This is the shell-injection guard: prefix/suffix are never merged into a
        single ``sh -c`` string, so they cannot be reinterpreted by the shell.
        """
        config["prefix_args"] = ["-x"]
        config["suffix_args"] = ["-y"]
        payload = ShellPayload(service, config, "dummy-payload")

        assert payload.declare_command() == ["-x", str(payload.config.file), "-y"]

    def test_explicit_path_overrides_configured_file(
        self, service: MagicMock, config: dict, tmp_path: Path
    ) -> None:
        payload = ShellPayload(service, config, "dummy-payload")
        payload.base_config = _base_config()
        other = tmp_path / "override.sh"
        other.write_text("echo overridden")
        other.chmod(0o755)

        command = payload.generate_calling_method() + payload.declare_command(other)
        result = payload.get_payload_from_job(command, _job())

        assert str(other) in command
        assert result[0].return_code == 0
        assert "overridden" in result[0].stdout


class TestToolchainValidation:
    def test_available_binary_validates(self, service: MagicMock, config: dict) -> None:
        payload = ShellPayload(service, config, "dummy-payload")
        payload.base_config = _base_config()

        result = payload.validate_toolchain_arg("sh")

        assert result[0].return_code == 0

    def test_missing_binary_fails_validation(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = ShellPayload(service, config, "dummy-payload")
        payload.base_config = _base_config()

        result = payload.validate_toolchain_arg("courier-no-such-binary-xyz")

        assert result[0].return_code != 0


class TestWriteScript:
    def test_without_a_path_creates_an_executable_temp_script(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = ShellPayload(service, config, "dummy-payload")

        script = payload.write_script("echo written\n")

        try:
            assert script.exists()
            assert script.suffix == ".sh"
            assert script.stat().st_mode & 0o111
            assert script.read_text() == "echo written\n"
        finally:
            script.unlink()


def _counter(payload_name: str, identifier: str, status: str) -> float | None:
    return REGISTRY.get_sample_value(
        "courier_payload_jobs_processed_total",
        {
            "payload_name": payload_name,
            "payload_identifier": identifier,
            "status": status,
        },
    )


def _duration_count(payload_name: str, identifier: str) -> float | None:
    return REGISTRY.get_sample_value(
        "courier_payload_job_execution_duration_seconds_count",
        {"payload_name": payload_name, "payload_identifier": identifier},
    )


class TestLoggingModes:
    """How the dispatcher's logging flags reach a job run."""

    def test_log_to_file_writes_the_supplied_log_file(
        self, service: MagicMock, config: dict, tmp_path: Path
    ) -> None:
        log_dir = tmp_path / "logs"
        payload = ShellPayload(service, config, "log-file-run")
        payload.base_config = DispatcherGroupConfig(
            log_to_file=True, log_dir=str(log_dir)
        )
        log_file = log_dir / "job.log"

        result = payload.get_payload_from_job(
            ["sh", "-c", "echo to-the-log"], _job(), log_file_path=log_file
        )

        assert result[0].return_code == 0
        assert result[0].log_file_path == str(log_file)
        assert "to-the-log" in log_file.read_text()

    def test_log_to_file_without_a_path_is_a_courier_error(
        self, service: MagicMock, config: dict, tmp_path: Path
    ) -> None:
        """A dispatcher that forgets the path fails the job, not the process."""
        payload = ShellPayload(service, config, "log-file-missing")
        payload.base_config = DispatcherGroupConfig(
            log_to_file=True, log_dir=str(tmp_path / "logs")
        )

        with pytest.raises(CourierError, match="log_file_path"):
            payload.get_payload_from_job(["sh", "-c", "echo hi"], _job())

    def test_log_only_errors_drops_job_stdout(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = ShellPayload(service, config, "log-only-errors")
        payload.base_config = DispatcherGroupConfig(log_only_errors=True)

        result = payload.get_payload_from_job(
            ["sh", "-c", "echo out; echo err >&2"], _job()
        )

        assert result[0].stdout == ""
        assert "err" in result[0].stderr

    def test_log_to_logger_streams_with_the_prefix(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = ShellPayload(service, config, "log-to-logger")
        payload.base_config = DispatcherGroupConfig(log_to_logger=True)
        payload._logger = MagicMock()  # noqa: SLF001

        payload.get_payload_from_job(
            ["sh", "-c", "echo streamed"], _job(), log_prefix="[job: job-1]"
        )

        messages = [str(c) for c in payload._logger.log.call_args_list]  # noqa: SLF001
        assert any("[job: job-1]" in m and "streamed" in m for m in messages)

    def test_timeout_seconds_kills_the_job_and_counts_a_failure(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = ShellPayload(service, config, "timeout-run")
        payload.base_config = DispatcherGroupConfig(timeout_seconds=0.5)
        before = _counter("shell_payload", "timeout-run", "failure") or 0.0

        result = payload.get_payload_from_job(["sh", "-c", "sleep 30"], _job())

        assert result[0].return_code == -1
        assert "timed out" in result[0].stderr
        assert _counter("shell_payload", "timeout-run", "failure") == before + 1


class TestToolchainProbe:
    """A toolchain probe is not a job run (probe=True)."""

    def test_probe_never_writes_a_log_file(
        self, service: MagicMock, config: dict, tmp_path: Path
    ) -> None:
        log_dir = tmp_path / "logs"
        payload = ShellPayload(service, config, "probe-log-file")
        payload.base_config = DispatcherGroupConfig(
            log_to_file=True, log_dir=str(log_dir)
        )
        stray = log_dir / "probe.log"

        result = payload.get_payload_from_job(
            ["sh", "-c", "command -v sh"], log_file_path=stray, probe=True
        )

        assert result[0].return_code == 0
        assert result[0].log_file_path is None
        assert not stray.exists()
        assert list(log_dir.iterdir()) == []

    def test_probe_keeps_stdout_under_log_only_errors(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = ShellPayload(service, config, "probe-log-only-errors")
        payload.base_config = DispatcherGroupConfig(log_only_errors=True)

        result = payload.get_payload_from_job(
            ["sh", "-c", "echo /usr/bin/found"], probe=True
        )

        assert "/usr/bin/found" in result[0].stdout

    def test_probe_records_no_payload_metrics(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = ShellPayload(service, config, "probe-no-metrics")
        payload.base_config = _base_config()

        payload.get_payload_from_job(["sh", "-c", "true"], _job(), probe=True)

        assert _counter("shell_payload", "probe-no-metrics", "success") is None
        assert _duration_count("shell_payload", "probe-no-metrics") is None

    def test_toolchain_validation_works_when_the_dispatcher_logs_to_file(
        self, service: MagicMock, config: dict, tmp_path: Path
    ) -> None:
        """Regression: the probe raised ValueError (no log_file_path) per job."""
        log_dir = tmp_path / "logs"
        payload = ShellPayload(service, config, "probe-validate")
        payload.base_config = DispatcherGroupConfig(
            log_to_file=True, log_dir=str(log_dir), log_only_errors=True
        )

        assert payload.validate_toolchain_arg("sh")[0].return_code == 0
        assert payload.validate_toolchain_arg("courier-no-such-xyz")[0].return_code
        assert list(log_dir.iterdir()) == []
        assert _counter("shell_payload", "probe-validate", "success") is None

    def test_toolchain_value_is_not_interpreted_by_the_shell(
        self, service: MagicMock, config: dict, tmp_path: Path
    ) -> None:
        marker = tmp_path / "injected"
        payload = ShellPayload(service, config, "probe-quoting")
        payload.base_config = _base_config()

        result = payload.validate_toolchain_arg(f"sh; touch {marker}")

        assert result[0].return_code != 0
        assert not marker.exists()


class TestPayloadMetrics:
    def test_job_run_is_counted_under_the_configured_payload_name(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = ShellPayload(service, config, "metrics-configured")
        payload.base_config = _base_config()
        payload.payload_name = "geoips_payload"
        success = _counter("geoips_payload", "metrics-configured", "success") or 0.0
        failure = _counter("geoips_payload", "metrics-configured", "failure") or 0.0
        runs = _duration_count("geoips_payload", "metrics-configured") or 0.0

        payload.get_payload_from_job(["sh", "-c", "true"], _job())
        payload.get_payload_from_job(["sh", "-c", "exit 3"], _job())

        assert _counter("geoips_payload", "metrics-configured", "success") == (
            success + 1
        )
        assert _counter("geoips_payload", "metrics-configured", "failure") == (
            failure + 1
        )
        assert _duration_count("geoips_payload", "metrics-configured") == runs + 2
        assert _counter("shell_payload", "metrics-configured", "success") is None

    def test_label_defaults_to_the_plugin_name(
        self, service: MagicMock, config: dict
    ) -> None:
        """A payload built by the plugin manager is labelled by its own name."""
        payload = ShellPayload(service, config, "metrics-fallback")
        payload.base_config = _base_config()
        assert payload.payload_name == "shell_payload"
        before = _counter("shell_payload", "metrics-fallback", "success") or 0.0

        payload.get_payload_from_job(["sh", "-c", "true"], _job())

        assert _counter("shell_payload", "metrics-fallback", "success") == before + 1

    def test_run_without_a_job_records_nothing(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = ShellPayload(service, config, "metrics-no-job")
        payload.base_config = _base_config()

        payload.get_payload_from_job(["sh", "-c", "true"])

        assert _counter("shell_payload", "metrics-no-job", "success") is None

    def test_hydrated_lower_representation_keeps_the_payload_name(
        self, service: MagicMock, config: dict
    ) -> None:
        """A payload lowered to shell_payload is still labelled by its own name."""
        spec = ShellPayload(service, config, "metrics-lowered").to_job_spec(_job())
        spec = spec.model_copy(update={"name": "geoips_payload"})

        hydrated = ShellPayload.from_job_spec(spec, service, _base_config())
        before = _counter("geoips_payload", "metrics-lowered", "success") or 0.0
        hydrated.get_payload_from_job(["sh", "-c", "true"], _job())

        assert hydrated.payload_name == "geoips_payload"
        assert _counter("geoips_payload", "metrics-lowered", "success") == before + 1
        assert _counter("shell_payload", "metrics-lowered", "success") is None


class TestConfigClass:
    def test_config_class_is_the_plugin_config(self) -> None:
        assert ShellPayload.config_class is ShellPayloadConfig

    def test_constructed_config_uses_the_config_class(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = ShellPayload(service, config, "config-class")

        assert isinstance(payload.config, ShellPayloadConfig)

    def test_unknown_config_key_is_rejected(
        self, service: MagicMock, config: dict
    ) -> None:
        config["no_such_option"] = True

        with pytest.raises(ValidationError):
            ShellPayload(service, config, "config-extra")


class TestInterpreter:
    def test_config_default_binary_overrides_the_class_default(
        self, service: MagicMock, config: dict
    ) -> None:
        config["default_binary"] = "dash"

        assert ShellPayload(service, config, "p").generate_calling_method() == ["dash"]

    def test_missing_interpreter_makes_the_job_unexecutable(
        self, service: MagicMock, config: dict
    ) -> None:
        """Not a TypeError from subprocess: the dispatcher parks the job."""

        class NoInterpreterPayload(ShellPayload):
            default_binary = None  # type: ignore[assignment]

        payload = NoInterpreterPayload(service, config, "no-interpreter")

        with pytest.raises(UnexecutableJobError, match="default_binary"):
            payload.generate_calling_method()
        with pytest.raises(UnexecutableJobError, match="default_binary"):
            payload.validate_toolchain_arg("sh")


class TestBinaryOnly:
    def test_binary_without_a_script_passes_no_empty_path(
        self, service: MagicMock
    ) -> None:
        payload = ShellPayload(
            service,
            {"binary": "echo", "prefix_args": ["-n"], "suffix_args": ["hi"]},
            "binary-only",
        )
        payload.base_config = _base_config()

        command = payload.generate_calling_method() + payload.declare_command()
        result = payload.get_payload_from_job(command, _job())

        assert command == ["sh", "-c", RUN_ARGV_SCRIPT, "sh", "echo", "-n", "hi"]
        assert result[0].return_code == 0
        assert result[0].stdout == "hi"

    def test_empty_arguments_stay_empty_argv_entries(self, service: MagicMock) -> None:
        """An intentionally empty argument is an argument, not nothing."""
        payload = ShellPayload(
            service,
            {"binary": "echo", "prefix_args": ["", "a"], "suffix_args": ["", "b"]},
            "binary-empty",
        )

        assert payload.declare_command() == [
            RUN_ARGV_SCRIPT,
            "sh",
            "echo",
            "",
            "a",
            "",
            "b",
        ]


def _dispatch(
    service: MagicMock,
    payload: ShellPayload,
    job: Job,
    dispatcher_config: dict | None = None,
) -> list[ExecutionLog]:
    """Send *payload* over the wire and run it through a real LocalDispatcher."""
    job.targets = ("ld",)
    job.payload = payload.to_job_spec(job)
    wire = Job.from_string(str(job))
    dispatcher = LocalDispatcher(service, dispatcher_config or {}, identifier="ld")
    return dispatcher.get_execution_log(wire)


class TestBinaryModeThroughLocalDispatcher:
    """Rendered values reach the binary as data, never as shell source."""

    def test_file_name_with_shell_syntax_is_passed_literally(
        self, service: MagicMock, tmp_path: Path
    ) -> None:
        """Regression: the parts were shell-joined, then rendered, then parsed."""
        marker = tmp_path / "injected"
        name = f"/d/x'$(touch {marker})'`touch {marker}`;touch {marker}.nc"
        job = Job("n", "job-1", {}, files=[File(file=Path(name)).freeze()])
        payload = ShellPayload(
            service,
            {"binary": "echo", "suffix_args": ["{{ files[0].file }}"]},
            "binary-literal",
        )

        logs = _dispatch(service, payload, job)

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout == f"{name}\n"
        assert not marker.exists()

    def test_argument_template_with_quotes_renders_intact(
        self, service: MagicMock
    ) -> None:
        """Quotes inside a template no longer collide with shell quoting."""
        job = Job(
            "n",
            "job-1",
            {},
            files=[
                File(file=Path("/d/a.nc")).freeze(),
                File(file=Path("/d/b.nc")).freeze(),
            ],
        )
        payload = ShellPayload(
            service,
            {
                "binary": "echo",
                "prefix_args": ["-n"],
                "suffix_args": ["{{ files | map(attribute='file') | join(', ') }}"],
            },
            "binary-quotes",
        )

        logs = _dispatch(service, payload, job)

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout == "/d/a.nc, /d/b.nc"

    def test_arguments_that_render_empty_reach_the_binary(
        self, service: MagicMock
    ) -> None:
        """Regression: empty entries were dropped from the argv in binary mode."""
        job = Job(
            "n",
            "job-1",
            {"none": ""},
            files=[File(file=Path("/d/a.nc")).freeze()],
        )
        payload = ShellPayload(
            service,
            {
                "binary": sys.executable,
                "prefix_args": ["-c", "import sys; print(sys.argv[1:])", ""],
                "suffix_args": ["{{ config.none }}", "{{ files[0].file }}", ""],
            },
            "binary-empty-args",
        )

        logs = _dispatch(service, payload, job)

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout.strip() == "['', '', '/d/a.nc', '']"

    def test_binary_receives_the_materialized_script(self, service: MagicMock) -> None:
        payload = ShellPayload(
            service,
            {"binary": "cat", "script": "rendered for {{ files[0].file }}"},
            "binary-script",
        )

        logs = _dispatch(service, payload, _job())

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout == "rendered for /d/a.nc"


@contextmanager
def _captured(logger_name: str) -> Iterator[list[logging.LogRecord]]:
    """Collect what *logger_name* emits; courier loggers do not propagate."""
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


class TestLoggingThroughLocalDispatcher:
    """Dispatcher logging flags, end to end through a real LocalDispatcher."""

    def test_log_to_logger_streams_job_output_with_the_job_prefix(
        self, service: MagicMock
    ) -> None:
        payload = ShellPayload(
            service,
            {"script": "echo streamed-out; echo streamed-err >&2"},
            "ld-log-to-logger",
        )

        with _captured("courier.plugin.shell_payload") as records:
            logs = _dispatch(service, payload, _job(), {"log_to_logger": True})

        assert logs[0].return_code == 0, logs[0].stderr
        messages = [r.getMessage() for r in records]
        assert any("[job: job-1] [stdout] streamed-out" in m for m in messages), (
            messages
        )
        assert any("[job: job-1] [stderr] streamed-err" in m for m in messages), (
            messages
        )

    def test_log_to_file_with_a_toolchain_runs_and_writes_one_log(
        self, service: MagicMock, tmp_path: Path
    ) -> None:
        """Regression: the toolchain probe raised ValueError (no log path)."""
        log_dir = tmp_path / "logs"
        payload = ShellPayload(
            service,
            {"script": "echo logged {{ files[0].file }}", "toolchain": ["sh"]},
            "ld-log-file",
        )

        logs = _dispatch(
            service,
            payload,
            _job(),
            {"log_to_file": True, "log_dir": str(log_dir)},
        )

        assert logs[0].return_code == 0, logs[0].stderr
        written = list(log_dir.iterdir())
        assert len(written) == 1
        assert logs[0].log_file_path == str(written[0])
        assert "logged /d/a.nc" in written[0].read_text()

    def test_log_only_errors_drops_stdout_but_keeps_stderr(
        self, service: MagicMock
    ) -> None:
        """A toolchain probe runs first; log_only_errors applies to the job."""
        payload = ShellPayload(
            service,
            {"script": "echo out; echo err >&2", "toolchain": ["sh"]},
            "ld-log-only-errors",
        )

        logs = _dispatch(service, payload, _job(), {"log_only_errors": True})

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout == ""
        assert "err" in logs[0].stderr

    def test_timeout_seconds_bounds_the_job(self, service: MagicMock) -> None:
        payload = ShellPayload(service, {"script": "sleep 30"}, "ld-timeout")

        logs = _dispatch(service, payload, _job(), {"timeout_seconds": 0.5})

        assert logs[0].return_code == -1
        assert "timed out" in logs[0].stderr

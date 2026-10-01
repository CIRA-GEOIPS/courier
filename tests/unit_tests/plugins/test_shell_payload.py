import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY
from pydantic import ValidationError

from courier.errors import CourierError, UnexecutableJobError
from courier.interfaces.payloads import DispatcherGroupConfig
from courier.plugins.payloads.bash_payload import BashPayload, BashPayloadConfig
from courier.plugins.payloads.python_payload import PythonPayload, PythonPayloadConfig
from courier.plugins.payloads.shell_payload import (
    RUN_ARGV_SCRIPT,
    ShellPayload,
    ShellPayloadConfig,
)
from courier.types.file import File
from courier.types.job import Job
from tests.unit_tests.plugins.conftest import file_job, run_locally


_SHELL_PLUGINS = [ShellPayload, BashPayload, PythonPayload]
_IDS = ["shell", "bash", "python"]


class TestEveryShellPlugin:
    @pytest.mark.parametrize(
        ("payload_cls", "config_cls", "calling_method"),
        [
            (ShellPayload, ShellPayloadConfig, ["sh"]),
            (BashPayload, BashPayloadConfig, ["bash"]),
            (PythonPayload, PythonPayloadConfig, ["python", "-c"]),
        ],
        ids=_IDS,
    )
    def test_config_is_validated_by_the_plugin_config_class(
        self,
        service: MagicMock,
        template_config: dict,
        payload_cls: type[ShellPayload],
        config_cls: type,
        calling_method: list[str],
    ) -> None:
        payload = payload_cls(service, template_config, "p")

        assert payload_cls.config_class is config_cls
        assert isinstance(payload.config, config_cls)
        assert payload.config.file == template_config["file"]
        assert payload.generate_calling_method() == calling_method

    @pytest.mark.parametrize("payload_cls", _SHELL_PLUGINS, ids=_IDS)
    def test_missing_template_file_is_rejected(
        self, service: MagicMock, payload_cls: type[ShellPayload]
    ) -> None:
        with pytest.raises(ValidationError):
            payload_cls(service, {"file": "invalid_file"}, "p")

    @pytest.mark.parametrize("payload_cls", _SHELL_PLUGINS, ids=_IDS)
    def test_unknown_config_key_is_rejected(
        self,
        service: MagicMock,
        template_config: dict,
        payload_cls: type[ShellPayload],
    ) -> None:
        with pytest.raises(ValidationError):
            payload_cls(service, {**template_config, "no_such_option": True}, "p")


class TestCommandRendering:
    def test_script_file_is_the_command_by_default(
        self, service: MagicMock, template_config: dict
    ) -> None:
        payload = ShellPayload(service, template_config, "dummy-payload")

        assert payload.generate_calling_method() == ["sh"]
        assert payload.declare_command() == [str(payload.config.file)]

    def test_rendered_script_runs_with_job_file_context(
        self, service: MagicMock, template_config: dict, tmp_path: Path
    ) -> None:
        """Render the template, write it, then run it against the job's files."""
        payload = ShellPayload(service, template_config, "dummy-payload")
        job = file_job()

        script = tmp_path / "rendered.sh"
        script.write_text(
            payload.render_script(job, template_config["file"].read_text()),
        )
        command = payload.generate_calling_method() + payload.declare_command(script)

        result = payload.get_payload_from_job(command, job)

        assert result[0].return_code == 0
        assert "hello world! file: /d/a.nc" in result[0].stdout

    def test_binary_prefix_and_suffix_are_separate_argv_entries(
        self, service: MagicMock, template_config: dict
    ) -> None:
        """Binary mode never joins its parts into one shell string."""
        template_config["binary"] = "file"
        template_config["prefix_args"] = ["-b"]
        template_config["suffix_args"] = ["--mime"]
        payload = ShellPayload(service, template_config, "dummy-payload")

        command = payload.generate_calling_method() + payload.declare_command()
        result = payload.get_payload_from_job(command, file_job())

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
        self, service: MagicMock, template_config: dict
    ) -> None:
        """A caller that keeps only the first entry must not succeed silently."""
        template_config["binary"] = "echo"
        payload = ShellPayload(service, template_config, "dummy-payload")

        command = [*payload.generate_calling_method(), payload.declare_command()[0]]
        result = payload.get_payload_from_job(command, file_job())

        assert result[0].return_code != 0
        assert "no command to run" in result[0].stderr

    def test_prefix_and_suffix_without_binary_stay_separate_argv(
        self, service: MagicMock, template_config: dict
    ) -> None:
        """Without a binary the args are argv entries, not a shell string.

        This is the shell-injection guard: prefix/suffix are never merged into a
        single ``sh -c`` string, so they cannot be reinterpreted by the shell.
        """
        template_config["prefix_args"] = ["-x"]
        template_config["suffix_args"] = ["-y"]
        payload = ShellPayload(service, template_config, "dummy-payload")

        assert payload.declare_command() == ["-x", str(payload.config.file), "-y"]

    def test_explicit_path_overrides_configured_file(
        self, service: MagicMock, template_config: dict, tmp_path: Path
    ) -> None:
        payload = ShellPayload(service, template_config, "dummy-payload")
        other = tmp_path / "override.sh"
        other.write_text("echo overridden")
        other.chmod(0o755)

        command = payload.generate_calling_method() + payload.declare_command(other)
        result = payload.get_payload_from_job(command, file_job())

        assert str(other) in command
        assert result[0].return_code == 0
        assert "overridden" in result[0].stdout


class TestToolchainValidation:
    def test_available_binary_validates(
        self, service: MagicMock, template_config: dict
    ) -> None:
        payload = ShellPayload(service, template_config, "dummy-payload")

        result = payload.validate_toolchain_arg("sh")

        assert result[0].return_code == 0

    def test_missing_binary_fails_validation(
        self, service: MagicMock, template_config: dict
    ) -> None:
        payload = ShellPayload(service, template_config, "dummy-payload")

        result = payload.validate_toolchain_arg("courier-no-such-binary-xyz")

        assert result[0].return_code != 0


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
        self, service: MagicMock, template_config: dict, tmp_path: Path
    ) -> None:
        log_dir = tmp_path / "logs"
        payload = ShellPayload(service, template_config, "log-file-run")
        payload.base_config = DispatcherGroupConfig(
            log_to_file=True, log_dir=str(log_dir)
        )
        log_file = log_dir / "job.log"

        result = payload.get_payload_from_job(
            ["sh", "-c", "echo to-the-log"], file_job(), log_file_path=log_file
        )

        assert result[0].return_code == 0
        assert result[0].log_file_path == str(log_file)
        assert "to-the-log" in log_file.read_text()

    def test_log_to_file_without_a_path_is_a_courier_error(
        self, service: MagicMock, template_config: dict, tmp_path: Path
    ) -> None:
        """A dispatcher that forgets the path fails the job, not the process."""
        payload = ShellPayload(service, template_config, "log-file-missing")
        payload.base_config = DispatcherGroupConfig(
            log_to_file=True, log_dir=str(tmp_path / "logs")
        )

        with pytest.raises(CourierError, match="log_file_path"):
            payload.get_payload_from_job(["sh", "-c", "echo hi"], file_job())

    def test_timeout_seconds_kills_the_job_and_counts_a_failure(
        self, service: MagicMock, template_config: dict
    ) -> None:
        payload = ShellPayload(service, template_config, "timeout-run")
        payload.base_config = DispatcherGroupConfig(timeout_seconds=0.5)
        before = _counter("shell_payload", "timeout-run", "failure") or 0.0

        result = payload.get_payload_from_job(["sh", "-c", "sleep 30"], file_job())

        assert result[0].return_code == -1
        assert "timed out" in result[0].stderr
        assert _counter("shell_payload", "timeout-run", "failure") == before + 1


class TestToolchainProbe:
    """A toolchain probe is not a job run (probe=True)."""

    def test_probe_keeps_stdout_under_log_only_errors(
        self, service: MagicMock, template_config: dict
    ) -> None:
        payload = ShellPayload(service, template_config, "probe-log-only-errors")
        payload.base_config = DispatcherGroupConfig(log_only_errors=True)

        result = payload.get_payload_from_job(
            ["sh", "-c", "echo /usr/bin/found"], probe=True
        )

        assert "/usr/bin/found" in result[0].stdout

    def test_probe_records_no_payload_metrics(
        self, service: MagicMock, template_config: dict
    ) -> None:
        payload = ShellPayload(service, template_config, "probe-no-metrics")

        payload.get_payload_from_job(["sh", "-c", "true"], file_job(), probe=True)

        assert _counter("shell_payload", "probe-no-metrics", "success") is None
        assert _duration_count("shell_payload", "probe-no-metrics") is None

    def test_toolchain_validation_works_when_the_dispatcher_logs_to_file(
        self, service: MagicMock, template_config: dict, tmp_path: Path
    ) -> None:
        """Regression: the probe raised ValueError (no log_file_path) per job."""
        log_dir = tmp_path / "logs"
        payload = ShellPayload(service, template_config, "probe-validate")
        payload.base_config = DispatcherGroupConfig(
            log_to_file=True, log_dir=str(log_dir), log_only_errors=True
        )

        assert payload.validate_toolchain_arg("sh")[0].return_code == 0
        assert payload.validate_toolchain_arg("courier-no-such-xyz")[0].return_code
        assert list(log_dir.iterdir()) == []
        assert _counter("shell_payload", "probe-validate", "success") is None

    def test_toolchain_value_is_not_interpreted_by_the_shell(
        self, service: MagicMock, template_config: dict, tmp_path: Path
    ) -> None:
        marker = tmp_path / "injected"
        payload = ShellPayload(service, template_config, "probe-quoting")

        result = payload.validate_toolchain_arg(f"sh; touch {marker}")

        assert result[0].return_code != 0
        assert not marker.exists()


class TestPayloadMetrics:
    def test_job_run_is_counted_under_the_configured_payload_name(
        self, service: MagicMock, template_config: dict
    ) -> None:
        payload = ShellPayload(service, template_config, "metrics-configured")
        payload.payload_name = "geoips_payload"
        success = _counter("geoips_payload", "metrics-configured", "success") or 0.0
        failure = _counter("geoips_payload", "metrics-configured", "failure") or 0.0
        runs = _duration_count("geoips_payload", "metrics-configured") or 0.0

        payload.get_payload_from_job(["sh", "-c", "true"], file_job())
        payload.get_payload_from_job(["sh", "-c", "exit 3"], file_job())

        assert _counter("geoips_payload", "metrics-configured", "success") == (
            success + 1
        )
        assert _counter("geoips_payload", "metrics-configured", "failure") == (
            failure + 1
        )
        assert _duration_count("geoips_payload", "metrics-configured") == runs + 2
        assert _counter("shell_payload", "metrics-configured", "success") is None

    def test_label_defaults_to_the_plugin_name(
        self, service: MagicMock, template_config: dict
    ) -> None:
        payload = ShellPayload(service, template_config, "metrics-fallback")
        assert payload.payload_name == "shell_payload"
        before = _counter("shell_payload", "metrics-fallback", "success") or 0.0

        payload.get_payload_from_job(["sh", "-c", "true"], file_job())

        assert _counter("shell_payload", "metrics-fallback", "success") == before + 1

    def test_run_without_a_job_records_nothing(
        self, service: MagicMock, template_config: dict
    ) -> None:
        payload = ShellPayload(service, template_config, "metrics-no-job")

        payload.get_payload_from_job(["sh", "-c", "true"])

        assert _counter("shell_payload", "metrics-no-job", "success") is None

    def test_hydrated_lower_representation_keeps_the_payload_name(
        self, service: MagicMock, template_config: dict
    ) -> None:
        """A payload lowered to shell_payload is still labelled by its own name."""
        payload = ShellPayload(service, template_config, "metrics-lowered")
        spec = payload.to_job_spec(file_job())
        spec = spec.model_copy(update={"name": "geoips_payload"})

        hydrated = ShellPayload.from_job_spec(spec, service)
        before = _counter("geoips_payload", "metrics-lowered", "success") or 0.0
        hydrated.get_payload_from_job(["sh", "-c", "true"], file_job())

        assert hydrated.payload_name == "geoips_payload"
        assert _counter("geoips_payload", "metrics-lowered", "success") == before + 1
        assert _counter("shell_payload", "metrics-lowered", "success") is None


class TestInterpreter:
    def test_config_default_binary_overrides_the_class_default(
        self, service: MagicMock, template_config: dict
    ) -> None:
        template_config["default_binary"] = "dash"
        payload = ShellPayload(service, template_config, "p")

        assert payload.generate_calling_method() == ["dash"]

    def test_missing_interpreter_makes_the_job_unexecutable(
        self, service: MagicMock, template_config: dict
    ) -> None:
        """Not a TypeError from subprocess: the dispatcher parks the job."""

        class NoInterpreterPayload(ShellPayload):
            default_binary = None  # type: ignore[assignment]

        payload = NoInterpreterPayload(service, template_config, "no-interpreter")

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

        command = payload.generate_calling_method() + payload.declare_command()
        result = payload.get_payload_from_job(command, file_job())

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


class TestBinaryModeThroughLocalDispatcher:
    """Rendered values reach the binary as data, never as shell source."""

    def test_file_name_with_shell_syntax_is_passed_literally(
        self, service: MagicMock, tmp_path: Path
    ) -> None:
        """Regression: the parts were shell-joined, then rendered, then parsed."""
        marker = tmp_path / "injected"
        name = f"/d/x'$(touch {marker})'`touch {marker}`;touch {marker}.nc"

        logs = run_locally(
            service,
            {"binary": "echo", "suffix_args": ["{{ files[0].file }}"]},
            payload_cls=ShellPayload,
            job=file_job(name),
        )

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

        logs = run_locally(
            service,
            {
                "binary": "echo",
                "prefix_args": ["-n"],
                "suffix_args": ["{{ files | map(attribute='file') | join(', ') }}"],
            },
            payload_cls=ShellPayload,
            job=job,
        )

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

        logs = run_locally(
            service,
            {
                "binary": sys.executable,
                "prefix_args": ["-c", "import sys; print(sys.argv[1:])", ""],
                "suffix_args": ["{{ config.none }}", "{{ files[0].file }}", ""],
            },
            payload_cls=ShellPayload,
            job=job,
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout.strip() == "['', '', '/d/a.nc', '']"

    def test_binary_receives_the_materialized_script(self, service: MagicMock) -> None:
        logs = run_locally(
            service,
            {"binary": "cat", "script": "rendered for {{ files[0].file }}"},
            payload_cls=ShellPayload,
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout == "rendered for /d/a.nc"

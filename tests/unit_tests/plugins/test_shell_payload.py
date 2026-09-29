from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from courier.interfaces.payloads import DispatcherGroupConfig
from courier.plugins.payloads.shell_payload import ShellPayload
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

    def test_binary_prefix_and_suffix_are_joined(
        self, service: MagicMock, config: dict
    ) -> None:
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
            f"file -b {payload.config.file} --mime",
        ]
        assert result[0].return_code == 0
        assert "text/" in result[0].stdout

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
    def test_available_binary_validates(
        self, service: MagicMock, config: dict
    ) -> None:
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

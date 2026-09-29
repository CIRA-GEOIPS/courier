from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from courier.interfaces.payloads import DispatcherGroupConfig
from courier.plugins.payloads.python_payload import PythonPayload
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
    file.chmod(0o755)
    return {"file": file}


def _job(identifier: str = "job-1") -> Job:
    return Job("n", identifier, {}, files=[File(file=Path("/d/a.nc")).freeze()])


def _base_config() -> DispatcherGroupConfig:
    return DispatcherGroupConfig.model_validate({})


class TestConstruction:
    def test_payload_construction_with_config(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = PythonPayload(service, config, "dummy-payload")

        assert payload.config.file == Path(config["file"])
        assert payload.generate_calling_method() == ["python", "-c"]

    def test_payload_construction_with_invalid_file_path(
        self, service: MagicMock
    ) -> None:
        with pytest.raises(ValidationError):
            PythonPayload(service, {"file": "invalid_file"}, "dummy-payload")


class TestPayloadWorkflow:
    def test_python_source_file_runs_without_dash_c(
        self, service, tmp_path
    ) -> None:
        """A ``.py`` payload runs directly: no ``-c``, no subprocess wrapping."""
        script = tmp_path / "demo.py"
        script.write_text('print("hello from python")')
        payload = PythonPayload(service, {"file": script}, "dummy-payload")
        payload.base_config = _base_config()

        assert payload.generate_calling_method() == ["python"]
        assert payload.declare_command() == [str(script)]
        command = payload.generate_calling_method() + payload.declare_command()
        result = payload.get_payload_from_job(command, _job())

        assert result[0].return_code == 0
        assert "hello from python" in result[0].stdout

    def test_non_python_file_is_lowered_to_a_python_subprocess_call(
        self, service, tmp_path
    ) -> None:
        """A non-``.py`` file runs via ``python -c subprocess.run([...])``."""
        marker = tmp_path / "ran.marker"
        script = tmp_path / "demo.sh"
        script.write_text(f"#!/bin/sh\ntouch {marker}")
        script.chmod(0o755)
        payload = PythonPayload(service, {"file": script}, "dummy-payload")
        payload.base_config = _base_config()

        assert payload.generate_calling_method() == ["python", "-c"]
        command = payload.generate_calling_method() + payload.declare_command()
        result = payload.get_payload_from_job(command, _job())

        assert result[0].return_code == 0
        assert marker.exists()

    def test_get_payload_from_binary(self, service, config):
        job = _job()

        config["binary"] = "file"
        config["prefix_args"] = ["-b"]
        payload = PythonPayload(service, config, "dummy-payload")
        payload.base_config = _base_config()
        command = payload.generate_calling_method() + payload.declare_command(
            payload.config.file
        )

        result = payload.get_payload_from_job(command, job)
        assert len(result) > 0
        assert result[0].return_code == 0
        assert result[0].stdout
        assert "POSIX" in result[0].stdout

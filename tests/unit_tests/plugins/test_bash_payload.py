from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from courier.interfaces.payloads import DispatcherGroupConfig
from courier.plugins.payloads.bash_payload import BashPayload
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
        payload = BashPayload(service, config, "dummy-payload")

        assert payload.config.file == Path(config["file"])
        assert payload.generate_calling_method() == ["bash"]

    def test_inline_binary_switches_to_bash_dash_c(
        self, service: MagicMock, config: dict
    ) -> None:
        config["binary"] = "echo"
        payload = BashPayload(service, config, "dummy-payload")

        assert payload.generate_calling_method() == ["bash", "-c"]

    def test_payload_construction_with_invalid_file_path(
        self, service: MagicMock
    ) -> None:
        with pytest.raises(ValidationError):
            BashPayload(service, {"file": "invalid_file"}, "dummy-payload")


class TestPayloadWorkflow:
    def test_get_payload_from_binary(self, service, config):
        job = _job()

        config["binary"] = "file"
        config["prefix_args"] = ["-b"]
        payload = BashPayload(service, config, "dummy-payload")
        payload.base_config = _base_config()
        result = payload.get_payload_from_job(
            ["bash", "-c", f"file -b {payload.config.file}"], job
        )

        assert len(result) > 0
        assert result[0].return_code == 0
        assert result[0].stdout
        assert "POSIX" in result[0].stdout

from pydantic import ValidationError
import pytest
from unittest.mock import MagicMock
from pathlib import Path

from courier.interfaces.falcons import DispatcherGroupConfig
from courier.plugins.falcons.shell_falcon import ShellFalcon
from courier.types.job import Job
from courier.types.file import File

@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc

@pytest.fixture
def config() -> dict:
    return {
        "file": "./assets/shell_falcon_demo.sh"
    }

def _job(identifier: str = "job-1") -> Job:
    return Job("n", identifier, {}, files=[File(file=Path("/d/a.nc")).freeze()])

def _base_config() -> DispatcherGroupConfig:
    return DispatcherGroupConfig.model_validate({})

class TestConstruction:
    def test_falcon_construction_with_config(self, service: MagicMock, config: dict) -> None:
        res = ShellFalcon(service, config, "dummyfalcon")

        assert res != None
    def test_falcon_construction_with_invalid_file_path(self, service: MagicMock) -> None:
        with pytest.raises(ValidationError):
            res = ShellFalcon(service, {"file": "invalid_file"}, "dummyfalcon")

class TestFalconWorkflow:
    def test_get_payload_from_binary(self, service, config):
        job = _job()

        config["binary"] = "file"
        config["prefix_args"] = ["-b"]
        falcon = ShellFalcon(service, config, "dummyfalcon")
        falcon.base_config = _base_config()
        result = falcon.get_payload_from_job(["sh", "-c", "file -b assets/shell_falcon_demo.sh"])

        assert len(result) > 0
        assert result[0].return_code == 0
        assert result[0].stdout == "POSIX shell script text executable, ASCII text\n"

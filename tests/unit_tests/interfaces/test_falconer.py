from pathlib import Path
import pytest
from unittest.mock import MagicMock
from courier.errors import CourierError
from courier.interfaces.falconers import Falconer
from courier.interfaces.falcons import DispatcherGroupConfig, Falcon
from courier.plugins.falcons.shell_falcon import ShellFalcon
from courier.service import Service
from courier.types.execution_log import ExecutionLog
from courier.types.file import File
from courier.types.job import Job


class _FalconerRecorder(Falconer):
    """Falconer that records the jobs passed to it."""

    name = "recording_falconer"
    version = "test"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier)
        self.executed: list[Job] = []
        self.raise_on_execute: Exception | None = None

    def cast_off_falcon(self, job: Job) -> list[ExecutionLog]:
        if self.raise_on_execute is not None:
            raise self.raise_on_execute
        self.executed.append(job)
        return [ExecutionLog(return_code=0, stdout="ok", stderr="", hostname="h")]


def _job(identifier: str = "job-1") -> Job:
    return Job("n", identifier, {}, files=[File(file=Path("/d/a.nc")).freeze()])


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


def _falconer(service: MagicMock, identifier: str) -> _FalconerRecorder:
    falconer = _FalconerRecorder(service, {}, identifier=identifier)
    return falconer


def _falcon(
    service: MagicMock, identifier: str, config: dict = {"file": ""}
) -> ShellFalcon:
    falcon = ShellFalcon(service, config, identifier=identifier)
    falcon.base_config = DispatcherGroupConfig()
    return falcon


def _feed(falconer: _FalconerRecorder, service: MagicMock, *jobs: Job) -> None:
    for job in jobs:
        falconer.cast_off_falcon(job)


class TestConstruction:
    def test_identifier_is_required(self, service: MagicMock) -> None:
        with pytest.raises(ValueError, match="requires an identifier"):
            _FalconerRecorder(service, {})


class TestEnvironmentConstruction:
    def test_render_script_file(self, service: MagicMock, tmp_path) -> None:
        job = _job()

        falcon = MagicMock()
        p = tmp_path / "tmp_render_test.sh"
        p.write_text("{{ files[0].file }}")
        falcon.config = MagicMock(file=p)

        falconer = _falconer(service, "dummyfalconer")
        falconer.falcon = falcon

        rendered_script = falconer._render_script_file(job)
        txt = rendered_script.read_text()

        assert txt == "/d/a.nc"

    def test_render_invalid_script_file(self, service: MagicMock, tmp_path) -> None:
        job = _job()

        falcon = MagicMock()
        falcon.config = MagicMock(file=tmp_path)

        falconer = _falconer(service, "dummyfalconer")
        falconer.falcon = falcon

        with pytest.raises(CourierError):
            rendered_script = falconer._render_script_file(job)

    def test_validate_valid_toolchain(self, service: MagicMock) -> None:
        falcon = _falcon(
            service, "dummyfalcon", {"file": "", "toolchain": ["gcc", "sh", "cat"]}
        )

        falconer = _falconer(service, "dummyfalconer")
        falconer.falcon = falcon

        falconer._validate_toolchain()

    def test_validate_invalid_toolchain(self, service: MagicMock) -> None:
        falcon = _falcon(
            service, "dummyfalcon", {"file": "", "toolchain": ["invalid_toolchain"]}
        )

        falconer = _falconer(service, "dummyfalconer")
        falconer.falcon = falcon

        with pytest.raises(CourierError):
            falconer._validate_toolchain()

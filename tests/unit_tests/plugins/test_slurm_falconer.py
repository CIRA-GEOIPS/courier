import pytest
from pathlib import Path
import re

from courier.interfaces.falconers import FalconerPayload
from courier.interfaces.falcons import DispatcherGroupConfig
from courier.plugins.falcons.shell_falcon import ShellFalcon
from courier.plugins.falcons.python_falcon import PythonFalcon
from courier.plugins.falcons.bash_falcon import BashFalcon
from courier.plugins.falconers.slurm_falconer import SlurmFalconer
from courier.types.file import File
from courier.types.job import Job
from courier.errors import CourierError

from unittest.mock import MagicMock, ANY

def _job(identifier: str = "job-1") -> Job:
    return Job("n", identifier, {}, files=[File(file=Path("/d/a.nc")).freeze()])

@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc

@pytest.fixture
def falcon_config(tmp_path) -> dict:
    file = tmp_path / "demo.sh"
    file.write_text("#!/bin/sh\n\necho \"hello world! file: {{ files[0].file }}\"")
    return {
        "file": file
    }

@pytest.fixture
def falconer_config() -> dict:
    return{
        "sbatch_template": "",
        "slurm_output_dir": "/tmp/"
    }

class TestCommandRendering:
    def test_falconer_generate_command(self, service, falcon_config, falconer_config) -> None:
        job = _job()

        falcon = ShellFalcon(service, falcon_config, "dummyfalcon")
        falconer = SlurmFalconer(service, falconer_config, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()

        payload = falconer.initialize_environment(job)
        assert payload == FalconerPayload(
            command=[
                "sbatch",
                "--parsable",
                "--job-name=courier-job-1",
                "--output=/tmp/job-1.out",
                "--error=/tmp/job-1.err",
                ANY,
            ]
        )
    def test_falconer_generate_shell_inline_command(self, service, falcon_config, falconer_config) -> None:
        job = _job()

        falcon_config["binary"] = "file"
        falcon_config["prefix_args"] = ["-b"]

        falcon = ShellFalcon(service, falcon_config, "dummyfalcon")
        falconer = SlurmFalconer(service, falconer_config, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()

        payload = falconer.initialize_environment(job)
        wrap_arg = payload.command[-1]
        assert re.fullmatch(
            r"""sh -c 'file -b /[^']+/tmp[^']+\.sh'""",
            wrap_arg,
        )

    def test_falconer_generate_bash_inline_command(self, service, falcon_config, falconer_config) -> None:
        job = _job()

        falcon_config["binary"] = "file"
        falcon_config["prefix_args"] = ["-b"]

        falcon = BashFalcon(service, falcon_config, "dummyfalcon")
        falconer = SlurmFalconer(service, falconer_config, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()

        payload = falconer.initialize_environment(job)
        wrap_arg = payload.command[-1]
        assert re.fullmatch(
            r"""bash -c 'file -b /[^']+/tmp[^']+\.sh'""",
            wrap_arg,
        )


    def test_falconer_generate_inline_python(self, service, falcon_config, falconer_config) -> None:
        job = _job()

        falcon_config["binary"] = "file"
        falcon_config["prefix_args"] = ["-b"]

        falcon = PythonFalcon(service, falcon_config, "dummyfalcon")
        falconer = SlurmFalconer(service, falconer_config, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()

        payload = falconer.initialize_environment(job)
        wrap_arg = payload.command[-1]

        assert re.fullmatch(
            r"""python -c 'import subprocess; subprocess\.run\(\['file', '-b', '[^']+\.sh'\], check=True\)'""",
            wrap_arg,
        )
    def test_falconer_generate_inline_python_file(self, service, falcon_config, falconer_config) -> None:
        job = _job()

        falcon = PythonFalcon(service, falcon_config, "dummyfalcon")
        falconer = SlurmFalconer(service, falconer_config, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()

        payload = falconer.initialize_environment(job)
        wrap_arg = payload.command[-1]

        assert re.fullmatch(
            r"""python -c 'import subprocess; subprocess\.run\(\['[^']+\.sh'\], check=True\)'""",
            wrap_arg,
        )

import pytest
from pathlib import Path

from courier.interfaces.falcons import DispatcherGroupConfig
from courier.plugins.falcons.shell_falcon import ShellFalcon
from courier.plugins.falcons.bash_falcon import BashFalcon
from courier.plugins.falcons.python_falcon import PythonFalcon
from courier.plugins.falconers.local_falconer import LocalFalconer
from courier.types.file import File
from courier.types.job import Job
from courier.errors import CourierError

from unittest.mock import MagicMock

def _job(identifier: str = "job-1") -> Job:
    return Job("n", identifier, {}, files=[File(file=Path("/d/a.nc")).freeze()])

@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc

@pytest.fixture
def falcon_config() -> dict:
    return {
        "file": "./assets/shell_falcon_demo.sh"
    }

class TestCommandRendering:
    def test_falconer_generate_command(self, service, falcon_config) -> None:
        job = _job()

        falcon = ShellFalcon(service, falcon_config, "dummyfalcon")
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()

        generated_command = falconer._generate_execution_command(job)

        assert generated_command == ["assets/shell_falcon_demo.sh"]
    def test_falconer_generate_command_with_prefix(self, service, falcon_config) -> None:
        job = _job()


        falcon_config["prefix_args"] = ["-v", "-f", "filename"]
        falcon = ShellFalcon(service, falcon_config, "dummyfalcon")
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()

        generated_command = falconer._generate_execution_command(job)

        assert generated_command == "-v -f filename assets/shell_falcon_demo.sh".split(" ")
    def test_falcon_generate_command_with_binary(self, service, falcon_config) -> None:
        job = _job()
        falcon_config["binary"] = "/bin/dash"

        falcon = ShellFalcon(service, falcon_config, "dummyfalcon")
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()
        generated_command = falconer._generate_execution_command(job)

        assert generated_command == ["/bin/dash assets/shell_falcon_demo.sh"]
    def test_falcon_generate_command_with_suffix(self, service, falcon_config) -> None:
        job = _job()
        falcon_config["suffix_args"] = ["-v", "-f", "filename"]

        falcon = ShellFalcon(service, falcon_config, "dummyfalcon")
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()
        generated_command = falconer._generate_execution_command(job)

        assert generated_command == "assets/shell_falcon_demo.sh -v -f filename".split(" ")
    def test_falcon_jinja2_rendering_suffix(self, service, falcon_config) -> None:
        job = _job()

        falcon_config["suffix_args"] = ["-v", "-f", "{{ files[0].file }}"]

        falcon = ShellFalcon(service, falcon_config, "dummyfalcon")
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()
        generated_command = falconer._generate_execution_command(job)

        assert generated_command == "assets/shell_falcon_demo.sh -v -f /d/a.nc".split(" ")
    def test_falcon_script_rendering(self, service, falcon_config) -> None:
        job = _job()

        falcon = ShellFalcon(service, falcon_config, "dummyfalcon")
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()
        output_file = falconer._render_script_file(job)
        output_file_txt = output_file.read_text()

        assert output_file_txt == "#!/bin/sh\n\necho \"hello world! file: /d/a.nc\""

class TestFalconerWorkflow:
    def test_validate_toolchain_valid(self, service, falcon_config) -> None:
        falcon_config["toolchain"] = ["python3", "bash", "command"]
        falcon = ShellFalcon(service, falcon_config, "dummyfalcon")
        falcon.base_config = DispatcherGroupConfig()
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()

    def test_validate_toolchain_valid_bash(self, service, falcon_config) -> None:
        falcon_config["toolchain"] = ["python3", "bash", "command"]
        falcon = BashFalcon(service, falcon_config, "dummyfalcon")
        falcon.base_config = DispatcherGroupConfig()
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()

        falconer._validate_toolchain()
    def test_validate_toolchain_valid_python(self, service, falcon_config) -> None:
        falcon_config["toolchain"] = ["python3", "bash", "command"]
        falcon = PythonFalcon(service, falcon_config, "dummyfalcon")
        falcon.base_config = DispatcherGroupConfig()
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()
    def test_validate_toolchain_invalid(self, service, falcon_config) -> None:
        falcon_config["toolchain"] = ["invalid_toolchain", "python3"]
        falcon = ShellFalcon(service, falcon_config, "dummyfalcon")
        falcon.base_config = DispatcherGroupConfig()
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()
        
        with pytest.raises(CourierError):
            falconer._validate_toolchain()
    def test_validate_toolchain_invalid_bash(self, service, falcon_config) -> None:
        falcon_config["toolchain"] = ["invalid_toolchain", "python3"]
        falcon = BashFalcon(service, falcon_config, "dummyfalcon")
        falcon.base_config = DispatcherGroupConfig()
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()
        
        with pytest.raises(CourierError):
            falconer._validate_toolchain()
    def test_validate_toolchain_invalid_python(self, service, falcon_config) -> None:
        falcon_config["toolchain"] = ["invalid_toolchain", "python3"]
        falcon = PythonFalcon(service, falcon_config, "dummyfalcon")
        falcon.base_config = DispatcherGroupConfig()
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()
        
        falcon.validate_toolchain_arg("invalid_toolchain")
    def test_run_script(self, service, falcon_config) -> None:
        job = _job()

        falcon = ShellFalcon(service, falcon_config, "dummyfalcon")
        falcon.base_config = DispatcherGroupConfig()
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()

        result = falconer.cast_off_falcon(job)

        assert len(result) > 0
        assert result[0].return_code == 0
        assert result[0].stdout == "hello world! file: /d/a.nc\n"

    def test_run_script_bash(self, service, falcon_config) -> None:
        job = _job()

        falcon = BashFalcon(service, falcon_config, "dummyfalcon")
        falcon.base_config = DispatcherGroupConfig()
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()

        result = falconer.cast_off_falcon(job)

        assert len(result) > 0
        assert result[0].return_code == 0
        assert result[0].stdout == "hello world! file: /d/a.nc\n"
    def test_run_script_python(self, service, falcon_config) -> None:
        job = _job()

        falcon = PythonFalcon(service, falcon_config, "dummyfalcon")
        falcon.base_config = DispatcherGroupConfig()
        falconer = LocalFalconer(service, {}, "dummyfalconer")
        falconer.falcon = falcon
        falconer.base_config = DispatcherGroupConfig()

        result = falconer.cast_off_falcon(job)

        assert len(result) > 0
        assert result[0].return_code == 0
        assert result[0].stdout == "hello world! file: /d/a.nc\n"

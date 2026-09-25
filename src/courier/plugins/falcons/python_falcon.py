"""Implementation for the python_falcon falcon class."""
from pathlib import Path
from typing import ClassVar

from courier.interfaces.falcons import DispatcherGroupConfig, FalconConfig
from courier.plugins.falcons.bash_falcon import BashFalcon
from courier.service import Service
from courier.types.execution_log import ExecutionLog

class PythonFalconConfig(FalconConfig):
    pass

class PythonFalconBaseConfig(DispatcherGroupConfig):
    pass

class PythonFalcon(BashFalcon):
    """Falcon for Python execution."""

    interface: ClassVar[str] = "falcons"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "python_falcon"
    version: ClassVar[str] = "-1"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self._default_binary = (
            self.config.default_binary if self.config.default_binary else "python"
        )
        self._file_suffix = ".py"

    def validate_toolchain_arg(self, value: str) -> list[ExecutionLog]:
        """Validate that a value exists on the runtime PATH.

        Parameters
        ----------
        value : str
            Executable name to locate.

        Returns
        -------
        list[ExecutionLog]
            Execution logs describing whether the executable was found.
        """
        command = list(self.config.toolchain_prepend)
        command.extend(
            [
                self._default_binary,
                "-c",
                (f"import shutil, sys; sys.exit(0 if shutil.which({value!r}) else 1)"),
            ],
        )
        payload = self.get_payload_from_job(command)
        self._logger.debug(f"Toolchain validation command {command} returned: {[p.return_code for p in payload]}")
        return payload


    def generate_calling_method(self) -> list[str]:
        """Generate in-line or standard Python calling structure.

        Returns
        -------
        list[str]
            Python interpreter arguments required to execute the configured
            Falcon. ``-c`` is included when execution uses an inline Python
            command rather than a Python source file.
        """
        command_arr = [self._default_binary]
        if self.config.binary or self.config.file.suffix != ".py":
            command_arr.append("-c")

        return command_arr

    def declare_command(self, path: Path | None = None) -> list[str]:
        """Declare the command string used to execute the Falcon.

        Parameters
        ----------
        path : Path | None, optional
            Path to the rendered script. If omitted, the configured Falcon
            file is used.

        Returns
        -------
        list[str]
            Command arguments or inline Python source required to execute the
            configured Falcon.
        """
        command_arr = []
        if not self.config.binary and self.config.file.suffix == ".py":
            for prefix in self.config.prefix_args:
                command_arr.append(prefix)
            command_arr.append(str(path) if path else str(self.config.file))
            for suffix in self.config.suffix_args:
                command_arr.append(suffix)
            return command_arr
        else:
            faux_command_arr = []
            if self.config.binary:
                faux_command_arr.append(self.config.binary)
            for prefix in self.config.prefix_args:
                faux_command_arr.append(prefix)
            faux_command_arr.append(str(path) if path else str(self.config.file))
            for suffix in self.config.suffix_args:
                faux_command_arr.append(suffix)

            command_str = (
                f"import subprocess; subprocess.run({faux_command_arr!r}, check=True)"
            )
            return [command_str]

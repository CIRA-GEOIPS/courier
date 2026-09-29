"""Implementation for the python_payload payload class."""

from pathlib import Path
from typing import ClassVar

from courier.interfaces.payloads import PayloadConfig
from courier.plugins.payloads.bash_payload import BashPayload
from courier.types.execution_log import ExecutionLog


# config classes for courier init discovery
class PythonPayloadConfig(PayloadConfig):  # noqa: D101
    pass


class PythonPayload(BashPayload):
    """Payload for Python execution."""

    interface: ClassVar[str] = "payloads"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "python_payload"
    version: ClassVar[str] = "-1"
    default_binary: ClassVar[str] = "python"
    file_suffix: ClassVar[str] = ".py"

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
        command = [
            *self.config.toolchain_prepend,
            self._default_binary,
            "-c",
            f"import shutil, sys; sys.exit(0 if shutil.which({value!r}) else 1)",
        ]
        return self._probe_toolchain(command)

    def generate_calling_method(self) -> list[str]:
        """Generate in-line or standard Python calling structure.

        Returns
        -------
        list[str]
            Python interpreter arguments required to execute the configured
            payload. ``-c`` is included when execution uses an inline Python
            command rather than a Python source file.
        """
        command_arr = [self._default_binary]
        if self.config.binary or (
            self.config.file and self.config.file.suffix != ".py"
        ):
            command_arr.append("-c")

        return command_arr

    def declare_command(self, path: Path | None = None) -> list[str]:
        """Declare the command string used to execute the payload.

        Parameters
        ----------
        path : Path | None, optional
            Path to the rendered script. If omitted, the configured payload
            file is used.

        Returns
        -------
        list[str]
            Command arguments or inline Python source required to execute the
            configured payload.
        """
        argv = [
            *([self.config.binary] if self.config.binary else []),
            *self.config.prefix_args,
            str(path or self.config.file),
            *self.config.suffix_args,
        ]
        is_python_source = (
            not self.config.binary
            and self.config.file is not None
            and self.config.file.suffix == ".py"
        )
        if is_python_source:
            return argv
        return [f"import subprocess; subprocess.run({argv!r}, check=True)"]

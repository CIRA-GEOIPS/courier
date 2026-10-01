"""Implementation for the python_payload payload class."""

from pathlib import Path
from typing import ClassVar

from courier.interfaces.payloads import PayloadConfig
from courier.plugins.payloads.bash_payload import BashPayload
from courier.types.execution_log import ExecutionLog

#: Inline program Python runs in subprocess mode.  The program and its
#: arguments follow as separate argv entries (``sys.argv[1:]``), so no
#: rendered value is ever spliced into Python source.
SUBPROCESS_WRAPPER = "import subprocess, sys; subprocess.run(sys.argv[1:], check=True)"


# config classes for courier init discovery
class PythonPayloadConfig(PayloadConfig):  # noqa: D101
    pass


class PythonPayload(BashPayload):
    """Payload for Python execution.

    * **Python source** (a ``.py`` ``file`` or an inline ``script``, no
      ``binary``): ``python [prefix_args] <script> [suffix_args]``.
    * **Subprocess** (a ``binary``, or a non-Python ``file``): ``python -c
      SUBPROCESS_WRAPPER [<binary>] [prefix_args] [<script>] [suffix_args]``;
      without a ``binary`` the first entry is the program (a script then
      needs a shebang).  A failing program surfaces as return code 1.
    """

    interface: ClassVar[str] = "payloads"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "python_payload"
    version: ClassVar[str] = "-1"
    default_binary: ClassVar[str] = "python"
    file_suffix: ClassVar[str] = ".py"
    config_class: ClassVar[type[PayloadConfig]] = PythonPayloadConfig

    def validate_toolchain_arg(self, value: str) -> list[ExecutionLog]:
        """Validate that a value exists on the runtime PATH.

        ``toolchain_prepend`` goes in front of this probe only, never the job.

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
            self._interpreter(),
            "-c",
            f"import shutil, sys; sys.exit(0 if shutil.which({value!r}) else 1)",
        ]
        return self._probe_toolchain(command)

    def _runs_python_source(self) -> bool:
        """Return whether the script is Python source (``.py`` file or inline)."""
        file = self.config.file
        return not self.config.binary and (file is None or file.suffix == ".py")

    def generate_calling_method(self) -> list[str]:
        """Generate in-line or standard Python calling structure.

        Returns
        -------
        list[str]
            Python interpreter arguments required to execute the configured
            payload. ``-c`` is included in subprocess mode, when execution uses
            an inline Python program rather than a Python source file.
        """
        command_arr = [self._interpreter()]
        if not self._runs_python_source():
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
            Arguments that follow :meth:`generate_calling_method`, one argv
            element each (see the class docstring); keep empty ones.
        """
        source = path or self.config.file
        script_arg = [str(source)] if source is not None else []
        if self._runs_python_source():
            return [*self.config.prefix_args, *script_arg, *self.config.suffix_args]
        return [
            SUBPROCESS_WRAPPER,
            *([self.config.binary] if self.config.binary else []),
            *self.config.prefix_args,
            *script_arg,
            *self.config.suffix_args,
        ]

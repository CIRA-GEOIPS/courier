"""Implementation for the python_payload payload class."""

from pathlib import Path
from typing import ClassVar

from courier.interfaces.payloads import PayloadConfig
from courier.plugins.payloads.bash_payload import BashPayload
from courier.types.execution_log import ExecutionLog

#: Inline program Python runs in subprocess mode.  The program and its
#: arguments follow as separate argv entries (``sys.argv[1:]``), so each is
#: rendered on its own and no rendered value -- say, a file name that contains
#: a quote -- is ever spliced into Python source.
SUBPROCESS_WRAPPER = "import subprocess, sys; subprocess.run(sys.argv[1:], check=True)"


# config classes for courier init discovery
class PythonPayloadConfig(PayloadConfig):  # noqa: D101
    pass


class PythonPayload(BashPayload):
    """Payload for Python execution.

    The payload runs in one of two modes:

    * **Python source** -- a ``.py`` template ``file``, or an inline ``script``
      (no ``file``), and no ``binary``.  The rendered script is run as
      ``python [prefix_args] <script> [suffix_args]``, so ``prefix_args`` are
      interpreter options and ``suffix_args`` are the script's arguments.
    * **Subprocess** -- a ``binary``, or a template ``file`` that is not
      Python.  Python runs ``python -c SUBPROCESS_WRAPPER <binary>
      [prefix_args] [<script>] [suffix_args]``, i.e. ``subprocess.run`` of
      that argv with ``check=True``.  Without a ``binary`` the first entry is
      the program: the first ``prefix_args`` entry if any, else the rendered
      file itself, which then needs a shebang.  A failing program surfaces as
      return code 1 (``CalledProcessError``).
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

        The probe is ``[*toolchain_prepend, <interpreter>, -c, <shutil.which
        check>]``: ``toolchain_prepend`` (e.g. ``env VAR=...``) applies to this
        probe only, never to the job's command.

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
        """Return whether the payload's script is Python source to run directly.

        Returns
        -------
        bool
            ``True`` without a ``binary`` when the template is a ``.py`` file
            or an inline ``script``.  A config with neither ``file`` nor
            ``binary`` can only carry an inline script (see
            :meth:`PayloadConfig.validate_payload_source`), whose materialized
            file always gets the ``.py`` suffix.
        """
        if self.config.binary:
            return False
        if self.config.file is not None:
            return self.config.file.suffix == ".py"
        return True

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
            Arguments that follow :meth:`generate_calling_method`.  Python
            source: ``[prefix_args..., <script>, suffix_args...]``.
            Subprocess: ``[SUBPROCESS_WRAPPER, <binary>, prefix_args...,
            <script>, suffix_args...]``.  Every entry is one argv element --
            an argument that rendered to ``""`` stays an empty argument -- so
            a caller must keep all of them.
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

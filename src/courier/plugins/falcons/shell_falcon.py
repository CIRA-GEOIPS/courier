from datetime import datetime
from pathlib import Path
import tempfile
from typing import ClassVar
from courier.interfaces.falcons import Falcon, FalconConfig
from courier.service import Service
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job

from courier.utils.shell_executor import execute_shell_script

from courier.utils.functional import slugify_for_filename

class ShellFalcon(Falcon):
    """Falcon class for shell execution."""
    interface: ClassVar[str] = "falcons"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "shell_falcon"
    version: ClassVar[str] = "-1"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self._default_binary = self.config.default_binary if self.config.default_binary else "sh"
        self._file_suffix = ".sh"

    def is_healthy(self) -> bool:
        return True 
    def validate_toolchain_arg(self, value: str) -> list[ExecutionLog]:
        """Validate toolchain arguments using the `command` command.

        Parameters
        ----------
        value : str
            Executable name to locate.

        Returns
        -------
        list[ExecutionLog]
            Execution logs describing the validation result.
        """
        command = [self._default_binary, "-c", f"command -v {value}"]
        return self.get_payload_from_job(command)
    def generate_calling_method(self) -> list[str]:
        """Generate the way this falcon calls itself. Either with a -c or without.

        Returns
        -------
        list[str]
            Shell interpreter arguments required to execute the configured
            Falcon. ``-c`` is included when an inline command is required.
        """
        command_arr = [self._default_binary]
        if self.config.binary:
            command_arr.append("-c")

        return command_arr
    def declare_command(self, path: Path | None = None) -> list[str]:
        """Generate the command-line execution array for this context.

        Parameters
        ----------
        path : Path | None, optional
            Path to the rendered script. If omitted, the configured Falcon
            file is used.

        Returns
        -------
        list[str]
            Command arguments or an inline shell command required to execute
            the configured Falcon.
        """
        command_arr = []

        if self.config.binary:
            parts = [
                self.config.binary,
                " ".join(self.config.prefix_args),
                str(path) if path else str(self.config.file),
                " ".join(self.config.suffix_args)
            ]
            command_arr.append(" ".join(part for part in parts if part))
        else:
            for prefix in self.config.prefix_args:
                command_arr.append(prefix)
            command_arr.append(str(path) if path else str(self.config.file))
            for suffix in self.config.suffix_args:
                command_arr.append(suffix)
        return command_arr
    def get_payload_from_job(self, command: list[str],
                             log_prefix: str = "",
                             log_file_path: Path | None = None) -> list[ExecutionLog]:         
        """Execute this command against the current context.

        Parameters
        ----------
        command : list[str]
            Command and arguments to execute.
        log_prefix : str, optional
            Prefix added to emitted log messages.
        log_file_path : Path | None, optional
            Optional file path for persisted execution logs.

        Returns
        -------
        list[ExecutionLog]
            Execution result containing the process return code, stdout,
            and stderr.
        """
        result = execute_shell_script(
            command,
            self.base_config.timeout_seconds,
            logger=self._logger,
            log_to_logger=self.base_config.log_to_logger,
            log_prefix=log_prefix,
            log_to_file=self.base_config.log_to_file,
            log_file_path=log_file_path,
            log_only_errors=self.base_config.log_only_errors,
        )

        return [ExecutionLog(
            return_code=result.return_code,
            stdout=result.stdout,
            stderr=result.stderr
        )]

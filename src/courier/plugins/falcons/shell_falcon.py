"""Implementation of the shell_falcon falcon class."""
from pathlib import Path
from socket import gethostname
import time
from typing import ClassVar

from contextlib import nullcontext

from courier.interfaces.falcons import DispatcherGroupConfig, Falcon, FalconConfig
from courier.service import Service
from courier.tracing import ATTR_CORRELATION_ID, ATTR_JOB_ID, extract_context, get_tracer
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job
from courier.utils.shell_executor import execute_shell_script

class PythonFalconConfig(FalconConfig):
    pass

class PythonFalconBaseConfig(DispatcherGroupConfig):
    pass

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
        self._default_binary = (
            self.config.default_binary if self.config.default_binary else "sh"
        )
        self._file_suffix = ".sh"

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
        payload = self.get_payload_from_job(command)

        self._logger.debug(f"Toolchain validation command {command} returned: {[p.return_code for p in payload]}")
        return payload

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
                " ".join(self.config.suffix_args),
            ]
            command_arr.append(" ".join(part for part in parts if part))
        else:
            for prefix in self.config.prefix_args:
                command_arr.append(prefix)
            command_arr.append(str(path) if path else str(self.config.file))
            for suffix in self.config.suffix_args:
                command_arr.append(suffix)
        return command_arr

    def get_payload_from_job(
        self,
        command: list[str],
        job: Job | None = None,
        log_prefix: str = "",
        log_file_path: Path | None = None,
    ) -> list[ExecutionLog]:
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
        tracer = get_tracer(__name__)
        hostname = gethostname()

        if job:
            trace_context = tracer.start_as_current_span(
                "falcon.get_payload_from_job",
                attributes={
                    ATTR_JOB_ID: job.identifier,
                    ATTR_CORRELATION_ID: job.correlation_id
                }
            )
            start_time = time.time()
            self.active_job_timestamps[job.identifier] = start_time
        else:
            trace_context = nullcontext()
        with trace_context:
            self._logger.debug(f"Executing command {' '.join(command)}")
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

            if job:
                execution_time = (
                    time.time() - self.active_job_timestamps[job.identifier]
                )
                status = "success" if result.return_code == 0 else "failure"
                self._jobs_processed.labels(
                    status = status,
                    falcon_name=self.name,
                    falcon_identifier=self.identifier
                ).inc()
                self._job_execution_duration.labels(
                    falcon_name=self.name,
                    falcon_identifier=self.identifier
                ).observe(execution_time)
                del self.active_job_timestamps[job.identifier]

            return [
                ExecutionLog(
                    return_code=result.return_code,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    hostname=hostname
                ),
            ]

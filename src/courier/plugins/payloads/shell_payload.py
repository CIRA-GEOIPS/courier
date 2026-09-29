"""Implementation of the shell_payload payload class."""

import shlex
import time
from contextlib import nullcontext
from pathlib import Path
from socket import gethostname
from typing import ClassVar

from courier.interfaces.payloads import Payload, PayloadConfig
from courier.tracing import (
    ATTR_CORRELATION_ID,
    ATTR_JOB_ID,
    get_tracer,
)
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job
from courier.utils.shell_executor import execute_shell_script


# classes for courier init discovery
class ShellPayloadConfig(PayloadConfig):  # noqa: D101
    pass


class ShellPayload(Payload):
    """Payload class for shell execution."""

    interface: ClassVar[str] = "payloads"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "shell_payload"
    version: ClassVar[str] = "-1"
    default_binary: ClassVar[str] = "sh"

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
        return self._probe_toolchain(
            [self._default_binary, "-c", f"command -v {value}"],
        )

    def generate_calling_method(self) -> list[str]:
        """Generate the way this payload calls itself.

        Returns
        -------
        list[str]
            Shell interpreter arguments required to execute the configured
            payload. ``-c`` is included when an inline command is required.
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
            Path to the rendered script. If omitted, the configured payload
            file is used.

        Returns
        -------
        list[str]
            Command arguments or an inline shell command required to execute
            the configured payload.
        """
        path_str = str(path or self.config.file or "")
        if self.config.binary:
            parts = [
                self.config.binary,
                *self.config.prefix_args,
                path_str,
                *self.config.suffix_args,
            ]
            return [shlex.join(part for part in parts if part)]
        return [*self.config.prefix_args, path_str, *self.config.suffix_args]

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

        start_time = time.time() if job else 0.0
        if job:
            trace_context = tracer.start_as_current_span(
                "payload.get_payload_from_job",
                attributes={
                    ATTR_JOB_ID: job.identifier,
                    ATTR_CORRELATION_ID: job.correlation_id,
                },
            )
        else:
            trace_context = nullcontext()
        with trace_context:
            self._logger.debug(f"Executing command {' '.join(command)}")
            try:
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
            finally:
                if job:
                    self._job_execution_duration.labels(
                        payload_name=self.name,
                        payload_identifier=self.identifier,
                    ).observe(time.time() - start_time)
            if job:
                status = "success" if result.return_code == 0 else "failure"
                self._jobs_processed.labels(
                    status=status,
                    payload_name=self.name,
                    payload_identifier=self.identifier,
                ).inc()

            return [
                ExecutionLog(
                    return_code=result.return_code,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    hostname=hostname,
                    log_file_path=result.log_file_path,
                ),
            ]

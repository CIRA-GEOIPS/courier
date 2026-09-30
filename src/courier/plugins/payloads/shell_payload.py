"""Implementation of the shell_payload payload class."""

import shlex
import time
from contextlib import nullcontext
from pathlib import Path
from socket import gethostname
from typing import ClassVar

from courier.errors import CourierError, UnexecutableJobError
from courier.interfaces.payloads import Payload, PayloadConfig
from courier.tracing import (
    ATTR_CORRELATION_ID,
    ATTR_JOB_ID,
    get_tracer,
)
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job
from courier.utils.shell_executor import execute_shell_script

#: Inline command the interpreter runs in ``binary`` mode.  The binary and its
#: arguments follow as separate argv entries (``$0`` is the interpreter), so
#: each is rendered on its own and no rendered value -- say, a file name that
#: contains a quote or ``$(...)`` -- is ever re-parsed by the shell.  The guard
#: fails loudly instead of succeeding silently if a caller drops those entries.
RUN_ARGV_SCRIPT = ': "${1:?no command to run}"; "$@"'


# classes for courier init discovery
class ShellPayloadConfig(PayloadConfig):  # noqa: D101
    pass


class ShellPayload(Payload):
    """Payload class for shell execution.

    Without a ``binary`` the rendered script is run by the interpreter:
    ``sh [prefix_args] <script> [suffix_args]``.  With a ``binary`` the
    interpreter runs it as ``sh -c RUN_ARGV_SCRIPT sh <binary> [prefix_args]
    [<script>] [suffix_args]``: every part is a separate argv entry, never
    joined into one shell string (see :data:`RUN_ARGV_SCRIPT`).
    """

    interface: ClassVar[str] = "payloads"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "shell_payload"
    version: ClassVar[str] = "-1"
    default_binary: ClassVar[str] = "sh"
    config_class: ClassVar[type[PayloadConfig]] = ShellPayloadConfig

    def _interpreter(self) -> str:
        """Return the interpreter that runs this payload's scripts.

        Returns
        -------
        str
            ``config.default_binary`` when set, else the class ``default_binary``.

        Raises
        ------
        UnexecutableJobError
            If neither the config nor the payload class names an interpreter,
            so a dispatcher parks the job instead of running it.  Without this
            check a ``None`` would reach ``subprocess`` as argv[0] and fail
            with an unhelpful ``TypeError``.
        """
        if not self._default_binary:
            raise UnexecutableJobError(
                f"Payload {self.name!r} has no interpreter: set default_binary "
                f"on the payload class or in its config",
            )
        return self._default_binary

    def validate_toolchain_arg(self, value: str) -> list[ExecutionLog]:
        """Validate toolchain arguments using the `command` command.

        The probe is ``<interpreter> -c 'command -v <value>'``;
        ``toolchain_prepend`` is not used by shell payloads.

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
            [self._interpreter(), "-c", f"command -v {shlex.quote(value)}"],
        )

    def generate_calling_method(self) -> list[str]:
        """Generate the way this payload calls itself.

        Returns
        -------
        list[str]
            Shell interpreter arguments required to execute the configured
            payload. ``-c`` is included when an inline command is required.
        """
        command_arr = [self._interpreter()]
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
            Arguments that follow :meth:`generate_calling_method`.  Without a
            ``binary``: ``[prefix_args..., <script>, suffix_args...]``.  With
            one: ``[RUN_ARGV_SCRIPT, <interpreter>, <binary>, prefix_args...,
            <script>, suffix_args...]``.  Every entry is one argv element --
            an argument that rendered to ``""`` stays an empty argument -- so
            a caller must keep all of them.
        """
        source = path or self.config.file
        script_arg = [str(source)] if source is not None else []
        arguments = [*self.config.prefix_args, *script_arg, *self.config.suffix_args]
        if self.config.binary:
            return [
                RUN_ARGV_SCRIPT,
                self._interpreter(),
                self.config.binary,
                *arguments,
            ]
        return arguments

    def get_payload_from_job(
        self,
        command: list[str],
        job: Job | None = None,
        log_prefix: str = "",
        log_file_path: Path | None = None,
        *,
        probe: bool = False,
    ) -> list[ExecutionLog]:
        """Execute this command against the current context.

        Parameters
        ----------
        command : list[str]
            Command and arguments to execute.
        job : Job | None, optional
            Job being executed. Payload job metrics and the execution span are
            recorded only when a job is given.
        log_prefix : str, optional
            Prefix added to emitted log messages.
        log_file_path : Path | None, optional
            File that receives the execution log when the dispatcher config
            enables ``log_to_file``.
        probe : bool, optional
            Mark a toolchain probe rather than a job run. A probe never writes
            a log file, keeps its stdout even under ``log_only_errors`` (the
            probe's answer is on stdout), and records no payload job metrics.

        Returns
        -------
        list[ExecutionLog]
            Execution result containing the process return code, stdout,
            and stderr.

        Raises
        ------
        CourierError
            If ``log_to_file`` is enabled for a job run but the caller supplied
            no ``log_file_path``.
        """
        log_to_file = self.base_config.log_to_file and not probe
        if log_to_file and log_file_path is None:
            raise CourierError(
                f"Payload {self.identifier!r}: log_to_file is enabled but the "
                f"dispatcher supplied no log_file_path",
            )
        record = job is not None and not probe
        tracer = get_tracer(__name__)
        hostname = gethostname()

        start_time = time.time()
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
                    log_to_file=log_to_file,
                    log_file_path=log_file_path if log_to_file else None,
                    log_only_errors=self.base_config.log_only_errors and not probe,
                )
            finally:
                if record:
                    self._job_execution_duration.labels(
                        payload_name=self.payload_name,
                        payload_identifier=self.identifier,
                    ).observe(time.time() - start_time)
            if record:
                status = "success" if result.return_code == 0 else "failure"
                self._jobs_processed.labels(
                    status=status,
                    payload_name=self.payload_name,
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

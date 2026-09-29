"""Implementation for the slurm_dispatcher dispatcher class."""

import re
import shlex
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, ClassVar

from pydantic import Field

from courier.errors import CourierError
from courier.interfaces.dispatchers import (
    Dispatcher,
    ExecutionPayload,
)
from courier.interfaces.payloads import DispatcherGroupConfig, Payload
from courier.metrics import (
    DISPATCHER_SLURM_JOBS_PENDING,
    DISPATCHER_SLURM_SUBMISSIONS,
    collect_labeled,
)
from courier.plugins.payloads.bash_payload import BashPayload
from courier.plugins.payloads.python_payload import PythonPayload
from courier.plugins.payloads.shell_payload import ShellPayload
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job
from courier.utils.functional import slugify_for_filename

_SBATCH_JOB_ID_RE = re.compile(r"Submitted batch job (\d+)")
_SACCT_MIN_PARTS = 2

_TERMINAL_STATES: frozenset[str] = frozenset(
    {
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "OUT_OF_MEMORY",
        "NODE_FAIL",
        "PREEMPTED",
        "BOOT_FAIL",
        "DEADLINE",
    },
)


class SlurmDispatcherConfig(DispatcherGroupConfig):
    """Validated configuration for the Slurm dispatcher."""

    slurm_output_dir: str
    poll_interval_seconds: float = Field(default=30.0, gt=0)
    max_concurrent_jobs: int = Field(default=10, ge=1)
    partition: str | None = None
    account: str | None = None
    qos: str | None = None
    time_limit: str | None = None
    ntasks: int | None = Field(default=None, ge=1)
    mem_per_node: str | None = None
    wait_for_completion: bool = True
    submission_timeout_seconds: float = Field(default=60.0, gt=0)
    polling_timeout_seconds: float = Field(default=86400.0, gt=0)
    sbatch_extra_args: list[str] = Field(default_factory=list)


class SlurmDispatcher(Dispatcher):
    """Dispatcher that submits payloads to Slurm via ``sbatch``."""

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "slurm_dispatcher"
    version: ClassVar[str] = "-1"

    representations: ClassVar[list[type[Payload]]] = [
        ShellPayload,
        BashPayload,
        PythonPayload,
    ]

    def __init__(
        self,
        service: Any,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self.config = SlurmDispatcherConfig.model_validate(config or {})
        self._slot_semaphore = threading.Semaphore(self.config.max_concurrent_jobs)
        self._output_dir = Path(self.config.slurm_output_dir)
        self._last_submit_error: str | None = None

    def start(self) -> None:
        """Validate the local Slurm toolchain and prepare the output directory.

        Raises
        ------
        CourierError
            If ``sbatch`` (and ``sacct`` when waiting) is not on ``PATH``.
        OSError
            If the configured output directory cannot be created.
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)
        requirements = ["sbatch"]
        if self.config.wait_for_completion:
            requirements.append("sacct")
        for requirement in requirements:
            if shutil.which(requirement) is None:
                raise CourierError(
                    f"slurm_dispatcher requires {requirement!r} on PATH",
                )
        super().start()

    def get_metrics(self) -> dict[str, Any]:
        """Get the specific prometheus metrics for this plugin."""
        metrics = {
            **collect_labeled(
                DISPATCHER_SLURM_JOBS_PENDING,
                "dispatcher_name",
                self.name,
            ),
            **collect_labeled(
                DISPATCHER_SLURM_SUBMISSIONS,
                "dispatcher_name",
                self.name,
            ),
        }
        return {**super().get_metrics(), **metrics}

    def _dispatcher_context(self, script_path: Path | None) -> dict[str, Any]:
        """Expose the Slurm output directory to the pass-two render."""
        context = super()._dispatcher_context(script_path)
        context["output_dir"] = str(self._output_dir)
        return context

    def _build_sbatch_args(self, job: Job) -> list[str]:
        """Build an argument array for sbatch.

        Parameters
        ----------
        job : Job
            Job whose Slurm submission arguments should be generated.

        Returns
        -------
        list[str]
            Complete ``sbatch`` command arguments excluding the job command or
            batch script.
        """
        args: list[str] = ["sbatch", "--parsable"]
        cfg = self.config
        out_base = self._out_base(job)
        args.extend(
            [
                f"--job-name=courier-{slugify_for_filename(job.identifier)}",
                f"--output={out_base}.out",
                f"--error={out_base}.err",
            ],
        )
        for attr, flag in (
            ("partition", "--partition"),
            ("account", "--account"),
            ("qos", "--qos"),
            ("time_limit", "--time"),
            ("ntasks", "--ntasks"),
            ("mem_per_node", "--mem"),
        ):
            value = getattr(cfg, attr)
            if value is not None and value != "":
                args.append(f"{flag}={value}")
        args.extend(cfg.sbatch_extra_args)
        return args

    def _out_base(self, job: Job) -> Path:
        """Return the base path (no extension) of *job*'s Slurm output files."""
        return self._output_dir / slugify_for_filename(job.identifier)

    def _count_submission(self, status: str) -> None:
        """Count one Slurm submission outcome."""
        DISPATCHER_SLURM_SUBMISSIONS.labels(
            status=status,
            **self._metric_labels,
        ).inc()

    def _rejected(self, reason: str | None) -> list[ExecutionLog]:
        """Build the single-log result for a submission that was rejected."""
        self._count_submission("rejected")
        return [
            ExecutionLog(
                return_code=-1,
                stdout="",
                stderr=reason,
                hostname=socket.gethostname(),
            ),
        ]

    def initialize_environment(
        self,
        job: Job,
        payload: Payload,
    ) -> ExecutionPayload:
        """Render the payload into the Slurm output directory and build sbatch args.

        Parameters
        ----------
        job : Job
            Job whose script and Slurm submission command should be prepared.
        payload : Payload
            Hydrated payload the job carries.

        Returns
        -------
        ExecutionPayload
            ``sbatch`` command and arguments required to submit the job.
        """
        clean_path: Path | None = None
        context = self._dispatcher_context(None)
        if job.payload is not None and job.payload.script is not None:
            target = self._output_dir / (
                f"{slugify_for_filename(job.identifier)}{job.payload.suffix}"
            )
            context = self._dispatcher_context(target)
            resolved = payload.resolve_deferred_expressions(
                job.payload.script,
                job,
                context,
                defer_nonce=job.payload.defer_nonce,
            )
            clean_path = payload.write_script(resolved, target)

        command = self._build_sbatch_args(job)
        raw_command = payload.declare_command(clean_path)
        if payload.config.binary or (
            payload.config.file and payload.config.file.suffix != "sh"
        ):
            inner = payload.render_script(job, raw_command[0], context)
            wrap_command = (
                f"{' '.join(payload.generate_calling_method())} {shlex.quote(inner)}"
            )
            command.extend(["--wrap", wrap_command])
        else:
            for part in raw_command:
                command.append(payload.render_script(job, part, context))

        self._logger.debug(f"Generated slurm command: {command}")
        return ExecutionPayload(
            command=command,
            file=clean_path,
        )

    def _get_slurm_job_id(self, result: ExecutionLog) -> str | None:
        """Use regex to get the job ID of the newly created slurm job.

        Parameters
        ----------
        result : ExecutionLog
            Execution result returned by the ``sbatch`` command.

        Returns
        -------
        str | None
            Parsed Slurm job ID, or ``None`` if the output could not be
            interpreted.
        """
        slurm_job_id: str | None = None
        stdout: str | None = None
        if result.stdout:
            stdout = result.stdout.strip()
            if stdout.isdigit():
                slurm_job_id = stdout
            else:
                match = _SBATCH_JOB_ID_RE.search(stdout)
                slurm_job_id = match.group(1) if match else None
        if slurm_job_id is None:
            self._logger.error(f"Could not parse sbatch output: {stdout!r}")
            self._last_submit_error = f"unparseable sbatch output: {stdout!r}"
            return None
        return slurm_job_id

    def _parse_sacct_output(self, stdout: str) -> tuple[str, int]:
        """Parse the first data row from ``sacct --parsable2`` output.

        Parameters
        ----------
        stdout : str
            Output from ``sacct --parsable2``.

        Returns
        -------
        tuple[str, int]
            Parsed Slurm job state and process exit code. An empty state is
            returned when no valid accounting row is found.
        """
        for line in stdout.splitlines():
            parts = line.strip().split("|")
            if len(parts) < _SACCT_MIN_PARTS or not parts[0]:
                continue
            state = parts[0].split()[0]
            exit_code_raw = parts[1]
            exit_code = 0
            if ":" in exit_code_raw:
                try:
                    exit_code = int(exit_code_raw.split(":")[0])
                except ValueError:
                    exit_code = -1
            return state, exit_code
        return "", 0

    def _poll_status(self, slurm_job_id: str) -> tuple[str, int]:
        """Poll the status of the slurm job created using its ID.

        Parameters
        ----------
        slurm_job_id : str
            Identifier of the submitted Slurm job.

        Returns
        -------
        tuple[str, int]
            Final Slurm state and reported exit code.

        Notes
        -----
        If the polling timeout expires, ``("TIMEOUT", -1)`` is returned.
        """
        deadline = time.time() + self.config.polling_timeout_seconds
        last_state = "PENDING"
        while time.time() < deadline:
            try:
                result = subprocess.run(  # noqa: S603
                    [  # noqa: S607
                        "sacct",
                        "-j",
                        slurm_job_id,
                        "--format=State,ExitCode",
                        "--noheader",
                        "--parsable2",
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=self.config.submission_timeout_seconds,
                )
            except (subprocess.SubprocessError, OSError) as exc:
                self._logger.warning(f"sacct poll failed: {exc}")
                time.sleep(self.config.poll_interval_seconds)
                continue

            state, exit_code = self._parse_sacct_output(result.stdout)
            last_state = state or last_state
            if last_state in _TERMINAL_STATES:
                return last_state, exit_code
            time.sleep(self.config.poll_interval_seconds)

        self._logger.error(
            f"SLURM job {slurm_job_id} did not terminate within "
            f"{self.config.polling_timeout_seconds}s; last state={last_state}",
        )
        return "TIMEOUT", -1

    def _read_output(self, job: Job) -> tuple[str, str]:
        """Read and return the ``.out`` and ``.err`` files for *job*.

        Parameters
        ----------
        job : Job
            Job whose output files should be read.

        Returns
        -------
        tuple[str, str]
            Contents of the job's stdout and stderr files. Missing files are
            represented by empty strings.
        """
        base = self._out_base(job)
        out_path = base.with_name(f"{base.name}.out")
        err_path = base.with_name(f"{base.name}.err")
        stdout = out_path.read_text() if out_path.exists() else ""
        stderr = err_path.read_text() if err_path.exists() else ""
        return stdout, stderr

    def _execute_job(
        self,
        job: Job,
        payload: Payload,
        env: ExecutionPayload,
    ) -> list[ExecutionLog]:
        """Submit the payload to Slurm and optionally wait for completion."""
        hostname = socket.gethostname()
        with self._slot_semaphore:
            DISPATCHER_SLURM_JOBS_PENDING.labels(**self._metric_labels).inc()
            try:
                payload_result = payload.get_payload_from_job(env.command, job)
            finally:
                DISPATCHER_SLURM_JOBS_PENDING.labels(**self._metric_labels).dec()

            if not payload_result:
                return self._rejected("sbatch produced no execution result.")

            slurm_job_id = self._get_slurm_job_id(payload_result[0])
            if slurm_job_id is None:
                return self._rejected(self._last_submit_error)

            if not self.config.wait_for_completion:
                self._count_submission("submitted")
                return [
                    ExecutionLog(
                        return_code=0,
                        stdout=f"SLURM job {slurm_job_id} submitted",
                        stderr=None,
                        hostname=hostname,
                    ),
                ]

            state, exit_code = self._poll_status(slurm_job_id)
            stdout, stderr = self._read_output(job)
            return_code = 0 if state == "COMPLETED" else (exit_code or -1)
            self._count_submission("submitted")
            return [
                ExecutionLog(
                    return_code=return_code,
                    stdout=stdout,
                    stderr=stderr
                    or f"SLURM job {slurm_job_id} ended with state {state}",
                    hostname=hostname,
                ),
            ]

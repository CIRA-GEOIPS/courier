"""SLURM Dispatcher — submit jobs via ``sbatch`` and optionally wait.

Renders a user-supplied Jinja2 ``sbatch`` script per :class:`Job`, writes
it to ``slurm_output_dir``, submits with ``sbatch``, and (optionally)
polls ``sacct`` until the job reaches a terminal state. Concurrency is
throttled by a :class:`threading.Semaphore` sized to
``max_concurrent_jobs``.
"""

from __future__ import annotations

import os
import re
import shutil
import socket
import subprocess
import threading
import time
import types
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

import jinja2
from pydantic import BaseModel, Field, field_validator

from courier.errors import InvalidPluginConfigError, PluginStartupError
from courier.interfaces.module_based.dispatchers import Dispatcher
from courier.metrics import (
    DISPATCHER_SLURM_JOBS_PENDING,
    DISPATCHER_SLURM_SUBMISSIONS,
)
from courier.types.execution_log import ExecutionLog

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.job import Job


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


class SlurmDispatcherConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`SlurmDispatcher`."""

    sbatch_template: str
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

    @field_validator("sbatch_template")
    @classmethod
    def _validate_template(cls, v: str) -> str:
        try:
            jinja2.Environment(autoescape=False).parse(v)  # noqa: S701
        except jinja2.TemplateSyntaxError as exc:
            raise ValueError(f"Invalid sbatch_template: {exc}") from exc
        return v


class SlurmDispatcher(Dispatcher):
    """Submit jobs to SLURM via ``sbatch`` and collect execution logs.

    Thread-safe: ``_slot_semaphore`` bounds in-flight sbatch submissions
    to ``max_concurrent_jobs``. Each dispatched job owns its own rendered
    script file and output paths inside ``slurm_output_dir``.
    """

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "slurm_dispatcher"
    version: ClassVar[str] = "0.1.0"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(service, config)
        if service is None or isinstance(service, types.ModuleType):
            return
        self.validated = SlurmDispatcherConfig.model_validate(config or {})
        self._slot_semaphore = threading.Semaphore(self.validated.max_concurrent_jobs)
        self._template = jinja2.Environment(autoescape=False).from_string(  # noqa: S701
            self.validated.sbatch_template,
        )
        self._output_dir = Path(self.validated.slurm_output_dir)
        self._last_submit_error: str | None = None

    def start(self) -> None:
        """Verify SLURM tooling is on PATH, then start the consumer thread."""
        if shutil.which("sbatch") is None:
            raise PluginStartupError(
                "slurm_dispatcher requires 'sbatch' on PATH; SLURM not installed?",
            )
        if self.validated.wait_for_completion and shutil.which("sacct") is None:
            raise PluginStartupError(
                "wait_for_completion=True requires 'sacct' on PATH",
            )
        try:
            self._output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise InvalidPluginConfigError(
                f"Could not create slurm_output_dir={self._output_dir!r}: {exc}",
            ) from exc
        super().start()

    def is_healthy(self) -> bool:
        """Healthy only while running; sbatch presence was verified at start."""
        from courier.constants import PluginRunState  # noqa: PLC0415

        return self._state == PluginRunState.RUNNING

    def _render_script(self, job: Job) -> str:
        """Render the sbatch template with job and config context."""
        return self._template.render(
            job=job,
            files=[f.file for f in job.files if f.file is not None],
            config=self.validated.model_dump(),
        )

    def _build_sbatch_args(self, script_path: Path, job: Job) -> list[str]:
        """Build the ``sbatch`` argument list from config and job identity."""
        args: list[str] = ["sbatch", "--parsable"]
        cfg = self.validated
        out_base = self._output_dir / job.identifier
        args.extend(
            [
                f"--job-name=courier-{job.identifier}",
                f"--output={out_base}.out",
                f"--error={out_base}.err",
            ],
        )
        if cfg.partition:
            args.append(f"--partition={cfg.partition}")
        if cfg.account:
            args.append(f"--account={cfg.account}")
        if cfg.qos:
            args.append(f"--qos={cfg.qos}")
        if cfg.time_limit:
            args.append(f"--time={cfg.time_limit}")
        if cfg.ntasks is not None:
            args.append(f"--ntasks={cfg.ntasks}")
        if cfg.mem_per_node:
            args.append(f"--mem={cfg.mem_per_node}")
        args.extend(cfg.sbatch_extra_args)
        args.append(str(script_path))
        return args

    def _submit(self, job: Job, script_path: Path) -> str | None:
        """Invoke ``sbatch``; return the SLURM job ID or ``None`` on failure."""
        args = self._build_sbatch_args(script_path, job)
        self._logger.info(f"Submitting SLURM job for {job.identifier}: {args}")
        try:
            result = subprocess.run(  # noqa: S603
                args,
                check=False,
                capture_output=True,
                text=True,
                timeout=self.validated.submission_timeout_seconds,
                env=os.environ.copy(),
            )
        except (subprocess.SubprocessError, OSError) as exc:
            self._logger.exception("sbatch invocation failed")
            DISPATCHER_SLURM_SUBMISSIONS.labels(
                dispatcher_name=self.name,
                status="error",
            ).inc()
            self._last_submit_error = f"sbatch invocation failed: {exc}"
            return None

        if result.returncode != 0:
            self._logger.error(
                f"sbatch failed rc={result.returncode}: {result.stderr!r}",
            )
            DISPATCHER_SLURM_SUBMISSIONS.labels(
                dispatcher_name=self.name,
                status="rejected",
            ).inc()
            self._last_submit_error = result.stderr
            return None

        stdout = result.stdout.strip()
        slurm_job_id: str | None
        if stdout.isdigit():
            slurm_job_id = stdout
        else:
            match = _SBATCH_JOB_ID_RE.search(stdout)
            slurm_job_id = match.group(1) if match else None

        if slurm_job_id is None:
            self._logger.error(f"Could not parse sbatch output: {stdout!r}")
            DISPATCHER_SLURM_SUBMISSIONS.labels(
                dispatcher_name=self.name,
                status="parse_error",
            ).inc()
            self._last_submit_error = f"unparseable sbatch output: {stdout!r}"
            return None

        DISPATCHER_SLURM_SUBMISSIONS.labels(
            dispatcher_name=self.name,
            status="submitted",
        ).inc()
        return slurm_job_id

    def _poll_status(self, slurm_job_id: str) -> tuple[str, int]:
        """Poll ``sacct`` until the job reaches a terminal state.

        Returns
        -------
        tuple[str, int]
            ``(final_state, exit_code)``. ``exit_code`` is the SLURM-reported
            exit code, not the return code of ``sacct`` itself.
        """
        deadline = time.time() + self.validated.polling_timeout_seconds
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
                    timeout=self.validated.submission_timeout_seconds,
                )
            except (subprocess.SubprocessError, OSError) as exc:
                self._logger.warning(f"sacct poll failed: {exc}")
                time.sleep(self.validated.poll_interval_seconds)
                continue

            state, exit_code = self._parse_sacct_output(result.stdout)
            last_state = state or last_state
            if last_state in _TERMINAL_STATES:
                return last_state, exit_code
            time.sleep(self.validated.poll_interval_seconds)

        self._logger.error(
            f"SLURM job {slurm_job_id} did not terminate within "
            f"{self.validated.polling_timeout_seconds}s; last state={last_state}",
        )
        return "TIMEOUT", -1

    def _parse_sacct_output(self, stdout: str) -> tuple[str, int]:
        """Parse the first data row from ``sacct --parsable2`` output."""
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

    def _read_output(self, job: Job) -> tuple[str, str]:
        """Read and return the ``.out`` and ``.err`` files for *job*."""
        out_path = self._output_dir / f"{job.identifier}.out"
        err_path = self._output_dir / f"{job.identifier}.err"
        stdout = out_path.read_text() if out_path.exists() else ""
        stderr = err_path.read_text() if err_path.exists() else ""
        return stdout, stderr

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Render, submit, optionally poll, and return a single ExecutionLog."""
        self._last_submit_error = None
        hostname = socket.gethostname()
        script_path = (
            self._output_dir / f"{job.identifier}-{uuid.uuid4().hex[:8]}.sbatch"
        )

        try:
            script_body = self._render_script(job)
        except jinja2.TemplateError as exc:
            return [
                ExecutionLog(
                    return_code=-1,
                    stdout="",
                    stderr=f"sbatch template render failed: {exc}",
                    hostname=hostname,
                ),
            ]

        try:
            script_path.write_text(script_body)
        except OSError as exc:
            return [
                ExecutionLog(
                    return_code=-1,
                    stdout="",
                    stderr=f"Could not write sbatch script {script_path!r}: {exc}",
                    hostname=hostname,
                ),
            ]

        with self._slot_semaphore:
            DISPATCHER_SLURM_JOBS_PENDING.labels(dispatcher_name=self.name).inc()
            try:
                slurm_job_id = self._submit(job, script_path)
                if slurm_job_id is None:
                    return [
                        ExecutionLog(
                            return_code=-1,
                            stdout="",
                            stderr=(
                                self._last_submit_error or "sbatch submission failed"
                            ),
                            hostname=hostname,
                        ),
                    ]

                if not self.validated.wait_for_completion:
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
                return [
                    ExecutionLog(
                        return_code=return_code,
                        stdout=stdout,
                        stderr=stderr
                        or f"SLURM job {slurm_job_id} ended with state {state}",
                        hostname=hostname,
                    ),
                ]
            finally:
                DISPATCHER_SLURM_JOBS_PENDING.labels(dispatcher_name=self.name).dec()


PLUGIN_CLASS = SlurmDispatcher

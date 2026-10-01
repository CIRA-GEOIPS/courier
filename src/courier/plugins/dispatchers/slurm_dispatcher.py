"""Implementation for the slurm_dispatcher dispatcher class.

``sbatch`` and ``sacct`` are scheduler control commands, not the job: they run
directly, bounded by ``submission_timeout_seconds``, never through the
payload, so the local-process options cannot break a submission.
"""

import re
import shlex
import shutil
import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, NamedTuple

from pydantic import Field

from courier.errors import InvalidPluginConfigError, PluginStartupError
from courier.interfaces.dispatchers import Dispatcher, ExecutionPayload
from courier.interfaces.payloads import DispatcherGroupConfig, Payload
from courier.metrics import (
    DISPATCHER_SLURM_JOBS_PENDING,
    DISPATCHER_SLURM_SUBMISSIONS,
    PAYLOAD_JOB_EXECUTION_DURATION,
    PAYLOAD_JOBS_PROCESSED,
    collect_labeled,
)
from courier.plugins.payloads.bash_payload import BashPayload
from courier.plugins.payloads.python_payload import PythonPayload
from courier.plugins.payloads.shell_payload import ShellPayload
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job
from courier.utils.functional import slugify_for_filename
from courier.utils.shell_executor import execute_shell_script

#: ``sbatch --parsable`` prints ``<job id>`` or ``<job id>;<cluster>``.
_PARSABLE_JOB_ID_RE = re.compile(r"^(\d+)(?:;(\S+))?$")
#: What ``sbatch`` prints without ``--parsable`` (e.g. behind a site wrapper).
_SBATCH_JOB_ID_RE = re.compile(r"Submitted batch job (\d+)(?: on cluster (\S+))?")
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

#: Terminal states Slurm may requeue the job from, so its script must stay.
_REQUEUEABLE_STATES: frozenset[str] = frozenset({"PREEMPTED", "NODE_FAIL"})

#: Inherited options that describe a local process; reported once at startup.
_LOCAL_PROCESS_OPTIONS = (
    "timeout_seconds",
    "log_to_file",
    "log_dir",
    "log_only_errors",
)


class SlurmDispatcherConfig(DispatcherGroupConfig):
    """Validated configuration for the Slurm dispatcher.

    ``timeout_seconds``, ``log_to_file``, ``log_dir`` and ``log_only_errors``
    do not apply; ``log_to_logger``, ``output_files`` and ``scan_stderr``
    apply to the job's output in wait mode.
    """

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


@dataclass
class SlurmSubmission(ExecutionPayload):
    """The ``sbatch`` command for one job.

    ``reads_script_at_run``: the Slurm job reads :attr:`file` when it starts
    (``--wrap``), so it must outlive the ``sbatch`` call.
    """

    reads_script_at_run: bool = False


class _SlurmJobId(NamedTuple):
    """A submitted Slurm job, and the cluster ``sbatch`` named for it."""

    job_id: str
    cluster: str | None = None


class _Submission(NamedTuple):
    """Outcome of one ``sbatch`` call; ``job`` is None if not accepted."""

    job: _SlurmJobId | None
    return_code: int  # negative: timed out, killed or not run
    reason: str


class _SacctRecord(NamedTuple):
    """The state, exit code and run time (``ElapsedRaw``) ``sacct`` reports."""

    state: str
    exit_code: int
    elapsed_seconds: float | None


def _parse_sbatch_job_id(stdout: str) -> _SlurmJobId | None:
    """Return the job id (and cluster) in ``sbatch``'s output, if any."""
    for line in reversed(stdout.strip().splitlines()):
        text = line.strip()
        match = _PARSABLE_JOB_ID_RE.match(text) or _SBATCH_JOB_ID_RE.search(text)
        if match:
            return _SlurmJobId(match.group(1), match.group(2))
    return None


def _parse_sacct_output(stdout: str) -> _SacctRecord:
    """Parse the first data row of ``sacct --parsable2`` output.

    The state is empty when no valid row was found (``sacct`` may not know a
    job yet right after it was submitted).
    """
    for line in stdout.splitlines():
        parts = line.strip().split("|")
        if len(parts) < _SACCT_MIN_PARTS or not parts[0]:
            continue
        state = parts[0].split()[0]  # "CANCELLED by 1234" -> "CANCELLED"
        exit_code = 0
        if ":" in parts[1]:
            try:
                exit_code = int(parts[1].split(":")[0])
            except ValueError:
                exit_code = -1
        elapsed = parts[2].strip() if len(parts) > _SACCT_MIN_PARTS else ""
        return _SacctRecord(
            state,
            exit_code,
            float(elapsed) if elapsed.isdigit() else None,
        )
    return _SacctRecord("", 0, None)


def _append_line(text: str, line: str) -> str:
    """Return *text* with *line* appended on a line of its own."""
    if text and not text.endswith("\n"):
        return f"{text}\n{line}"
    return f"{text}{line}"


def _shebang(interpreter: str) -> str:
    """Return a shebang line that runs a script with *interpreter*."""
    if Path(interpreter).is_absolute():
        return f"#!{interpreter}"
    return f"#!/usr/bin/env {interpreter}"


def _read_text(path: Path) -> str:
    """Return the contents of *path*, or ``""`` if it cannot be read."""
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


class SlurmDispatcher(Dispatcher):
    """Dispatcher that submits payloads to Slurm via ``sbatch``.

    A shell script that runs directly is submitted as the batch script itself
    (its ``#SBATCH`` directives apply).  Anything else is submitted as
    ``--wrap`` with the shell-quoted command a local dispatcher would run, and
    reads the script from ``slurm_output_dir`` when it starts.
    """

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "slurm_dispatcher"
    version: ClassVar[str] = "-1"

    representations: ClassVar[list[type[Payload]]] = [
        ShellPayload,
        BashPayload,
        PythonPayload,
    ]
    config_class: ClassVar[type[DispatcherGroupConfig]] = SlurmDispatcherConfig

    config: SlurmDispatcherConfig

    def __init__(
        self,
        service: Any,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self._slot_semaphore = threading.Semaphore(self.config.max_concurrent_jobs)
        # Absolute: the path is handed to Slurm, which resolves a relative one
        # against the job's working directory on the compute node.
        self._output_dir = Path(self.config.slurm_output_dir).expanduser().absolute()

    def start(self) -> None:
        """Check the Slurm client tools, prepare the output directory, and start.

        Raises
        ------
        PluginStartupError
            If ``sbatch`` (and ``sacct`` when waiting) is not on ``PATH``.
        InvalidPluginConfigError
            If ``slurm_output_dir`` cannot be created.
        """
        tools = ["sbatch", "sacct"] if self.config.wait_for_completion else ["sbatch"]
        missing = [tool for tool in tools if shutil.which(tool) is None]
        if missing:
            raise PluginStartupError(
                f"slurm_dispatcher {self.identifier!r} requires "
                f"{' and '.join(map(repr, missing))} on PATH; is this host a "
                f"Slurm submit host?",
            )
        try:
            self._output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise InvalidPluginConfigError(
                f"slurm_dispatcher {self.identifier!r} cannot create "
                f"slurm_output_dir {str(self._output_dir)!r}: {exc}",
            ) from exc
        self._warn_about_inapplicable_options()
        super().start()

    def _warn_about_inapplicable_options(self) -> None:
        """Log the configured options that have no effect on a Slurm job."""
        defaults = DispatcherGroupConfig.model_fields
        ignored = [
            key
            for key in _LOCAL_PROCESS_OPTIONS
            if getattr(self.config, key) != defaults[key].default
        ]
        if ignored:
            self._logger.warning(
                f"slurm_dispatcher {self.identifier!r}: {', '.join(ignored)} "
                f"describe a local process and do not apply to Slurm jobs. "
                f"sbatch and sacct are bounded by submission_timeout_seconds, "
                f"a job's run time by time_limit, and its output goes to "
                f"{self._output_dir}.",
            )
        if self.config.output_files and not self.config.wait_for_completion:
            self._logger.warning(
                f"slurm_dispatcher {self.identifier!r}: output_files never "
                f"match with wait_for_completion: false; the execution log "
                f"only records the submission, not the job's output.",
            )

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

    # -- Preparing the submission -------------------------------------------

    @staticmethod
    def _submits_batch_script(job: Job, payload: Payload) -> bool:
        """Return whether *job*'s script is submitted as the batch script itself.

        Only a non-Python script with no ``binary`` and no ``prefix_args``
        (which ``sbatch`` would read as its own options) is; Python must run
        under its own interpreter.  Everything else uses ``--wrap``.
        """
        spec = job.payload
        config = payload.config
        return (
            spec is not None
            and spec.script is not None
            and not isinstance(payload, PythonPayload)
            and not (config.binary or config.prefix_args)
            and len(payload.generate_calling_method()) == 1
        )

    def _finalize_script_text(self, text: str, job: Job, payload: Payload) -> str:
        """Give a batch script without one a shebang naming its interpreter."""
        if text.startswith("#!") or not self._submits_batch_script(job, payload):
            return text
        return f"{_shebang(payload.generate_calling_method()[0])}\n{text}"

    def _output_pattern(self, job: Job) -> str:
        """Return the ``--output``/``--error`` base for *job*, without suffix.

        ``%j`` (the Slurm job id) keeps two submissions of one job apart; a
        literal ``%`` in the directory is escaped.
        """
        directory = str(self._output_dir).replace("%", "%%")
        return f"{directory}/{slugify_for_filename(job.identifier)}-%j"

    def _output_paths(self, job: Job, slurm_job_id: str) -> tuple[Path, Path]:
        """Return the ``.out`` and ``.err`` files Slurm writes for a job."""
        base = f"{slugify_for_filename(job.identifier)}-{slurm_job_id}"
        return self._output_dir / f"{base}.out", self._output_dir / f"{base}.err"

    def _build_sbatch_args(self, job: Job) -> list[str]:
        """Return the ``sbatch`` command, excluding the job's script or ``--wrap``."""
        cfg = self.config
        pattern = self._output_pattern(job)
        args = [
            "sbatch",
            "--parsable",
            f"--job-name=courier-{slugify_for_filename(job.identifier)}",
            f"--output={pattern}.out",
            f"--error={pattern}.err",
        ]
        options: tuple[tuple[str, str | int | None], ...] = (
            ("--partition", cfg.partition),
            ("--account", cfg.account),
            ("--qos", cfg.qos),
            ("--time", cfg.time_limit),
            ("--ntasks", cfg.ntasks),
            ("--mem", cfg.mem_per_node),
        )
        args.extend(
            f"{flag}={value}" for flag, value in options if value not in {None, ""}
        )
        args.extend(cfg.sbatch_extra_args)
        return args

    def _job_arguments(
        self,
        job: Job,
        payload: Payload,
        script_path: Path | None,
        context: dict[str, Any],
    ) -> list[str]:
        """Return what follows the ``sbatch`` options: the script or ``--wrap``.

        A wrapped command is :func:`shlex.join`-ed so the wrap shell splits it
        back into exactly the argv a local dispatcher would run.
        """
        if self._submits_batch_script(job, payload):
            return payload.with_rendered_arguments(job, context).declare_command(
                script_path,
            )
        argv = self._render_command(job, payload, script_path, context)
        return ["--wrap", shlex.join(argv)]

    def initialize_environment(
        self,
        job: Job,
        payload: Payload,
    ) -> ExecutionPayload:
        """Write the job's script to ``slurm_output_dir`` and build ``sbatch``.

        Parameters
        ----------
        job : Job
            Job whose script and Slurm submission command should be prepared.
        payload : Payload
            Hydrated payload the job carries.

        Returns
        -------
        ExecutionPayload
            A :class:`SlurmSubmission`: the ``sbatch`` command and the script.
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)
        script_path, context = self._materialize_script(
            job,
            payload,
            directory=self._output_dir,
            prefix=f"{slugify_for_filename(job.identifier)}-",
        )
        try:
            command = [
                *self._build_sbatch_args(job),
                *self._job_arguments(job, payload, script_path, context),
            ]
        except BaseException:
            if script_path is not None:
                self._discard_script(script_path)
            raise
        self._logger.debug(f"Generated slurm command: {command}")
        return SlurmSubmission(
            command=command,
            file=script_path,
            reads_script_at_run=(
                script_path is not None and not self._submits_batch_script(job, payload)
            ),
        )

    # -- Submitting and waiting ----------------------------------------------

    def _execute_job(
        self,
        job: Job,
        payload: Payload,
        env: ExecutionPayload,
    ) -> list[ExecutionLog]:
        """Submit the job to Slurm and, in wait mode, wait for it to finish.

        A script the job reads when it starts is kept from submission on and
        released only once no job can read it any more.
        """
        reads_script = self._reads_script(env)
        with self._slot_semaphore:
            env.keep_file = env.keep_file or reads_script
            submission = self._submit(job, env.command)
            if submission.job is None:
                if reads_script:
                    self._release_rejected_script(job, env, submission)
                return [self._rejection_log(job, submission)]
            self._count_submission("submitted")
            if not self.config.wait_for_completion:
                return [self._submitted_log(job, submission.job.job_id)]
            return [self._await_job(job, payload, env, submission.job)]

    @staticmethod
    def _reads_script(env: ExecutionPayload) -> bool:
        """Return whether the submitted job reads *env*'s script when it starts."""
        return isinstance(env, SlurmSubmission) and env.reads_script_at_run

    def _submit(self, job: Job, command: list[str]) -> _Submission:
        """Run ``sbatch``, bounded by ``submission_timeout_seconds``."""
        self._logger.debug(f"Submitting job {job.identifier!r}: {shlex.join(command)}")
        result = execute_shell_script(command, self.config.submission_timeout_seconds)
        stderr = result.stderr.strip()
        code = result.return_code
        if code != 0:
            # A negative code is the executor's: timed out, killed, not run.
            outcome = (
                f"failed with return code {code}"
                if code > 0
                else f"did not complete (return code {code})"
            )
            return _Submission(
                None,
                code,
                f"sbatch {outcome}: {stderr or 'no error output'}",
            )
        parsed = _parse_sbatch_job_id(result.stdout)
        if parsed is None:
            return _Submission(
                None,
                0,
                f"sbatch printed no job id (stdout {result.stdout.strip()!r}, "
                f"stderr {stderr!r})",
            )
        on_cluster = f" on cluster {parsed.cluster}" if parsed.cluster else ""
        self._logger.info(
            f"Submitted job {job.identifier!r} as SLURM job "
            f"{parsed.job_id}{on_cluster}",
            extra={"correlation_id": job.correlation_id},
        )
        return _Submission(parsed, 0, "")

    def _release_rejected_script(
        self,
        job: Job,
        env: ExecutionPayload,
        submission: _Submission,
    ) -> None:
        """Release the script unless a job may have been queued after all."""
        if submission.return_code > 0:
            env.keep_file = False
            return
        self._logger.warning(
            f"The outcome of submitting job {job.identifier!r} is unknown; a "
            f"queued Slurm job may still read {env.file}, so it is left in place",
            extra={"correlation_id": job.correlation_id},
        )

    def _rejection_log(self, job: Job, submission: _Submission) -> ExecutionLog:
        """Count a rejected submission and build its execution log."""
        self._count_submission("rejected")
        self._logger.error(
            f"Slurm did not accept job {job.identifier!r}: {submission.reason}",
            extra={"correlation_id": job.correlation_id},
        )
        return ExecutionLog(
            return_code=submission.return_code if submission.return_code > 0 else -1,
            stdout="",
            stderr=submission.reason,
            hostname=socket.gethostname(),
        )

    def _submitted_log(self, job: Job, slurm_job_id: str) -> ExecutionLog:
        """Build the execution log of a job submitted in no-wait mode."""
        out_path, err_path = self._output_paths(job, slurm_job_id)
        self._logger.info(
            f"Not waiting for SLURM job {slurm_job_id}; its output will be in "
            f"{out_path} and {err_path}",
            extra={"correlation_id": job.correlation_id},
        )
        return ExecutionLog(
            return_code=0,
            stdout=f"SLURM job {slurm_job_id} submitted",
            stderr=None,
            hostname=socket.gethostname(),
        )

    def _count_submission(self, status: str) -> None:
        """Count one Slurm submission outcome."""
        DISPATCHER_SLURM_SUBMISSIONS.labels(
            status=status,
            **self._metric_labels,
        ).inc()

    def _await_job(
        self,
        job: Job,
        payload: Payload,
        env: ExecutionPayload,
        slurm_job: _SlurmJobId,
    ) -> ExecutionLog:
        """Wait for a submitted job and report its outcome.

        The job counts as pending until polling returns; a terminal outcome is
        recorded in the payload metrics, like a job a local dispatcher ran.
        """
        slurm_job_id = slurm_job.job_id
        submitted_at = time.monotonic()
        pending = DISPATCHER_SLURM_JOBS_PENDING.labels(**self._metric_labels)
        pending.inc()
        try:
            record = self._poll_status(slurm_job)
        finally:
            pending.dec()
        stdout, stderr = map(_read_text, self._output_paths(job, slurm_job_id))
        self._log_job_output(job, stdout, stderr)
        if record.state not in _TERMINAL_STATES:
            return self._unfinished_log(env, slurm_job_id, record, stdout, stderr)
        if record.state not in _REQUEUEABLE_STATES and self._reads_script(env):
            env.keep_file = False
        return_code = 0 if record.state == "COMPLETED" else (record.exit_code or -1)
        self._record_payload_outcome(
            payload,
            return_code,
            record.elapsed_seconds
            if record.elapsed_seconds is not None
            else time.monotonic() - submitted_at,
        )
        if return_code != 0:
            # A successful job's stderr stays exactly what it wrote.
            stderr = _append_line(
                stderr,
                f"SLURM job {slurm_job_id} ended with state {record.state}",
            )
        return ExecutionLog(
            return_code=return_code,
            stdout=stdout,
            stderr=stderr,
            hostname=socket.gethostname(),
        )

    def _unfinished_log(
        self,
        env: ExecutionPayload,
        slurm_job_id: str,
        record: _SacctRecord,
        stdout: str,
        stderr: str,
    ) -> ExecutionLog:
        """Build the execution log of a job polling gave up on."""
        note = (
            f"SLURM job {slurm_job_id} did not reach a terminal state within "
            f"{self.config.polling_timeout_seconds}s (last state "
            f"{record.state}); it may still be running"
        )
        if env.keep_file and env.file is not None:
            note = f"{note}, so its script {env.file} is left in place"
        return ExecutionLog(
            return_code=-1,
            stdout=stdout,
            stderr=_append_line(stderr, note),
            hostname=socket.gethostname(),
        )

    def _record_payload_outcome(
        self,
        payload: Payload,
        return_code: int,
        seconds: float,
    ) -> None:
        """Record a finished Slurm job in the payload metrics."""
        labels = {
            "payload_name": payload.payload_name,
            "payload_identifier": payload.identifier,
        }
        PAYLOAD_JOB_EXECUTION_DURATION.labels(**labels).observe(seconds)
        PAYLOAD_JOBS_PROCESSED.labels(
            status="success" if return_code == 0 else "failure",
            **labels,
        ).inc()

    def _poll_status(self, slurm_job: _SlurmJobId) -> _SacctRecord:
        """Poll ``sacct`` until the job is terminal or polling times out.

        On timeout, returns the last (non-terminal) state seen, exit code -1.
        """
        deadline = time.monotonic() + self.config.polling_timeout_seconds
        record = _SacctRecord("PENDING", 0, None)
        while (remaining := deadline - time.monotonic()) > 0:
            observed = self._query_sacct(slurm_job)
            if observed is not None and observed.state:
                record = observed
            if record.state in _TERMINAL_STATES:
                return record
            time.sleep(min(self.config.poll_interval_seconds, remaining))
        self._logger.error(
            f"SLURM job {slurm_job.job_id} did not terminate within "
            f"{self.config.polling_timeout_seconds}s; last state={record.state}",
        )
        return _SacctRecord(record.state, -1, None)

    def _query_sacct(self, slurm_job: _SlurmJobId) -> _SacctRecord | None:
        """Ask ``sacct`` (on the job's cluster) for its state; None on failure."""
        clusters = [f"--clusters={slurm_job.cluster}"] if slurm_job.cluster else []
        result = execute_shell_script(
            [
                "sacct",
                "-j",
                slurm_job.job_id,
                *clusters,
                "--format=State,ExitCode,ElapsedRaw",
                "--noheader",
                "--parsable2",
            ],
            self.config.submission_timeout_seconds,
        )
        if result.return_code != 0:
            self._logger.warning(
                f"sacct poll for SLURM job {slurm_job.job_id} failed with return "
                f"code {result.return_code}: {result.stderr.strip()}",
            )
            return None
        return _parse_sacct_output(result.stdout)

    def _log_job_output(self, job: Job, stdout: str, stderr: str) -> None:
        """Log a finished job's output when ``log_to_logger`` is enabled."""
        if not self.config.log_to_logger:
            return
        prefix = f"[job: {job.identifier}]"
        for line in stdout.splitlines():
            self._logger.debug(f"{prefix} [stdout] {line}")
        for line in stderr.splitlines():
            self._logger.warning(f"{prefix} [stderr] {line}")

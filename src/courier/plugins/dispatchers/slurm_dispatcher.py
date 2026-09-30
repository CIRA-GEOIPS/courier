"""Implementation for the slurm_dispatcher dispatcher class.

:class:`SlurmDispatcher` submits each job's payload to Slurm with ``sbatch``
and, unless ``wait_for_completion`` is off, polls ``sacct`` until the Slurm job
reaches a terminal state.

``sbatch`` and ``sacct`` are scheduler control commands, not the job: they are
run directly, each bounded by ``submission_timeout_seconds``, and never through
the payload's job-execution path.  The dispatcher's local-process options
(``timeout_seconds``, ``log_to_file``, ``log_only_errors``) therefore cannot
break a submission, and the payload metrics describe the Slurm job's outcome
rather than the ``sbatch`` call.
"""

import re
import shlex
import shutil
import socket
import subprocess
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

#: ``sbatch --parsable`` prints ``<job id>``, or ``<job id>;<cluster>`` when the
#: job went to a cluster chosen with ``-M``/``--clusters``.
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

#: Terminal states after which Slurm may requeue the job and run it again, so
#: a script the job reads from disk must be left in place.
_REQUEUEABLE_STATES: frozenset[str] = frozenset({"PREEMPTED", "NODE_FAIL"})

#: Options every dispatcher takes that describe a local process.  They do not
#: apply to a Slurm job, so setting one is reported once at startup.
_LOCAL_PROCESS_OPTIONS = (
    "timeout_seconds",
    "log_to_file",
    "log_dir",
    "log_only_errors",
)


class SlurmDispatcherConfig(DispatcherGroupConfig):
    """Validated configuration for the Slurm dispatcher.

    Of the inherited options, ``timeout_seconds``, ``log_to_file``,
    ``log_dir`` and ``log_only_errors`` describe a local process and do not
    apply: ``sbatch`` and ``sacct`` are bounded by
    ``submission_timeout_seconds``, the job's run time by ``time_limit``, and
    its output goes to ``slurm_output_dir``.  ``log_to_logger``,
    ``output_files`` and ``scan_stderr`` apply to the job's output in wait mode.
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
    """The ``sbatch`` command prepared for one job.

    Attributes
    ----------
    reads_script_at_run : bool
        The Slurm job reads :attr:`file` from ``slurm_output_dir`` when it
        starts (a ``--wrap`` command), rather than running Slurm's own copy of
        a batch script.  The file must then outlive the ``sbatch`` call for as
        long as the job may still run; see :meth:`SlurmDispatcher._execute_job`.
    """

    reads_script_at_run: bool = False


class _SlurmJobId(NamedTuple):
    """A submitted Slurm job, as ``sbatch`` identified it."""

    job_id: str
    #: The cluster the job was submitted to, when ``sbatch`` named one
    #: (``-M``/``--clusters``); ``sacct`` must then be asked on that cluster.
    cluster: str | None = None


class _Submission(NamedTuple):
    """Outcome of one ``sbatch`` call."""

    #: Slurm job id, or ``None`` if the submission was not accepted.
    job_id: str | None
    #: ``sbatch``'s return code (negative: timed out, killed or not run).
    return_code: int
    #: Why the submission was not accepted; empty when it was.
    reason: str
    #: The cluster ``sbatch`` named for the job, if any.
    cluster: str | None = None


class _SacctRecord(NamedTuple):
    """The state ``sacct`` reports for a Slurm job."""

    state: str
    exit_code: int
    #: The job's run time (``ElapsedRaw``), when ``sacct`` reported it.
    elapsed_seconds: float | None


def _parse_sbatch_job_id(stdout: str) -> _SlurmJobId | None:
    """Return the job id in ``sbatch``'s output, or ``None`` if there is none.

    Parameters
    ----------
    stdout : str
        What ``sbatch`` printed on stdout.

    Returns
    -------
    _SlurmJobId or None
        The Slurm job id, and the cluster ``sbatch`` named for it, if any.
    """
    for line in reversed(stdout.strip().splitlines()):
        text = line.strip()
        match = _PARSABLE_JOB_ID_RE.match(text) or _SBATCH_JOB_ID_RE.search(text)
        if match:
            return _SlurmJobId(match.group(1), match.group(2))
    return None


def _parse_sacct_output(stdout: str) -> _SacctRecord:
    """Parse the first data row of ``sacct --parsable2`` output.

    Parameters
    ----------
    stdout : str
        Output of ``sacct --format=State,ExitCode,ElapsedRaw --parsable2``.

    Returns
    -------
    _SacctRecord
        The job's state, exit code and run time.  The state is empty when no
        valid accounting row was found (``sacct`` may not know a job yet right
        after it was submitted).
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

    A shell script that runs directly is submitted as the batch script itself,
    so ``#SBATCH`` directives in it apply and Slurm keeps its own copy.
    Anything else -- a Python script, a ``binary``, interpreter options -- is
    submitted as ``--wrap`` with the command a local dispatcher would run,
    each argument shell-quoted.  Such a job reads the script from
    ``slurm_output_dir`` when it starts, so the script is left in place for as
    long as the job may still run.
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

        That is a shell payload's script run directly by its interpreter: no
        ``binary``, and no interpreter options (``prefix_args``), which
        ``sbatch`` would otherwise read as its own options.
        (``toolchain_prepend`` is never part of a job's command -- only
        python_payload uses it, in front of its toolchain probes -- so it does
        not matter here.)  A Python script is never
        a batch script: its interpreter (``default_binary``, e.g. a venv's
        python) must be the one that runs it.  Everything else is submitted
        with ``--wrap``.
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
        """Ensure a batch script starts with a shebang, as ``sbatch`` requires.

        The shebang names the payload's interpreter, the one a local
        dispatcher would run the script with.  A script that has one keeps it.
        """
        if text.startswith("#!") or not self._submits_batch_script(job, payload):
            return text
        return f"{_shebang(payload.generate_calling_method()[0])}\n{text}"

    def _output_pattern(self, job: Job) -> str:
        """Return the ``--output``/``--error`` base for *job*, without suffix.

        ``%j`` (the Slurm job id) keeps two submissions of one job -- a
        redelivery, or two dispatchers sharing ``slurm_output_dir`` -- from
        writing the same files.  A literal ``%`` in the directory is escaped
        so Slurm does not read it as a pattern.
        """
        directory = str(self._output_dir).replace("%", "%%")
        return f"{directory}/{slugify_for_filename(job.identifier)}-%j"

    def _output_paths(self, job: Job, slurm_job_id: str) -> tuple[Path, Path]:
        """Return the ``.out`` and ``.err`` files Slurm writes for a job."""
        base = f"{slugify_for_filename(job.identifier)}-{slurm_job_id}"
        return self._output_dir / f"{base}.out", self._output_dir / f"{base}.err"

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

        A batch script is followed by its arguments (``suffix_args``).  A
        wrapped command is the argv a local dispatcher would run, joined with
        :func:`shlex.join` so the shell ``sbatch`` wraps it in splits it back
        into exactly those arguments: no rendered value is re-parsed.
        """
        if self._submits_batch_script(job, payload):
            # Only the argument templates are rendered: the script path (in
            # slurm_output_dir) is spliced in literally.
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
        """Write the job's script to the Slurm output directory and build sbatch.

        The script is created by :meth:`_materialize_script` under a new,
        random name beginning with the job identifier (exclusively: an existing
        file or symlink is never followed or overwritten).  If building the
        command fails, the script is removed before the error propagates.

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

        A script the Slurm job reads when it starts (``--wrap``) is kept
        (``env.keep_file``) from submission on, and released only once no job
        can read it any more: ``sbatch`` refused the submission, or the job
        reached a terminal state it will not be requeued from.  It is left in
        place in no-wait mode, when polling gives up, and when the outcome of
        ``sbatch`` itself is unknown.
        """
        reads_script = self._reads_script(env)
        with self._slot_semaphore:
            env.keep_file = env.keep_file or reads_script
            submission = self._submit(job, env.command)
            if submission.job_id is None:
                if reads_script:
                    self._release_rejected_script(job, env, submission)
                return [self._rejection_log(job, submission)]
            self._count_submission("submitted")
            if not self.config.wait_for_completion:
                return [self._submitted_log(job, submission.job_id)]
            slurm_job = _SlurmJobId(submission.job_id, submission.cluster)
            return [self._await_job(job, payload, env, slurm_job)]

    @staticmethod
    def _reads_script(env: ExecutionPayload) -> bool:
        """Return whether the submitted job reads *env*'s script when it starts."""
        return isinstance(env, SlurmSubmission) and env.reads_script_at_run

    def _submit(self, job: Job, command: list[str]) -> _Submission:
        """Run ``sbatch``, bounded by ``submission_timeout_seconds``.

        It is run directly, not through the payload: the job's logging options
        must not apply to it (``log_only_errors`` would discard the job id it
        prints), and the payload metrics are for the Slurm job.
        """
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
        return _Submission(parsed.job_id, 0, "", parsed.cluster)

    def _release_rejected_script(
        self,
        job: Job,
        env: ExecutionPayload,
        submission: _Submission,
    ) -> None:
        """Release the script of a submission that ``sbatch`` did not confirm.

        ``sbatch`` exiting with an error of its own means nothing was queued.
        Otherwise -- it timed out, was killed, or exited 0 without a job id --
        a job may have been queued after all, and it would read the script.
        """
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

        The job counts as pending (``courier_dispatcher_slurm_jobs_pending``)
        from submission until polling returns.  Once the job is terminal its
        outcome is recorded in the payload metrics, like a job a local
        dispatcher ran.
        """
        slurm_job_id = slurm_job.job_id
        submitted_at = time.monotonic()
        pending = DISPATCHER_SLURM_JOBS_PENDING.labels(**self._metric_labels)
        pending.inc()
        try:
            record = self._poll_status(slurm_job)
        finally:
            pending.dec()
        stdout, stderr = self._read_output(job, slurm_job_id)
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
            # A successful job's stderr stays exactly what it wrote, as under
            # a local dispatcher.
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
        """Build the execution log of a job polling gave up on.

        The job's outcome is unknown, so nothing is recorded in the payload
        metrics, and a script it reads is left in place.
        """
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

        Parameters
        ----------
        slurm_job : _SlurmJobId
            The submitted Slurm job, and the cluster it was submitted to.

        Returns
        -------
        _SacctRecord
            The job's terminal state, exit code and run time.  If
            ``polling_timeout_seconds`` expires first, the last state seen
            (not a terminal one) with exit code ``-1``.
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
        """Ask ``sacct`` for the job's state; ``None`` if it could not answer.

        A job ``sbatch`` placed on a named cluster is looked up there
        (``--clusters``): the local cluster does not know it, or knows a
        different job with the same id.
        """
        slurm_job_id = slurm_job.job_id
        clusters = [f"--clusters={slurm_job.cluster}"] if slurm_job.cluster else []
        try:
            result = subprocess.run(  # noqa: S603
                [  # noqa: S607
                    "sacct",
                    "-j",
                    slurm_job_id,
                    *clusters,
                    "--format=State,ExitCode,ElapsedRaw",
                    "--noheader",
                    "--parsable2",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=self.config.submission_timeout_seconds,
            )
        except (subprocess.SubprocessError, OSError) as exc:
            self._logger.warning(
                f"sacct poll for SLURM job {slurm_job_id} failed: {exc}",
            )
            return None
        if result.returncode != 0:
            self._logger.warning(
                f"sacct poll for SLURM job {slurm_job_id} failed with return "
                f"code {result.returncode}: {result.stderr.strip()}",
            )
            return None
        return _parse_sacct_output(result.stdout)

    def _read_output(self, job: Job, slurm_job_id: str) -> tuple[str, str]:
        """Read and return the ``.out`` and ``.err`` files of a Slurm job.

        Parameters
        ----------
        job : Job
            Job whose output files should be read.
        slurm_job_id : str
            The Slurm job it was submitted as.

        Returns
        -------
        tuple[str, str]
            Contents of the job's stdout and stderr files. Missing files are
            represented by empty strings.
        """
        out_path, err_path = self._output_paths(job, slurm_job_id)
        return _read_text(out_path), _read_text(err_path)

    def _log_job_output(self, job: Job, stdout: str, stderr: str) -> None:
        """Log a finished job's output when ``log_to_logger`` is enabled.

        As for a local job, stdout lines go to DEBUG and stderr lines to
        WARNING; here they are logged once the job has finished.
        """
        if not self.config.log_to_logger:
            return
        prefix = f"[job: {job.identifier}]"
        for line in stdout.splitlines():
            self._logger.debug(f"{prefix} [stdout] {line}")
        for line in stderr.splitlines():
            self._logger.warning(f"{prefix} [stderr] {line}")

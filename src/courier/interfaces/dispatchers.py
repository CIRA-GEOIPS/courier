"""Python class for the dispatchers courier interface."""

from __future__ import annotations

import contextlib
import math
import os
import tempfile
import threading
import time
import traceback
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from socket import gethostname
from typing import TYPE_CHECKING, Any, ClassVar, cast

from opentelemetry.trace import Status, StatusCode, get_current_span

from courier.constants import (
    DISPATCHER_QUEUE,
    FILE_FOUND_EXCHANGE,
    PluginRunState,
    job_ready_queue_for,
)
from courier.dispatchers._output_scanner import _scan_and_emit_output_files
from courier.errors import CourierError, UnexecutableJobError
from courier.interfaces.discovery import ENTRY_POINT_PREFIX, ClassPluginRegistry
from courier.interfaces.payloads import (
    DispatcherGroupConfig,
    Payload,
    payloads,
)
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.metrics import (
    DISPATCHER_ACTIVE_JOBS,
    DISPATCHER_DEDUPE_SKIPS,
    DISPATCHER_DISPATCH_LATENCY_SECONDS,
    DISPATCHER_EXECUTION_LOGS_EMITTED,
    DISPATCHER_JOB_EXECUTION_DURATION,
    DISPATCHER_JOBS_CONSUMED,
    DISPATCHER_JOBS_PROCESSED,
    DISPATCHER_QUEUE_DEPTH,
    DISPATCHER_QUEUE_WAIT_DURATION,
    collect_labeled,
)
from courier.tracing import (
    ATTR_CORRELATION_ID,
    ATTR_EXECUTION_RETURN_CODE,
    ATTR_JOB_ID,
    ATTR_PLUGIN_NAME,
    ATTR_PLUGIN_VERSION,
    get_tracer,
)
from courier.types.job import Job
from courier.utils.decorators import log_execution
from courier.utils.functional import slugify_for_filename
from courier.utils.logging import get_logger

_DEDUPE_LRU_SIZE = 1024

#: Seconds a queue-depth probe may wait for the broker before giving up. The
#: probe runs on the consumer thread between receiving a job and acknowledging
#: it, so a probe that never returns is a consumer that never acknowledges.
#: Bounding it turns a silent stall into a suppressed error and a zeroed gauge.
_QUEUE_DEPTH_TIMEOUT_SECONDS = 5.0

#: ``jobs_processed`` status for a job this dispatcher could not execute at all
#: and parked on the dead-letter queue (see :class:`UnexecutableJobError`).
#: Kept apart from ``failure`` -- a job that ran and failed -- so an operator
#: can tell a deployment problem from a workload problem.
_UNEXECUTABLE_STATUS = "unexecutable"

#: Mode of a materialized job script: the payload's interpreter reads it, and a
#: batch scheduler may execute it directly.
_SCRIPT_MODE = 0o755

#: Characters a job's script suffix may not contain.  The suffix arrives in the
#: message and becomes part of a file name; a separator would put the script
#: somewhere other than the directory the dispatcher chose.
_UNSAFE_SUFFIX_CHARACTERS = frozenset({"/", "\\", "\x00"})

if TYPE_CHECKING:
    from collections.abc import Hashable

    import kombu

    from courier.service import Service
    from courier.types.execution_log import ExecutionLog
    from courier.types.file import File


# config class for courier init discovery
class DispatcherConfig(DispatcherGroupConfig):  # noqa: D101
    pass


def _is_timestamp(value: object) -> bool:
    """Return True if *value* is a finite number of seconds."""
    if not isinstance(value, int | float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:  # an int too large to be a float
        return False


def _check_job_envelope(job: Job) -> None:
    """Reject a parsed job whose envelope fields have unusable types.

    :meth:`Job.from_string` does not type-check them, and the consume loop
    uses them -- as a dedupe key, in latency arithmetic -- before any per-job
    error handling applies, so a malformed value there would end the process.

    Raises
    ------
    TypeError
        If ``identifier`` is not a string, ``last_modified`` is not a finite
        number, or ``emit_time`` is neither ``None`` nor a finite number.
    """
    if not isinstance(job.identifier, str):
        raise TypeError(
            f"'identifier' must be a string, not {type(job.identifier).__name__}",
        )
    if not _is_timestamp(job.last_modified):
        raise TypeError(f"'last_modified' is not a timestamp: {job.last_modified!r}")
    if job.emit_time is not None and not _is_timestamp(job.emit_time):
        raise TypeError(f"'emit_time' is not a timestamp: {job.emit_time!r}")


@dataclass
class ExecutionPayload:
    """Command and logging metadata prepared by a dispatcher for execution.

    Attributes
    ----------
    command : list[str]
        Argv to execute.
    file : Path or None
        Script materialized for this job, if any.  Removed once the job has
        been executed unless :attr:`keep_file` is set.
    log_prefix : str
        Prefix for each line streamed to the logger.
    log_file_path : Path or None
        File receiving the execution log when ``log_to_file`` is enabled.
    keep_file : bool
        Leave :attr:`file` in place after execution returns.  Set by a
        dispatcher whose submitted work still reads the script after the call
        comes back -- for example a Slurm job that was queued but has not run.
    """

    command: list[str]
    file: Path | None
    log_prefix: str = ""
    log_file_path: Path | None = None
    keep_file: bool = False


class Dispatcher(ServicePlugin):
    """Base dispatcher plugin.

    A dispatcher consumes jobs from its own ``JobReady-<identifier>`` queue,
    hydrates the payload each job carries, and executes it.  One bad job never
    ends the service: a job this dispatcher cannot execute at all is parked on
    the dead-letter queue (:class:`~courier.errors.UnexecutableJobError`), and
    any other failure while preparing or running a job is contained as a
    :class:`~courier.errors.CourierError` and counted.  Publishing a job's
    results is not part of the job: a broker fault there propagates, so the
    message is redelivered rather than acknowledged as a failure.
    """

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "dispatcher"

    #: Payload representation classes this dispatcher can execute. A payload
    #: lowered to any of these is compatible; the most specific match wins.
    representations: ClassVar[list[type[Payload]]] = []

    #: Model this dispatcher's config is validated with.  Subclasses with
    #: options of their own set this rather than re-validating in ``__init__``,
    #: so unknown keys are reported against the model that really applies.
    config_class: ClassVar[type[DispatcherGroupConfig]] = DispatcherGroupConfig

    config: DispatcherGroupConfig

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        if identifier is None:
            raise ValueError(
                f"Dispatcher {type(self).__name__} requires an identifier "
                "(from spec.run[*].identifier); preflight should have "
                "supplied it.",
            )
        self.parent_service = service
        self._logger = get_logger("plugin", self.name, service.config)
        self.identifier = identifier
        self.queue = DISPATCHER_QUEUE
        self.incoming_queue = job_ready_queue_for(identifier)
        self._state = PluginRunState.STOPPED
        self._main_thread: threading.Thread | None = None
        # Per-instance shutdown signal. Passed to Service.consume() so the
        # broker loop returns promptly on stop() — without it the consume
        # generator blocks forever and a non-daemon thread wedges interpreter
        # shutdown. Per-instance (not service-wide) so PluginManager can
        # restart one dispatcher without tearing down the others.
        self._stop_event = threading.Event()
        # Set once bound to the per-identifier job queue; see JobBuilder.
        self._subscribed = threading.Event()
        self.config = self.config_class.model_validate(config or {})

        self._jobs_processed = DISPATCHER_JOBS_PROCESSED
        self._job_execution_duration = DISPATCHER_JOB_EXECUTION_DURATION
        self._active_jobs = DISPATCHER_ACTIVE_JOBS
        self._execution_logs_emitted = DISPATCHER_EXECUTION_LOGS_EMITTED
        self._queue_wait_duration = DISPATCHER_QUEUE_WAIT_DURATION
        self._jobs_consumed = DISPATCHER_JOBS_CONSUMED
        self._dispatch_latency = DISPATCHER_DISPATCH_LATENCY_SECONDS
        self._dedupe_skips = DISPATCHER_DEDUPE_SKIPS
        self._metric_labels = {
            "dispatcher_name": self.name,
            "dispatcher_identifier": self.identifier,
        }
        self.active_job_timestamps: dict[str, float] = {}
        # Bounded LRU of recently-seen jobs, keyed by _dedupe_key. Catches
        # same-replica duplicates; cross-replica strict dedupe is opt-in via
        # state sync. Thread-safe: only touched by handle_incoming_jobs thread.
        self._seen_jobs: OrderedDict[Hashable, None] = OrderedDict()
        # Cache of payload name -> the representation it runs as here, and of
        # already-validated (payload name, toolchain) pairs, so a per-job
        # payload does not pay registry lookup and toolchain probing on every
        # dispatch.
        self._representations: dict[str, type[Payload]] = {}
        self._validated_toolchains: set[
            tuple[str, tuple[str, ...], tuple[str, ...], str | None, str | None]
        ] = set()
        # Connection this dispatcher uses for its queue-depth probe. Opened
        # lazily by the consumer thread and closed by it, so it is owned by
        # exactly one thread for its whole life; see _emit_queue_depth.
        self._depth_connection: kombu.Connection | None = None

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Resolve the job's payload, prepare its environment, and execute it.

        Parameters
        ----------
        job : Job
            Job to execute.

        Returns
        -------
        list[ExecutionLog]
            Execution logs produced by the payload.  Publishing them, and
            re-emitting the output files they name, is the consumer loop's
            job (:meth:`_run_job`), outside this method's containment: a
            broker fault while publishing is not a failure of this job, so it
            propagates and the message is redelivered instead of acknowledged.

        Raises
        ------
        UnexecutableJobError
            If this dispatcher cannot execute the job at all; see
            :meth:`_resolve_job_payload`.
        CourierError
            If preparing or running the job fails.  Any other exception raised
            on the way is converted into one, so a single bad job fails on its
            own instead of taking the service down.
        """
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "dispatcher.execute_job",
            attributes={
                ATTR_JOB_ID: job.identifier,
                ATTR_CORRELATION_ID: job.correlation_id,
            },
        ):
            payload = self._resolve_job_payload(job)
            env = self._prepare_environment(job, payload)
            try:
                self._logger.debug(f"Yielding execution log for job: {job}")
                logs = self._execute_job(job, payload, env)
                self._report_failed_runs(job, logs)
            except CourierError:
                raise
            except Exception as exc:
                raise CourierError(
                    f"Failed to execute job {job.identifier!r} on dispatcher "
                    f"{self.identifier!r}: {type(exc).__name__}: {exc}",
                ) from exc
            finally:
                self._release_environment(env)
            return logs

    def _prepare_environment(self, job: Job, payload: Payload) -> ExecutionPayload:
        """Run :meth:`initialize_environment`, reporting failures as CourierError.

        Raises
        ------
        CourierError
            If the environment cannot be prepared.  An
            :class:`~courier.errors.UnexecutableJobError` raised by a subclass
            passes through unchanged so the job is still parked.
        """
        try:
            return self.initialize_environment(job, payload)
        except UnexecutableJobError:
            raise
        except Exception as exc:
            raise CourierError(
                f"Failed to initialize environment for job {job.identifier!r} "
                f"on dispatcher {self.identifier!r}: {type(exc).__name__}: {exc}",
            ) from exc

    def _release_environment(self, env: ExecutionPayload) -> None:
        """Remove the job's script once it has run, unless it must outlive us."""
        if env.file is not None and not env.keep_file:
            self._discard_script(env.file)

    def _discard_script(self, path: Path) -> None:
        """Delete a materialized script; a failure is logged, never raised."""
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            self._logger.warning(f"Could not remove job script {path}: {exc}")

    def _report_failed_runs(self, job: Job, logs: list[ExecutionLog]) -> None:
        """Log, and mark the span, when the payload exited non-zero.

        A failed run is still a job this dispatcher executed, so it is counted
        as processed; this keeps the failure visible in logs and traces.
        """
        codes = [log.return_code for log in logs if log.return_code not in {0, None}]
        if not codes:
            return
        self._logger.error(
            f"Job {job.identifier!r} on dispatcher {self.identifier!r} exited "
            f"with return code(s) {codes}",
            extra={"correlation_id": job.correlation_id},
        )
        get_current_span().set_status(Status(StatusCode.ERROR))

    def _collect_output_files(
        self,
        job: Job,  # noqa: ARG002 -- for overrides; see below
        logs: list[ExecutionLog],
    ) -> list[File]:
        """Return the output files of *job* to feed back into the pipeline.

        Chained dispatcher-to-builder workflows depend on this: a script prints
        the paths it produced, the patterns configured under ``output_files``
        match them, and :meth:`_run_job` publishes each discovered ``File`` to
        the file-found exchange via :meth:`emit_file`.

        Collecting is part of the job: an error here is this job's and is
        raised as a :class:`CourierError`, so the job fails on its own.
        Publishing the files is not (see :meth:`_publish_results`).  A
        dispatcher that decides in code which files a job produced overrides
        this, extending ``super()``'s list, rather than calling
        :meth:`emit_file` while the job runs: a broker fault there would be
        contained as a failure of the job, and the message acknowledged.

        Parameters
        ----------
        job : Job
            The job that ran.
        logs : list[ExecutionLog]
            Its execution logs.

        Returns
        -------
        list[File]
            The files to re-emit; empty without ``output_files``.

        Raises
        ------
        CourierError
            If scanning the output fails.
        """
        files: list[File] = []
        if not self.config.output_files:
            return files
        try:
            _scan_and_emit_output_files(
                stdout="\n".join(log.stdout or "" for log in logs),
                stderr="\n".join(log.stderr or "" for log in logs),
                patterns=self.config.output_files,
                scan_stderr=self.config.scan_stderr,
                hostname=gethostname(),
                emit_file=files.append,
            )
        except Exception as exc:
            raise CourierError(
                f"Failed to scan the output of a job on dispatcher "
                f"{self.identifier!r} for output files: "
                f"{type(exc).__name__}: {exc}",
            ) from exc
        return files

    def _execute_job(
        self,
        job: Job,
        payload: Payload,
        env: ExecutionPayload,
    ) -> list[ExecutionLog]:
        """Execute *env*'s command through *payload*.

        The default implementation runs the command on this host.  Dispatchers
        that manage a scheduler (e.g. Slurm) override this.
        """
        return payload.get_payload_from_job(
            env.command,
            job,
            log_prefix=env.log_prefix,
            log_file_path=env.log_file_path,
        )

    def _resolve_job_payload(self, job: Job) -> Payload:
        """Hydrate and validate the payload a job carries.

        Parameters
        ----------
        job : Job
            Job whose payload should be hydrated.

        Returns
        -------
        Payload
            A payload instance of the most specific representation this
            dispatcher can run, with its toolchain validated on this host.

        Raises
        ------
        UnexecutableJobError
            If the job carries no payload, its payload plugin is not installed
            here, no representation of it is compatible with this dispatcher,
            its spec or config is invalid, or its toolchain is unavailable on
            this host.  Any other failure while hydrating is reported the same
            way: none of them is about the job's own run, and each is fixed by
            changing the deployment, after which the parked job can be
            re-driven.
        """
        try:
            payload = self._hydrate_payload(job)
            self._validate_payload_toolchain(payload)
        except UnexecutableJobError:
            raise
        except Exception as exc:
            raise UnexecutableJobError(
                f"Dispatcher {self.identifier!r} could not prepare the payload "
                f"of job {job.identifier!r}: {type(exc).__name__}: {exc}",
            ) from exc
        return payload

    def _hydrate_payload(self, job: Job) -> Payload:
        """Build the payload instance for *job*; see :meth:`_resolve_job_payload`."""
        spec = job.payload
        if spec is None:
            raise UnexecutableJobError(
                f"Job {job.identifier!r} carries no payload; nothing to execute. "
                f"It was most likely published by a job builder from a courier "
                f"release that did not attach payloads to jobs. Upgrade that "
                f"builder and re-feed this job's files (listed in the parked "
                f"message) through it; re-driving this message as-is will "
                f"park it again.",
            )
        if _UNSAFE_SUFFIX_CHARACTERS.intersection(spec.suffix):
            raise UnexecutableJobError(
                f"Job {job.identifier!r} carries an invalid {spec.name!r} "
                f"payload spec: script suffix {spec.suffix!r} contains a path "
                f"separator or NUL",
            )
        representation = self._representation_for(spec.name)
        try:
            return representation.from_job_spec(
                spec,
                self.parent_service,
                self.config,
            )
        except Exception as exc:
            raise UnexecutableJobError(
                f"Job {job.identifier!r} carries an invalid {spec.name!r} "
                f"payload spec: {exc}",
            ) from exc

    def _representation_for(self, name: str) -> type[Payload]:
        """Return (and cache) the representation payload plugin *name* runs as.

        That is the most specific class of the payload that this dispatcher
        lists.  When it is not the payload's own class -- a third-party payload
        run as ``BashPayload``, say -- the payload's own command generation is
        not used, so the lowering is logged the first time it happens.

        Raises
        ------
        UnexecutableJobError
            If the payload plugin is not installed here, or this dispatcher
            cannot run any representation of it.
        """
        if name not in self._representations:
            payload_cls = self._payload_class(name)
            compatible = self.compatible_representation(payload_cls)
            if compatible is None:
                raise UnexecutableJobError(
                    f"Dispatcher {self.identifier!r} cannot execute payload "
                    f"{name!r}: no compatible representation "
                    f"(supports {self.representation_names()})",
                )
            if compatible is not payload_cls:
                self._logger.info(
                    f"Dispatcher {self.identifier!r} runs payload {name!r} "
                    f"({payload_cls.__name__}) as {compatible.__name__}, the "
                    f"most specific representation it supports",
                )
            self._representations[name] = compatible
        return self._representations[name]

    @classmethod
    def compatible_representation(
        cls,
        payload_cls: type[Payload],
    ) -> type[Payload] | None:
        """Return the most specific representation *payload_cls* shares with us.

        A classmethod so compatibility can be checked from configuration alone,
        before (or without) constructing the dispatcher.

        Parameters
        ----------
        payload_cls : type[Payload]
            Payload plugin class to check.

        Returns
        -------
        type[Payload] or None
            The representation to hydrate the payload as, or ``None`` if this
            dispatcher cannot run any representation of it.
        """
        return next(
            (
                candidate
                for candidate in reversed(payload_cls.get_representation_hierarchy())
                if candidate in cls.representations
            ),
            None,
        )

    @classmethod
    def representation_names(cls) -> str:
        """Return a human-readable list of the representations we accept."""
        return ", ".join(c.__name__ for c in cls.representations) or "(none)"

    @property
    def supported_representations(self) -> str:
        """Human-readable list of the representations this dispatcher accepts."""
        return self.representation_names()

    def _payload_class(self, name: str) -> type[Payload]:
        """Return the payload class declared under *name*.

        Raises
        ------
        UnexecutableJobError
            If no payload plugin is installed here under *name*, or it cannot
            be loaded.
        """
        try:
            loaded = payloads.get_plugin(name)
        except Exception as exc:
            raise UnexecutableJobError(
                f"Payload plugin {name!r} is not available on dispatcher "
                f"{self.identifier!r}: {exc}",
            ) from exc
        # ClassPluginRegistry has already checked the Payload subclass.
        return cast("type[Payload]", loaded)

    def _validate_payload_toolchain(self, payload: Payload) -> None:
        """Validate a payload's toolchain on this host, once per configuration.

        Toolchain checks must run where the payload executes, which is now the
        dispatcher rather than a startup-time pairing.  Results are cached by
        (payload name, toolchain) so repeated jobs do not re-probe; a failure
        is not cached, so a fixed host is picked up without a restart.

        Raises
        ------
        UnexecutableJobError
            If a toolchain entry is unavailable, or probing it fails.
        """
        key = (
            payload.name,
            tuple(payload.config.toolchain),
            tuple(payload.config.toolchain_prepend),
            payload.config.binary,
            payload.config.default_binary,
        )
        if key in self._validated_toolchains:
            return
        for value in payload.config.toolchain:
            self._validate_toolchain_value(payload, value)
        self._validated_toolchains.add(key)

    def _validate_toolchain_value(self, payload: Payload, value: str) -> None:
        """Probe one toolchain entry; see :meth:`_validate_payload_toolchain`."""
        try:
            result = payload.validate_toolchain_arg(value)
        except Exception as exc:
            raise UnexecutableJobError(
                f"Toolchain validation for {value!r} on dispatcher "
                f"{self.identifier!r} raised {type(exc).__name__}: {exc}",
            ) from exc
        if not result:
            raise UnexecutableJobError(
                f"Toolchain validation for {value!r} on dispatcher "
                f"{self.identifier!r} produced no result",
            )
        if result[0].return_code != 0:
            detail = (result[0].stderr or "").strip()
            raise UnexecutableJobError(
                f"Toolchain validation failed for {value!r} on dispatcher "
                f"{self.identifier!r} (return code {result[0].return_code}): "
                f"{detail or 'not found on this host'}",
            )

    def _dispatcher_context(self, script_path: Path | None) -> dict[str, Any]:
        """Context exposed to the payload template during pass two.

        ``dispatcher.config`` is this dispatcher's config in its JSON form,
        like every other value a payload template sees.  Subclasses extend
        the context (Slurm adds ``output_dir``); only the names in
        :data:`~courier.interfaces.payloads.DISPATCHER_CONTEXT_NAMES` can be
        reached from a builder-rendered script.
        """
        return {
            "dispatcher": {
                "name": self.name,
                "identifier": self.identifier,
                "config": self.config.model_dump(mode="json"),
            },
            "script_path": str(script_path) if script_path is not None else "",
            "hostname": gethostname(),
        }

    def _materialize_script(
        self,
        job: Job,
        payload: Payload,
        directory: Path | str | None = None,
        prefix: str = "courier-",
    ) -> tuple[Path | None, dict[str, Any]]:
        """Resolve the job's script (pass two) into a new private file.

        The file is created with :func:`tempfile.mkstemp`: a random name,
        opened exclusively (never following a symlink or reusing a path that
        already exists), in *directory* or the temp directory, which honours
        ``TMPDIR``.  The resolved text, passed through
        :meth:`_finalize_script_text`, is written through the descriptor
        ``mkstemp`` returned, never by reopening the path.  If anything fails
        after the file exists, it is removed before the error propagates.

        Parameters
        ----------
        job : Job
            Job whose ``payload.script`` should be materialized.
        payload : Payload
            Hydrated payload that resolves the builder's deferred markers.
        directory : Path or str or None, optional
            Directory to create the script in.  Defaults to
            :func:`tempfile.gettempdir`.
        prefix : str, optional
            File name prefix.

        Returns
        -------
        tuple[Path or None, dict[str, Any]]
            The script path (``None`` when the job carries no script) and the
            dispatcher context it was resolved with, for rendering the command.
        """
        spec = job.payload
        if spec is None or spec.script is None:
            return None, self._dispatcher_context(None)
        fd, name = tempfile.mkstemp(
            suffix=spec.suffix,
            prefix=prefix,
            dir=os.fspath(directory) if directory else tempfile.gettempdir(),
        )
        path = Path(name)
        try:
            context = self._dispatcher_context(path)
            resolved = payload.resolve_deferred_expressions(
                spec.script,
                job,
                context,
                defer_nonce=spec.defer_nonce,
            )
            text = self._finalize_script_text(resolved, job, payload)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                fd = -1  # the handle owns (and closes) the descriptor now
                handle.write(text)
                handle.flush()
                os.fchmod(handle.fileno(), _SCRIPT_MODE)
        except BaseException:
            if fd >= 0:
                os.close(fd)
            self._discard_script(path)
            raise
        return path, context

    def _finalize_script_text(
        self,
        text: str,
        job: Job,  # noqa: ARG002
        payload: Payload,  # noqa: ARG002
    ) -> str:
        """Return the text to write for a job's fully resolved script.

        The default writes it unchanged.  A dispatcher that hands the file to
        something stricter than the payload's interpreter overrides this --
        for example to ensure a batch script starts with a shebang.

        Parameters
        ----------
        text : str
            The script after pass two.
        job : Job
            Job the script belongs to.
        payload : Payload
            Hydrated payload that will run it.

        Returns
        -------
        str
            The text :meth:`_materialize_script` writes.
        """
        return text

    def _render_command(
        self,
        job: Job,
        payload: Payload,
        script_path: Path | None,
        context: dict[str, Any],
    ) -> list[str]:
        """Render the payload's calling method and command for *script_path*.

        Only the payload's argument templates (``binary``, ``prefix_args``,
        ``suffix_args``) are rendered; the script path is spliced in
        literally, so a ``{{`` in ``TMPDIR`` is never evaluated.
        """
        rendered = payload.with_rendered_arguments(job, context)
        return [
            *rendered.generate_calling_method(),
            *rendered.declare_command(script_path),
        ]

    def _log_destination(self, job: Job) -> tuple[str, Path | None]:
        """Return the log prefix and log file path configured for *job*."""
        log_prefix = f"[job: {job.identifier}]" if self.config.log_to_logger else ""
        if not self.config.log_to_file:
            return log_prefix, None
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
        safe_id = slugify_for_filename(job.identifier)
        return (
            log_prefix,
            Path(self.config.log_dir) / f"dispatch_{safe_id}_{timestamp}.log",
        )

    def initialize_environment(
        self,
        job: Job,
        payload: Payload,
    ) -> ExecutionPayload:
        """Prepare the script, command, and logging metadata for *job*.

        This generic implementation runs a payload on the local host.  The
        builder's deferred markers are resolved here, after the script path is
        known, so the payload can fill in dispatcher-specific details.  If
        rendering the command fails, the script is removed before the error
        propagates.

        Parameters
        ----------
        job : Job
            Job to prepare.
        payload : Payload
            Hydrated payload the job carries.

        Returns
        -------
        ExecutionPayload
            Command, script and logging metadata for :meth:`_execute_job`.
        """
        script_path, context = self._materialize_script(job, payload)
        try:
            command = self._render_command(job, payload, script_path, context)
            log_prefix, log_file_path = self._log_destination(job)
        except BaseException:
            if script_path is not None:
                self._discard_script(script_path)
            raise
        return ExecutionPayload(
            command=command,
            file=script_path,
            log_prefix=log_prefix,
            log_file_path=log_file_path,
        )

    def emit(self, execution_log: ExecutionLog) -> None:
        """Emit execution log to parent service.

        Like :meth:`emit_file`, called after the job has run, outside the
        job's containment: a publish failure propagates and the message is
        redelivered.
        """
        self._logger.debug(f"Emitting execution log: {execution_log}")
        self.parent_service.emit(queue=self.queue, message=str(execution_log))

    def emit_file(self, file: File) -> None:
        """Emit output file to the found-file exchange for downstream processing.

        Publishes a :class:`File` to :data:`~courier.constants.FILE_FOUND_EXCHANGE`
        so job builders can pick it up and create new jobs — enabling chained
        dispatcher-to-builder pipeline workflows.

        Parameters
        ----------
        file : File
            The output file to feed back into the pipeline.

        Raises
        ------
        TransientBrokerError
            On a retryable publish failure.
        FatalBrokerError
            On a non-retryable publish failure.

        Notes
        -----
        Called by the consumer loop after the job has run, outside the job's
        containment: whatever this raises -- a broker error, or anything an
        override raises -- propagates, the message is left unacknowledged and
        redelivered, and the job is not counted as failed.  An override must
        therefore let a publish failure escape rather than catch it.
        """
        self._logger.debug(f"Emitting file: {file}")
        self.parent_service.emit(queue=FILE_FOUND_EXCHANGE, message=str(file))

    @staticmethod
    def _dedupe_key(job: Job) -> tuple[str, str]:
        """Return the key *job* is remembered under for dedupe.

        Job identifiers are only unique within the builder that minted them:
        two builders targeting one dispatcher routinely emit the same
        identifier (the same file path, the same time bucket) for different
        work.  Scoping the key by the payload identifier -- unique per builder
        -- keeps both jobs, while a genuine redelivery of either is still
        skipped.

        Parameters
        ----------
        job : Job
            Received job.

        Returns
        -------
        tuple[str, str]
            ``(payload identifier or "", job identifier)``.
        """
        payload_identifier = job.payload.identifier if job.payload is not None else ""
        return payload_identifier, job.identifier

    def _recently_seen(self, key: Hashable) -> bool:
        """Return True if *key* is in the bounded LRU.

        On miss, records the key; evicts oldest when the LRU is full.
        Catches same-replica duplicates from at-least-once delivery;
        cross-replica exactly-once requires the optional state-sync dedupe.

        Parameters
        ----------
        key : Hashable
            Dedupe key, normally from :meth:`_dedupe_key`.

        Returns
        -------
        bool
            ``True`` if *key* was already recorded.
        """
        if key in self._seen_jobs:
            self._seen_jobs.move_to_end(key)
            return True
        self._seen_jobs[key] = None
        if len(self._seen_jobs) > _DEDUPE_LRU_SIZE:
            self._seen_jobs.popitem(last=False)
        return False

    def _queue_depth_connection(self) -> kombu.Connection:
        """Return this dispatcher's own broker connection, opening it if needed.

        Returns
        -------
        kombu.Connection
            A connection used by no other thread.
        """
        if self._depth_connection is None or not self._depth_connection.connected:
            self._depth_connection = (
                self.parent_service._broker_manager.open_private_connection(
                    read_timeout=_QUEUE_DEPTH_TIMEOUT_SECONDS,
                )
            )
        return self._depth_connection

    def _close_queue_depth_connection(self) -> None:
        """Close the probe connection, if one was opened.

        Called by the consumer thread as it leaves, so the connection is
        released by the same thread that opened and used it.
        """
        connection, self._depth_connection = self._depth_connection, None
        if connection is not None:
            with contextlib.suppress(Exception):
                connection.release()

    def _emit_queue_depth(self) -> None:
        """Emit the per-dispatcher queue-depth gauge.

        Best-effort, and sampled only here, once per received job: the gauge
        is the number of messages ready in the incoming queue at that moment
        (a passive ``queue_declare``), which excludes the job in hand and any
        other unacknowledged message, and it keeps that value until the next
        job arrives.  A probe that fails sets it to 0.  (The memory transport
        answers too, from the queues of this process.)

        The probe runs on a connection belonging to this dispatcher rather
        than on the service's shared one.  Sharing it deadlocked the
        pipeline: this method runs on the plugin's own thread, once per
        received job, and every dispatcher in the service was issuing a
        synchronous ``queue_declare`` down the same socket.  Two of them
        arriving together -- which is what a service with two dispatchers
        receiving jobs concurrently does on its very first pair of jobs --
        left both blocked in ``read_frame`` on a reply the other had already
        read.  That is a hang rather than an exception, so the ``except``
        below could not have caught it either; and because a message is
        acknowledged only after this returns, both consumers sat on an
        unacknowledged message forever.  At the default
        ``broker_prefetch_count`` of 1 the broker then delivers neither of
        them anything again.  Dispatch stopped dead while every plugin still
        reported ``RUNNING``, every heartbeat kept beating and every manager
        health check stayed green.

        See :meth:`~courier.broker.kombu.MessageBrokerManager.open_private_connection`.
        """
        message_count = 0
        try:
            queue_name = self.parent_service._broker_manager.get_queue_name(
                self.incoming_queue,
            )
            with self._queue_depth_connection().channel() as channel:
                _, message_count, _ = channel.queue_declare(
                    queue=queue_name,
                    passive=True,
                )
        except Exception:  # a gauge must never break dispatch
            # Includes the read timeout. Drop the connection so a transport
            # left mid-frame is replaced rather than reused for every
            # subsequent job.
            self._close_queue_depth_connection()
            message_count = 0
        DISPATCHER_QUEUE_DEPTH.labels(
            dispatcher_identifier=self.identifier,
        ).set(message_count)

    def _run_handle_incoming_jobs(self) -> None:
        """Exit the process on any unhandled exception.

        :meth:`handle_incoming_jobs` contains every failure it can attribute to
        a single job, so what reaches here is a broker or consume-level fault
        (or a failure to park a message).  The message in hand is left
        unacknowledged and is redelivered after the restart.
        """
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "dispatcher.handle_incoming_jobs",
            attributes={
                ATTR_PLUGIN_NAME: self.name,
                ATTR_PLUGIN_VERSION: self.version,
            },
        ) as span:
            try:
                self.handle_incoming_jobs()
            except Exception:
                traceback.print_exc()
                span.set_status(Status(StatusCode.ERROR))
                self._logger.critical(
                    "Fatal error in dispatcher %s: exiting",
                    self.name,
                )
                os._exit(1)
            finally:
                # This thread opened the probe connection, so this thread
                # closes it. stop() joins with a timeout and cannot assume
                # the loop has left, and closing a connection out from under
                # a thread still using it is the class of bug being fixed.
                self._close_queue_depth_connection()

    def handle_incoming_jobs(self) -> None:
        """Consume this dispatcher's job queue and execute each job in turn.

        Each message is acknowledged once its iteration returns.  A job that
        fails while being prepared or run is logged and counted as a
        ``failure``.  A message this dispatcher cannot execute at all -- one
        that is not a job, carries no payload, or needs a payload plugin,
        representation or toolchain this host lacks (see
        :class:`~courier.errors.UnexecutableJobError`) -- is first parked on
        the queue's dead-letter queue via
        :meth:`~courier.service.Service.park_message` and counted as
        ``unexecutable``, so it is kept for a re-drive rather than dropped.
        Only a fault the loop cannot attribute to one job escapes: any failure
        publishing a job's results (its execution logs and output files, e.g.
        a :class:`~courier.errors.TransientBrokerError` or
        :class:`~courier.errors.FatalBrokerError`), or a failure to park.  The
        message in hand is then left unacknowledged and redelivered.
        """
        for job_string, parent_ctx in self.parent_service.consume(
            self.incoming_queue,
            stop_event=self._stop_event,
            on_subscribed=self._subscribed.set,
        ):
            with contextlib.suppress(Exception):
                self._emit_queue_depth()
            self._jobs_consumed.labels(
                dispatcher_identifier=self.identifier,
            ).inc()
            body = str(job_string)
            job = self._parse_job(body, parent_ctx)
            if job is not None:
                self._dispatch_job(job, body, parent_ctx)
        self._logger.debug("Dispatcher %s consume loop exited", self.name)

    def _parse_job(self, body: str, parent_ctx: Any) -> Job | None:
        """Deserialize a message body, parking it if it is not a valid job.

        Parameters
        ----------
        body : str
            Message body as received.
        parent_ctx : Any
            Trace context extracted from the message, parenting the span the
            parking is recorded on.

        Returns
        -------
        Job or None
            The job, or ``None`` when the body was parked.
        """
        try:
            job = Job.from_string(body)
            _check_job_envelope(job)
        except Exception as exc:
            with get_tracer(__name__).start_as_current_span(
                "dispatcher.dispatch_job",
                context=parent_ctx,
            ):
                self._park_unexecutable(
                    body,
                    None,
                    UnexecutableJobError(
                        f"Message is not a valid job: {type(exc).__name__}: {exc}",
                    ),
                )
            return None
        else:
            return job

    def _dispatch_job(self, job: Job, body: str, parent_ctx: Any) -> None:
        """Record receipt of *job*, skip it if it is a duplicate, else run it."""
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "dispatcher.dispatch_job",
            context=parent_ctx,
            attributes={
                ATTR_JOB_ID: job.identifier,
                ATTR_CORRELATION_ID: job.correlation_id,
            },
        ):
            self._logger.debug(
                f"Received Job: {job}",
                extra={"correlation_id": job.correlation_id},
            )
            if job.emit_time is not None:
                self._dispatch_latency.labels(
                    dispatcher_identifier=self.identifier,
                ).observe(time.time() - job.emit_time)

            key = self._dedupe_key(job)
            if self._recently_seen(key):
                self._dedupe_skips.labels(
                    dispatcher_identifier=self.identifier,
                ).inc()
                self._logger.info(
                    f"Duplicate job {job.identifier} (payload {key[0]!r}); skipping",
                    extra={"correlation_id": job.correlation_id},
                )
                return
            self._run_job(job, body, key)

    def _run_job(self, job: Job, body: str, key: tuple[str, str]) -> None:
        """Execute *job*, publish its results, and account for it.

        Parameters
        ----------
        job : Job
            Job to execute.
        body : str
            The message *job* was parsed from, parked verbatim if the job
            cannot be executed here.
        key : tuple[str, str]
            The job's dedupe key.

        Raises
        ------
        Exception
            Whatever publishing the job's results raises (see
            :meth:`_publish_results`).  A failure of the job itself never
            escapes: it is parked or counted by :meth:`_execute_contained`.
        """
        start_time = time.time()
        job_id = job.identifier
        self.active_job_timestamps[job_id] = start_time
        self._active_jobs.labels(**self._metric_labels).inc()
        self._queue_wait_duration.labels(**self._metric_labels).observe(
            start_time - job.last_modified,
        )
        try:
            results = self._execute_contained(job, body, key)
            if results is not None:
                self._publish_results(key, *results)
                self._jobs_processed.labels(
                    status="success",
                    **self._metric_labels,
                ).inc()
        finally:
            execution_time = time.time() - self.active_job_timestamps.pop(
                job_id,
            )
            self._job_execution_duration.labels(
                **self._metric_labels,
            ).observe(execution_time)
            self._active_jobs.labels(**self._metric_labels).dec()

    def _execute_contained(
        self,
        job: Job,
        body: str,
        key: tuple[str, str],
    ) -> tuple[list[ExecutionLog], list[File]] | None:
        """Execute *job* and collect its output files, containing its failures.

        Returns
        -------
        tuple[list[ExecutionLog], list[File]] or None
            The execution logs and the output files to publish; ``None`` when
            the job was parked as unexecutable or failed (and was counted).
        """
        try:
            execution_logs = self._contained_execution_log(job)
            get_current_span().add_event(
                "job.executed",
                attributes={
                    ATTR_JOB_ID: job.identifier,
                    ATTR_CORRELATION_ID: job.correlation_id,
                },
            )
            files = self._contained_output_files(job, execution_logs)
        except UnexecutableJobError as exc:
            # Forget the job, so re-driving it from the dead-letter queue once
            # the deployment is fixed runs it rather than skipping a duplicate.
            self._seen_jobs.pop(key, None)
            self._park_unexecutable(body, job, exc)
            return None
        except CourierError as exc:
            self._logger.exception(
                f"Error processing job {job.identifier}",
                extra={"correlation_id": job.correlation_id},
            )
            span = get_current_span()
            span.set_status(Status(StatusCode.ERROR))
            span.record_exception(exc)
            self._jobs_processed.labels(
                status="failure",
                **self._metric_labels,
            ).inc()
            return None
        return execution_logs, files

    def _contained_output_files(
        self,
        job: Job,
        logs: list[ExecutionLog],
    ) -> list[File]:
        """Call :meth:`_collect_output_files`, converting stray errors.

        Like :meth:`_contained_execution_log`, this covers subclasses that
        override the hook, so a bug in one fails the job instead of ending
        the process (and the redelivered message ending the next one).
        """
        try:
            return self._collect_output_files(job, logs)
        except CourierError:
            raise
        except Exception as exc:
            raise CourierError(
                f"Failed to collect the output files of job {job.identifier!r} "
                f"on dispatcher {self.identifier!r}: {type(exc).__name__}: {exc}",
            ) from exc

    def _publish_results(
        self,
        key: tuple[str, str],
        logs: list[ExecutionLog],
        files: list[File],
    ) -> None:
        """Re-emit the job's output files, then publish its execution logs.

        This runs outside the job-level containment on purpose.  A publish
        failure is the broker's, not the job's: a
        :class:`~courier.errors.TransientBrokerError` or
        :class:`~courier.errors.FatalBrokerError` (both
        :class:`~courier.errors.CourierError`), or a raw transport error,
        propagates to :meth:`_run_handle_incoming_jobs`, which exits so the
        unacknowledged message is redelivered -- rather than being
        acknowledged as a failed job with its results and output files lost.
        The job is forgotten by the dedupe LRU first, so a redelivery that
        reaches this same instance runs it again instead of skipping it as a
        duplicate.

        Raises
        ------
        Exception
            Whatever :meth:`emit_file` or :meth:`emit` raises.
        """
        try:
            for file in files:
                self.emit_file(file)
            self._emit_execution_logs(logs)
        except BaseException:
            self._seen_jobs.pop(key, None)
            raise

    def _contained_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Call :meth:`get_execution_log`, converting stray errors to CourierError.

        The base implementation already converts; this also covers subclasses
        that override :meth:`get_execution_log`, so no job-level error can end
        the process.
        """
        try:
            return self.get_execution_log(job)
        except CourierError:
            raise
        except Exception as exc:
            raise CourierError(
                f"Unexpected error executing job {job.identifier!r} on "
                f"dispatcher {self.identifier!r}: {type(exc).__name__}: {exc}",
            ) from exc

    def _emit_execution_logs(self, logs: list[ExecutionLog]) -> None:
        """Publish each execution log to the dispatcher queue."""
        tracer = get_tracer(__name__)
        for ex_log in logs:
            with tracer.start_as_current_span(
                "dispatcher.emit_execution_log",
                attributes={
                    ATTR_EXECUTION_RETURN_CODE: (
                        str(ex_log.return_code)
                        if ex_log.return_code is not None
                        else ""
                    ),
                },
            ):
                self.emit(ex_log)
            self._execution_logs_emitted.labels(
                **self._metric_labels,
            ).inc()

    def _park_unexecutable(
        self,
        body: str,
        job: Job | None,
        error: UnexecutableJobError,
    ) -> None:
        """Park a message this dispatcher cannot execute on the dead-letter queue.

        Parameters
        ----------
        body : str
            The message exactly as received, so a re-drive replays it.
        job : Job or None
            The parsed job, or ``None`` when *body* is not a valid job.
        error : UnexecutableJobError
            Why the message cannot be executed; recorded as the park reason.

        Raises
        ------
        Exception
            Whatever :meth:`~courier.service.Service.park_message` raises.  It
            propagates on purpose: the message is then left unacknowledged and
            redelivered by the consume loop instead of being lost.
        """
        extra = {"correlation_id": job.correlation_id} if job is not None else {}
        label = repr(job.identifier) if job is not None else "(unparseable message)"
        self._logger.error(
            f"Dispatcher {self.identifier!r} cannot execute job {label}: "
            f"{error} -- parking it on the dead-letter queue of "
            f"{self.incoming_queue!r}",
            extra=extra,
        )
        span = get_current_span()
        span.set_status(Status(StatusCode.ERROR))
        span.record_exception(error)
        self.parent_service.park_message(self.incoming_queue, body, str(error))
        self._jobs_processed.labels(
            status=_UNEXECUTABLE_STATUS,
            **self._metric_labels,
        ).inc()

    @log_execution
    def start(self) -> None:
        """Start main thread."""
        if self._state == PluginRunState.RUNNING:
            return
        self._stop_event.clear()
        self._subscribed.clear()
        # daemon=True is a backstop, not the shutdown mechanism: stop() sets
        # _stop_event and joins, which is how the thread is meant to end. If a
        # job is wedged past the join timeout the interpreter can still exit
        # rather than hanging forever; the unacked message is redelivered.
        self._main_thread = threading.Thread(
            target=self._run_handle_incoming_jobs,
            name=self.name,
            daemon=True,
        )
        self._state = PluginRunState.RUNNING
        self._main_thread.start()

    @log_execution
    def stop(self) -> None:
        """Stop main thread."""
        self._state = PluginRunState.STOPPED
        self._stop_event.set()
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=5)

    def is_healthy(self) -> bool:
        """Check if plugin is healthy."""
        return self._state == PluginRunState.RUNNING

    def wait_until_subscribed(self, timeout: float) -> bool:
        """Block until this dispatcher is bound to its job queue.

        Returns
        -------
        bool
            ``True`` if the subscription completed within *timeout*.
        """
        return self._subscribed.wait(timeout=timeout)

    def get_metrics(self) -> dict[str, Any]:
        """Return plugin-specific metrics."""
        return {
            **collect_labeled(DISPATCHER_JOBS_PROCESSED, "dispatcher_name", self.name),
            **collect_labeled(
                DISPATCHER_JOB_EXECUTION_DURATION,
                "dispatcher_name",
                self.name,
            ),
            **collect_labeled(DISPATCHER_ACTIVE_JOBS, "dispatcher_name", self.name),
            **collect_labeled(
                DISPATCHER_EXECUTION_LOGS_EMITTED,
                "dispatcher_name",
                self.name,
            ),
            **collect_labeled(
                DISPATCHER_QUEUE_WAIT_DURATION,
                "dispatcher_name",
                self.name,
            ),
        }


dispatchers = ClassPluginRegistry(
    name="dispatchers",
    group=f"{ENTRY_POINT_PREFIX}.dispatchers",
    expected_base=Dispatcher,
)

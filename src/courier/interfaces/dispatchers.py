"""Python class for the dispatchers courier interface."""

from __future__ import annotations

import contextlib
import math
import os
import queue
import tempfile
import threading
import time
import traceback
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
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
from courier.errors import UnexecutableJobError
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

#: ``jobs_processed`` status of a job parked as unexecutable, kept apart from
#: ``failure`` (a job that ran and failed).
_UNEXECUTABLE_STATUS = "unexecutable"

#: Mode of a materialized job script.
_SCRIPT_MODE = 0o755

#: Characters a script suffix (which arrives in the message) may not contain.
_UNSAFE_SUFFIX_CHARACTERS = frozenset({"/", "\\", "\x00"})

if TYPE_CHECKING:
    from collections.abc import Hashable

    import kombu

    from courier.service import Service
    from courier.types.datum import Datum
    from courier.types.execution_log import ExecutionLog


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
    """Reject envelope fields the consume loop uses before any job containment.

    Raises
    ------
    TypeError
        If ``identifier`` is not a string, or ``last_modified`` / ``emit_time``
        is not a finite number (``emit_time`` may be ``None``).
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

    ``file`` is removed after execution unless ``keep_file`` is set (e.g. by
    a dispatcher whose submitted work still reads it).
    """

    command: list[str]
    file: Path | None
    log_prefix: str = ""
    log_file_path: Path | None = None
    keep_file: bool = False


class Dispatcher(ServicePlugin):
    """Base dispatcher plugin.

    Consumes jobs from its ``JobReady-<identifier>`` queue, hydrates the
    payload each job carries, and executes it.  A job this dispatcher cannot
    execute at all is parked on the dead-letter queue
    (:class:`~courier.errors.UnexecutableJobError`); any other failure,
    publishing the job's results included, fails only that job.
    """

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "dispatcher"

    #: Payload representation classes this dispatcher can execute. A payload
    #: lowered to any of these is compatible; the most specific match wins.
    representations: ClassVar[list[type[Payload]]] = []

    #: Model this dispatcher's config is validated with.
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
        # Bounded LRU of recently-seen jobs, keyed by _dedupe_key. Per replica
        # only: nothing dedupes across replicas, so a job redelivered to
        # another replica runs again. (Builder state sync stops duplicate jobs
        # being published; it does not dedupe here.) Thread-safe: only touched
        # by handle_incoming_jobs thread.
        self._seen_jobs: OrderedDict[Hashable, None] = OrderedDict()
        # Payload name -> its class and the representation it runs as here,
        # and validated (payload, toolchain) keys, so jobs do not re-pay
        # lookup and probing.
        self._representations: dict[str, tuple[type[Payload], type[Payload]]] = {}
        self._validated_toolchains: set[
            tuple[str, str, tuple[str, ...], tuple[str, ...], str | None, str | None]
        ] = set()
        # Connection this dispatcher uses for its queue-depth probe. Opened
        # lazily by the consumer thread and closed by it, so it is owned by
        # exactly one thread for its whole life; see _emit_queue_depth.
        self._depth_connection: kombu.Connection | None = None
        # Thread pool for concurrent execution
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=self.config.max_workers)
        # Synchronized queue containing the asynchronous results from the thread pool.
        self._futures_queue : queue.Queue = queue.Queue()

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Resolve the job's payload, prepare its environment, and execute it.

        Parameters
        ----------
        job : Job
            Job to execute.

        Returns
        -------
        list[ExecutionLog]
            Execution logs produced by the payload.

        Raises
        ------
        UnexecutableJobError
            If this dispatcher cannot execute the job at all; the consume loop
            parks it.  Any other exception fails only this job.
        """
        with get_tracer(__name__).start_as_current_span(
            "dispatcher.execute_job",
            attributes={
                ATTR_JOB_ID: job.identifier,
                ATTR_CORRELATION_ID: job.correlation_id,
            },
        ):
            payload = self._resolve_job_payload(job)
            env = self.initialize_environment(job, payload)
            try:
                self._logger.debug(f"Yielding execution log for job: {job}")
                logs = self._execute_job(job, payload, env)
            finally:
                if env.file is not None and not env.keep_file:
                    self._discard_script(env.file)
            self._report_failed_runs(job, logs)
            return logs

    def _discard_script(self, path: Path) -> None:
        """Delete a materialized script; a failure is logged, never raised."""
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            self._logger.warning(f"Could not remove job script {path}: {exc}")

    def _report_failed_runs(self, job: Job, logs: list[ExecutionLog]) -> None:
        """Log, and mark the span, when the payload exited non-zero."""
        codes = [log.return_code for log in logs if log.return_code not in {0, None}]
        if not codes:
            return
        self._logger.error(
            f"Job {job.identifier!r} on dispatcher {self.identifier!r} exited "
            f"with return code(s) {codes}",
            extra={"correlation_id": job.correlation_id},
        )
        get_current_span().set_status(Status(StatusCode.ERROR))

    def _emit_output_files(self, logs: list[ExecutionLog]) -> None:
        """Re-emit the files ``output_files`` finds in the job's output."""
        if self.config.output_files:
            _scan_and_emit_output_files(
                stdout="\n".join(log.stdout or "" for log in logs),
                stderr="\n".join(log.stderr or "" for log in logs),
                patterns=self.config.output_files,
                scan_stderr=self.config.scan_stderr,
                hostname=gethostname(),
                emit_file=self.emit_file,
            )

    def _execute_job(
        self,
        job: Job,
        payload: Payload,
        env: ExecutionPayload,
    ) -> list[ExecutionLog]:
        """Execute *env*'s command through *payload* on this host.

        Dispatchers that manage a scheduler (e.g. Slurm) override this.
        """
        return payload.get_payload_from_job(
            env.command,
            job,
            log_prefix=env.log_prefix,
            log_file_path=env.log_file_path,
        )

    def _resolve_job_payload(self, job: Job) -> Payload:
        """Hydrate the job's payload and validate its toolchain on this host.

        Raises
        ------
        UnexecutableJobError
            On any failure: no payload, payload plugin not installed, no
            compatible representation, invalid spec or config, or missing
            toolchain.  Each is fixed by changing the deployment, after which
            the parked job can be re-driven.
        """
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
        try:
            payload_cls, representation = self._representation_for(spec.name)
            payload = payload_cls.from_job_spec(
                spec,
                self.parent_service,
                self.config,
                representation=representation,
            )
            self._validate_payload_toolchain(payload)
        except UnexecutableJobError:
            raise
        except Exception as exc:
            raise UnexecutableJobError(
                f"Dispatcher {self.identifier!r} could not prepare the "
                f"{spec.name!r} payload of job {job.identifier!r}: "
                f"{type(exc).__name__}: {exc}",
            ) from exc
        return payload

    def _representation_for(self, name: str) -> tuple[type[Payload], type[Payload]]:
        """Return (and cache) payload plugin *name*'s class and representation.

        The payload is hydrated as its own class and runs as the
        representation, which :meth:`Payload.render_script` lowers it to.
        """
        if name not in self._representations:
            # ClassPluginRegistry has already checked the Payload subclass.
            payload_cls = cast("type[Payload]", payloads.get_plugin(name))
            compatible = self.compatible_representation(payload_cls)
            if compatible is None:
                raise UnexecutableJobError(
                    f"Dispatcher {self.identifier!r} cannot execute payload "
                    f"{name!r}: no compatible representation "
                    f"(supports {self.representation_names()})",
                )
            self._representations[name] = (payload_cls, compatible)
        return self._representations[name]

    @classmethod
    def compatible_representation(
        cls,
        payload_cls: type[Payload],
    ) -> type[Payload] | None:
        """Return the most specific representation *payload_cls* shares with us.

        ``None`` if this dispatcher cannot run any representation of it.  A
        classmethod so compatibility can be checked from configuration alone.
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

    def _validate_payload_toolchain(self, payload: Payload) -> None:
        """Probe a payload's toolchain on this host; only successes are cached.

        Raises
        ------
        UnexecutableJobError
            If a toolchain entry is unavailable.
        """
        key = (
            payload.name,
            payload.representation.__name__,
            tuple(payload.config.toolchain),
            tuple(payload.config.toolchain_prepend),
            payload.config.binary,
            payload.config.default_binary,
        )
        if key in self._validated_toolchains:
            return
        for value in payload.config.toolchain:
            result = payload.validate_toolchain_arg(value)
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
        self._validated_toolchains.add(key)

    def _dispatcher_context(self, script_path: Path | None) -> dict[str, Any]:
        """Context exposed to the payload template during pass two.

        ``dispatcher.config`` is in JSON form.  Subclasses extend the context
        (Slurm adds ``output_dir``); only the names in
        :data:`~courier.interfaces.payloads.DISPATCHER_CONTEXT_NAMES` are
        reachable from a builder-rendered script.
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

        The file is created with :func:`tempfile.mkstemp` in *directory* (by
        default the temp directory, which honours ``TMPDIR``) and written
        through the returned descriptor; it is removed if anything fails.

        Returns
        -------
        tuple[Path or None, dict[str, Any]]
            The script path (``None`` when the job carries no script) and the
            dispatcher context it was resolved with.
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
        """Return the text to write for a resolved script (default: unchanged).

        Overridden by a dispatcher that hands the file to something stricter
        than the payload's interpreter (Slurm adds a shebang).
        """
        return text

    def _render_command(
        self,
        job: Job,
        payload: Payload,
        script_path: Path | None,
        context: dict[str, Any],
    ) -> list[str]:
        """Render the payload's argument templates and splice in *script_path*.

        The path is never rendered, so a ``{{`` in ``TMPDIR`` is not evaluated.
        The result is lowered by :meth:`Payload.render_script` when the payload
        runs as a lower representation.
        """
        rendered = payload.with_rendered_arguments(job, context)
        return rendered.render_script(
            [
                *rendered.generate_calling_method(),
                *rendered.declare_command(script_path),
            ],
        )

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
        builder's deferred markers are resolved once the script path is known;
        if rendering the command then fails, the script is removed.

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
        """Emit execution log to parent service."""
        self._logger.debug(f"Emitting execution log: {execution_log}")
        self.parent_service.emit(queue=self.queue, message=str(execution_log))

    def emit_file(self, file: Datum) -> None:
        """Emit output file to the found-file exchange for downstream processing.

        Publishes a :class:`Datum` to :data:`~courier.constants.FILE_FOUND_EXCHANGE`
        so job builders can pick it up and create new jobs — enabling chained
        dispatcher-to-builder pipeline workflows.

        Parameters
        ----------
        file : Datum
            The output file to feed back into the pipeline.
        """
        self._logger.debug(f"Emitting file: {file}")
        self.parent_service.emit(queue=FILE_FOUND_EXCHANGE, message=str(file))

    @staticmethod
    def _dedupe_key(job: Job) -> tuple[str, str]:
        """Return ``(payload identifier, job identifier)`` for dedupe.

        Job identifiers are unique only per builder, so two builders targeting
        one dispatcher may emit the same one for different work.
        """
        payload_identifier = job.payload.identifier if job.payload is not None else ""
        return payload_identifier, job.identifier

    def _recently_seen(self, key: Hashable) -> bool:
        """Return True if *key* is in the bounded LRU.

        On miss, records the key; evicts oldest when the LRU is full.
        Catches duplicates from at-least-once delivery to this replica only.
        There is no cross-replica dedupe: a job redelivered to another
        replica is not in its LRU and runs again. Job builder state sync
        prevents duplicate jobs from being published, upstream of here.
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
        """Exit the process on any unhandled exception."""
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

    def _consume_jobs(self) -> None:
        """Handle a steady stream of jobs.

        Initialize job processing and add submit a job to be processed in the
        thread pool.
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

    def handle_incoming_jobs(self) -> None:
        """Handle the consumption and processing of consumed jobs."""
        threading.Thread(target=self._consume_jobs).start()
        while not self._stop_event.is_set():
            if not self._futures_queue.empty():
                self._futures_queue.get().result()
        while not self._futures_queue.empty():
            self._futures_queue.get().result()

    def _parse_job(self, body: str, parent_ctx: Any) -> Job | None:
        """Deserialize a message body; park it and return None if not a job."""
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
            self._futures_queue.put(
                self._executor.submit(
                    self._run_job, job, body, key),
                )

    def _run_job(self, job: Job, body: str, key: tuple[str, str]) -> None:
        """Execute *job*, publish its results, and account for it."""
        start_time = time.time()
        job_id = job.identifier
        self.active_job_timestamps[job_id] = start_time
        self._active_jobs.labels(**self._metric_labels).inc()
        self._queue_wait_duration.labels(**self._metric_labels).observe(
            start_time - job.last_modified,
        )
        try:
            execution_logs = self.get_execution_log(job)
            get_current_span().add_event(
                "job.executed",
                attributes={
                    ATTR_JOB_ID: job.identifier,
                    ATTR_CORRELATION_ID: job.correlation_id,
                },
            )
            self._emit_output_files(execution_logs)
            self._emit_execution_logs(execution_logs)
            self._jobs_processed.labels(status="success", **self._metric_labels).inc()
        except UnexecutableJobError as exc:
            # Forget the job, so re-driving it from the dead-letter queue once
            # the deployment is fixed runs it rather than skipping a duplicate.
            self._seen_jobs.pop(key, None)
            self._park_unexecutable(body, job, exc)
        except Exception as exc:  # one bad job must never end the service
            self._logger.exception(
                f"Error processing job {job.identifier}",
                extra={"correlation_id": job.correlation_id},
            )
            span = get_current_span()
            span.set_status(Status(StatusCode.ERROR))
            span.record_exception(exc)
            self._jobs_processed.labels(status="failure", **self._metric_labels).inc()
        finally:
            execution_time = time.time() - self.active_job_timestamps.pop(job_id)
            self._job_execution_duration.labels(
                **self._metric_labels,
            ).observe(execution_time)
            self._active_jobs.labels(**self._metric_labels).dec()

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

        *body* is parked verbatim so a re-drive replays it; *job* is ``None``
        when it did not parse.  A failure to park propagates, so the message
        is redelivered instead of lost.
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

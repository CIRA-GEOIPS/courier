"""Python class for the dispatchers courier interface."""

from __future__ import annotations

import contextlib
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
from typing import TYPE_CHECKING, Any, ClassVar

from opentelemetry.trace import Status, StatusCode, get_current_span

from courier.constants import (
    DISPATCHER_QUEUE,
    FILE_FOUND_EXCHANGE,
    PluginRunState,
    job_ready_queue_for,
)
from courier.dispatchers._output_scanner import _scan_and_emit_output_files
from courier.errors import CourierError
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

if TYPE_CHECKING:
    import kombu

    from courier.service import Service
    from courier.types.execution_log import ExecutionLog
    from courier.types.file import File


# config class for courier init discovery
class DispatcherConfig(DispatcherGroupConfig):  # noqa: D101
    pass


@dataclass
class ExecutionPayload:
    """Command and logging metadata prepared by a dispatcher for execution."""

    command: list[str]
    file: Path | None
    log_prefix: str = ""
    log_file_path: Path | None = None


class Dispatcher(ServicePlugin):
    """Base dispatcher plugin."""

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "dispatcher"

    #: Payload representation classes this dispatcher can execute. A payload
    #: lowered to any of these is compatible; the most specific match wins.
    representations: ClassVar[list[type[Payload]]] = []

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
        self.config = DispatcherGroupConfig.model_validate(config or {})

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
        # Bounded LRU of recently-seen job identifiers. Catches same-replica
        # duplicates; cross-replica strict dedupe is opt-in via state sync.
        # Thread-safe: only touched by handle_incoming_jobs thread.
        self._seen_jobs: OrderedDict[str, None] = OrderedDict()
        # Cache of payload name -> payload class, and of already-validated
        # (payload name, toolchain) pairs, so a per-job payload does not pay
        # registry lookup and toolchain probing on every dispatch.
        self._payload_classes: dict[str, type[Payload]] = {}
        self._validated_toolchains: set[
            tuple[str, tuple[str, ...], tuple[str, ...], str | None, str | None]
        ] = set()
        # Connection this dispatcher uses for its queue-depth probe. Opened
        # lazily by the consumer thread and closed by it, so it is owned by
        # exactly one thread for its whole life; see _emit_queue_depth.
        self._depth_connection: kombu.Connection | None = None

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Resolve the job's payload, prepare its environment, and execute it."""
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "dispatcher.execute_job",
            attributes={
                ATTR_JOB_ID: job.identifier,
                ATTR_CORRELATION_ID: job.correlation_id,
            },
        ):
            payload = self._resolve_job_payload(job)
            try:
                env = self.initialize_environment(job, payload)
            except Exception as e:
                raise CourierError(
                    f"Failed to initialize environment for dispatcher "
                    f"{self.identifier}",
                    e,
                ) from e
            try:
                self._logger.debug(f"Yielding execution log for job: {job}")
                logs = self._execute_job(job, payload, env)
                self._emit_output_files(logs)
                return logs
            finally:
                if env.file is not None:
                    env.file.unlink(missing_ok=True)

    def _emit_output_files(self, logs: list[ExecutionLog]) -> None:
        """Re-emit output files named in the execution logs into the pipeline.

        Chained dispatcher-to-builder workflows depend on this: a script prints
        the paths it produced, the patterns configured under ``output_files``
        match them, and each discovered ``File`` is published to the file-found
        exchange via :meth:`emit_file`.
        """
        if not self.config.output_files:
            return
        stdout = "\n".join(log.stdout or "" for log in logs)
        stderr = "\n".join(log.stderr or "" for log in logs)
        _scan_and_emit_output_files(
            stdout=stdout,
            stderr=stderr,
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

        Raises
        ------
        CourierError
            If the job carries no payload, no representation of it is
            compatible with this dispatcher, or its toolchain is unavailable.
        """
        spec = job.payload
        if spec is None:
            raise CourierError(
                f"Job {job.identifier!r} carries no payload; nothing to execute",
            )
        payload_cls = self._payload_class(spec.name)
        compatible = self.compatible_representation(payload_cls)
        if compatible is None:
            raise CourierError(
                f"Dispatcher {self.identifier!r} cannot execute payload "
                f"{spec.name!r}: no compatible representation "
                f"(supports {self.supported_representations})",
            )
        payload = compatible.from_job_spec(spec, self.parent_service, self.config)
        self._validate_payload_toolchain(payload)
        return payload

    def compatible_representation(
        self,
        payload_cls: type[Payload],
    ) -> type[Payload] | None:
        """Return the most specific representation *payload_cls* shares with us."""
        return next(
            (
                candidate
                for candidate in reversed(payload_cls.get_representation_hierarchy())
                if candidate in self.representations
            ),
            None,
        )

    @property
    def supported_representations(self) -> str:
        """Human-readable list of the representations this dispatcher accepts."""
        return ", ".join(c.__name__ for c in self.representations) or "(none)"

    def _payload_class(self, name: str) -> type[Payload]:
        """Return (and cache) the payload class declared under *name*."""
        if name not in self._payload_classes:
            self._payload_classes[name] = payloads.get_plugin(name)
        return self._payload_classes[name]

    def _validate_payload_toolchain(self, payload: Payload) -> None:
        """Validate a payload's toolchain on this host, once per configuration.

        Toolchain checks must run where the payload executes, which is now the
        dispatcher rather than a startup-time pairing.  Results are cached by
        (payload name, toolchain) so repeated jobs do not re-probe.
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
            result = payload.validate_toolchain_arg(value)
            if not result:
                raise CourierError(
                    f"Toolchain validation for {value!r} on dispatcher "
                    f"{self.identifier!r} produced no result",
                )
            if result[0].return_code != 0:
                raise CourierError(
                    f"Toolchain validation failed for {value!r} on dispatcher "
                    f"{self.identifier!r}",
                    result[0].stderr,
                )
        self._validated_toolchains.add(key)

    def _dispatcher_context(self, script_path: Path | None) -> dict[str, Any]:
        """Context exposed to the payload template during pass two."""
        return {
            "dispatcher": {
                "name": self.name,
                "identifier": self.identifier,
                "config": self.config.model_dump(),
            },
            "script_path": str(script_path) if script_path is not None else "",
            "hostname": gethostname(),
        }

    def initialize_environment(
        self,
        job: Job,
        payload: Payload,
    ) -> ExecutionPayload:
        """Prepare the script, command, and logging metadata for *job*.

        This generic implementation runs a payload on the local host.  The
        builder's deferred markers are resolved here, after the script path is
        known, so the payload can fill in dispatcher-specific details.
        """
        script_path: Path | None = None
        context = self._dispatcher_context(None)
        if job.payload is not None and job.payload.script is not None:
            fd, temp_name = tempfile.mkstemp(
                suffix=job.payload.suffix,
                dir="/tmp/",
            )
            os.close(fd)
            script_path = Path(temp_name)
            context = self._dispatcher_context(script_path)
            resolved = payload.resolve_deferred_expressions(
                job.payload.script,
                job,
                context,
                defer_nonce=job.payload.defer_nonce,
            )
            script_path = payload.write_script(resolved, script_path)
        call = payload.generate_calling_method()
        command = call + [
            payload.render_script(job, part, context)
            for part in payload.declare_command(script_path)
        ]
        log_prefix = f"[job: {job.identifier}]" if self.config.log_to_logger else ""
        log_file_path: Path | None = None
        if self.config.log_to_file:
            timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
            safe_id = slugify_for_filename(job.identifier)
            log_file_path = (
                Path(self.config.log_dir) / f"dispatch_{safe_id}_{timestamp}.log"
            )
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

    def emit_file(self, file: File) -> None:
        """Emit output file to the found-file exchange for downstream processing.

        Publishes a :class:`File` to :data:`~courier.constants.FILE_FOUND_EXCHANGE`
        so job builders can pick it up and create new jobs — enabling chained
        dispatcher-to-builder pipeline workflows.

        Parameters
        ----------
        file : File
            The output file to feed back into the pipeline.
        """
        self._logger.debug(f"Emitting file: {file}")
        self.parent_service.emit(queue=FILE_FOUND_EXCHANGE, message=str(file))

    def _recently_seen(self, job_identifier: str) -> bool:
        """Return True if *job_identifier* is in the bounded LRU.

        On miss, records the identifier; evicts oldest when the LRU is
        full.  Catches same-replica duplicates from at-least-once
        delivery; cross-replica exactly-once requires the optional
        state-sync dedupe.
        """
        if job_identifier in self._seen_jobs:
            self._seen_jobs.move_to_end(job_identifier)
            return True
        self._seen_jobs[job_identifier] = None
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

        Best-effort: memory transport always reports 0 (``queue.qsize()``
        is not meaningful for in-memory Kombu channels).  Wire transports
        query the underlying broker queue depth.

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

    def handle_incoming_jobs(self) -> None:
        """Execute given a steady stream of jobs, log and execute them."""
        tracer = get_tracer(__name__)
        for job_string, parent_ctx in self.parent_service.consume(
            self.incoming_queue,
            stop_event=self._stop_event,
            on_subscribed=self._subscribed.set,
        ):
            with contextlib.suppress(Exception):
                self._emit_queue_depth()
            job = Job.from_string(str(job_string))
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
                self._jobs_consumed.labels(
                    dispatcher_identifier=self.identifier,
                ).inc()
                if job.emit_time is not None:
                    self._dispatch_latency.labels(
                        dispatcher_identifier=self.identifier,
                    ).observe(time.time() - job.emit_time)

                if self._recently_seen(job.identifier):
                    self._dedupe_skips.labels(
                        dispatcher_identifier=self.identifier,
                    ).inc()
                    self._logger.info(
                        f"Duplicate job {job.identifier}; skipping",
                        extra={"correlation_id": job.correlation_id},
                    )
                    continue

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
                    for ex_log in execution_logs:
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

                    self._jobs_processed.labels(
                        status="success",
                        **self._metric_labels,
                    ).inc()

                except CourierError as exc:
                    self._logger.exception(
                        f"Error processing job {job_id}",
                        extra={"correlation_id": job.correlation_id},
                    )
                    span = get_current_span()
                    span.set_status(Status(StatusCode.ERROR))
                    span.record_exception(exc)
                    self._jobs_processed.labels(
                        status="failure",
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
        self._logger.debug("Dispatcher %s consume loop exited", self.name)

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

"""Python class for the dispatchers courier interface."""

from __future__ import annotations

import contextlib
import threading
import time
import types
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, ClassVar

from opentelemetry.trace import Status, StatusCode, get_current_span
from pluginify.interfaces.base import BaseClassInterface

from courier.constants import (
    DISPATCHER_QUEUE,
    FILE_FOUND_EXCHANGE,
    PluginRunState,
    job_ready_queue_for,
)
from courier.errors import CourierError
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
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job
from courier.utils.decorators import log_execution
from courier.utils.logging import get_logger

_DEDUPE_LRU_SIZE = 1024

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.file import File


class Dispatcher(ServicePlugin):
    """Base dispatcher plugin."""

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "dispatcher"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        # pluginify registration path: instantiated with only a module (or nothing).
        if service is None or isinstance(service, types.ModuleType):
            return
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
        self.config = config or {}

        self._jobs_processed = DISPATCHER_JOBS_PROCESSED
        self._job_execution_duration = DISPATCHER_JOB_EXECUTION_DURATION
        self._active_jobs = DISPATCHER_ACTIVE_JOBS
        self._execution_logs_emitted = DISPATCHER_EXECUTION_LOGS_EMITTED
        self._queue_wait_duration = DISPATCHER_QUEUE_WAIT_DURATION
        self._jobs_consumed = DISPATCHER_JOBS_CONSUMED
        self._dispatch_latency = DISPATCHER_DISPATCH_LATENCY_SECONDS
        self._dedupe_skips = DISPATCHER_DEDUPE_SKIPS
        self.active_job_timestamps = {}  # type: dict[str, float]
        # Bounded LRU of recently-seen job identifiers. Catches same-replica
        # duplicates; cross-replica strict dedupe is opt-in via state sync.
        # Thread-safe: only touched by handle_incoming_jobs thread.
        self._seen_jobs: OrderedDict[str, None] = OrderedDict()

    def call(self) -> None:
        """Plugins are driven by start()/stop(); call() is not used at runtime."""
        raise NotImplementedError("Dispatcher plugins are invoked via start().")

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Yield ExecutionLogs."""
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "dispatcher.execute_job",
            attributes={
                ATTR_JOB_ID: job.identifier,
                ATTR_CORRELATION_ID: job.correlation_id,
            },
        ):
            self._logger.debug(f"Yielding execution log for job: {job}")
            return [
                ExecutionLog(
                    return_code=None, stdout=None, stderr=None, hostname=None,
                ),
            ]

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

    def _emit_queue_depth(self) -> None:
        """Emit the per-dispatcher queue-depth gauge.

        Best-effort: memory transport always reports 0 (``queue.qsize()``
        is not meaningful for in-memory Kombu channels).  Wire transports
        query the underlying broker queue depth.

        """
        with contextlib.suppress(Exception):
            broker = self.parent_service._broker_manager
            if broker._connection and broker._connection.connected:
                with broker._connection.channel() as channel:
                    queue_name = self.parent_service._broker_manager.get_queue_name(
                        self.incoming_queue
                    )
                    _, message_count, _ = channel.queue_declare(
                        queue=queue_name, passive=True,
                    )
                    DISPATCHER_QUEUE_DEPTH.labels(
                        dispatcher_identifier=self.identifier,
                    ).set(message_count)
                    return
        DISPATCHER_QUEUE_DEPTH.labels(
            dispatcher_identifier=self.identifier,
        ).set(0)

    def _run_handle_incoming_jobs(self) -> None:
        """Wrapper that exits the process on any unhandled exception."""
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
                import os
                import traceback
                traceback.print_exc()
                span.set_status(Status(StatusCode.ERROR))
                self._logger.critical(
                    "Fatal error in dispatcher %s: exiting", self.name,
                )
                os._exit(1)

    def handle_incoming_jobs(self) -> None:
        """Execute given a steady stream of jobs, log and execute them."""
        tracer = get_tracer(__name__)
        while not self._stop_event.is_set():
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
                    self._active_jobs.labels(
                        dispatcher_name=self.name,
                        dispatcher_identifier=self.identifier,
                    ).inc()
                    self._queue_wait_duration.labels(
                        dispatcher_name=self.name,
                        dispatcher_identifier=self.identifier,
                    ).observe(start_time - job.last_modified)

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
                                    ATTR_EXECUTION_RETURN_CODE: str(ex_log.return_code)
                                    if ex_log.return_code is not None
                                    else "",
                                },
                            ):
                                self.emit(ex_log)
                            self._execution_logs_emitted.labels(
                                dispatcher_name=self.name,
                                dispatcher_identifier=self.identifier,
                            ).inc()

                        self._jobs_processed.labels(
                            status="success",
                            dispatcher_name=self.name,
                            dispatcher_identifier=self.identifier,
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
                            dispatcher_name=self.name,
                            dispatcher_identifier=self.identifier,
                        ).inc()

                    finally:
                        if job_id in self.active_job_timestamps:
                            execution_time = (
                                time.time() - self.active_job_timestamps[job_id]
                            )
                            self._job_execution_duration.labels(
                                dispatcher_name=self.name,
                                dispatcher_identifier=self.identifier,
                            ).observe(execution_time)
                            del self.active_job_timestamps[job_id]
                            self._active_jobs.labels(
                                dispatcher_name=self.name,
                                dispatcher_identifier=self.identifier,
                            ).dec()

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
        return

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


class DispatcherInterface(BaseClassInterface):
    """Interface for courier dispatcher plugins."""

    name: ClassVar[str] = "dispatchers"
    plugin_class: ClassVar[type] = Dispatcher
    required_args: ClassVar[dict[str, list[str]]] = {"standard": []}
    required_kwargs: ClassVar[dict[str, list[str]]] = {"standard": []}
    # ignoring odd capitalization to match Kubernetes apiVersion conventions
    apiVersion: ClassVar[str] = "runcourier.dev/v1alpha1"  # noqa: N815


dispatchers = DispatcherInterface()

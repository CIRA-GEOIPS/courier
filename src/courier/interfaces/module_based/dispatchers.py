"""Python class for the dispatchers courier interface."""

from __future__ import annotations

import threading
import time
import types
from typing import TYPE_CHECKING, Any, ClassVar

from pluginify.interfaces.base import BaseClassInterface

from courier.constants import DISPATCHER_QUEUE, JOB_READY_QUEUE, PluginRunState
from courier.errors import CourierError
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.metrics import (
    DISPATCHER_ACTIVE_JOBS,
    DISPATCHER_EXECUTION_LOGS_EMITTED,
    DISPATCHER_JOB_EXECUTION_DURATION,
    DISPATCHER_JOBS_PROCESSED,
    DISPATCHER_QUEUE_WAIT_DURATION,
    collect_labeled,
)
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job
from courier.utils.decorators import log_execution
from courier.utils.logging import get_logger

if TYPE_CHECKING:
    from courier.service import Service


class Dispatcher(ServicePlugin):
    """Base dispatcher plugin."""

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "dispatcher"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict | None = None,
    ) -> None:
        # pluginify registration path: instantiated with only a module (or nothing).
        if service is None or isinstance(service, types.ModuleType):
            return
        self.parent_service = service
        self._logger = get_logger("plugin", self.name, service.config)
        self.queue = DISPATCHER_QUEUE
        self._state = PluginRunState.STOPPED
        self._main_thread: threading.Thread | None = None
        self.config = config or {}

        self._jobs_processed = DISPATCHER_JOBS_PROCESSED
        self._job_execution_duration = DISPATCHER_JOB_EXECUTION_DURATION
        self._active_jobs = DISPATCHER_ACTIVE_JOBS
        self._execution_logs_emitted = DISPATCHER_EXECUTION_LOGS_EMITTED
        self._queue_wait_duration = DISPATCHER_QUEUE_WAIT_DURATION
        self.active_job_timestamps = {}  # type: dict[str, float]

    def call(self) -> None:
        """Plugins are driven by start()/stop(); call() is not used at runtime."""
        raise NotImplementedError("Dispatcher plugins are invoked via start().")

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Yield ExecutionLogs."""
        self._logger.debug(f"Yielding execution log for job: {job}")
        return [ExecutionLog(return_code=None, stdout=None, stderr=None, hostname=None)]

    def emit(self, execution_log: ExecutionLog) -> None:
        """Emit execution log to parent service."""
        self._logger.debug(f"Emitting execution log: {execution_log}")
        self.parent_service.emit(queue=self.queue, message=str(execution_log))

    def handle_incoming_jobs(self) -> None:
        """Execute given a steady stream of jobs, log and execute them."""
        while True:
            for job_string in self.parent_service.consume(JOB_READY_QUEUE):
                job = Job.from_string(str(job_string))
                self._logger.debug(f"Received Job: {job}")

                start_time = time.time()
                job_id = job.identifier
                self.active_job_timestamps[job_id] = start_time
                self._active_jobs.labels(dispatcher_name=self.name).inc()
                self._queue_wait_duration.labels(
                    dispatcher_name=self.name,
                ).observe(start_time - job.last_modified)

                try:
                    execution_logs = self.get_execution_log(job)
                    for ex_log in execution_logs:
                        self.emit(ex_log)
                        self._execution_logs_emitted.labels(
                            dispatcher_name=self.name,
                        ).inc()

                    self._jobs_processed.labels(
                        status="success",
                        dispatcher_name=self.name,
                    ).inc()

                except CourierError:
                    self._logger.exception(f"Error processing job {job_id}")
                    self._jobs_processed.labels(
                        status="failure",
                        dispatcher_name=self.name,
                    ).inc()

                finally:
                    if job_id in self.active_job_timestamps:
                        execution_time = (
                            time.time() - self.active_job_timestamps[job_id]
                        )
                        self._job_execution_duration.labels(
                            dispatcher_name=self.name,
                        ).observe(execution_time)
                        del self.active_job_timestamps[job_id]
                        self._active_jobs.labels(dispatcher_name=self.name).dec()

    @log_execution
    def start(self) -> None:
        """Start main thread."""
        if self._state == PluginRunState.RUNNING:
            return
        self._main_thread = threading.Thread(
            target=self.handle_incoming_jobs,
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
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=5)

    def is_healthy(self) -> bool:
        """Check if plugin is healthy."""
        return self._state == PluginRunState.RUNNING

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

"""Python class for the dispatchers lazylemon interface."""

import threading
import time
from typing import Any, ClassVar

from geoips.interfaces.base import BaseModuleInterface  # type: ignore[import-untyped]

from lazylemon.constants import JOB_READY_QUEUE
from lazylemon.interfaces.plugin_protocol import ServicePlugin
from lazylemon.metrics import (
    DISPATCHER_ACTIVE_JOBS,
    DISPATCHER_EXECUTION_LOGS_EMITTED,
    DISPATCHER_JOB_EXECUTION_DURATION,
    DISPATCHER_JOBS_PROCESSED,
    collect_labeled,
)
from lazylemon.service import Service
from lazylemon.types.execution_log import ExecutionLog
from lazylemon.types.job import Job
from lazylemon.utils.decorators import log_execution
from lazylemon.utils.logging import get_logger


class Dispatcher(ServicePlugin):
    """Base dispatcher plugin."""

    name = "dispatcher"

    def __init__(self, service: Service, config: dict) -> None:
        self.parent_service = service
        self._logger = get_logger("plugin", self.name, service._config)
        self.queue = "Dispatcher"
        self._running = False
        self.config = config

        self._jobs_processed = DISPATCHER_JOBS_PROCESSED
        self._job_execution_duration = DISPATCHER_JOB_EXECUTION_DURATION
        self._active_jobs = DISPATCHER_ACTIVE_JOBS
        self._execution_logs_emitted = DISPATCHER_EXECUTION_LOGS_EMITTED
        self.active_job_timestamps = {}  # type: dict[str, float]

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

                except Exception:
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
        if self._running:
            return
        self._main_thread = threading.Thread(
            target=self.handle_incoming_jobs,
            name=self.name,
            daemon=True,
        )
        self._running = True
        self._main_thread.start()
        return

    @log_execution
    def stop(self) -> None:
        """Stop main thread."""
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=5)

    def is_healthy(self) -> bool:
        """Check if plugin is healthy."""
        return self._running

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
        }


def call() -> None:
    """Raise error if called directly."""
    raise NotImplementedError("You cannot call this plugin directly.")


class DispatcherInterface(BaseModuleInterface):
    """Interface for creating GeoIPS formatted titles."""

    name: ClassVar[str] = "dispatchers"
    required_args: ClassVar[dict[str, list[str]]] = {"standard": []}
    required_kwargs: ClassVar[dict[str, list[str]]] = {"standard": []}
    # ignoring odd capitalization to match existing code style in GeoIPS
    # which itself is matching Kubernetes conventions
    apiVersion: ClassVar[str] = "lazylemon.dev/v1alpha1"  # noqa: N815


dispatchers = DispatcherInterface()

"""Python class for the data_monitors geoips_driver interface."""

import json
import threading
import time
from dataclasses import dataclass
from typing import Any, ClassVar

from geoips.interfaces.base import BaseModuleInterface  # type: ignore[import-untyped]
from prometheus_client import Counter, Gauge, Histogram

from geoips_driver.interfaces.module_based.job_builders import JOB_READY_QUEUE, Job
from geoips_driver.interfaces.module_based.service import (
    Service,
    ServicePlugin,
    log_execution,
)
from geoips_driver.utils.logging import get_logger


@dataclass(frozen=True)
class ExecutionLog:
    """Execution log DataClass."""

    return_code: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    hostname: str | None = None

    def __str__(self) -> str:
        """Convert ExecutionLog to JSON string."""
        return json.dumps(
            {
                "return_code": self.return_code,
                "stdout": self.stdout,
                "stderr": self.stderr,
                "hostname": self.hostname,
            },
        )

    @classmethod
    def from_string(cls, s: str) -> "ExecutionLog":
        """Initialize ExecutionLog from JSON string."""
        return cls(**json.loads(s))


class Dispatcher(ServicePlugin):
    """Base dispatcher plugin."""

    name = "dispatcher"

    def __init__(self, service: Service, config: dict) -> None:
        self.parent_service = service
        self._logger = get_logger("plugin", self.name, service._config)
        self.queue = "Dispatcher"
        self._running = False
        self.config = config

        # Prometheus metrics
        self.jobs_processed = Counter(
            f"dispatcher_jobs_processed_total_{self.name}",
            f"Total number of jobs processed by {self.name} dispatcher",
            ["status", "dispatcher_name"],
        )
        self.job_execution_duration = Histogram(
            f"dispatcher_job_execution_duration_seconds_{self.name}",
            f"Job execution duration in seconds for {self.name} dispatcher",
            ["dispatcher_name"],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0),
        )
        self.active_jobs = Gauge(
            f"dispatcher_active_jobs_{self.name}",
            f"Number of currently active jobs for {self.name} dispatcher",
            ["dispatcher_name"],
        )
        self.execution_logs_emitted = Counter(
            f"dispatcher_execution_logs_emitted_total_{self.name}",
            f"Total number of execution logs emitted by {self.name} dispatcher",
            ["dispatcher_name"],
        )
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
        """Execute given a steady stream of jobs, log and execute them.

        Once a job is complete, yield the result of its execution to a downstream
        service.
        """
        while True:
            for job_string in self.parent_service.consume(JOB_READY_QUEUE):
                job = Job.from_string(str(job_string))
                self._logger.debug(f"Received Job: {job}")

                # Track job start time
                start_time = time.time()
                job_id = job.identifier
                self.active_job_timestamps[job_id] = start_time
                self.active_jobs.labels(dispatcher_name=self.name).inc()

                try:
                    execution_logs = self.get_execution_log(job)
                    for ex_log in execution_logs:
                        self.emit(ex_log)
                        self.execution_logs_emitted.labels(
                            dispatcher_name=self.name,
                        ).inc()

                    # Record successful processing
                    self.jobs_processed.labels(
                        status="success",
                        dispatcher_name=self.name,
                    ).inc()

                except Exception:
                    self._logger.exception(f"Error processing job {job_id}")
                    self.jobs_processed.labels(
                        status="failure",
                        dispatcher_name=self.name,
                    ).inc()

                finally:
                    # Track job execution duration
                    if job_id in self.active_job_timestamps:
                        execution_time = (
                            time.time() - self.active_job_timestamps[job_id]
                        )
                        self.job_execution_duration.labels(
                            dispatcher_name=self.name,
                        ).observe(execution_time)
                        del self.active_job_timestamps[job_id]
                        self.active_jobs.labels(dispatcher_name=self.name).dec()

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
        metrics_dict = {}

        # Collect jobs_processed metrics
        for sample in self.jobs_processed.collect():
            for s in sample.samples:
                if s.name == self.jobs_processed._name:
                    metrics_dict[f"{self.name}_jobs_processed"] = {
                        "value": s.value,
                        "labels": s.labels,
                    }

        # Collect job_execution_duration metrics (histogram)
        for sample in self.job_execution_duration.collect():
            for s in sample.samples:
                if s.name == self.job_execution_duration._name:
                    metrics_dict[f"{self.name}_job_execution_duration"] = {
                        "value": s.value,
                        "labels": s.labels,
                    }

        # Collect active_jobs metrics
        for sample in self.active_jobs.collect():
            for s in sample.samples:
                if s.name == self.active_jobs._name:
                    metrics_dict[f"{self.name}_active_jobs"] = {
                        "value": s.value,
                        "labels": s.labels,
                    }

        # Collect execution_logs_emitted metrics
        for sample in self.execution_logs_emitted.collect():
            for s in sample.samples:
                if s.name == self.execution_logs_emitted._name:
                    metrics_dict[f"{self.name}_execution_logs_emitted"] = {
                        "value": s.value,
                        "labels": s.labels,
                    }

        return metrics_dict


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
    apiVersion: ClassVar[str] = "geoips_driver/v1"  # noqa: N815


dispatchers = DispatcherInterface()

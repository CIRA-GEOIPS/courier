"""Python class for the job_builders lazylemon interface."""

import threading
import time
from typing import ClassVar

from geoips.interfaces.base import BaseModuleInterface  # type: ignore[import-untyped]
from prometheus_client import Counter, Gauge, Histogram

from lazylemon.constants import FILE_FOUND_QUEUE, JOB_READY_QUEUE
from lazylemon.interfaces.plugin_protocol import ServicePlugin
from lazylemon.service import Service
from lazylemon.types.file import FrozenFile
from lazylemon.types.job import Job, JobGroup
from lazylemon.utils.decorators import log_execution
from lazylemon.utils.logging import get_logger


class JobBuilder(ServicePlugin):  # , GeoIPSPlugin):
    """Base data filter plugin."""

    name = "JobBuilder"

    def __init__(self, service: Service, config: dict) -> None:
        self.parent_service = service
        self._logger = get_logger("plugin", self.name, service._config)
        self.queue = JOB_READY_QUEUE
        self._running = False
        self.job_groups: list[JobGroup] = []
        self.config = config

        # Prometheus metrics
        self.files_received = Counter(
            f"job_builder_files_received_total_{self.name}",
            f"Total number of files received by {self.name} job builder",
            ["job_builder_name"],
        )
        self.jobs_built = Counter(
            f"job_builder_jobs_built_total_{self.name}",
            f"Total number of jobs built by {self.name} job builder",
            ["status", "job_builder_name"],
        )
        self.active_job_groups = Gauge(
            f"job_builder_active_job_groups_{self.name}",
            f"Number of currently active job groups for {self.name} job builder",
            ["job_builder_name"],
        )
        self.jobs_discarded = Counter(
            f"job_builder_jobs_discarded_total_{self.name}",
            f"Total number of old jobs discarded by {self.name} job builder",
            ["job_builder_name"],
        )
        self.file_processing_duration = Histogram(
            f"job_builder_file_processing_duration_seconds_{self.name}",
            f"File processing duration in seconds for {self.name} job builder",
            ["job_builder_name"],
            buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0),
        )

    def emit(self, job: Job) -> None:
        """Emit job to parent service."""
        message = str(job)
        self._logger.info(f"Queueing job: {message}")
        self.parent_service.emit(queue=self.queue, message=message)

    def handle_incoming_files(self) -> None:
        """Listen to incoming files and mark job as ready when appropriate."""
        self._logger.debug("Starting to handle incoming files")
        for file_string in self.parent_service.consume(FILE_FOUND_QUEUE):
            start_time = time.time()

            self.files_received.labels(job_builder_name=self.name).inc()
            self._logger.debug(f"Received file {file_string} from file queue")

            file = FrozenFile.from_string(str(file_string))

            for job_group in self.job_groups:
                self._logger.debug(
                    f"Processing file {file} in job group {job_group.name}",
                )
                if job_group.add_file(file):
                    self._logger.debug(
                        f"File {file} added to job group {job_group.name}",
                    )
                    for ready_job in job_group.ready_jobs():
                        self._logger.info(
                            f"Job {ready_job.identifier} is ready; emitting",
                        )
                        self.emit(ready_job)
                        self.jobs_built.labels(
                            status="ready",
                            job_builder_name=self.name,
                        ).inc()

                jobs_to_delete = []
                for job_id, job in job_group.jobs.items():
                    if job.is_old():
                        self._logger.info(f"Discarding old job {job.identifier}")
                        self.jobs_discarded.labels(job_builder_name=self.name).inc()
                        self.jobs_built.labels(
                            status="old",
                            job_builder_name=self.name,
                        ).inc()
                        jobs_to_delete.append(job_id)

                for job_id in jobs_to_delete:
                    del job_group.jobs[job_id]

            processing_time = time.time() - start_time
            self.file_processing_duration.labels(job_builder_name=self.name).observe(
                processing_time,
            )

            self.active_job_groups.labels(job_builder_name=self.name).set(
                len(self.job_groups),
            )

        self._logger.error("Exiting handle_incoming_files loop unexpectedly")

    @log_execution
    def start(self) -> None:
        """Start main thread."""
        if self._running:
            return
        self._main_thread = threading.Thread(
            target=self.handle_incoming_files,
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


def call() -> None:
    """Raise error if called directly."""
    raise NotImplementedError("You cannot call this plugin directly.")


class JobBuilderInterface(BaseModuleInterface):
    """Interface for creating GeoIPS formatted titles."""

    name: ClassVar[str] = "job_builders"
    required_args: ClassVar[dict[str, list[str]]] = {"standard": []}
    required_kwargs: ClassVar[dict[str, list[str]]] = {"standard": []}
    # ignoring odd capitalization to match existing code style in GeoIPS
    # which itself is matching Kubernetes conventions
    apiVersion: ClassVar[str] = "lazylemon/v1"  # noqa: N815


job_builders = JobBuilderInterface()

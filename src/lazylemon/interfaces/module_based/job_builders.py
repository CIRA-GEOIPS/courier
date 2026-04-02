"""Python class for the job_builders lazylemon interface."""

import threading
import time
from typing import Any, ClassVar

from geoips.interfaces.base import BaseModuleInterface  # type: ignore[import-untyped]

from lazylemon.constants import FILE_FOUND_QUEUE, JOB_READY_QUEUE
from lazylemon.interfaces.plugin_protocol import ServicePlugin
from lazylemon.metrics import (
    JOB_BUILDER_ACTIVE_GROUPS,
    JOB_BUILDER_FILE_PROCESSING_DURATION,
    JOB_BUILDER_FILES_RECEIVED,
    JOB_BUILDER_JOBS_BUILT,
    JOB_BUILDER_JOBS_DISCARDED,
    collect_labeled,
)
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

        self._files_received = JOB_BUILDER_FILES_RECEIVED
        self._jobs_built = JOB_BUILDER_JOBS_BUILT
        self._active_job_groups = JOB_BUILDER_ACTIVE_GROUPS
        self._jobs_discarded = JOB_BUILDER_JOBS_DISCARDED
        self._file_processing_duration = JOB_BUILDER_FILE_PROCESSING_DURATION

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

            self._files_received.labels(job_builder_name=self.name).inc()
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
                        self._jobs_built.labels(
                            status="ready",
                            job_builder_name=self.name,
                        ).inc()

                jobs_to_delete = []
                for job_id, job in job_group.jobs.items():
                    if job.is_old():
                        self._logger.info(f"Discarding old job {job.identifier}")
                        self._jobs_discarded.labels(job_builder_name=self.name).inc()
                        self._jobs_built.labels(
                            status="old",
                            job_builder_name=self.name,
                        ).inc()
                        jobs_to_delete.append(job_id)

                for job_id in jobs_to_delete:
                    del job_group.jobs[job_id]

            processing_time = time.time() - start_time
            self._file_processing_duration.labels(
                job_builder_name=self.name,
            ).observe(processing_time)

            self._active_job_groups.labels(job_builder_name=self.name).set(
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

    def is_healthy(self) -> bool:
        """Check if plugin is healthy."""
        return self._running

    def get_metrics(self) -> dict[str, Any]:
        """Return plugin-specific metrics."""
        return {
            **collect_labeled(
                JOB_BUILDER_FILES_RECEIVED,
                "job_builder_name",
                self.name,
            ),
            **collect_labeled(JOB_BUILDER_JOBS_BUILT, "job_builder_name", self.name),
            **collect_labeled(
                JOB_BUILDER_ACTIVE_GROUPS,
                "job_builder_name",
                self.name,
            ),
            **collect_labeled(
                JOB_BUILDER_JOBS_DISCARDED,
                "job_builder_name",
                self.name,
            ),
            **collect_labeled(
                JOB_BUILDER_FILE_PROCESSING_DURATION,
                "job_builder_name",
                self.name,
            ),
        }


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
    apiVersion: ClassVar[str] = "lazylemon.dev/v1alpha1"  # noqa: N815


job_builders = JobBuilderInterface()

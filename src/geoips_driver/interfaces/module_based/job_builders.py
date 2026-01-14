"""Python class for the data_filter geoips_driver interface."""

import json
import threading
import time
from typing import Any, ClassVar, Never

from geoips.interfaces.base import BaseModuleInterface  # type: ignore[import-untyped]
from prometheus_client import Counter, Gauge, Histogram

from geoips_driver.interfaces.module_based.data_monitors import (
    FILE_FOUND_QUEUE,
)
from geoips_driver.interfaces.module_based.service import (
    Service,
    ServicePlugin,
    log_execution,
    setup_logging,
)
from geoips_driver.types.file import File, FrozenFile

logger = setup_logging()

JOB_READY_QUEUE = "JobReadyQueue"


class Job:
    """Job class."""

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        identifier: str,
        config: Any,
        files: set[File | FrozenFile] | frozenset[Never] = frozenset(),
        last_modified: float | None = None,
        timeout: float = 60 * 60 * 24,
    ) -> None:
        self.name = name
        self.identifier = identifier
        self.config = config
        self.files = files
        self.last_modified = last_modified if last_modified is not None else time.time()
        self.timeout = timeout
        if self.files == frozenset():
            self.files = set()

    def __str__(self) -> str:
        """Convert Job to JSON string."""
        return json.dumps(
            {
                "name": self.name,
                "identifier": self.identifier,
                "config": self.config,
                "files": [str(f) for f in self.files],
                "last_modified": self.last_modified,
                "timeout": self.timeout,
            },
        )

    @classmethod
    def from_string(cls, s: str) -> "Job":
        """Initialize Job from JSON string.

        Args:
            s: JSON string representation of Job

        Returns
        -------
            Job instance
        """
        data = json.loads(s)
        return cls(
            name=data["name"],
            identifier=data["identifier"],
            config=data["config"],
            files={FrozenFile.from_string(f) for f in data.get("files", [])},
            last_modified=data.get("last_modified"),
            timeout=data.get("timeout", 60 * 60 * 24),
        )

    def ready(self) -> bool:
        """Return true if job is ready to be emitted."""
        return False

    def add_file(self, file: File | FrozenFile) -> None:
        """Add file to job."""
        # ignore type because self.files is initialized as frozenset by default
        # but..... is set in init anyways if empty
        self.files.add(file)  # type: ignore
        self.last_modified = time.time()

    def is_old(self) -> bool:
        """Return true if job is old and ready to be discarded."""
        return time.time() - self.last_modified > self.timeout


class JobGroup:
    """Job group class."""

    def __init__(self, job_name: str, config: Any) -> None:
        self.name = job_name
        self.config = config
        self.jobs: dict[str, Job] = {}
        self.job = Job

    def ready_jobs(self) -> list[Job]:
        """Return list of ready jobs."""
        return [self.jobs[jid] for jid in self.jobs if self.jobs[jid].ready()]

    def file_is_relevant(self, file: File | FrozenFile) -> bool:  # noqa: ARG002
        """Return true if file is relevant to this job group."""
        return False

    def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
        """Return job ID from file."""
        return [str(file.file)]

    def add_file(self, file: File | FrozenFile) -> bool:
        """Add file to appropriate job in job group.

        Return true if file was added to a job, false otherwise.
        """
        if not self.file_is_relevant(file):
            return False
        job_ids = self.get_job_ids_from_file(file)
        for job_id in job_ids:
            if job_id in self.jobs:
                self.jobs[job_id].add_file(file)
            else:
                self.jobs[job_id] = self.job(self.name, job_id, self.config)
                self.jobs[job_id].add_file(file)
        return True


class JobBuilder(ServicePlugin):  # , GeoIPSPlugin):
    """Base data filter plugin."""

    name = "JobBuilder"

    def __init__(self, service: Service, config: dict) -> None:
        self.parent_service = service
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
        logger.info(f"Queueing job: {message}")
        self.parent_service.emit(queue=self.queue, message=message)

    def handle_incoming_files(self) -> None:
        """Listen to incoming files and mark job as ready when appropriate."""
        logger.debug("Starting to handle incoming files")
        for file_string in self.parent_service.consume(FILE_FOUND_QUEUE):
            start_time = time.time()

            # Record file received
            self.files_received.labels(job_builder_name=self.name).inc()
            logger.debug(f"Received file {file_string} from file queue")

            file = FrozenFile.from_string(str(file_string))

            for job_group in self.job_groups:
                logger.debug(f"Processing file {file} in job group {job_group.name}")

                if job_group.add_file(file):  # aka file added
                    logger.debug(f"File {file} added to job group {job_group.name}")

                    for ready_job in job_group.ready_jobs():
                        logger.info(f"Job {ready_job.identifier} is ready; emitting")
                        self.emit(ready_job)
                        self.jobs_built.labels(
                            status="ready",
                            job_builder_name=self.name,
                        ).inc()

                # Clean up old jobs
                jobs_to_delete = []
                for job_id, job in job_group.jobs.items():
                    if job.is_old():
                        logger.info(f"Discarding old job {job.identifier}")
                        self.jobs_discarded.labels(job_builder_name=self.name).inc()
                        self.jobs_built.labels(
                            status="old",
                            job_builder_name=self.name,
                        ).inc()
                        jobs_to_delete.append(job_id)

                # Delete old jobs after iteration
                for job_id in jobs_to_delete:
                    del job_group.jobs[job_id]

            # Record file processing duration
            processing_time = time.time() - start_time
            self.file_processing_duration.labels(job_builder_name=self.name).observe(
                processing_time,
            )

            # Update active job groups count
            self.active_job_groups.labels(job_builder_name=self.name).set(
                len(self.job_groups),
            )

        logger.error("Exiting handle_incoming_files loop unexpectedly")

    @log_execution
    def start(self) -> None:
        """Start main thread."""
        if self._running:
            return
        else:
            self._running = False
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
    apiVersion: ClassVar[str] = "geoips_driver/v1"  # noqa: N815


job_builders = JobBuilderInterface()

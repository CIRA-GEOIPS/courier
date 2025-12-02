"""Python class for the data_filter geoips_driver interface."""

import json
import threading
import time
from typing import Any, Never

from geoips_driver.interfaces.module_based.data_monitors import FILE_FOUND_QUEUE, File
from geoips_driver.interfaces.module_based.service import (
    Service,
    ServicePlugin,
    log_execution,
    setup_logging,
)

logger = setup_logging()

JOB_READY_QUEUE = "JobReadyQueue"


class Job:
    """Job class."""

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        identifier: str,
        config: Any,
        files: set[File] | frozenset[Never] = frozenset(),
        last_modified: float | None = None,
        timeout: float = 60 * 60 * 24,
    ) -> None:
        self.name = name
        self.identifier = identifier
        self.config = config
        self.files = files
        self.last_modified = last_modified if last_modified is not None else time.time()
        self.timeout = timeout
        if self.files == {}:
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
            files={File.from_string(f) for f in data.get("files", [])},
            last_modified=data.get("last_modified"),
            timeout=data.get("timeout", 60 * 60 * 24),
        )

    def ready(self) -> bool:
        """Return true if job is ready to be emitted."""
        return False

    def add_file(self, file: File) -> None:
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

    def file_is_relevant(self, file: File) -> bool:  # noqa: ARG002
        """Return true if file is relevant to this job group."""
        return False

    def get_job_id_from_file(self, file: File) -> str:
        """Return job ID from file."""
        return str(file.file)

    def add_file(self, file: File) -> bool:
        """Add file to appropriate job in job group.

        Return true if file was added to a job, false otherwise.
        """
        if not self.file_is_relevant(file):
            return False
        jid = self.get_job_id_from_file(file)
        if jid in self.jobs:
            self.jobs[jid].add_file(file)
        else:
            self.jobs[jid] = self.job(self.name, jid, self.config)
            self.jobs[jid].add_file(file)
        return True


class JobBuilder(ServicePlugin):  # , GeoIPSPlugin):
    """Base data filter plugin."""

    def __init__(self, service: Service, config: dict) -> None:
        self.parent_service = service
        self.queue = JOB_READY_QUEUE
        self._running = False
        self.job_groups: list[JobGroup] = []
        self.config = config

    name = "JobBuilder"

    def emit(self, job: Job) -> None:
        """Emit job to parent service."""
        message = str(job)
        logger.info(f"Queueing job: {message}")
        self.parent_service.emit(queue=self.queue, message=message)

    def handle_incoming_files(self) -> None:
        """Listen to incoming files and mark job as ready when appropriate."""
        logger.debug("Starting to handle incoming files")
        for file_string in self.parent_service.consume(FILE_FOUND_QUEUE):
            logger.debug(f"Received file {file_string} from file queue")
            file = File.from_string(str(file_string))
            for job_group in self.job_groups:
                logger.debug(f"Processing file {file} in job group {job_group.name}")
                if job_group.add_file(file):  # aka file added
                    logger.debug(f"File {file} added to job group {job_group.name}")
                    for ready_job in job_group.ready_jobs():
                        logger.info(f"Job {ready_job.identifier} is ready; emitting")
                        self.emit(ready_job)
                for job in job_group.jobs.values():  # Clean up old jobs
                    if job.is_old():
                        logger.info(f"Discarding old job {job.identifier}")
                        del job
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

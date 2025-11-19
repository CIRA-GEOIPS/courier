"""Python class for the data_filter geoips_driver interface."""

import threading
import time
from typing import Any

from geoips_driver.interfaces.module_based.data_monitors import FILE_FOUND_QUEUE, File
from geoips_driver.interfaces.module_based.service import (
    Service,
    ServicePlugin,
    log_execution,
    setup_logging,
)

logger = setup_logging()


class Job:
    """Job class."""

    def __init__(self, job_name: str, jid: str, config: Any) -> None:
        self.name = job_name
        self.identifier = jid
        self.config = config
        self.files: set[File] = set()
        self.last_modified = time.time()
        self.timeout = 60 * 60 * 24  # 24 hours

    def ready(self) -> bool:
        """Return true if job is ready to be emitted."""
        return False

    def add_file(self, file: File) -> None:
        """Add file to job."""
        self.files.add(file)
        self.last_modified = time.time()

    def is_old(self) -> bool:
        """Return true if job is old and ready to be discarded."""
        return time.time() - self.timeout < self.last_modified


class JobGroup:
    """Job group class."""

    def __init__(self, job_name: str, config: Any) -> None:
        self.name = job_name
        self.config = config
        self.jobs: dict[str, Job] = {}

    def ready_jobs(self) -> list[Job]:
        """Return list of ready jobs."""
        return [self.jobs[jid] for jid in self.jobs if self.jobs[jid].ready()]

    def file_is_relevant(self, file: File) -> bool:
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
            self.jobs[jid] = Job(self.name, jid, self.config)
        return True


class JobReady(ServicePlugin):  # , GeoIPSPlugin):
    """Base data filter plugin."""

    def __init__(self, service: Service) -> None:
        self.parent_service = service
        self.queue = "JobReady"
        self._running = False
        self.job_groups: list[JobGroup] = []

    @property
    def name(self) -> str:
        """Service name."""
        return "JobBuilder"

    def emit(self, job: Job) -> None:
        """Emit job to parent service."""
        message = str(job)
        logger.info(f"Queueing job with message {message}")
        self.parent_service.emit(queue=self.queue, message=message)

    def handle_incoming_files(self) -> None:
        """Listen to incoming files and mark job as ready when appropriate."""
        logger.debug("Starting to handle incoming files")
        for file in self.parent_service.consume(FILE_FOUND_QUEUE):
            logger.debug(f"Received file {file} from file queue")
            for job_group in self.job_groups:
                if job_group.add_file(file):  # aka file added
                    for ready_job in job_group.ready_jobs():
                        self.emit(ready_job)
                for job in job_group.jobs.values():  # Clean up old jobs
                    if job.is_old():
                        del job

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

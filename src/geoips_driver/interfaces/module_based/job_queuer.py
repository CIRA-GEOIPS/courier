"""Python class for the data_filter geoips_driver interface."""

import threading
import time

#import geoips.interfaces.base as GeoIPSPlugin # TODO: actually.... import the class lol
from geoips_driver.interfaces.module_based.data_monitors import FILE_FOUND_QUEUE, File
from geoips_driver.interfaces.module_based.service import (
    ServicePlugin,
    log_execution,
    setup_logging,
)

logger = setup_logging()

class JobGroup:
    def __init__(self, job_name, config) -> None:
        self.name = job_name
        self.config = config
        self.jobs = {}

    def ready_jobs(self):
        return [job for job in self.jobs if job.ready()]

    def file_is_relevant(file):
        return False

    def get_job_id_from_file(self, file: File):
        return file.name

    def add_file(self, file):
        if not self.file_is_relevant(file):
            return False
        jid = self.get_job_id_from_file(file)
        if jid in self.jobs:
            self.jobs[jid].add_file(file)
        else:
            self.jobs[jid] = Job(self.name, jid, self.config)
        return True


class Job:
    def __init__(self, job_name, jid, config):
        self.name = job_name
        self.identifier = jid
        self.config = config
        self.files = set()
        self.last_modified = time.time()
        self.timeout = 60 * 60 * 24 # 24 hours

    def ready():
        return False

    def add_file(self, file):
        self.files.append(file)
        self.last_modified = time.time()

    def is_old(self):
        return time.time() - self.timeout < self.last_modified


class JobReady(ServicePlugin):#, GeoIPSPlugin):
    """Base data filter plugin."""

    def __init__(self, service):
        self.parent_service = service
        self.queue = "JobReady"
        self._running=False
        self.job_groups = None

    def emit(self, job) -> None:
        message = str(job)
        logger.info(f"Queueing job with message {message}")
        self.parent_service.emit(queue=self.queue, message=message)

    def handle_incoming_files(self) -> None:
        """Listen to incoming files and mark job as ready when appropriate."""
        logger.debug("Starting to handle incoming files")
        for file in self.parent_service.consume(FILE_FOUND_QUEUE):
            logger.debug(f"Received file {file} from file queue")
            for job_group in enumerate(self.job_groups):
                if job_group.add_file(file): # aka file added
                    for ready_job in job_group.ready_jobs():
                        self.emit(ready_job)
                elif job_group.is_old():
                    self.job_groups.remove(job_group)


    @log_execution
    def start(self) -> None:
        if self._running:
            return
        else:
            self._running = False
        self._main_thread = threading.Thread(target=self.handle_incoming_files,
                                             name=self.name, daemon=True)
        self._running = True
        self._main_thread.start()
        return

    @log_execution
    def stop(self) -> None:
        """Stop main thread."""
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=5)

def call():
    raise NotImplementedError("You cannot call this plugin directly.")

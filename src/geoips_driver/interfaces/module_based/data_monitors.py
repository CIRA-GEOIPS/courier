"""Python class for the data_monitors geoips_driver interface."""

import threading
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

# import geoips.interfaces.base as GeoIPSPlugin # TODO Fix this to .... be runnable..
from geoips_driver.interfaces.module_based.service import (
    ServicePlugin,
    log_execution,
    setup_logging,
)

logger = setup_logging()

FILE_FOUND_QUEUE = "FilesFoundQueue"


@dataclass
class File:
    """File dataclass"""

    frozen = True
    file: Path
    hostname: str


class DataMonitor(ServicePlugin):  # , GeoIPSPlugin):
    """Base data monitor plugin."""

    def __init__(self, service):
        self.parent_service = service
        self.queue = FILE_FOUND_QUEUE
        self.message_template = "file: '{file}', hostname: '{hostname}'"
        self._running = False

    def find_file(self) -> Generator[File, None, None]:
        """Generator that yields Files"""
        yield File(file=None, hostname=None)

    def emit(self, file: File) -> None:
        message = self.message_template.format(file=file.file, hostname=file.hostname)
        self.parent_service.emit(queue=self.queue, message=message)

    def find_and_emit_files(self) -> None:
        """Find file and put in file queue"""
        for file in self.find_file():
            logger.info(f"Found file: {file}")
            self.emit(file)

    @log_execution
    def start(self) -> None:
        if self._running:
            return
        else:
            self._running = False
        self._main_thread = threading.Thread(
            target=self.find_and_emit_files,
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


def call():
    raise NotImplementedError("You cannot call this plugin directly.")

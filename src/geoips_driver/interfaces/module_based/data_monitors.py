"""Python class for the data_monitors geoips_driver interface."""

import os
import time
import signal
import logging
import re
import uuid
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from geoips_driver.interfaces.module_based.service import setup_logging

from typing import Generator
from geoips_driver.interfaces.module_based.service import Plugin, log_execution
import threading

from geoips_driver.interfaces.module_based.service import setup_logging

logger = setup_logging()

@dataclass
class File:
    """File dataclass"""
    frozen=True
    file: Path
    hostname: str


class DataMonitor(Plugin):
    """Base data monitor plugin."""

    def __init__(self, service):
        self.parent_service = service
        self.queue = "DataMonitor"
        self.message_template = "file: '{file}', hostname: '{hostname}'"
        self._running=False

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
        self._main_thread = threading.Thread(target=self.find_and_emit_files, name=self.name, daemon=True)
        self._running = True
        self._main_thread.start()
        return

    @log_execution
    def stop(self) -> None:
        """Stop main thread."""
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=5)

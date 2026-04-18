"""File System Polling Data Monitor Plugin for courier."""

from __future__ import annotations

import queue
import types
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from prometheus_client import Gauge
from watchdog.events import DirCreatedEvent, FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from courier.interfaces.module_based.data_monitors import DataMonitorBasePlugin
from courier.types.file import File

if TYPE_CHECKING:
    from collections.abc import Generator

    from courier.service import Service


class FileSystemPoller(DataMonitorBasePlugin):
    """File System Polling Data Monitor Plugin."""

    interface: ClassVar[str] = "data_monitors"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "file_system_poller_watchdog"
    version: ClassVar[str] = "0.0.0"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict | None = None,
    ) -> None:
        super().__init__(service, config)
        if service is None or isinstance(service, types.ModuleType):
            return
        self.health = False
        self.path_to_watch = (config or {})["path"]
        # Gauge that stores the Unix timestamp of last processing
        # more as a demonstration than for actual use here
        self.last_file_processed_timestamp = Gauge(
            f"last_file_emitted_timestamp_seconds_{self.name}",
            "Unix timestamp when the last file was processed",
        )

    def is_healthy(self) -> bool:
        """Check if the data monitor is healthy."""
        return self.health

    def find_file(self) -> Generator[File, None, None]:
        """
        Watches a directory for new files and yields their paths.

        This is a generator function that uses the `watchdog` library to monitor
        a directory. It yields the absolute path of a new file as soon as it's
        created.

        Uses a simple queue to communicate between the watchdog thread and main thread.
        The event handler is a simple, nested class.
        The observer is started and stopped cleanly within the generator's life cycle.

        Args:
            path (str): The directory path to watch. Defaults to the current directory.

        Yields
        ------
            str: The path of a newly created file.
        """
        path = self.path_to_watch
        self._logger.info(f"Starting to watch directory: {path}")
        file_queue: queue.Queue[str | bytes] = queue.Queue()

        # This handler simply puts the path of any new file into the queue
        class NewFileHandler(FileSystemEventHandler):
            def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
                if not event.is_directory:
                    file_queue.put(event.src_path)

        # Set up the observer
        event_handler = NewFileHandler()
        observer = Observer()
        observer.schedule(event_handler, path, recursive=True)
        try:
            observer.start()
        except FileNotFoundError as e:
            raise RuntimeError(f"Directory '{path}' does not exist.") from e

        self._logger.info(f"Watching for new files in '{path}'...")

        try:
            self.health = True
            while True:
                yield File(file=Path(str(file_queue.get())), hostname="localhost")
                self.last_file_processed_timestamp.set_to_current_time()
        finally:
            observer.stop()
            observer.join()


PLUGIN_CLASS = FileSystemPoller

"""File System Polling Data Monitor Plugin for GeoIPS Driver."""

import queue
from collections.abc import Generator
from pathlib import Path

from watchdog.events import DirCreatedEvent, FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from geoips_driver.interfaces.module_based.data_monitors import (
    DataMonitorBasePlugin,
)
from geoips_driver.interfaces.module_based.service import Service, setup_logging
from geoips_driver.types.file import File

logger = setup_logging("FSPolling")


interface: str = "data_monitors"
family: str = "standard"
name: str = "FileSystemPoller-Watchdog"


class FileSystemPoller(DataMonitorBasePlugin):
    """File System Polling Data Monitor Plugin."""

    name = "FileSystemPoller-Watchdog"
    version = "0.0.0"

    def __init__(self, service: Service, config: dict) -> None:
        super().__init__(service, config)
        self.health = False
        self.path_to_watch = config["path"]

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
        logger.info(f"Starting to watch directory: {path}")
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
            raise RuntimeError(f"Directory '{path}' does not exist.") from e  # noqa: TRY003

        logger.info(f"Watching for new files in '{path}'...")

        try:
            self.health = True
            while True:
                yield File(file=Path(str(file_queue.get())), hostname="localhost")
        finally:
            observer.stop()
            observer.join()


def call() -> None:
    """Raise error if called directly."""
    raise NotImplementedError("You cannot call this plugin directly.")

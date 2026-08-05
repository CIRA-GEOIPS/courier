"""File System Polling Data Monitor Plugin for courier."""

from __future__ import annotations

import queue
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from pydantic import BaseModel, Field
from watchdog.events import DirCreatedEvent, FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from courier.interfaces.data_monitors import DataMonitorBasePlugin
from courier.metrics import DATA_MONITOR_LAST_PROCESSED_TIMESTAMP
from courier.types.file import File

if TYPE_CHECKING:
    from collections.abc import Generator

    from courier.service import Service


class FileSystemPollerConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`FileSystemPoller`."""

    path: str = Field(
        ...,
        description="Directory path to watch for new files",
    )
    hostname: str = Field(
        default="localhost",
        description="Hostname to attach to emitted files",
    )


class FileSystemPoller(DataMonitorBasePlugin):
    """File System Polling Data Monitor Plugin."""

    interface: ClassVar[str] = "data_monitors"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "file_system_poller_watchdog"
    version: ClassVar[str] = "0.0.0"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self.validated = FileSystemPollerConfig.model_validate(config or {})
        self.health = False
        # expanduser() so "~/data" in a config behaves the way operators expect
        # (and the way the quick-start guide documents it).
        self.path_to_watch = str(Path(self.validated.path).expanduser())
        # Use the shared labelled metric rather than constructing a Gauge per
        # instance: a per-instance Gauge with the name baked in raises
        # "Duplicated timeseries in CollectorRegistry" as soon as a config
        # declares two file watchers.
        self.last_file_processed_timestamp = (
            DATA_MONITOR_LAST_PROCESSED_TIMESTAMP.labels(
                plugin_name=self.name,
                monitor_identifier=self.identifier,
            )
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

        # Set up the observer. schedule() -- not start() -- is what raises on a
        # missing directory, so both calls have to be inside the try.
        event_handler = NewFileHandler()
        observer = Observer()
        try:
            observer.schedule(event_handler, path, recursive=True)
            observer.start()
        except OSError as e:
            raise RuntimeError(
                f"Cannot watch directory '{path}': {e}",
            ) from e

        self._logger.info(f"Watching for new files in '{path}'...")

        try:
            self.health = True
            while not self._stop_event.is_set():
                # Bounded get() so shutdown is observed on an idle directory;
                # a bare blocking get() never returns and wedges the thread.
                try:
                    raw_path = file_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                yield File(
                    file=Path(str(raw_path)),
                    hostname=self.validated.hostname,
                )
                self.last_file_processed_timestamp.set_to_current_time()
        finally:
            self.health = False
            observer.stop()
            observer.join()

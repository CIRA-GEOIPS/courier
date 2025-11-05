import queue
from collections.abc import Generator

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from geoips_driver.interfaces.module_based.data_monitors import DataMonitor, File
from geoips_driver.interfaces.module_based.service import setup_logging

logger = setup_logging()

class FileSystemPoller(DataMonitor):
    name = "FileSystemPoller-Watchdog"
    version = "-1"

    def __init__(self, service):
        super().__init__(service)
    def initialize(self, config):
        pass
    def is_healthy(self):
        return True
    def find_file(self) -> Generator[File, None, None]:
        """
        Watches a directory for new files and yields their paths.

        This is a generator function that uses the `watchdog` library to monitor
        a directory. It yields the absolute path of a new file as soon as it's
        created.

        Uses a simple queue to communicate between the watchdog thread and the main thread.
        The event handler is a simple, nested class.
        The observer is started and stopped cleanly within the generator's life cycle.

        Args:
            path (str): The directory path to watch. Defaults to the current directory.

        Yields
        ------
            str: The path of a newly created file.
        """
        path = "/workspaces/geoips-driver/fake_files"
        file_queue = queue.Queue()

        # This handler simply puts the path of any new file into the queue
        class NewFileHandler(FileSystemEventHandler):
            def on_created(self, event):
                if not event.is_directory:
                    file_queue.put(event.src_path)

        # Set up the observer
        event_handler = NewFileHandler()
        observer = Observer()
        observer.schedule(event_handler, path, recursive=True)
        observer.start()

        logger.info(f"Watching for new files in '{path}'...")

        try:
            while True:
                yield File(file=file_queue.get(), hostname="localhost")
        finally:
            observer.stop()
            observer.join()

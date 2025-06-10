"""Generic file system watching module."""

from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files
import os
from pathlib import Path
import time

import sqlite3 as sql
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler


interface = "data_monitors"
name = "event_handler"
family = "standard"


class EventListener(FileSystemEventHandler):

    def __init__(self, monitor_config, db, end_time=None):
        self.monitor_config = monitor_config
        self.db = db
        self.end_time = end_time
        super().__init__()

    def on_created(self, event):
        """If a file in 'watchdir' was created, call this function.

        If the created source path was an actual file, not a directory, then add this
        file to a database and report that an added entry occurred.

        Parameters
        ----------
        event: FileSystemEvent
            - An event caught on the file system being watched.
        """
        file_path = event.src_path
        bname = os.path.basename(file_path)
        pl_path = Path(file_path)
        # NOTE: This will only work for GOES clavrx files right now. Need to add new
        # functionality which checks for the 'correct' file depending on algorithm,
        # satellite, sensor, and sector.
        print(f"EVENT TYPE = {event.event_type}")
        print(f"EVENT PATH = {file_path}")


def start_monitor(monitor_config, end_time=None, **kwargs):
    """Activate a data_monitor directed using the provided monitor_config plugin.

    Parameters
    ----------
    monitor_config: List[MonitorConfigPlugin]
        - List of yaml monitor_config plugins used to direct the file_system
          data_monitor.
    start_time: datetime.datetime, default=None
        - The datetime to begin searching at
    end_time: datetime.datetime, default=None
        - The datetime to stop searching at
    kwargs: unpacked dict
        - An unpacked dictionary of additional keyword arguments. Currently not used.
    """
    event_handler = EventListener()
    observer = PollingObserver()
    # observer.schedule(event_handler, path, recursive=True)
    # observer.start()
    # print(f"Started monitoring {path}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


def run_monitors(monitor_configs, end_time=None, **kwargs):
    """Run 1+ event_handler data monitors for the monitor configs provided.

    Parameters
    ----------
    monitor_configs: List[MonitorConfigPlugin]
        - List of yaml monitor_config plugins used to direct the file_system
          data_monitor.
    end_time: datetime.datetime, default=None
        - The datetime to end the data monitor at if present. Otherwise, this will
          continue until the user cancels this process or some failure occurs.
    kwargs: unpacked dict
        - An unpacked dictionary of additional keyword arguments. Currently not used.
    """
    with ThreadPoolExecutor(max_workers=len(monitor_configs)) as executor:
        for monitor_config in monitor_configs:
            executor.submit(start_monitor, monitor_config, end_time, **kwargs)


def call(monitor_configs, end_time=None, **kwargs):
    """Activate an event handler data_monitor using the specified monitor_configs.

    This will activate a data_monitor plugin that will handle file system events until
    canceled by the user or the process fails for another reason. When a file
    that matches the monitor_configs specifications is created, it will be added to a
    central database that a driver plugin will query. In the case that all required
    files are present in the database, the dispatcher will kick off processing.

    Parameters
    ----------
    monitor_configs: List[MonitorConfigPlugin]
        - List of yaml monitor_config plugins used to direct the file_system
          data_monitor.
    end_time: datetime.datetime, default=None
        - The datetime to end the data monitor at if present. Otherwise, this will
          continue until the user cancels this process or some failure occurs.
    kwargs: unpacked dict
        - An unpacked dictionary of additional keyword arguments. Currently not used.
    """
    db_path = str(files("geoips_driver") / "databases/stitched.db")
    db = sql.connect(db_path, check_same_thread=False)
    # enable Write-Ahead Logging; makes sure only one write can occur at a time
    # while enabling multi-read access
    db.execute("PRAGMA journal_mode=WAL;")
    # Wait up to 5 seconds if DB is locked
    db.execute("PRAGMA busy_timeout = 5000;")

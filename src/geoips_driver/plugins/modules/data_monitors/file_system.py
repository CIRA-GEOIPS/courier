"""Generic file system watching module."""

import os
import sqlite3 as sql
from importlib.resources import files
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver  # NOQA

interface = "data_monitors"
name = "file_system"
family = "standard"


class EventListener(FileSystemEventHandler):

    def __init__(self, **arguments):
        self.args = arguments
        self.start_time = self.args.get("start_time")
        self.end_time = self.args.get("end_time")
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
        os.path.basename(file_path)
        Path(file_path)
        # NOTE: This will only work for GOES clavrx files right now. Need to add new
        # functionality which checks for the 'correct' file depending on algorithm,
        # satellite, sensor, and sector.
        print(f"EVENT TYPE = {event.event_type}")
        print(f"EVENT PATH = {file_path}")


def call(monitor_configs, start_time=None, end_time=None, **kwargs):
    """Activate a file system data_monitor using the specified monitor_config plugin.

    This will activate a data_monitor plugin that will handle system events until
    canceled by the user or the process fails for another reason. When a file
    that matches the monitor_configs specifications is created, it will be added to a
    central database that a driver plugin will query. In the case that all required
    files are present in the database, the dispatcher will kick off processing.

    Parameters
    ----------
    monitor_config: List[MonitorConfigPlugin]
        - List of yaml monitor_config plugins used to direct the file_system
          data_monitor.
    start_time: datetime.datetime, default=None
        - The datetime to begin searching at
    end_time:
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
    pass

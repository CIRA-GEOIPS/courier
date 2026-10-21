"""Monitor changes to the designated config file."""

import os
from pathlib import Path

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from courier.managers.base import ServiceManager

class ConfigMonitorEventHandler(FileSystemEventHandler):
    def __init__(self, config_monitor: ConfigMonitor):
        self._config_monitor: ConfigMonitor = config_monitor

    def on_any_event(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return

        if event.event_type not in {"modified", "created", "moved"}:
            return
    
        if Path(str(event.src_path)).resolve() == self._config_monitor._config_file.resolve():
            self._config_monitor._on_config_changed(event)

class ConfigMonitor:
    def __init__(self,
                 config_file,
                 parent_service):
        self._config_file = config_file
        self._parent_service = parent_service
        self._observer = Observer()
        self._observer.schedule(ConfigMonitorEventHandler(self), str(Path(self._config_file).parent), recursive=False)
    def _on_config_changed(self, event: FileSystemEvent):
        print("Event occurred: ", event)
    def start(self):
        self._observer.start()
    def stop(self):
        self._observer.stop()
        self._observer.join()

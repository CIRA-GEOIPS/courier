"""Monitor changes to the designated config file."""

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from courier.managers.base import ServiceManager

class ConfigMonitorEventHandler(FileSystemEventHandler):
    def on_event(self, event: FileSystemEvent) -> None:
        print(event)

class ConfigMonitor(ServiceManager):
    def __init__(self,
                 config_file):
        self._config_file = config_file
        self._observer = Observer()
        self._observer.schedule(ConfigMonitorEventHandler(), self._config_file)
    def start(self):
        self._observer.start()
    def stop(self):
        self._observer.stop()
        self._observer.join()
    def is_healthy(self) -> bool:
        return True

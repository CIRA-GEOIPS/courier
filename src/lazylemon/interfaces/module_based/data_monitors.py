"""Python class for the data_monitors lazylemon interface."""

import threading
from collections.abc import Generator
from typing import Any, ClassVar

from geoips.interfaces.base import BaseModuleInterface  # type: ignore[import-untyped]

from lazylemon.constants import FILE_FOUND_QUEUE
from lazylemon.interfaces.plugin_protocol import ServicePlugin
from lazylemon.metrics import DATA_MONITOR_FILES_PROCESSED, collect_labeled
from lazylemon.schema import DataMonitorConfig
from lazylemon.service import Service
from lazylemon.types.file import File
from lazylemon.utils.decorators import log_execution
from lazylemon.utils.logging import get_logger
from lazylemon.utils.metadata import apply_metadata_from_configs


class DataMonitorBasePlugin(ServicePlugin):
    """Base data monitor plugin."""

    def __init__(self, service: Service, config: dict) -> None:
        self.parent_service = service
        self._logger = get_logger("plugin", self.name, service._config)
        self.queue = FILE_FOUND_QUEUE
        self._running = False
        self.config = config
        # importing here to prevent circular import
        from lazylemon.interfaces import data_monitor_configs  # noqa: PLC0415

        self.metadata_matchers = [
            DataMonitorConfig(**data_monitor_configs.get_plugin(tool))
            for tool in config.get("metadata-tools", [])
        ]

        self._files_processed = DATA_MONITOR_FILES_PROCESSED

    def find_file(self) -> Generator[File, None, None]:
        """Yield File objects."""
        yield File(file=None, hostname=None)

    def emit(self, file: File) -> None:
        """Emit file to parent service."""
        self._logger.debug(f"Emitting file: {file}")
        self.parent_service.emit(queue=self.queue, message=str(file))

    def add_metadata_to_file(self, file: File) -> File:
        """Add metadata to file before emitting."""
        return apply_metadata_from_configs(
            file_obj=file,
            configs=self.metadata_matchers,
            require_match=False,
        )

    def find_and_emit_files(self) -> None:
        """Find file and put in file queue."""
        for incoming_file in self.find_file():
            try:
                file_with_metadata = self.add_metadata_to_file(incoming_file)
                self._logger.info(f"Found file: {file_with_metadata}")
                self.emit(file_with_metadata)
                self._files_processed.labels(
                    monitor_name=self.name,
                    status="success",
                ).inc()
            except Exception:
                self._files_processed.labels(
                    monitor_name=self.name,
                    status="failure",
                ).inc()
                self._logger.exception(f"Error processing file {incoming_file}")

    @log_execution
    def start(self) -> None:
        """Start main thread."""
        if self._running:
            return
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

    def is_healthy(self) -> bool:
        """Check if plugin is healthy."""
        return self._running

    def get_metrics(self) -> dict[str, Any]:
        """Return plugin-specific metrics."""
        return collect_labeled(
            DATA_MONITOR_FILES_PROCESSED,
            "monitor_name",
            self.name,
        )


def call() -> None:
    """Raise error if called directly."""
    raise NotImplementedError("You cannot call this plugin directly.")


class DataMonitorInterface(BaseModuleInterface):
    """Interface for creating GeoIPS formatted titles."""

    name: ClassVar[str] = "data_monitors"
    required_args: ClassVar[dict[str, list[str]]] = {"standard": []}
    required_kwargs: ClassVar[dict[str, list[str]]] = {"standard": []}
    # ignoring odd capitalization to match existing code style in GeoIPS
    # which itself is matching Kubernetes conventions
    apiVersion: ClassVar[str] = "lazylemon.dev/v1alpha1"  # noqa: N815


data_monitors = DataMonitorInterface()

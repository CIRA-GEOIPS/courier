"""Python class for the data_monitors geoips_driver interface."""

import threading
from collections.abc import Generator
from typing import Any, ClassVar

from geoips.interfaces.base import BaseModuleInterface  # type: ignore[import-untyped]
from prometheus_client import Counter

from geoips_driver.interfaces.module_based.logging import get_logger
from geoips_driver.interfaces.module_based.service import (
    Service,
    ServicePlugin,
    log_execution,
)
from geoips_driver.pydantic.data_monitor_configs import DataMonitorConfig
from geoips_driver.types.file import File
from geoips_driver.utils.metadata import apply_metadata_from_configs

FILE_FOUND_QUEUE = "FilesFoundQueue"


class DataMonitorBasePlugin(ServicePlugin):
    """Base data monitor plugin."""

    def __init__(self, service: Service, config: dict) -> None:
        self.parent_service = service
        self._logger = get_logger("plugin", self.name, service._config)
        self.queue = FILE_FOUND_QUEUE
        self._running = False
        self.config = config
        # importing here to prevent circular import
        from geoips_driver.interfaces import data_monitor_configs  # noqa: PLC0415

        self.metadata_matchers = [
            DataMonitorConfig(**data_monitor_configs.get_plugin(tool))
            for tool in config.get("metadata-tools", [])
        ]

        self.files_processed = Counter(
            f"files_processed_{self.name}",
            f"Total number of files processed by {self.name} data monitor",
            [
                "status",
            ],  # Labels for prometheus metric
        )

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
<<<<<<< HEAD
                self._logger.info(f"Found file: {file_with_metadata}")
=======
                logger.info(f"Found file: {file_with_metadata}")
>>>>>>> 99294a2 (Add example prometheus metrics)
                self.emit(file_with_metadata)
                self.files_processed.labels(status="success").inc()
            except Exception:
                self.files_processed.labels(status="failure").inc()
<<<<<<< HEAD
                self._logger.exception(f"Error processing file {incoming_file}")
=======
                logger.exception(f"Error processing file {incoming_file}")
>>>>>>> 99294a2 (Add example prometheus metrics)

    @log_execution
    def start(self) -> None:
        """Start main thread."""
        if self._running:
            return
        else:
            self._running = False
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
        # Extract metrics from the prometheus counters
        metrics_dict = {}
        # Get the counter value by collecting all samples
        for item in self.files_processed.collect():
            for sample in item.samples:
                if sample.name == self.files_processed._name:
                    metrics_dict[f"{self.name}_files_processed"] = {
                        "value": sample.value,
                        "labels": sample.labels,
                    }
        return metrics_dict


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
    apiVersion: ClassVar[str] = "geoips_driver/v1"  # noqa: N815


data_monitors = DataMonitorInterface()

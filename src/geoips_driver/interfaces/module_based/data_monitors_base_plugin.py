"""Python class for the data_monitors geoips_driver interface."""

import json
import threading
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from geoips.interfaces.base import BaseModuleInterface  # type: ignore[import-untyped]

from geoips_driver.interfaces.module_based.service import (
    Service,
    ServicePlugin,
    log_execution,
    setup_logging,
)

logger = setup_logging()

FILE_FOUND_QUEUE = "FilesFoundQueue"


@dataclass(frozen=True)
class File:
    """File dataclass."""

    file: Path | None = None
    hostname: str | None = None

    def __str__(self) -> str:
        """Convert File to JSON string."""
        return json.dumps(
            {
                "file": str(self.file) if self.file else None,
                "hostname": self.hostname,
            },
        )

    @classmethod
    def from_string(cls, s: str) -> "File":
        """Initialize File from JSON string.

        Args:
            s: JSON string representation of File

        Returns
        -------
            File instance
        """
        data = json.loads(s)
        return cls(
            file=Path(data["file"]) if data.get("file") else None,
            hostname=data.get("hostname"),
        )


class DataMonitorBasePlugin(ServicePlugin):
    """Base data monitor plugin."""

    def __init__(self, service: Service, config: dict) -> None:
        self.parent_service = service
        self.queue = FILE_FOUND_QUEUE
        self._running = False
        self.config = config

    def find_file(self) -> Generator[File, None, None]:
        """Yield File objects."""
        yield File(file=None, hostname=None)

    def emit(self, file: File) -> None:
        """Emit file to parent service."""
        logger.debug(f"Emitting file: {file}")
        self.parent_service.emit(queue=self.queue, message=str(file))

    def find_and_emit_files(self) -> None:
        """Find file and put in file queue."""
        for file in self.find_file():
            logger.info(f"Found file: {file}")
            self.emit(file)

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

"""Python class for the data_monitors courier interface."""

from __future__ import annotations

import threading
import time
import types
from typing import TYPE_CHECKING, Any, ClassVar

from opentelemetry.trace import Status, StatusCode, get_current_span

from courier.constants import FILE_FOUND_EXCHANGE, PluginRunState
from courier.errors import CourierError
from courier.interfaces.discovery import (
    ENTRY_POINT_PREFIX,
    ClassPluginRegistry,
)
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.metrics import (
    DATA_MONITOR_FILES_PROCESSED,
    DATA_MONITOR_LAST_PROCESSED_TIMESTAMP,
    collect_labeled,
)
from courier.tracing import (
    ATTR_FILE_HOSTNAME,
    ATTR_FILE_PATH,
    ATTR_FILE_SOURCE,
    ATTR_PLUGIN_FAMILY,
    ATTR_PLUGIN_NAME,
    ATTR_PLUGIN_VERSION,
    get_tracer,
)
from courier.types.file import File
from courier.utils.decorators import log_execution
from courier.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Generator

    from courier.service import Service


class DataMonitorBasePlugin(ServicePlugin):
    """Base data monitor plugin."""

    interface: ClassVar[str] = "data_monitors"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "data_monitor_base"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        # pluginify registration path: instantiated with only a module (or nothing).
        # Skip runtime setup; metadata collection reads class attributes directly.
        if service is None or isinstance(service, types.ModuleType):
            return
        self.parent_service = service
        self._logger = get_logger("plugin", self.name, service.config)
        self.identifier = identifier or self.name
        self.queue = FILE_FOUND_EXCHANGE
        self._state = PluginRunState.STOPPED
        self._main_thread: threading.Thread | None = None
        # Per-instance shutdown signal. Subclasses whose find_file() blocks
        # (queue reads, sleeps, network polls) MUST poll this so stop() can
        # end the generator; several already define their own -- setting it
        # here gives every monitor one by default.
        self._stop_event = threading.Event()
        self.config = config or {}
        # importing here to prevent circular import
        from courier.interfaces import data_monitor_configs  # noqa: PLC0415

        # Already validated DataMonitorConfig instances -- the registry
        # constructs them at import time.
        self.metadata_matchers = [
            data_monitor_configs.get_plugin(tool)
            for tool in self.config.get("metadata-tools", [])
        ]

        self._files_processed = DATA_MONITOR_FILES_PROCESSED

    def call(self) -> None:
        """Plugins are driven by start()/stop(); call() is not used at runtime."""
        raise NotImplementedError("Data monitor plugins are invoked via start().")

    def find_file(self) -> Generator[File, None, None]:
        """Yield File objects."""
        yield File(file=None, hostname=None)

    def emit(self, file: File) -> None:
        """Emit file to parent service."""
        self._logger.debug(f"Emitting file: {file}")
        self.parent_service.emit(queue=self.queue, message=str(file))

    def add_metadata_to_file(self, file: File) -> File:
        """Add metadata to file before emitting."""
        from courier.utils.metadata import apply_metadata_from_configs  # noqa: PLC0415

        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "data_monitor.add_metadata",
            attributes={"courier.num_matchers": len(self.metadata_matchers)},
        ):
            return apply_metadata_from_configs(
                file_obj=file,
                configs=self.metadata_matchers,
                require_match=False,
            )

    def _run_find_and_emit_files(self) -> None:
        """Wrapper that exits the process on any unhandled exception."""
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "data_monitor.handle_incoming_files",
            attributes={
                ATTR_PLUGIN_NAME: self.name,
                ATTR_PLUGIN_VERSION: self.version,
            },
        ) as span:
            try:
                self.find_and_emit_files()
            except Exception:
                import os
                import traceback
                traceback.print_exc()
                span.set_status(Status(StatusCode.ERROR))
                self._logger.critical(
                    "Fatal error in data monitor %s: exiting", self.name,
                )
                os._exit(1)

    def find_and_emit_files(self) -> None:
        """Find file and put in file queue."""
        tracer = get_tracer(__name__)
        for incoming_file in self.find_file():
            incoming_path = (
                str(incoming_file.file) if incoming_file.file else ""
            )
            with tracer.start_as_current_span(
                "data_monitor.process_file",
                attributes={
                    ATTR_PLUGIN_NAME: self.name,
                    ATTR_PLUGIN_VERSION: str(getattr(self, "version", "")),
                    ATTR_PLUGIN_FAMILY: self.family,
                    ATTR_FILE_PATH: incoming_path,
                    ATTR_FILE_SOURCE: incoming_file.source or "",
                },
            ):
                try:
                    file_with_metadata = self.add_metadata_to_file(incoming_file)
                    get_current_span().add_event(
                        "file.found",
                        attributes={
                            ATTR_FILE_PATH: incoming_path,
                        },
                    )
                    self._logger.info(f"Found file: {file_with_metadata}")
                    emitted_path = (
                        str(file_with_metadata.file)
                        if file_with_metadata.file
                        else ""
                    )
                    with tracer.start_as_current_span(
                        "data_monitor.emit_file",
                        attributes={
                            ATTR_FILE_PATH: emitted_path,
                            ATTR_FILE_HOSTNAME: (
                                file_with_metadata.hostname or ""
                            ),
                        },
                    ):
                        self.emit(file_with_metadata)
                        get_current_span().add_event(
                            "file.emitted",
                            attributes={
                                ATTR_FILE_PATH: emitted_path,
                            },
                        )
                    self._files_processed.labels(
                        monitor_name=self.name,
                        monitor_identifier=self.identifier,
                        status="success",
                    ).inc()

                    # Update the last processed timestamp for peer latency dashboards
                    DATA_MONITOR_LAST_PROCESSED_TIMESTAMP.labels(
                        plugin_name=self.name,
                        monitor_identifier=self.identifier,
                    ).set(time.time())
                except CourierError as exc:
                    span = get_current_span()
                    span.set_status(Status(StatusCode.ERROR))
                    span.record_exception(exc)
                    self._files_processed.labels(
                        monitor_name=self.name,
                        monitor_identifier=self.identifier,
                        status="failure",
                    ).inc()
                    self._logger.exception(f"Error processing file {incoming_file}")

    @log_execution
    def start(self) -> None:
        """Start main thread."""
        if self._state == PluginRunState.RUNNING:
            return
        self._stop_event.clear()
        # daemon=True is a backstop only; stop() sets _stop_event and joins.
        self._main_thread = threading.Thread(
            target=self._run_find_and_emit_files,
            name=self.name,
            daemon=True,
        )
        self._state = PluginRunState.RUNNING
        self._main_thread.start()
        return

    @log_execution
    def stop(self) -> None:
        """Stop main thread."""
        self._state = PluginRunState.STOPPED
        self._stop_event.set()
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=5)

    def is_healthy(self) -> bool:
        """Check if plugin is healthy."""
        return self._state == PluginRunState.RUNNING

    def get_metrics(self) -> dict[str, Any]:
        """Return plugin-specific metrics including last-processed timestamp."""
        result = collect_labeled(
            DATA_MONITOR_FILES_PROCESSED,
            "monitor_name",
            self.name,
        )
        result.update(
            collect_labeled(
                DATA_MONITOR_LAST_PROCESSED_TIMESTAMP,
                "plugin_name",
                self.name,
            ),
        )
        return result


#: Registry of data monitor plugins, read from the ``courier.data_monitors``
#: entry-point group. Hands back classes; ``PluginManager`` constructs them.
data_monitors = ClassPluginRegistry(
    name="data_monitors",
    group=f"{ENTRY_POINT_PREFIX}.data_monitors",
    expected_base=DataMonitorBasePlugin,
)

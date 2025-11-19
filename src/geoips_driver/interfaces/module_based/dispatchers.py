"""Python class for the data_monitors geoips_driver interface."""

import threading
from collections.abc import Generator
from dataclasses import dataclass

from geoips_driver.interfaces.module_based.service import (
    Service,
    ServicePlugin,
    log_execution,
    setup_logging,
)

logger = setup_logging()


@dataclass
class ExecutionLog:
    """Execution log DataClass."""

    frozen = True
    return_code: int | None
    stdout: str | None
    stderr: str | None
    hostname: str | None


class Dispatcher(ServicePlugin):
    """Base dispatcher plugin."""

    @property
    def name(self) -> str:
        """Service name."""
        return "dispatcher"

    def __init__(self, service: Service) -> None:
        self.parent_service = service
        self.queue = "Dispatcher"
        self.message_template = (
            "return_code: '{execution_log}', stdout: '{stdout}', stderr: '{stderr}', "
            "hostname: '{hostname}'"
        )
        self._running = False

    def yield_execution_log(self) -> Generator[ExecutionLog, None, None]:
        """Yield ExecutionLogs."""
        yield ExecutionLog(return_code=None, stdout=None, stderr=None, hostname=None)

    def emit(self, execution_log: ExecutionLog) -> None:
        """Emit execution log to parent service."""
        message = self.message_template.format(
            return_code=execution_log.return_code,
            stdout=execution_log.stdout,
            stderr=execution_log.stderr,
            hostname=execution_log.hostname,
        )
        self.parent_service.emit(queue=self.queue, message=message)

    def handle_incoming_jobs(self) -> None:
        """Execute given a steady stream of jobs, log and execute them.

        Once a job is complete, yield the result of its execution to a downstream
        service.
        """
        for ex_log in self.yield_execution_log():
            logger.info(f"Found file: {ex_log}")
            self.emit(ex_log)

    @log_execution
    def start(self) -> None:
        """Start main thread."""
        if self._running:
            return
        else:
            self._running = False
        self._main_thread = threading.Thread(
            target=self.handle_incoming_jobs,
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

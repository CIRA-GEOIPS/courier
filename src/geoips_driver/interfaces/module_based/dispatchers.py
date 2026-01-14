"""Python class for the data_monitors geoips_driver interface."""

import json
import threading
from dataclasses import dataclass
from typing import ClassVar

from geoips.interfaces.base import BaseModuleInterface  # type: ignore[import-untyped]

from geoips_driver.interfaces.module_based.job_builders import JOB_READY_QUEUE, Job
from geoips_driver.interfaces.module_based.service import (
    Service,
    ServicePlugin,
    log_execution,
)
from geoips_driver.utils.logging import get_logger


@dataclass(frozen=True)
class ExecutionLog:
    """Execution log DataClass."""

    return_code: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    hostname: str | None = None

    def __str__(self) -> str:
        """Convert ExecutionLog to JSON string."""
        return json.dumps(
            {
                "return_code": self.return_code,
                "stdout": self.stdout,
                "stderr": self.stderr,
                "hostname": self.hostname,
            },
        )

    @classmethod
    def from_string(cls, s: str) -> "ExecutionLog":
        """Initialize ExecutionLog from JSON string."""
        return cls(**json.loads(s))


class Dispatcher(ServicePlugin):
    """Base dispatcher plugin."""

    name = "dispatcher"

    def __init__(self, service: Service, config: dict) -> None:
        self.parent_service = service
        self._logger = get_logger("plugin", self.name, service._config)
        self.queue = "Dispatcher"
        self._running = False
        self.config = config

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Yield ExecutionLogs."""
        self._logger.debug(f"Yielding execution log for job: {job}")
        return [ExecutionLog(return_code=None, stdout=None, stderr=None, hostname=None)]

    def emit(self, execution_log: ExecutionLog) -> None:
        """Emit execution log to parent service."""
        self._logger.debug(f"Emitting execution log: {execution_log}")
        self.parent_service.emit(queue=self.queue, message=str(execution_log))

    def handle_incoming_jobs(self) -> None:
        """Execute given a steady stream of jobs, log and execute them.

        Once a job is complete, yield the result of its execution to a downstream
        service.
        """
        while True:
            for job_string in self.parent_service.consume(JOB_READY_QUEUE):
                job = Job.from_string(str(job_string))
                self._logger.debug(f"Received Job: {job}")
                for ex_log in self.get_execution_log(job):
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


class DispatcherInterface(BaseModuleInterface):
    """Interface for creating GeoIPS formatted titles."""

    name: ClassVar[str] = "dispatchers"
    required_args: ClassVar[dict[str, list[str]]] = {"standard": []}
    required_kwargs: ClassVar[dict[str, list[str]]] = {"standard": []}
    # ignoring odd capitalization to match existing code style in GeoIPS
    # which itself is matching Kubernetes conventions
    apiVersion: ClassVar[str] = "geoips_driver/v1"  # noqa: N815


dispatchers = DispatcherInterface()

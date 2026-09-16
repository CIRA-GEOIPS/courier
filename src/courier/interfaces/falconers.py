from typing import ClassVar

from pydantic import BaseModel

from courier.interfaces.plugin_protocol import ServicePlugin
from courier.interfaces.discovery import (
        ClassPluginRegistry,
        ENTRY_POINT_PREFIX
)
from courier.service import Service
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job
from courier.utils.logging import get_logger

class FalconerConfig(BaseModel):
    hello: str

class Falconer(ServicePlugin):
    interface: ClassVar[str] = "falconers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "falconer"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        if identifier is None:
            raise ValueError(
                f"Falconer {type(self).__name__} requires an identifier"
            )
        self._logger = get_logger("plugin", self.name, service.config)
        self.parent_service = service
        self.config = config or {}

    def send_for_payload(self, job: Job) -> list[ExecutionLog]:
        return [ExecutionLog()]
    def start(self) -> None:
        return
    def stop(self) -> None:
        return
    def is_healthy(self) -> bool:
        return True

falconers = ClassPluginRegistry(
    name="falconers",
    group=f"{ENTRY_POINT_PREFIX}.falconers",
    expected_base=Falconer,
)

from typing import Any, ClassVar

from courier.constants import PluginRunState
from courier.plugins.falcons.shell_falcon import ShellFalcon
from pydantic import BaseModel

from courier.interfaces.falcons import DispatcherGroupConfig, Falcon
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

    representations: list[type[Falcon]] = [ShellFalcon]
    falcon: Falcon
    base_config: DispatcherGroupConfig

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
        self._state = PluginRunState.STOPPED
        self.identifier = identifier

    def cast_off_falcon(self, job: Job) -> list[ExecutionLog]:
        return [ExecutionLog()]
    def get_metrics(self) -> dict[str, Any]:
        return {}
    def set_falcon(self, falcon: Falcon):
        self.falcon = falcon
    def send_for_payload(self, job: Job) -> list[ExecutionLog]:
        return [ExecutionLog()]
    def initialize_environment(self) -> None:
        return
    def start(self) -> None:
        # eager loading by default
        if self._state == PluginRunState.RUNNING:
            return
        try:
            self.initialize_environment()
        except Exception:
            self._logger.exception(
                f"Failed to initialize falconer environment; ID {self.identifier}"
            )
            raise
        self._state = PluginRunState.RUNNING
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

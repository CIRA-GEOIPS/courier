from typing import ClassVar
from pydantic import BaseModel
from courier.interfaces.discovery import ENTRY_POINT_PREFIX, ClassPluginRegistry
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.service import Service
from courier.utils.logging import get_logger

class FalconConfig(BaseModel):
    foo: str

class Falcon(ServicePlugin):
    interface: ClassVar[str] = "falcons"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "falcon"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None
    ) -> None:
        if identifier is None:
            raise ValueError(
                f"Falcon {type(self).__name__} requires an identifier"
            )
        self._logger = get_logger("plugin", self.name, service.config)
        self.parent_service = service
        self.config = config or {}

    def start(self) -> None:
        return
    def stop(self) -> None:
        return
    def is_healthy(self) -> bool:
        return True

falcons = ClassPluginRegistry(
    name="falcons",
    group=f"{ENTRY_POINT_PREFIX}.falcons",
    expected_base=Falcon,
)

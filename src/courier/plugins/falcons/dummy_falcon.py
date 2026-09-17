from typing import ClassVar
from courier.interfaces.falcons import Falcon
from courier.service import Service

class DummyFalcon(Falcon):
    interface: ClassVar[str] = "falcons"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "dummy_falcon"
    version: ClassVar[str] = "-1"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)

    def is_healthy(self) -> bool:
        return True

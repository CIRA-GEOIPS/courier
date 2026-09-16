from typing import ClassVar, TYPE_CHECKING
from courier.interfaces.falconers import Falconer, FalconerConfig

from courier.service import Service

class DummyFalconer(Falconer):
    interface: ClassVar[str] = "falconers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "dummy_falconer"
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

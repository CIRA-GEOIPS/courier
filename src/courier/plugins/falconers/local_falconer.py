from typing import ClassVar, TYPE_CHECKING
from courier.interfaces.falconers import Falconer, FalconerConfig
from courier.interfaces.falcons import Falcon

from courier.plugins.falcons.shell_falcon import ShellFalcon
from courier.service import Service
from courier.types.job import Job

class LocalFalconer(Falconer):
    interface: ClassVar[str] = "falconers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "local_falconer"
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
    def cast_off_falcon(self, job: Job):
        return self.falcon.get_payload_from_job(job)

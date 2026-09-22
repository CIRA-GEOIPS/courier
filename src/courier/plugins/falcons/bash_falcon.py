from typing import ClassVar
from courier.plugins.falcons.shell_falcon import ShellFalcon
from courier.service import Service


class BashFalcon(ShellFalcon):
    interface: ClassVar[str] = "falcons"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "bash_falcon"
    version: ClassVar[str] = "-1"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self._default_binary = self.config.default_binary if self.config.default_binary else "bash"
        self._file_suffix = ".sh"
    def is_healthy(self) -> bool:
        return True

"""Implementation for the bash_falcon falcon class."""

from typing import ClassVar

from courier.interfaces.falcons import DispatcherGroupConfig, FalconConfig
from courier.plugins.falcons.shell_falcon import ShellFalcon
from courier.service import Service


# classes for courier init discovery
class BashFalconConfig(FalconConfig):  # noqa: D101
    pass


class BashFalconBaseConfig(DispatcherGroupConfig):  # noqa: D101
    pass


class BashFalcon(ShellFalcon):
    """Falcon class for Bash script execution."""

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
        self._default_binary = (
            self.config.default_binary if self.config.default_binary else "bash"
        )
        self._file_suffix = ".sh"

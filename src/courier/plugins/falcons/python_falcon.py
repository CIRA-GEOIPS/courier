from pathlib import Path
from typing import ClassVar
from courier.plugins.falcons.bash_falcon import BashFalcon
from courier.service import Service


class PythonFalcon(BashFalcon):
    interface: ClassVar[str] = "falcons"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "python_falcon"
    version: ClassVar[str] = "-1"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self._default_binary = "python"
        self._file_suffix = ".py"
    def is_healthy(self) -> bool:
        return True
    def declare_command(self, path: Path | None = None) -> list[str]:
        command_arr = []
        if not self.config.binary:
            for prefix in self.config.prefix_args:
                command_arr.append(prefix)
            command_arr.append(str(path) if path else str(self.config.file))
            for suffix in self.config.suffix_args:
                command_arr.append(suffix)
            return command_arr
        else:
            faux_command_arr = [self.config.binary]
            for prefix in self.config.prefix_args:
                faux_command_arr.append(prefix)
            faux_command_arr.append(str(path) if path else str(self.config.file))
            for suffix in self.config.suffix_args:
                faux_command_arr.append(suffix)

            command_str = f"import subprocess; subprocess.run({faux_command_arr!r})"
            return [command_str]

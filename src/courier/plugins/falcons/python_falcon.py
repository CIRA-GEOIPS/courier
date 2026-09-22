from pathlib import Path
from typing import ClassVar
from courier.plugins.falcons.bash_falcon import BashFalcon
from courier.service import Service
from courier.types.execution_log import ExecutionLog


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
        self._default_binary = self.config.default_binary if self.config.default_binary else "python"
        self._file_suffix = ".py"
    def is_healthy(self) -> bool:
        return True
    def validate_toolchain_arg(self, value: str) -> list[ExecutionLog]:
        command = self.config.toolchain_prepend
        command.extend([self._default_binary, "-c", f"import shutil; print(shutil.which('{value}'))"])
        return self.get_payload_from_job(command)
    def generate_calling_method(self) -> list[str]:
        command_arr = [self._default_binary]
        if self.config.binary or self.config.file.suffix != ".py":
            command_arr.append("-c")

        return command_arr
    def declare_command(self, path: Path | None = None) -> list[str]:
        command_arr = []
        if not self.config.binary and self.config.file.suffix == ".py":
            for prefix in self.config.prefix_args:
                command_arr.append(prefix)
            command_arr.append(str(path) if path else str(self.config.file))
            for suffix in self.config.suffix_args:
                command_arr.append(suffix)
            return command_arr
        else:
            faux_command_arr = []
            if self.config.binary:
                faux_command_arr.append(self.config.binary)
            for prefix in self.config.prefix_args:
                faux_command_arr.append(prefix)
            faux_command_arr.append(str(path) if path else str(self.config.file))
            for suffix in self.config.suffix_args:
                faux_command_arr.append(suffix)

            command_str = f"import subprocess; subprocess.run({faux_command_arr!r})"
            return [command_str]

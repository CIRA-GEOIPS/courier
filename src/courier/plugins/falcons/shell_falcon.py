from datetime import datetime
from pathlib import Path
import tempfile
from typing import ClassVar
from courier.interfaces.falcons import Falcon, FalconConfig
from courier.service import Service
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job

from courier.utils.shell_executor import execute_shell_script

from courier.utils.functional import slugify_for_filename

class ShellFalcon(Falcon):
    interface: ClassVar[str] = "falcons"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "shell_falcon"
    version: ClassVar[str] = "-1"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self._default_binary = "sh"
        self._file_suffix = ".sh"

    def is_healthy(self) -> bool:
        return True 
        return Path(rendered_script_path)
    def get_payload_from_job(self, job: Job,
                             command: list[str],
                             log_prefix: str = "",
                             log_file_path: Path | None = None) -> list[ExecutionLog]:         
        result = execute_shell_script(
            command,
            self.base_config.timeout_seconds,
            logger=self._logger,
            log_to_logger=self.base_config.log_to_logger,
            log_prefix=log_prefix,
            log_to_file=self.base_config.log_to_file,
            log_file_path=log_file_path,
            log_only_errors=self.base_config.log_only_errors,
        )

        return [ExecutionLog(
            return_code=result.return_code,
            stdout=result.stdout,
            stderr=result.stderr
        )]

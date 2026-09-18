from datetime import datetime
from pathlib import Path
from typing import ClassVar
from courier.interfaces.falcons import Falcon
from courier.service import Service
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job

from courier.utils.shell_executor import execute_shell_script

import subprocess

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
        self._default_binary = "sh"
        super().__init__(service, config, identifier=identifier)

    def is_healthy(self) -> bool:
        return True
    def _generate_execution_command(self, job: Job) -> str:
        raw_command = f"{self.config.binary} {self.config.prefix_args} {self.config.file} {self.config.suffix_args}"
        try:
            command = self.render_script(job, raw_command)
            return command
        except Exception:
            raise
    def _render_script_file(self, job: Job) -> None:
        script = self.config.file.read_text()
        rendered_script = self.render_script(job, script)
        self.config.file.write_text(rendered_script)
    def get_payload_from_job(self, job: Job) -> list[ExecutionLog]: 
        self._render_script_file(job)
        command = [self._default_binary, "-c", self._generate_execution_command(job)]

        log_prefix = (
            f"[job: {job.identifier}]" if self.base_config.log_to_logger else ""
        )

        log_file_path: Path | None = None
        if self.base_config.log_to_file:
            ts = datetime.now().strftime("%Y%m%dT%H%M%S%f")
            safe_id = slugify_for_filename(job.identifier)
            log_file_path = (
                Path(self.base_config.log_dir) / f"dispatch_{safe_id}_{ts}.log"
            )

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

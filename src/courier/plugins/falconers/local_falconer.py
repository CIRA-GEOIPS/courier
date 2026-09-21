from datetime import datetime
from pathlib import Path
from typing import ClassVar, TYPE_CHECKING
from courier.interfaces.falconers import Falconer, FalconerPayload
from courier.interfaces.falcons import Falcon

from courier.plugins.falcons.shell_falcon import ShellFalcon
from courier.service import Service
from courier.types.job import Job
from courier.utils.functional import slugify_for_filename

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
        payload = self.initialize_environment(job)
        return self.falcon.get_payload_from_job(job,
                                                payload.command,
                                                log_prefix=payload.log_prefix,
                                                log_file_path=payload.log_file_path)
    def initialize_environment(self, job) -> FalconerPayload:
        clean_path = self._render_script_file(job)
        command = [self.falcon._default_binary, "-c", self._generate_execution_command(job, clean_path)]
 
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
        payload = FalconerPayload()
        payload.command = command
        payload.log_prefix = log_prefix
        payload.log_file_path = log_file_path
        return payload

from dataclasses import field
from typing import Any, ClassVar
import jinja2
import tempfile

from courier.constants import PluginRunState
from courier.plugins.falcons.shell_falcon import ShellFalcon
from pydantic import BaseModel
from pathlib import Path

from courier.interfaces.falcons import DispatcherGroupConfig, Falcon, FalconConfig
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.interfaces.discovery import (
        ClassPluginRegistry,
        ENTRY_POINT_PREFIX
)
from courier.service import Service
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job
from courier.utils.logging import get_logger

class FalconerPayload:
    command: list[str]
    log_prefix: str = ""
    log_file_path: Path | None = None

class Falconer(ServicePlugin):
    interface: ClassVar[str] = "falconers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "falconer"

    representations: list[type[Falcon]] = [ShellFalcon]
    falcon: Falcon
    base_config: DispatcherGroupConfig

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        if identifier is None:
            raise ValueError(
                f"Falconer {type(self).__name__} requires an identifier"
            )
        self._logger = get_logger("plugin", self.name, service.config)
        self.parent_service = service
        self.config = config or {}
        self._state = PluginRunState.STOPPED
        self.identifier = identifier

    def cast_off_falcon(self, job: Job) -> list[ExecutionLog]:
        p = self.initialize_environment(job)
        return self.falcon.get_payload_from_job(job,
                                                p.command)
    def initialize_environment(self, job) -> FalconerPayload:
        return FalconerPayload()
    def get_metrics(self) -> dict[str, Any]:
        return {}
    def set_falcon(self, falcon: Falcon):
        self.falcon = falcon
    def send_for_payload(self, job: Job) -> list[ExecutionLog]:
        return [ExecutionLog()]
    def _render_script_file(self, job: Job) -> Path:
        falcon_config = self.falcon.config
        rendered_script_path: str | None = None

        script = falcon_config.file.read_text()
        rendered_script = self.render_script(job, script)

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=self.falcon._file_suffix,
            delete=False,
        ) as script_file:
            script_file.write(rendered_script)
            rendered_script_path = script_file.name
        return Path(rendered_script_path)
    def _generate_execution_command(self, job: Job, path: Path | None = None) -> str:
        falcon_config = self.falcon.config
        if not path:
            path = falcon_config.file
        prefix = " ".join(falcon_config.prefix_args)
        suffix = " ".join(falcon_config.suffix_args)
        parts = [
            falcon_config.binary or self.falcon._default_binary,
            prefix,
            str(path),
            suffix,
        ]

        raw_command = " ".join(part for part in parts if part)
        try:
            command = self.render_script(job, raw_command)
            return command
        except Exception:
            raise
    def render_script(self, job: Job, script: str) -> str:
        """Render the Jinja2 bash template with job and config context."""
        context = {
            "files": [
                f.to_dict() for f in sorted(job.files, key=lambda f: str(f.file))
            ],
            "job": {
                "name": job.name,
                "identifier": job.identifier,
                "config": job.config,
                "last_modified": job.last_modified,
                "timeout": job.timeout,
                "correlation_id": job.correlation_id,
                "emit_time": job.emit_time,
            },
            "config": job.config,
        }
        return jinja2.Environment(
            undefined=jinja2.DebugUndefined,
            autoescape=False,
            finalize=lambda value: "" if value is None or value == [] else value,
        ).from_string(script).render(**context)
    def start(self) -> None:
        # eager loading by default
        if self._state == PluginRunState.RUNNING:
            return
        self._state = PluginRunState.RUNNING
        return
    def stop(self) -> None:
        return
    def is_healthy(self) -> bool:
        return True

falconers = ClassPluginRegistry(
    name="falconers",
    group=f"{ENTRY_POINT_PREFIX}.falconers",
    expected_base=Falconer,
)

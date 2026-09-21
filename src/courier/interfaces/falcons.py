import os
from typing import Any, ClassVar, Self
import jinja2
from pydantic import BaseModel, field_validator, model_validator, Field
from courier.interfaces.discovery import ENTRY_POINT_PREFIX, ClassPluginRegistry
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.service import Service
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job
from courier.utils.logging import get_logger

from dataclasses import dataclass

from pathlib import Path

class DispatcherGroupConfig(BaseModel, frozen=True):
    """Validated configuration for the entire dispatcher group."""
    timeout_seconds: float = Field(default=3600.0, gt=0)
    log_to_logger: bool = Field(default=False)
    log_to_file: bool = Field(default=False)
    log_dir: str = Field(default="")
    log_only_errors: bool = Field(default=False)
    scan_stderr: bool = Field(default=False)

    @model_validator(mode="after")
    def _validate_logging_config(self) -> Self:
        if self.log_to_file and not self.log_dir:
            raise ValueError("log_dir is required when log_to_file=True")
        if self.log_to_file:
            log_dir_path = Path(self.log_dir)
            if not log_dir_path.is_dir():
                log_dir_path.mkdir(parents=True, exist_ok=True)
            elif not os.access(self.log_dir, os.W_OK):
                raise ValueError(f"log_dir is not writable: {self.log_dir}")
        return self

# The transient nature of falcons does not allow for modifications to the base config.
@dataclass(frozen=True)
class FalconConfig(BaseModel):
    file: Path
    prefix_args: list[str] = []
    suffix_args: list[str] = []
    binary: str | None = None

    @field_validator("file")
    @classmethod
    def validate_file(cls, value: Path) -> Path:
        if not value.exists():
            raise ValueError(f"File does not exist {value}")
        return value

class Falcon(ServicePlugin):
    interface: ClassVar[str] = "falcons"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "falcon"

    base_config: DispatcherGroupConfig

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None
    ) -> None:
        if identifier is None:
            raise ValueError(
                f"Falcon {type(self).__name__} requires an identifier"
            )
        self._logger = get_logger("plugin", self.name, service.config)
        self.parent_service = service
        self._default_binary = None
        self._file_suffix = ".sh"
        self.config = FalconConfig.model_validate(config)

    @classmethod
    def get_representation_hierarchy(cls) -> list[type["Falcon"]]:
        return [
            parent
            for parent in reversed(cls.__mro__)
            if issubclass(parent, Falcon) and parent is not Falcon
        ]
    @classmethod
    def from_falcon(cls, falcon: "Falcon") -> "Falcon":
        return cls(
            falcon.parent_service,
            falcon.config.model_dump(),
            falcon.name
        )
    def get_payload_from_job(self, job: Job,
                             command: list[str],
                             log_prefix: str = "",
                             log_file_path: Path | None = None) -> list[ExecutionLog]:         
        return [
            ExecutionLog()
        ]
    def get_metrics(self) -> dict[str, Any]:
        return {}
    def start(self) -> None:
        return
    def stop(self) -> None:
        return
    def is_healthy(self) -> bool:
        return True

falcons = ClassPluginRegistry(
    name="falcons",
    group=f"{ENTRY_POINT_PREFIX}.falcons",
    expected_base=Falcon,
)

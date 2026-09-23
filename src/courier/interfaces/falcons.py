"""Implementation for the base falcon plugin."""
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Self

from pydantic import BaseModel, Field, field_validator, model_validator

from courier.interfaces.discovery import ENTRY_POINT_PREFIX, ClassPluginRegistry
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.service import Service
from courier.types.execution_log import ExecutionLog
from courier.utils.logging import get_logger


class DispatcherGroupConfig(BaseModel):
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
class FalconConfig(BaseModel):
    """Validated configuration for a Falcon."""

    file: Path
    toolchain: list[str] = Field(default_factory=list)
    toolchain_prepend: list[str] = Field(default_factory=list)
    prefix_args: list[str] = Field(default_factory=list)
    suffix_args: list[str] = Field(default_factory=list)
    binary: str | None = None
    default_binary: str | None = None

    @field_validator("file")
    @classmethod
    def validate_file(cls, value: Path) -> Path:
        """Validate if the provided file exists."""
        if not value.exists():
            raise ValueError(f"File does not exist {value}")
        return value


class Falcon(ServicePlugin):
    """Base class for Falcons."""

    interface: ClassVar[str] = "falcons"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "falcon"

    base_config: DispatcherGroupConfig

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        if identifier is None:
            raise ValueError(
                f"Falcon {type(self).__name__} requires an identifier",
            )
        self.identifier = identifier
        self._logger = get_logger("plugin", self.name, service.config)
        self.parent_service = service
        self._default_binary = None
        self._file_suffix = ".sh"
        self.config = FalconConfig.model_validate(config)

    @classmethod
    def get_representation_hierarchy(cls) -> list[type["Falcon"]]:
        """Generate the Falcon representation hierarchy.

        Returns
        -------
        list[type[Falcon]]
            Falcon subclasses in base-to-most-specific inheritance order.
        """
        return [
            parent
            for parent in reversed(cls.__mro__)
            if issubclass(parent, Falcon) and parent is not Falcon
        ]

    @classmethod
    def from_falcon(cls, falcon: "Falcon") -> "Falcon":
        """Generate a Falcon represntation from another Falcon.

        Parameters
        ----------
        falcon : Falcon
            Falcon whose service and configuration should be reused.

        Returns
        -------
        Falcon
            A new instance of this Falcon representation.
        """
        return cls(
            falcon.parent_service,
            falcon.config.model_dump(),
            falcon.identifier,
        )

    def validate_toolchain_arg(self, value: str) -> list[ExecutionLog]: # noqa: ARG002
        """Validate a toolchain argument through a self-defined method.

        Parameters
        ----------
        value : str
            Tool or executable name to validate.

        Returns
        -------
        list[ExecutionLog]
            Execution logs describing the result of validation.
        """
        return []

    def get_payload_from_job(
        self,
        command: list[str], # noqa: ARG002
        log_prefix: str = "", # noqa: ARG002
        log_file_path: Path | None = None, # noqa: ARG002
    ) -> list[ExecutionLog]:
        """Get an execution log from execution a command.

        Parameters
        ----------
        command : list[str]
            Command and arguments to execute.
        log_prefix : str, optional
            Prefix to apply to generated log output.
        log_file_path : Path | None, optional
            Optional path for persisted execution logs.

        Returns
        -------
        list[ExecutionLog]
            Execution logs produced by the command.
        """
        return [
            ExecutionLog(),
        ]

    def declare_command(self, path: Path | None = None) -> list[str]: # noqa: ARG002
        """Declare the syntax to call a command.

        Parameters
        ----------
        path : Path | None, optional
            Path to the rendered script or executable.

        Returns
        -------
        list[str]
            Command arguments required to execute the Falcon.
        """
        return []

    def generate_calling_method(self) -> list[str]:
        """Declare the first part of a command, e.g. `python3 -c` or `bash -c`.

        Returns
        -------
        list[str]
            Command arguments used to invoke this Falcon representation.
        """
        return []

    def get_metrics(self) -> dict[str, Any]:
        """Return plugin-specific metrics."""
        return {}

    def start(self) -> None:
        """Start execution of the falcon."""
        return

    def stop(self) -> None:
        """Stop execution of the falcon."""
        return

    def is_healthy(self) -> bool:
        """Declare the health of the falcon."""
        return True


falcons = ClassPluginRegistry(
    name="falcons",
    group=f"{ENTRY_POINT_PREFIX}.falcons",
    expected_base=Falcon,
)

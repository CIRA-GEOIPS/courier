"""Pydantic models for service configuration validation."""

from __future__ import annotations

from typing import Any

from pydantic import (
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from lazylemon.schema.base import FrozenModel, _ensure_non_empty, _find_duplicate_values
from lazylemon.schema.broker_config import BrokerConfig  # noqa: TC001

__all__ = [
    "MicroserviceDefinitionModel",
    "MicroserviceModel",
    "ServiceConfigModel",
    "ServiceSpecModel",
]


class MicroserviceDefinitionModel(FrozenModel):
    """Definition of a microservice used within a service."""

    kind: str = Field(..., description="Plugin kind for the microservice.")
    name: str = Field(..., description="Plugin name for the microservice.")
    config: Any = Field(
        default=None,
        description="Arbitrary configuration passed to the plugin (may be null).",
    )

    @field_validator("kind", "name")
    @classmethod
    def _validate_non_empty(cls, value: str, info: ValidationInfo) -> str:
        """Ensure `kind` and `name` are non-empty strings."""
        return _ensure_non_empty(value, field_name=info.field_name)


class MicroserviceModel(FrozenModel):
    """A uniquely identified step within the service `run` sequence."""

    identifier: str = Field(
        ...,
        description="Unique identifier for the run step.",
    )
    spec: MicroserviceDefinitionModel = Field(
        ...,
        description="Detailed configuration for the microservice.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_from_singleton_mapping(cls, data: Any) -> Any:
        """Adapt `{identifier: {...}}` mappings into the canonical model form."""
        if isinstance(data, dict) and "identifier" not in data and "spec" not in data:
            if len(data) != 1:
                raise ValueError(
                    "Microservice entries must contain exactly one identifier mapping.",
                )
            identifier, spec = next(iter(data.items()))
            if not isinstance(spec, dict):
                raise TypeError(
                    f"Microservice '{identifier}' "
                    "must map to an object describing the microservice.",
                )
            return {"identifier": identifier, "spec": spec}
        return data

    @field_validator("identifier")
    @classmethod
    def _validate_identifier(cls, value: str, info: ValidationInfo) -> str:
        """Ensure the run step identifier is a non-empty string."""
        return _ensure_non_empty(value, field_name=info.field_name)


class ServiceSpecModel(FrozenModel):
    """The `spec` section of a GeoIPS service configuration."""

    service_namespace: str = Field(
        ...,
        description="Namespace used to group related service assets.",
    )
    heartbeat_interval: int = Field(
        ...,
        description="Interval in seconds between service heartbeat messages.",
    )
    broker: BrokerConfig = Field(
        ...,
        description="Message broker connection configuration.",
    )
    run: list[MicroserviceModel] = Field(
        ...,
        min_length=1,
        description="Ordered collection of steps to execute for the service.",
    )

    @field_validator("service_namespace")
    @classmethod
    def _validate_namespace(cls, value: str, info: ValidationInfo) -> str:
        """Ensure the service namespace is provided."""
        return _ensure_non_empty(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _ensure_unique_run_identifiers(self) -> ServiceSpecModel:
        """Guarantee that run step identifiers are unique."""
        duplicates = _find_duplicate_values(step.identifier for step in self.run)
        if duplicates:
            dupes = ", ".join(sorted(duplicates))
            raise ValueError(f"Duplicate run step identifiers detected: {dupes}")
        return self


class ServiceConfigModel(FrozenModel):
    """Top-level validation model for GeoIPS service configuration files."""

    apiVersion: str = Field(..., description="API version of the service document.")  # noqa: N815
    kind: str = Field(..., description="Resource kind; expected to be 'Service'.")
    name: str = Field(..., description="Unique service name.")
    description: str = Field(..., description="Concise description of the service.")
    docstring: str | None = Field(
        default=None,
        description="Optional long-form documentation for the service.",
    )
    spec: ServiceSpecModel = Field(
        ...,
        description="Detailed service configuration specification.",
    )

    @field_validator("apiVersion", "kind", "name", "description")
    @classmethod
    def _validate_non_empty(cls, value: str, info: ValidationInfo) -> str:
        """Ensure required top-level string fields are populated."""
        return _ensure_non_empty(value, field_name=info.field_name)

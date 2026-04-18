"""Pydantic models for service configuration validation."""

from __future__ import annotations

from typing import Any

from pydantic import (
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from courier.schema.v1alpha1.base import (
    FrozenModel,
    _ensure_api_version,
    _ensure_dns_name,
    _ensure_non_empty,
    _find_duplicate_values,
)
from courier.schema.v1alpha1.broker_config import (
    BrokerConfig,
    MemoryBrokerConfig,
)

__all__ = [
    "MicroserviceDefinitionModel",
    "MicroserviceModel",
    "ResourceMetadataModel",
    "ServiceConfigModel",
    "ServiceSpecModel",
]


class ResourceMetadataModel(FrozenModel):
    """Kubernetes-style metadata block for a courier resource."""

    name: str = Field(..., description="DNS-subdomain name for this resource.")
    namespace: str | None = Field(
        default=None,
        description="Target namespace for grouping related service assets.",
    )
    description: str = Field(
        ...,
        description="Concise description of the resource.",
    )
    docstring: str | None = Field(
        default=None,
        description="Optional long-form documentation.",
    )
    labels: dict[str, str] = Field(
        default_factory=dict,
        description="Key/value pairs for identifying and selecting resources.",
    )
    annotations: dict[str, str] = Field(
        default_factory=dict,
        description="Key/value pairs for non-identifying metadata.",
    )

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str, info: ValidationInfo) -> str:
        """Ensure name is a valid DNS subdomain name."""
        return _ensure_dns_name(value, field_name=info.field_name)

    @field_validator("namespace")
    @classmethod
    def _validate_namespace(
        cls,
        value: str | None,
        info: ValidationInfo,
    ) -> str | None:
        """Ensure namespace, if provided, is a valid DNS subdomain name."""
        if value is not None:
            return _ensure_dns_name(value, field_name=info.field_name)
        return value

    @field_validator("description")
    @classmethod
    def _validate_description(cls, value: str, info: ValidationInfo) -> str:
        """Ensure description is a non-empty string."""
        return _ensure_non_empty(value, field_name=info.field_name)


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
        """Ensure the run step identifier is a valid DNS subdomain name."""
        return _ensure_dns_name(value, field_name=info.field_name)


class ServiceSpecModel(FrozenModel):
    """The `spec` section of a courier service configuration."""

    heartbeat_interval: int = Field(
        default=30,
        description="Interval in seconds between service heartbeat messages.",
    )
    broker: BrokerConfig = Field(
        default_factory=MemoryBrokerConfig,
        description="Broker connection config. Defaults to in-memory when omitted.",
    )
    run: list[MicroserviceModel] = Field(
        ...,
        min_length=1,
        description="Ordered collection of steps to execute for the service.",
    )

    @model_validator(mode="after")
    def _ensure_unique_run_identifiers(self) -> ServiceSpecModel:
        """Guarantee that run step identifiers are unique."""
        duplicates = _find_duplicate_values(step.identifier for step in self.run)
        if duplicates:
            dupes = ", ".join(sorted(duplicates))
            raise ValueError(f"Duplicate run step identifiers detected: {dupes}")
        return self


class ServiceConfigModel(FrozenModel):
    """Top-level validation model for courier service configuration files."""

    apiVersion: str = Field(..., description="API version of the service document.")  # noqa: N815
    kind: str = Field(..., description="Resource kind; expected to be 'Service'.")
    metadata: ResourceMetadataModel = Field(
        ...,
        description="Resource metadata (name, namespace, labels, annotations).",
    )
    spec: ServiceSpecModel = Field(
        ...,
        description="Detailed service configuration specification.",
    )

    @field_validator("apiVersion")
    @classmethod
    def _validate_api_version(cls, value: str, info: ValidationInfo) -> str:
        """Ensure apiVersion follows the CRD ``<group>/v<N>`` format."""
        return _ensure_api_version(value, field_name=info.field_name)

    @field_validator("kind")
    @classmethod
    def _validate_non_empty(cls, value: str, info: ValidationInfo) -> str:
        """Ensure required top-level string fields are populated."""
        return _ensure_non_empty(value, field_name=info.field_name)

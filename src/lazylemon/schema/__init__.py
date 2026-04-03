"""Pydantic schema models for lazylemon configuration validation.

Re-exports all public models from the current schema version (v1alpha1).
External code should import from ``lazylemon.schema`` directly.
"""

from lazylemon.schema.v1alpha1.broker_config import (
    AmqpBrokerConfig,
    BrokerConfig,
    MemoryBrokerConfig,
    RedisBrokerConfig,
    UrlBrokerConfig,
)
from lazylemon.schema.v1alpha1.data_monitor_configs import (
    DataMonitorConfig,
    FileMetadataEntry,
)
from lazylemon.schema.v1alpha1.service_config import (
    ResourceMetadataModel,
    ServiceConfigModel,
)

__all__ = [
    "AmqpBrokerConfig",
    "BrokerConfig",
    "DataMonitorConfig",
    "FileMetadataEntry",
    "MemoryBrokerConfig",
    "RedisBrokerConfig",
    "ResourceMetadataModel",
    "ServiceConfigModel",
    "UrlBrokerConfig",
]


_VERSION_MAP: dict[str, type[ServiceConfigModel]] = {
    "lazylemon.dev/v1alpha1": ServiceConfigModel,
}


def get_model_for_version(api_version: str) -> type[ServiceConfigModel]:
    """Return the config model class for the given apiVersion string."""
    if api_version not in _VERSION_MAP:
        supported = sorted(_VERSION_MAP)
        msg = f"Unsupported apiVersion '{api_version}'. Supported: {supported}"
        raise ValueError(msg)
    return _VERSION_MAP[api_version]

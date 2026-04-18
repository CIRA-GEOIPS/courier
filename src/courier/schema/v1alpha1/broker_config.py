"""Pydantic models for Kombu broker configuration.

Supports all Kombu transports via four config types:

- ``AmqpBrokerConfig``  — structured AMQP / AMQPS connections
- ``RedisBrokerConfig`` — structured Redis / Rediss connections
- ``MemoryBrokerConfig``— in-memory transport (testing)
- ``UrlBrokerConfig``   — raw URL passthrough for any Kombu transport
  (SQS, Kafka, MongoDB, Filesystem, Azure, GCP Pub/Sub, Consul, etc.)

The ``BrokerConfig`` type alias is a discriminated union over the
``transport`` field.  ``AmqpBrokerConfig`` defaults ``transport`` to
``"amqp"``, so existing YAML files that omit the field remain valid.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, Field, ValidationInfo, field_validator

from courier.schema.v1alpha1.base import FrozenModel, _ensure_non_empty

__all__ = [
    "AmqpBrokerConfig",
    "BrokerConfig",
    "MemoryBrokerConfig",
    "RedisBrokerConfig",
    "UrlBrokerConfig",
]


class AmqpBrokerConfig(FrozenModel):
    """AMQP broker connection settings (RabbitMQ, Qpid, etc.).

    Implementations: MessageBrokerManager (courier.broker.kombu)
    """

    transport: Literal["amqp"] = Field(
        "amqp",
        description="Transport discriminator.",
    )
    host: str = Field(..., description="Hostname or IP address of the broker.")
    port: int = Field(
        5672,
        ge=1,
        le=65535,
        description="TCP port exposed by the broker.",
    )
    username: str = Field(..., description="Username for broker authentication.")
    password: str = Field(..., description="Password for broker authentication.")
    vhost: str = Field("/", description="AMQP virtual host.")
    ssl: bool = Field(False, description="Use TLS (amqps://) when True.")
    max_retries: int = Field(
        5,
        ge=0,
        description="Maximum broker connection retry attempts.",
    )

    @field_validator("host", "username", "password", "vhost")
    @classmethod
    def _validate_non_empty(cls, value: str, info: ValidationInfo) -> str:
        """Ensure required string fields are populated."""
        return _ensure_non_empty(value, field_name=info.field_name)

    def to_url(self) -> str:
        """Build an AMQP connection URL from structured fields."""
        scheme = "amqps" if self.ssl else "amqp"
        vhost = self.vhost.lstrip("/")
        return f"{scheme}://{self.username}:{self.password}@{self.host}:{self.port}/{vhost}"


class RedisBrokerConfig(FrozenModel):
    """Redis broker connection settings.

    Implementations: MessageBrokerManager (courier.broker.kombu)
    """

    transport: Literal["redis"] = Field(
        "redis",
        description="Transport discriminator.",
    )
    host: str = Field("localhost", description="Redis server hostname.")
    port: int = Field(
        6379,
        ge=1,
        le=65535,
        description="Redis server port.",
    )
    password: str = Field("", description="Redis AUTH password (empty to skip).")
    db: int = Field(0, ge=0, description="Redis database index.")
    ssl: bool = Field(False, description="Use TLS (rediss://) when True.")
    max_retries: int = Field(
        5,
        ge=0,
        description="Maximum broker connection retry attempts.",
    )

    @field_validator("host")
    @classmethod
    def _validate_non_empty(cls, value: str, info: ValidationInfo) -> str:
        """Ensure host is populated."""
        return _ensure_non_empty(value, field_name=info.field_name)

    def to_url(self) -> str:
        """Build a Redis connection URL from structured fields."""
        scheme = "rediss" if self.ssl else "redis"
        auth = f":{self.password}@" if self.password else ""
        return f"{scheme}://{auth}{self.host}:{self.port}/{self.db}"


class MemoryBrokerConfig(FrozenModel):
    """In-memory transport for testing (no external broker required).

    Implementations: MessageBrokerManager (courier.broker.kombu)
    """

    transport: Literal["memory"] = Field(
        "memory",
        description="Transport discriminator.",
    )
    max_retries: int = Field(
        5,
        ge=0,
        description="Maximum broker connection retry attempts.",
    )

    def to_url(self) -> str:
        """Return the in-memory transport URL."""
        return "memory://"


class UrlBrokerConfig(FrozenModel):
    """Raw URL passthrough for any Kombu transport.

    Use this for transports that do not have a dedicated config model:
    SQS, Kafka, MongoDB, Zookeeper, SQLAlchemy, Filesystem, Azure
    Service Bus, Azure Storage Queues, GCP Pub/Sub, Consul, etcd,
    Pyro, SLMQ, and any future Kombu transport.

    Implementations: MessageBrokerManager (courier.broker.kombu)
    """

    transport: Literal["url"] = Field(
        "url",
        description="Transport discriminator.",
    )
    url: str = Field(..., description="Full Kombu connection URL.")
    max_retries: int = Field(
        5,
        ge=0,
        description="Maximum broker connection retry attempts.",
    )

    @field_validator("url")
    @classmethod
    def _validate_non_empty(cls, value: str, info: ValidationInfo) -> str:
        """Ensure the URL is populated."""
        return _ensure_non_empty(value, field_name=info.field_name)

    def to_url(self) -> str:
        """Return the raw URL unchanged."""
        return self.url


def _infer_transport(value: Any) -> Any:
    """Inject a ``transport`` tag when the field is missing from raw input.

    Inference rules (applied only to plain dicts without ``transport``):

    * ``host`` present → ``"amqp"``  (backward compat with legacy YAMLs)
    * otherwise        → ``"memory"`` (safest zero-dependency default)
    """
    if isinstance(value, dict) and "transport" not in value:
        transport = "amqp" if "host" in value else "memory"
        return {**value, "transport": transport}
    return value


BrokerConfig = Annotated[
    AmqpBrokerConfig | RedisBrokerConfig | MemoryBrokerConfig | UrlBrokerConfig,
    BeforeValidator(_infer_transport),
    Field(discriminator="transport"),
]
"""Broker configuration supporting all Kombu transports.

Discriminated on the ``transport`` field.  When ``transport`` is omitted,
the pre-validator infers it: ``"amqp"`` if ``host`` is present (backward
compatibility), ``"memory"`` otherwise.
"""

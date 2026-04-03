"""Pydantic model for Redis state synchronization configuration."""

from __future__ import annotations

from pydantic import Field, ValidationInfo, field_validator

from lazylemon.schema.v1alpha1.base import FrozenModel, _ensure_non_empty

__all__ = ["RedisStateSyncConfig"]


class RedisStateSyncConfig(FrozenModel):
    """Redis connection settings for job builder HA state synchronization.

    When set under a job builder's ``state_sync`` YAML key, the builder
    will push job mutations to a shared Redis hash and subscribe to a
    pub/sub channel so that peer instances can apply the changes locally.

    On startup the builder loads existing job state from Redis, enabling
    crash recovery without losing in-progress groupings.

    Implementations: JobBuilder (lazylemon.interfaces.module_based.job_builders)

    Configuration keys
    ------------------
    host : str, optional
        Redis server hostname or IP address. Default: ``"localhost"``.
    port : int, optional
        Redis server port (1-65535). Default: ``6379``.
    db : int, optional
        Redis database index (≥ 0). Default: ``0``.
    password : str or None, optional
        Redis AUTH password. ``None`` or empty string skips authentication.
        Default: ``None``.
    ssl : bool, optional
        Use TLS (``rediss://``) when ``True``. Default: ``False``.
    channel_prefix : str, optional
        Prefix applied to all Redis keys and pub/sub channels.
        Set a unique value when multiple tenants share one Redis instance.
        Default: ``"lazylemon"``.

    Examples
    --------
    Minimal YAML fragment under a job builder's ``config`` block::

        config:
          state_sync:
            host: redis.internal
            port: 6379
            db: 1
    """

    host: str = Field("localhost", description="Redis server hostname.")
    port: int = Field(
        6379,
        ge=1,
        le=65535,
        description="Redis server port.",
    )
    db: int = Field(0, ge=0, description="Redis database index.")
    password: str | None = Field(
        None,
        description="Redis AUTH password; None or empty string to skip.",
    )
    ssl: bool = Field(False, description="Use TLS when True.")
    channel_prefix: str = Field(
        "lazylemon",
        description="Key prefix for multi-tenant Redis isolation.",
    )

    @field_validator("host", "channel_prefix")
    @classmethod
    def _validate_non_empty(cls, value: str, info: ValidationInfo) -> str:
        """Ensure host and channel_prefix are non-empty strings."""
        return _ensure_non_empty(value, field_name=info.field_name)

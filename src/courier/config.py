"""Service configuration dataclass."""

import os
import uuid
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ServiceConfig:
    """Immutable service configuration with environment variable defaults.

    This configuration class provides default values from environment variables
    for service initialization. All fields are frozen to ensure immutability
    after instantiation.

    Parameters
    ----------
    service_id : str, optional
        Unique identifier for this service instance. Defaults to environment
        variable SERVICE_ID or auto-generated UUID-based identifier.
    namespace : str, optional
        Namespace for service isolation. Defaults to environment variable
        SERVICE_NAMESPACE or 'default'.
    database_url : str, optional
        PostgreSQL database connection URL. Defaults to environment variable
        DATABASE_URL or localhost connection.
    prometheus_port : int, optional
        Port number for Prometheus metrics HTTP server. Defaults to environment
        variable PROMETHEUS_PORT or 8000.
    broker_url : str, optional
        Message broker connection URL. Defaults to environment variable BROKER_URL
        or localhost AMQP connection.
    broker_max_retries : int, optional
        Maximum retry attempts for broker operations. Defaults to environment
        variable BROKER_MAX_RETRIES or 5.
    heartbeat_interval : int, optional
        Interval in seconds between heartbeat metric updates. Default is 30.
    plugin_restart_delay : int, optional
        Delay in seconds before attempting to restart a failed plugin.
        Defaults to environment variable PLUGIN_RESTART_DELAY or 5.
    plugin_max_restart_attempts : int, optional
        Maximum number of restart attempts for a plugin. Defaults to
        environment variable PLUGIN_MAX_RESTARTS or 3.
    plugin_health_check_interval : int, optional
        Interval in seconds between plugin health checks. Defaults to
        environment variable PLUGIN_HEALTH_CHECK_INTERVAL or 2.
    loki_url : str, optional
        Grafana Loki push API URL for log shipping. Defaults to environment
        variable LOKI_URL or empty string (Loki disabled).
    loki_enabled : bool, optional
        Enable log shipping to Grafana Loki. Defaults to environment variable
        LOKI_ENABLED or False.
    log_level : str, optional
        Logging level (TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults
        to environment variable COURIER_LOG_LEVEL or 'DEBUG'.
    production_mode : bool, optional
        Enable production mode with enforced minimum INFO log level. Defaults
        to environment variable PRODUCTION or False.

    Examples
    --------
    >>> config = ServiceConfig()
    >>> isinstance(config.service_id, str)
    True
    >>> config.heartbeat_interval
    30
    >>> config.prometheus_port >= 1024
    True
    """

    service_id: str = field(
        default_factory=lambda: os.environ.get(
            "SERVICE_ID",
            f"watcher-service-{uuid.uuid4().hex[:8]}",
        ),
    )
    namespace: str = field(
        default_factory=lambda: os.environ.get(
            "SERVICE_NAMESPACE",
            "default",
        ),
    )
    database_url: str = field(
        default_factory=lambda: os.environ.get(
            "DATABASE_URL",
            "postgresql://admin:admin@localhost:5432/courier",
        ),
    )
    prometheus_port: int = field(
        default_factory=lambda: int(os.environ.get("PROMETHEUS_PORT", "8000")),
    )
    broker_url: str = field(
        default_factory=lambda: os.environ.get(
            "BROKER_URL",
            "amqp://admin:admin@localhost:5672/",
        ),
    )
    broker_max_retries: int = field(
        default_factory=lambda: int(os.environ.get("BROKER_MAX_RETRIES", "5")),
    )
    heartbeat_interval: int = 30
    plugin_restart_delay: int = field(
        default_factory=lambda: int(os.environ.get("PLUGIN_RESTART_DELAY", "5")),
    )
    plugin_max_restart_attempts: int = field(
        default_factory=lambda: int(os.environ.get("PLUGIN_MAX_RESTARTS", "3")),
    )
    plugin_health_check_interval: int = field(
        default_factory=lambda: int(
            os.environ.get("PLUGIN_HEALTH_CHECK_INTERVAL", "2"),
        ),
    )
    loki_url: str = field(
        default_factory=lambda: os.environ.get("LOKI_URL", ""),
    )
    loki_enabled: bool = field(
        default_factory=lambda: (
            os.environ.get("LOKI_ENABLED", "false").lower() == "true"
        ),
    )
    log_level: str = field(
        default_factory=lambda: os.environ.get("COURIER_LOG_LEVEL", "DEBUG"),
    )
    production_mode: bool = field(
        default_factory=lambda: os.environ.get("PRODUCTION", "false").lower() == "true",
    )

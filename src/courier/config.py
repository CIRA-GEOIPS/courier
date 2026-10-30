"""Service configuration dataclass."""

import os
import uuid
from dataclasses import dataclass, field

from courier.errors import ConfigurationError


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
    prometheus_port : int, optional
        Port number for Prometheus metrics HTTP server. Defaults to environment
        variable PROMETHEUS_PORT or 8000.
    broker_url : str, optional
        Message broker connection URL. Defaults to environment variable BROKER_URL
        or localhost AMQP connection.
    broker_max_retries : int, optional
        Maximum retry attempts for broker operations. Defaults to environment
        variable BROKER_MAX_RETRIES or 5.
    broker_prefetch_count : int, optional
        Maximum unacknowledged messages the broker may push to one courier
        consumer (AMQP ``basic.qos``). Applies to both the file-found and the
        job-ready consumers. Defaults to environment variable
        BROKER_PREFETCH_COUNT or 1, and must be at least 1 because 0 means
        unlimited in AMQP. Raising it speeds up draining a backlog, at the cost
        of more redeliveries if a replica dies mid-drain.
    broker_max_redeliveries : int, optional
        How many further attempts a message gets after the consumer raises on
        it, before it is parked on ``<queue>-DeadLetter``. Defaults to
        environment variable BROKER_MAX_REDELIVERIES or 3, and must not be
        negative; 0 parks on the first failure. There is no unlimited setting:
        retrying one message forever is what made a single unprocessable
        message block every message behind it.
    heartbeat_interval : int, optional
        Interval in seconds between heartbeat metric updates. Default is 30.
    plugin_restart_delay : int, optional
        Delay in seconds before attempting to restart a failed plugin.
        Defaults to environment variable PLUGIN_RESTART_DELAY or 5.
    plugin_max_restart_attempts : int, optional
        Maximum number of restart attempts for a plugin. Defaults to
        environment variable PLUGIN_MAX_RESTARTS or 3. A plugin that
        exhausts them is failed for good and the service exits, so an
        orchestrator can replace the process.
    plugin_restart_window : int, optional
        Seconds a restarted plugin must stay healthy before its restart
        attempts are forgiven and the count returns to zero. Without this a
        plugin would be allowed only ``plugin_max_restart_attempts`` failures
        over the whole life of the process. Defaults to environment variable
        PLUGIN_RESTART_WINDOW or 300.
    plugin_health_check_interval : float, optional
        Interval in seconds between plugin health checks. Defaults to
        environment variable PLUGIN_HEALTH_CHECK_INTERVAL or 2.
    plugin_start_timeout : float, optional
        Seconds a plugin's ``start()`` may take before it is given up on. A
        thread blocked inside ``start()`` (a Redis that accepts and never
        answers, a broker confirm that never comes) cannot be interrupted
        from outside, so a restart would only add a second stuck thread. The
        plugin is marked failed with its restart attempts used up and the
        service exits, which is the only recovery there is. Only the
        ``start()`` call counts, not the health gate after it. Must be
        positive. Defaults to environment variable PLUGIN_START_TIMEOUT or 60.
    loki_url : str, optional
        Grafana Loki push API URL for log shipping. Defaults to environment
        variable LOKI_URL or empty string (Loki disabled).
    loki_enabled : bool, optional
        Enable log shipping to Grafana Loki. Defaults to environment variable
        LOKI_ENABLED or False.
    loki_flush_interval : float, optional
        Seconds between batched pushes to Loki. Defaults to environment
        variable LOKI_FLUSH_INTERVAL or 2. Also roughly the most logs that
        can be lost on a hard kill, since buffered records are only flushed
        on this interval (plus an atexit flush on clean shutdown).
    log_level : str, optional
        Logging level (TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults
        to environment variable COURIER_LOG_LEVEL or 'INFO'.
    production_mode : bool, optional
        Enable production mode with enforced minimum INFO log level. Defaults
        to environment variable PRODUCTION or False.
    tracing_enabled : bool, optional
        Enable OpenTelemetry tracing. Defaults to environment variable
        COURIER_TRACING_ENABLED or False. Set to "true" to enable.
    tracing_endpoint : str, optional
        OTLP collector endpoint URL. Defaults to OTEL_EXPORTER_OTLP_ENDPOINT,
        then COURIER_TRACING_ENDPOINT, then http://localhost:4318/v1/traces.
    tracing_service_name : str, optional
        OpenTelemetry service name. Defaults to COURIER_TRACING_SERVICE_NAME
        or empty string (falls back to ``service_id``).
    tracing_sample_rate : float, optional
        Trace sampling rate between 0.0 and 1.0. Defaults to
        COURIER_TRACING_SAMPLE_RATE or 1.0 (always on).
    live_reload : str, optional
        Reconfigure service configuration while running. Three options:
            - off: Does not reconfigure service configuration while running
            - hard: Stops updated service and initializes the new one.
            - blue-green: Get the new service running. If it succeeds, stop the old one.
    config_path : str, optional
        -----Testing purposes only-----
        To be routed into the Service object instead of defined explicitly
        Path to watch for live reload

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

    # this is filled in at runtime, and really should not be overridden by users.
    config_path: str = field(
        default_factory=lambda: os.environ.get(
            "SERVICE_CONFIG_PATH",
            "",
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
    broker_prefetch_count: int = field(
        default_factory=lambda: int(os.environ.get("BROKER_PREFETCH_COUNT", "1")),
    )
    broker_max_redeliveries: int = field(
        default_factory=lambda: int(os.environ.get("BROKER_MAX_REDELIVERIES", "3")),
    )
    heartbeat_interval: int = 30
    plugin_restart_delay: int = field(
        default_factory=lambda: int(os.environ.get("PLUGIN_RESTART_DELAY", "5")),
    )
    plugin_max_restart_attempts: int = field(
        default_factory=lambda: int(os.environ.get("PLUGIN_MAX_RESTARTS", "3")),
    )
    plugin_restart_window: int = field(
        default_factory=lambda: int(os.environ.get("PLUGIN_RESTART_WINDOW", "300")),
    )
    plugin_health_check_interval: float = field(
        default_factory=lambda: float(
            os.environ.get("PLUGIN_HEALTH_CHECK_INTERVAL", "2"),
        ),
    )
    plugin_start_timeout: float = field(
        default_factory=lambda: float(os.environ.get("PLUGIN_START_TIMEOUT", "60")),
    )
    loki_url: str = field(
        default_factory=lambda: os.environ.get("LOKI_URL", ""),
    )
    loki_enabled: bool = field(
        default_factory=lambda: (
            os.environ.get("LOKI_ENABLED", "false").lower() == "true"
        ),
    )
    loki_flush_interval: float = field(
        default_factory=lambda: float(
            os.environ.get("LOKI_FLUSH_INTERVAL", "2"),
        ),
    )
    log_level: str = field(
        default_factory=lambda: os.environ.get("COURIER_LOG_LEVEL", "INFO"),
    )
    production_mode: bool = field(
        default_factory=lambda: os.environ.get("PRODUCTION", "false").lower() == "true",
    )
    tracing_enabled: bool = field(
        default_factory=lambda: (
            os.environ.get("COURIER_TRACING_ENABLED", "false").lower() == "true"
        ),
    )
    tracing_endpoint: str = field(
        default_factory=lambda: os.environ.get(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            os.environ.get(
                "COURIER_TRACING_ENDPOINT",
                "http://localhost:4318/v1/traces",
            ),
        ),
    )
    tracing_service_name: str = field(
        default_factory=lambda: os.environ.get("COURIER_TRACING_SERVICE_NAME", ""),
    )
    tracing_sample_rate: float = field(
        default_factory=lambda: float(
            os.environ.get("COURIER_TRACING_SAMPLE_RATE", "1.0"),
        ),
    )
    live_reload: str = field(
        default_factory=lambda: os.environ.get("COURIER_LIVE_RELOAD", "off"),
    )

    def __post_init__(self) -> None:
        """Validate the sample rate, prefetch, redelivery and start bounds."""
        if not (0.0 <= self.tracing_sample_rate <= 1.0):
            raise ConfigurationError(
                "tracing_sample_rate must be between 0.0 and 1.0, "
                f"got {self.tracing_sample_rate}",
            )
        if self.broker_prefetch_count < 1:
            raise ConfigurationError(
                "broker_prefetch_count must be at least 1 "
                f"(0 means unlimited in AMQP and is refused), "
                f"got {self.broker_prefetch_count}",
            )
        if self.broker_max_redeliveries < 0:
            raise ConfigurationError(
                "broker_max_redeliveries must not be negative "
                "(0 parks a failing message on the first attempt), "
                f"got {self.broker_max_redeliveries}",
            )
        if self.live_reload not in {"off", "hard", "blue-green"}:
            raise ConfigurationError(
                "live reload must be one of off, hard, or blue-green, "
                f"got {self.live_reload}",
        if self.plugin_start_timeout <= 0:
            raise ConfigurationError(
                "plugin_start_timeout must be positive, "
                f"got {self.plugin_start_timeout}",
            )
        if self.loki_flush_interval <= 0:
            raise ConfigurationError(
                "loki_flush_interval must be greater than 0, "
                f"got {self.loki_flush_interval}",
            )

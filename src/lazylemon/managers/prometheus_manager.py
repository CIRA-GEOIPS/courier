"""Prometheus metrics server lifecycle management."""

import time

import prometheus_client

from lazylemon.config import ServiceConfig
from lazylemon.managers.base import ServiceManager
from lazylemon.metrics import APP_HEARTBEAT
from lazylemon.utils.decorators import log_execution
from lazylemon.utils.logging import get_logger


class PrometheusManager(ServiceManager):
    """Manages Prometheus metrics server and heartbeat metric collection.

    Handles Prometheus HTTP server lifecycle and provides heartbeat metric
    functionality for service monitoring. Uses immutable configuration and
    maintains server state.

    Parameters
    ----------
    config : ServiceConfig
        Service configuration containing Prometheus port and other settings.

    Attributes
    ----------
    _config : ServiceConfig
        Service configuration.
    _server_started : bool
        Whether the Prometheus HTTP server has been started.

    Methods
    -------
    send_heartbeat()
        Update heartbeat metric with current timestamp.

    Examples
    --------
    >>> config = ServiceConfig()
    >>> manager = PrometheusManager(config)
    >>> manager.is_healthy()
    False
    >>> manager.start()
    >>> manager.is_healthy()
    True
    """

    def __init__(self, config: ServiceConfig) -> None:
        """Initialize Prometheus manager with configuration.

        Parameters
        ----------
        config : ServiceConfig
            Service configuration.
        """
        self._config = config
        self._logger = get_logger("manager", "PrometheusManager", config)
        self._server_started = False

    @log_execution
    def start(self) -> None:
        """Start Prometheus HTTP server if not already running."""
        if not self._server_started:
            self._logger.info(
                f"Starting Prometheus server on port {self._config.prometheus_port}",
            )
            prometheus_client.start_http_server(self._config.prometheus_port)
            self._server_started = True

    def stop(self) -> None:
        """Stop Prometheus server and log shutdown."""
        self._logger.info("Prometheus manager stopped")

    def is_healthy(self) -> bool:
        """Check if Prometheus HTTP server is running.

        Returns
        -------
        bool
            True if Prometheus server has been started, False otherwise.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = PrometheusManager(config)
        >>> manager.is_healthy()
        False
        """
        return self._server_started

    def send_heartbeat(self) -> None:
        """Update heartbeat metric with current Unix timestamp."""
        current_time = time.time()
        APP_HEARTBEAT.set(current_time)
        self._logger.debug(f"Heartbeat sent at {current_time}")

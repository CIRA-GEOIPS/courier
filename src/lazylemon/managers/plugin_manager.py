"""Plugin lifecycle management, health monitoring, and auto-restart."""

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any

import prometheus_client

from lazylemon.config import ServiceConfig
from lazylemon.constants import PluginRunState
from lazylemon.interfaces.plugin_protocol import ServicePlugin
from lazylemon.managers.base import ServiceManager
from lazylemon.utils.decorators import log_execution
from lazylemon.utils.functional import filter_map
from lazylemon.utils.logging import get_logger


@dataclass
class PluginStateInfo:
    """Information about a plugin instance.

    Parameters
    ----------
    plugin : ServicePlugin
        The plugin instance.
    state : PluginRunState, default=PluginRunState.STOPPED
        Current state of the plugin.
    thread : threading.Thread or None, default=None
        Thread running the plugin, if any.
    last_health_check : datetime or None, default=None
        Timestamp of last health check.
    restart_count : int, default=0
        Number of times plugin has been restarted.
    last_restart : datetime or None, default=None
        Timestamp of last restart attempt.
    error_message : str or None, default=None
        Last error message if plugin failed.
    """

    plugin: ServicePlugin
    state: PluginRunState = PluginRunState.STOPPED
    thread: threading.Thread | None = None
    last_health_check: datetime | None = None
    restart_count: int = 0
    last_restart: datetime | None = None
    error_message: str | None = None


class PluginManager(ServiceManager):
    """Manages plugin lifecycle, health monitoring, and auto-restart functionality.

    This manager handles plugins running in separate threads, monitors their
    health, and automatically attempts to restart failed plugins according
    to configured policies.

    Parameters
    ----------
    config : ServiceConfig
        Service configuration containing plugin-related settings.
    parent_service : Any
        Reference to the parent service instance.

    Attributes
    ----------
    _config : ServiceConfig
        Service configuration.
    _plugins : dict[str, PluginStateInfo]
        Registered plugins and their state information.
    _running : bool
        Whether the plugin manager is running.

    Methods
    -------
    register_plugin(plugin, config)
        Register a new plugin with the manager.
    get_plugin_status()
        Get current status of all plugins.
    """

    def __init__(self, config: ServiceConfig, parent_service: Any) -> None:
        """Initialize plugin manager with configuration and parent service.

        Parameters
        ----------
        config : ServiceConfig
            Service configuration.
        parent_service : Any
            Reference to parent service instance.
        """
        self._config = config
        self._logger = get_logger("manager", "PluginManager", config)
        self._plugins: dict[str, PluginStateInfo] = {}
        self._lock = threading.RLock()
        self._running = False
        self._monitor_thread: threading.Thread | None = None
        self._service = parent_service

        # Metrics
        self._plugin_state_metric = prometheus_client.Gauge(
            "plugin_state",
            "Current state of plugins",
            ["plugin_name"],
        )
        self._plugin_restart_metric = prometheus_client.Counter(
            "plugin_restarts_total",
            "Total number of plugin restarts",
            ["plugin_name"],
        )
        self._plugin_health_metric = prometheus_client.Gauge(
            "plugin_health",
            "Plugin health status (1 = healthy, 0 = unhealthy)",
            ["plugin_name"],
        )

    def register_plugin(
        self,
        plugin: type[ServicePlugin],
        config: dict[str, Any],
    ) -> None:
        """Register a new plugin with the manager.

        Parameters
        ----------
        plugin : type[ServicePlugin]
            Plugin class to register. Will be instantiated with service
            reference and config.
        config : dict[str, Any]
            Configuration to pass to the plugin constructor.

        Raises
        ------
        ValueError
            If a plugin with the same name is already registered.
        """
        with self._lock:
            plugin_instance = plugin(self._service, config)
            if plugin_instance.name in self._plugins:
                raise ValueError(f"Plugin {plugin_instance.name} already registered")

            self._logger.info(plugin_instance)
            self._logger.info(plugin_instance.name)
            self._plugins[plugin_instance.name] = PluginStateInfo(
                plugin=plugin_instance,
            )
            self._logger.info(
                f"Registered plugin: {plugin_instance.name} v{plugin_instance.version}",
            )

    def _start_plugin(self, plugin_info: PluginStateInfo) -> None:
        """Start a plugin in a separate thread.

        Parameters
        ----------
        plugin_info : PluginStateInfo
            Information about the plugin to start.
        """

        def run_plugin() -> None:
            try:
                plugin_info.state = PluginRunState.STARTING
                self._logger.info(f"Starting plugin: {plugin_info.plugin.name}")

                plugin_info.plugin.start()
                plugin_info.state = PluginRunState.RUNNING
                plugin_info.error_message = None

                self._plugin_state_metric.labels(
                    plugin_name=plugin_info.plugin.name,
                ).set(plugin_info.state.value)

                self._logger.info(
                    f"Plugin started successfully: {plugin_info.plugin.name}",
                )

                while self._running and plugin_info.state == PluginRunState.RUNNING:
                    time.sleep(1)

            except Exception as e:
                plugin_info.state = PluginRunState.FAILED
                plugin_info.error_message = str(e)
                self._logger.exception(f"Plugin {plugin_info.plugin.name} failed")
                self._plugin_state_metric.labels(
                    plugin_name=plugin_info.plugin.name,
                ).set(plugin_info.state.value)

        plugin_info.thread = threading.Thread(
            target=run_plugin,
            name=f"Plugin-{plugin_info.plugin.name}",
            daemon=True,
        )
        plugin_info.thread.start()

    def _stop_plugin(self, plugin_info: PluginStateInfo) -> None:
        """Stop a plugin gracefully.

        Parameters
        ----------
        plugin_info : PluginStateInfo
            Information about the plugin to stop.
        """
        if plugin_info.state in (PluginRunState.RUNNING, PluginRunState.STARTING):
            try:
                plugin_info.state = PluginRunState.STOPPING
                plugin_info.plugin.stop()

                if plugin_info.thread and plugin_info.thread.is_alive():
                    plugin_info.thread.join(timeout=5)

                plugin_info.state = PluginRunState.STOPPED
                plugin_info.thread = None

                self._plugin_state_metric.labels(
                    plugin_name=plugin_info.plugin.name,
                ).set(plugin_info.state.value)

                self._logger.info(f"Plugin stopped: {plugin_info.plugin.name}")
            except Exception as e:
                self._logger.warning(
                    f"Error stopping plugin {plugin_info.plugin.name}: {e}",
                )

    def _monitor_plugins(self) -> None:
        """Monitor plugin health and restart failed plugins."""
        while self._running:
            with self._lock:
                for plugin_name, plugin_info in self._plugins.items():
                    try:
                        now = datetime.now()
                        if (
                            plugin_info.last_health_check is None
                            or (now - plugin_info.last_health_check).seconds
                            >= self._config.plugin_health_check_interval
                        ):
                            plugin_info.last_health_check = now

                            if plugin_info.state == PluginRunState.RUNNING:
                                is_healthy = plugin_info.plugin.is_healthy()
                                self._plugin_health_metric.labels(
                                    plugin_name=plugin_name,
                                ).set(1 if is_healthy else 0)

                                if not is_healthy:
                                    self._logger.warning(
                                        f"Plugin {plugin_name} is unhealthy",
                                    )
                                    plugin_info.state = PluginRunState.FAILED
                                    self._handle_failed_plugin(plugin_info)

                            elif (plugin_info.state == PluginRunState.RUNNING) and (
                                not plugin_info.thread  # type: ignore
                                or not plugin_info.thread.is_alive()
                            ):
                                self._logger.error(f"Plugin {plugin_name} thread died")  # type: ignore
                                plugin_info.state = PluginRunState.FAILED
                                self._handle_failed_plugin(plugin_info)

                    except Exception:
                        self._logger.exception(f"Error monitoring plugin {plugin_name}")

            time.sleep(1)

    def _handle_failed_plugin(self, plugin_info: PluginStateInfo) -> None:
        """Handle a failed plugin with restart logic.

        Parameters
        ----------
        plugin_info : PluginStateInfo
            Information about the failed plugin.
        """
        plugin_name = plugin_info.plugin.name
        now = datetime.now()

        can_restart = (
            plugin_info.restart_count < self._config.plugin_max_restart_attempts
        )

        if plugin_info.last_restart:
            time_since_restart = (now - plugin_info.last_restart).seconds
            if time_since_restart < self._config.plugin_restart_delay:
                can_restart = False

        if can_restart:
            self._logger.info(
                f"Attempting to restart plugin {plugin_name} "
                f"(attempt {plugin_info.restart_count + 1}/"
                f"{self._config.plugin_max_restart_attempts})",
            )

            plugin_info.state = PluginRunState.RESTARTING
            plugin_info.restart_count += 1
            plugin_info.last_restart = now

            self._plugin_restart_metric.labels(plugin_name=plugin_name).inc()

            self._stop_plugin(plugin_info)
            time.sleep(self._config.plugin_restart_delay)
            self._start_plugin(plugin_info)
        else:
            self._logger.error(
                f"Plugin {plugin_name} failed and cannot be restarted "
                f"(max attempts reached or too soon)",
            )
            plugin_info.state = PluginRunState.FAILED
            self._plugin_state_metric.labels(
                plugin_name=plugin_name,
            ).set(plugin_info.state.value)

    @log_execution
    def start(self) -> None:
        """Start the plugin manager and all registered plugins."""
        if self._running:
            return

        self._running = True

        self._monitor_thread = threading.Thread(
            target=self._monitor_plugins,
            name="PluginMonitor",
            daemon=True,
        )
        self._monitor_thread.start()

        with self._lock:
            for plugin_info in self._plugins.values():
                self._start_plugin(plugin_info)

    def stop(self) -> None:
        """Stop all plugins and the plugin manager."""
        self._running = False

        with self._lock:
            stop_tasks = [
                partial(self._stop_plugin, plugin_info)
                for plugin_info in self._plugins.values()
            ]
            [f() for f in stop_tasks]

        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)

        self._logger.info("Plugin manager stopped")

    def is_healthy(self) -> bool:
        """Check if plugin manager is healthy.

        Returns True if running and at least one plugin is healthy,
        or if not running (valid state), or if no plugins are registered.

        Returns
        -------
        bool
            True if manager is healthy, False otherwise.
        """
        if not self._running:
            return True

        with self._lock:
            health = [
                f"{info.plugin.name} is {info.state} and {info.plugin.is_healthy()}"
                for info in self._plugins.values()
            ]
            self._logger.debug(", ".join(health))
            healthy_plugins = filter_map(
                lambda info: (
                    info.state in [PluginRunState.RUNNING, PluginRunState.STARTING]
                ),
                lambda info: info.plugin.is_healthy(),
                self._plugins.values(),
            )
            return any(healthy_plugins) if self._plugins else True

    def get_plugin_status(self) -> dict[str, dict[str, Any]]:
        """Get current status of all plugins.

        Returns
        -------
        dict[str, dict[str, Any]]
            Dictionary mapping plugin names to their status information.
        """
        with self._lock:
            return {
                name: {
                    "state": info.state.name,
                    "version": info.plugin.version,
                    "restart_count": info.restart_count,
                    "last_restart": (
                        info.last_restart.isoformat() if info.last_restart else None
                    ),
                    "error": info.error_message,
                    "metrics": (
                        info.plugin.get_metrics()
                        if info.state == PluginRunState.RUNNING
                        else {}
                    ),
                }
                for name, info in self._plugins.items()
            }

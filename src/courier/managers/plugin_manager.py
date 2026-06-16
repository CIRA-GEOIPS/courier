"""Plugin lifecycle management, health monitoring, and auto-restart."""

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from courier.config import ServiceConfig
from courier.constants import PluginRunState
from courier.errors import CourierError
from courier.interfaces.module_based.dispatchers import Dispatcher
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.managers.base import ServiceManager
from courier.metrics import (
    PLUGIN_HEALTH,
    PLUGIN_REGISTRATION_FAILURES,
    PLUGIN_RESTARTS,
    PLUGIN_STATE,
)
from courier.tracing import (
    ATTR_PLUGIN_NAME,
    get_tracer,
)
from courier.utils.decorators import log_execution
from courier.utils.functional import filter_map
from courier.utils.logging import get_logger


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
    ready : threading.Event
        Event set when the plugin reaches RUNNING state.
    """

    plugin: ServicePlugin
    state: PluginRunState = PluginRunState.STOPPED
    thread: threading.Thread | None = None
    last_health_check: datetime | None = None
    restart_count: int = 0
    last_restart: datetime | None = None
    error_message: str | None = None
    ready: threading.Event = field(default_factory=threading.Event)


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
        self._state = PluginRunState.STOPPED
        self._monitor_thread: threading.Thread | None = None
        self._service = parent_service

        self._plugin_state_metric = PLUGIN_STATE
        self._plugin_restart_metric = PLUGIN_RESTARTS
        self._plugin_health_metric = PLUGIN_HEALTH
        self._registration_failures_metric = PLUGIN_REGISTRATION_FAILURES

        self._tracer = get_tracer(__name__)

    @staticmethod
    def _plugin_identifier(plugin: ServicePlugin) -> str:
        """Return the config identifier or type name for *plugin*."""
        return getattr(plugin, "identifier", plugin.name)

    def register_plugin(
        self,
        plugin: type[ServicePlugin],
        config: dict[str, Any],
        identifier: str | None = None,
    ) -> None:
        """Register a new plugin with the manager.

        Parameters
        ----------
        plugin : type[ServicePlugin]
            Plugin class to register. Will be instantiated with service
            reference and config.
        config : dict[str, Any]
            Configuration to pass to the plugin constructor.
        identifier : str or None, optional
            ``spec.run[*].identifier`` from the service YAML.  Passed to
            the plugin constructor as the ``identifier`` keyword
            argument when supplied — required for dispatchers so they
            can consume from their per-identifier queue.

        Raises
        ------
        ValueError
            If a plugin with the same registry key is already registered.
        """
        with self._lock:
            kwargs: dict[str, Any] = {}
            if identifier is not None and issubclass(plugin, Dispatcher):
                kwargs["identifier"] = identifier
                self._logger.debug(f"Registering plugin with identifier: {identifier}")
            plugin_instance = plugin(self._service, config, **kwargs)
            registry_key = identifier or plugin_instance.name
            if registry_key in self._plugins:
                self._registration_failures_metric.labels(
                    plugin_name=plugin_instance.name,
                    plugin_identifier=self._plugin_identifier(plugin_instance),
                    reason="duplicate_key",
                ).inc()
                raise ValueError(f"Plugin {registry_key} already registered")

            plugin_name = plugin_instance.name

            self._logger.info(plugin_instance)
            self._logger.info(registry_key)
            self._plugins[registry_key] = PluginStateInfo(
                plugin=plugin_instance,
            )

            # Emit initial metric values so the series exists from container
            # start, before run_plugin transitions them to RUNNING / healthy.
            plugin_ident = self._plugin_identifier(plugin_instance)
            self._plugin_state_metric.labels(
                plugin_name=plugin_name,
                plugin_identifier=plugin_ident,
            ).set(PluginRunState.STARTING.value)
            self._plugin_health_metric.labels(
                plugin_name=plugin_name,
                plugin_identifier=plugin_ident,
            ).set(0)

            self._logger.info(
                f"Registered plugin: {registry_key} "
                f"(class={plugin_name} v{plugin_instance.version})",
            )

    def _start_plugin(self, plugin_info: PluginStateInfo) -> None:
        """Start a plugin in a separate thread.

        Parameters
        ----------
        plugin_info : PluginStateInfo
            Information about the plugin to start.
        """

        def run_plugin() -> None:
            health_warn_interval = 5.0
            _last_health_warn: float = 0.0

            try:
                plugin_info.plugin.start()

                self._logger.debug(
                    "plugin.start() returned for %s; entering health gate",
                    plugin_info.plugin.name,
                )

                # Wait for plugin to report healthy before declaring RUNNING.
                # Timeout after health_check_interval: proceed to RUNNING,
                # let monitor catch failures.
                deadline = time.time() + self._config.plugin_health_check_interval
                healthy = False
                while time.time() < deadline:
                    try:
                        if plugin_info.plugin.is_healthy():
                            healthy = True
                            break
                    except Exception:
                        now = time.time()
                        if now - _last_health_warn > health_warn_interval:
                            _last_health_warn = now
                            self._logger.warning(
                                "Health check for %s raised an exception; "
                                "retrying...",
                                plugin_info.plugin.name,
                            )
                    time.sleep(0.1)

                if not healthy:
                    self._logger.warning(
                        "Plugin %s did not report healthy within deadline; "
                        "proceeding to RUNNING, monitor will catch failures.",
                        plugin_info.plugin.name,
                    )

                with self._lock:
                    plugin_info.state = PluginRunState.RUNNING
                    plugin_info.last_health_check = None
                    plugin_info.error_message = None

                self._plugin_state_metric.labels(
                    plugin_name=plugin_info.plugin.name,
                    plugin_identifier=self._plugin_identifier(plugin_info.plugin),
                ).set(plugin_info.state.value)

                self._logger.info(
                    f"Plugin started successfully: {plugin_info.plugin.name}",
                )

                span = self._tracer.start_span("plugin_lifecycle")
                span.add_event(
                    "plugin.started",
                    attributes={
                        ATTR_PLUGIN_NAME: plugin_info.plugin.name,
                    },
                )
                span.end()

                plugin_info.ready.set()

                while (
                    self._state == PluginRunState.RUNNING
                    and plugin_info.state == PluginRunState.RUNNING
                ):
                    time.sleep(1)

            except CourierError as e:
                with self._lock:
                    plugin_info.state = PluginRunState.FAILED
                    plugin_info.error_message = str(e)
                self._logger.exception(f"Plugin {plugin_info.plugin.name} failed")
                self._plugin_state_metric.labels(
                    plugin_name=plugin_info.plugin.name,
                    plugin_identifier=self._plugin_identifier(plugin_info.plugin),
                ).set(plugin_info.state.value)
            except Exception as e:
                with self._lock:
                    plugin_info.state = PluginRunState.FAILED
                    plugin_info.error_message = f"Unexpected error: {e}"
                self._logger.exception(
                    f"Plugin {plugin_info.plugin.name} encountered an unexpected error",
                )
                self._plugin_state_metric.labels(
                    plugin_name=plugin_info.plugin.name,
                    plugin_identifier=self._plugin_identifier(plugin_info.plugin),
                ).set(plugin_info.state.value)

        with self._lock:
            plugin_info.state = PluginRunState.STARTING
            plugin_info.last_health_check = None
            plugin_info.error_message = None
            plugin_info.ready.clear()
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
        should_stop = False
        thread_to_join = None

        with self._lock:
            if plugin_info.state in (
                PluginRunState.RUNNING,
                PluginRunState.STARTING,
                PluginRunState.RESTARTING,
            ):
                plugin_info.state = PluginRunState.STOPPING
                should_stop = True
                if plugin_info.thread and plugin_info.thread.is_alive():
                    thread_to_join = plugin_info.thread

        if not should_stop:
            return

        try:
            plugin_info.plugin.stop()

            if thread_to_join:
                thread_to_join.join(timeout=5)
        except CourierError as e:
            self._logger.warning(
                f"Error stopping plugin {plugin_info.plugin.name}: {e}",
            )
        except Exception:
            self._logger.exception(
                f"Unexpected error stopping plugin {plugin_info.plugin.name}",
            )

        with self._lock:
            plugin_info.state = PluginRunState.STOPPED
            plugin_info.thread = None
            self._plugin_state_metric.labels(
                plugin_name=plugin_info.plugin.name,
                plugin_identifier=self._plugin_identifier(plugin_info.plugin),
            ).set(plugin_info.state.value)

        self._logger.info(f"Plugin stopped: {plugin_info.plugin.name}")

        span = self._tracer.start_span("plugin_lifecycle")
        span.add_event(
            "plugin.stopped",
            attributes={
                ATTR_PLUGIN_NAME: plugin_info.plugin.name,
            },
        )
        span.end()

    def _monitor_plugins(self) -> None:
        """Monitor plugin health and restart failed plugins."""
        while self._state == PluginRunState.RUNNING:
            failed_plugins: list[PluginStateInfo] = []

            with self._lock:
                for plugin_name, plugin_info in self._plugins.items():
                    try:
                        now = datetime.now()
                        if plugin_info.last_health_check is not None and (
                            (now - plugin_info.last_health_check).seconds
                            < self._config.plugin_health_check_interval
                        ):
                            continue

                        plugin_info.last_health_check = now

                        if plugin_info.state == PluginRunState.RUNNING:
                            if (
                                not plugin_info.thread
                                or not plugin_info.thread.is_alive()
                            ):
                                self._logger.error(
                                    f"Plugin {plugin_name} thread died",
                                )
                                plugin_info.state = PluginRunState.FAILED
                                failed_plugins.append(plugin_info)
                            else:
                                is_healthy = plugin_info.plugin.is_healthy()
                                self._plugin_health_metric.labels(
                                    plugin_name=plugin_info.plugin.name,
                                    plugin_identifier=self._plugin_identifier(plugin_info.plugin),
                                ).set(1 if is_healthy else 0)

                                if not is_healthy:
                                    self._logger.warning(
                                        f"Plugin {plugin_name} is unhealthy",
                                    )
                                    plugin_info.state = PluginRunState.FAILED
                                    failed_plugins.append(plugin_info)

                                    span = self._tracer.start_span(
                                        "plugin_lifecycle",
                                    )
                                    span.add_event(
                                        "plugin.health_check_failed",
                                        attributes={
                                            ATTR_PLUGIN_NAME: (
                                                plugin_info.plugin.name
                                            ),
                                        },
                                    )
                                    span.end()

                    except CourierError:
                        self._logger.exception(
                            f"Error monitoring plugin {plugin_name}",
                        )
                    except Exception:
                        self._logger.exception(
                            f"Unexpected error monitoring plugin {plugin_name}",
                        )

            for plugin_info in failed_plugins:
                self._handle_failed_plugin(plugin_info)

            time.sleep(1)

    def _handle_failed_plugin(self, plugin_info: PluginStateInfo) -> None:
        """Handle a failed plugin with restart logic.

        Parameters
        ----------
        plugin_info : PluginStateInfo
            Information about the failed plugin.
        """
        if self._state != PluginRunState.RUNNING:
            return

        plugin_name = plugin_info.plugin.name
        plugin_ident = self._plugin_identifier(plugin_info.plugin)
        now = datetime.now()

        with self._lock:
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

                span = self._tracer.start_span("plugin_lifecycle")
                span.add_event(
                    "plugin.restarting",
                    attributes={
                        ATTR_PLUGIN_NAME: plugin_name,
                    },
                )
                span.end()

                plugin_info.state = PluginRunState.RESTARTING
                plugin_info.restart_count += 1
                plugin_info.last_restart = now

                self._plugin_restart_metric.labels(
                    plugin_name=plugin_name,
                    plugin_identifier=plugin_ident,
                ).inc()
            else:
                self._logger.error(
                    f"Plugin {plugin_name} failed and cannot be restarted "
                    f"(max attempts reached or too soon)",
                )
                plugin_info.state = PluginRunState.FAILED
                self._plugin_state_metric.labels(
                    plugin_name=plugin_name,
                    plugin_identifier=plugin_ident,
                ).set(plugin_info.state.value)

        if can_restart:
            self._stop_plugin(plugin_info)
            time.sleep(self._config.plugin_restart_delay)
            self._start_plugin(plugin_info)

    @log_execution
    def start(self) -> None:
        """Start the plugin manager and all registered plugins."""
        if self._state == PluginRunState.RUNNING:
            return

        self._state = PluginRunState.RUNNING

        self._monitor_thread = threading.Thread(
            target=self._monitor_plugins,
            name="PluginMonitor",
            daemon=True,
        )
        self._monitor_thread.start()

        # Phase 1: Spawn all plugin threads under lock.
        with self._lock:
            for plugin_info in self._plugins.values():
                self._start_plugin(plugin_info)

        # Phase 2: Wait for all plugins to signal readiness outside lock.
        deadline = time.time() + self._config.plugin_health_check_interval
        with self._lock:
            plugins = list(self._plugins.values())
        for plugin_info in plugins:
            remaining = max(0.0, deadline - time.time())
            plugin_info.ready.wait(timeout=remaining)

        # Phase 3: Verify at least one plugin started successfully.
        with self._lock:
            running = [info for info in plugins
                       if info.state == PluginRunState.RUNNING]
            failed = [info for info in plugins
                      if info.state == PluginRunState.FAILED]
        if failed and not running:
            names = ", ".join(
                f"{info.plugin.name}={info.error_message or 'unknown'}"
                for info in failed
            )
            raise RuntimeError(f"All plugins failed to start: {names}")
        for info in plugins:
            if not info.ready.is_set() and info.state != PluginRunState.FAILED:
                self._logger.warning(
                    "Plugin %s did not signal readiness within deadline; "
                    "monitor will catch failures.",
                    info.plugin.name,
                )

    def stop(self) -> None:
        """Stop all plugins and the plugin manager."""
        self._state = PluginRunState.STOPPED

        with self._lock:
            plugins = list(self._plugins.values())

        for plugin_info in plugins:
            self._stop_plugin(plugin_info)

        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)

        self._logger.info("Plugin manager stopped")

    def get_plugins(self) -> dict[str, PluginStateInfo]:
        """Return a snapshot of all registered plugins and their state.

        The returned dict is a shallow copy taken under the internal lock,
        safe for read-only access by external callers such as
        :class:`~courier.service.Service` during preflight routing discovery.

        Returns
        -------
        dict[str, PluginStateInfo]
            Mapping from registry key to plugin state information.
        """
        with self._lock:
            return self._plugins.copy()

    def is_healthy(self) -> bool:
        """Check if plugin manager is healthy.

        Returns True if running and at least one plugin is healthy,
        or if not running and no plugins are registered, or if no
        plugins are registered at all.

        Returns
        -------
        bool
            True if manager is healthy, False otherwise.
        """
        with self._lock:
            if self._state != PluginRunState.RUNNING:
                # If plugins are registered but not started, not healthy.
                # If no plugins are registered, nothing to monitor — healthy.
                return not bool(self._plugins)

            health = [
                f"{info.plugin.name} is {info.state} and {info.plugin.is_healthy()}"
                for info in self._plugins.values()
            ]
            self._logger.debug(", ".join(health))
            healthy_plugins = filter_map(
                lambda info: (
                    info.state in [PluginRunState.RUNNING, PluginRunState.STARTING]
                    and info.thread is not None
                    and info.thread.is_alive()
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

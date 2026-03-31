"""Service orchestrator: coordinates plugins, broker, and managers."""

import time
from collections.abc import Generator
from typing import Any

from lazylemon.broker.kombu import MessageBrokerManager, declare_queue, publish
from lazylemon.broker.kombu import messages as broker_messages
from lazylemon.config import ServiceConfig
from lazylemon.interfaces.plugin_protocol import ServicePlugin
from lazylemon.managers.base import ServiceManager
from lazylemon.managers.plugin_manager import PluginManager
from lazylemon.managers.prometheus_manager import PrometheusManager
from lazylemon.metrics import SERVICE_HEALTH, SERVICE_UPTIME
from lazylemon.utils.decorators import log_execution
from lazylemon.utils.logging import get_logger
from lazylemon.utils.signals import SignalHandler


class Service:
    """Service class with plugin support.

    Coordinates startup, health monitoring, heartbeat loop, and graceful shutdown
    of all service components including plugins. Uses dependency injection for
    manager instances and provides centralized service lifecycle management.

    Parameters
    ----------
    config : ServiceConfig or None, optional
        Service configuration. If None, creates default ServiceConfig instance.

    Attributes
    ----------
    namespace : str
        Service namespace for resource isolation.

    Methods
    -------
    emit(queue, message)
        Publish a message to a message broker queue.
    consume(queue)
        Yield messages from a message broker queue.
    register_plugin(plugin, config)
        Register a plugin with the service.
    start()
        Start service with complete lifecycle management.

    Examples
    --------
    >>> config = ServiceConfig()
    >>> service = Service(config)
    >>> service._config.heartbeat_interval
    10
    >>> len(service._managers)
    3
    """

    def __init__(self, config: ServiceConfig | None = None) -> None:
        """Initialize service with configuration and managers.

        Parameters
        ----------
        config : ServiceConfig or None, optional
            Service configuration. If None, uses default ServiceConfig.
        """
        self._config = config or ServiceConfig()
        self._logger = get_logger("service", self._config.service_id, self._config)
        self._signal_handler = SignalHandler()

        self._prometheus_manager = PrometheusManager(self._config)
        self._broker_manager = MessageBrokerManager(
            self._config,
            stop_event=self._signal_handler.stop_event,
        )
        self._plugin_manager = PluginManager(self._config, self)

        self._managers: list[ServiceManager] = [
            self._prometheus_manager,
            self._broker_manager,
            self._plugin_manager,
        ]
        self.namespace = "default"
        self._start_time = time.time()

        self._service_uptime_metric = SERVICE_UPTIME
        self._service_health_metric = SERVICE_HEALTH

    @log_execution
    def emit(self, queue: str, message: str) -> None:
        """Publish a message to a message broker queue.

        Parameters
        ----------
        queue : str
            Name of the queue to publish to.
        message : str
            Message content to publish.
        """
        queue_name = self._broker_manager.add_queue(
            queue,
            durable=True,
            exclusive=False,
        )
        with self._broker_manager.get_connection_context() as conn:
            self._logger.debug(f"Emitting message to queue '{queue_name}': {message}")
            q = declare_queue(conn, queue_name, durable=True)
            publish(conn, q, message)

    def consume(self, queue: str) -> Generator[str, None, None]:
        """Yield messages from a message broker queue.

        Parameters
        ----------
        queue : str
            The name of the queue to consume messages from.

        Yields
        ------
        str
            The decoded message content from the queue.
        """
        queue_name = self._broker_manager.add_queue(
            queue,
            durable=True,
            exclusive=False,
        )

        self._logger.debug(f"Consuming from queue: {queue_name}")

        with self._broker_manager.get_connection_context() as conn:
            q = declare_queue(conn, queue_name, durable=True)
            for body, ack, reject in broker_messages(conn, q):
                try:
                    self._logger.debug(
                        f"Received message from queue '{queue_name}': {body}",
                    )
                    yield body
                    ack()
                except GeneratorExit:
                    reject()
                    raise
                except Exception:
                    reject()
                    raise

    def register_plugin(
        self,
        plugin: type[ServicePlugin],
        config: dict[str, Any],
    ) -> None:
        """Register a plugin with the service.

        Parameters
        ----------
        plugin : type[ServicePlugin]
            Plugin class to register.
        config : dict[str, Any]
            Configuration dictionary for the plugin.
        """
        self._plugin_manager.register_plugin(plugin, config)

    def _start_managers(self) -> None:
        """Start all service managers in sequence with error handling."""

        def start_manager(manager: ServiceManager) -> None:
            try:
                manager.start()
            except Exception:
                self._logger.exception(f"Failed to start {manager.__class__.__name__}")
                raise

        list(map(start_manager, self._managers))

    def _stop_managers(self) -> None:
        """Stop all managers safely in reverse order."""

        def stop_manager(manager: ServiceManager) -> None:
            try:
                manager.stop()
            except Exception as e:
                self._logger.warning(
                    f"Error stopping {manager.__class__.__name__}: {e}",
                )

        list(map(stop_manager, reversed(self._managers)))

    def _health_check(self) -> bool:
        """Check health status of all service managers."""
        self._logger.debug(
            "Monitor health checks: "
            + ", ".join(
                f"{manager}: {manager.is_healthy()}" for manager in self._managers
            ),
        )
        return all(manager.is_healthy() for manager in self._managers)

    def _run_heartbeat_loop(self) -> None:
        """Execute main heartbeat loop with interruptable sleep intervals."""
        sleep_time = 1.0
        sleep_iterations = int(self._config.heartbeat_interval / sleep_time)

        while not self._signal_handler.shutdown_requested:
            for _ in range(sleep_iterations):
                if self._signal_handler.shutdown_requested:
                    break  # type: ignore
                time.sleep(sleep_time)

            if not self._signal_handler.shutdown_requested:
                self._prometheus_manager.send_heartbeat()

                self._service_uptime_metric.set(time.time() - self._start_time)
                self._service_health_metric.set(1 if self._health_check() else 0)

                plugin_status = self._plugin_manager.get_plugin_status()
                if plugin_status:
                    self._logger.debug(f"Plugin status: {plugin_status}")

    @log_execution
    def start(self) -> None:
        """Start service with complete lifecycle management and error handling."""
        self._logger.info(f"Starting Service {self._config.service_id}")

        try:
            self._start_managers()

            if not self._health_check():
                raise RuntimeError("Service health check failed after startup")  # noqa: TRY301

            self._logger.info(f"Service {self._config.service_id} started successfully")
            self._run_heartbeat_loop()

        except KeyboardInterrupt:
            self._logger.info("Received keyboard interrupt")
        except Exception:
            self._logger.exception("Service startup failed")
            raise
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        """Perform complete resource cleanup for service shutdown."""
        self._logger.info("Cleaning up resources...")
        self._stop_managers()
        self._logger.info(f"Service {self._config.service_id} stopped")


def create_service_with_plugins(
    config: ServiceConfig | None = None,
    plugins: list[tuple[type[ServicePlugin], dict[str, Any]]] | None = None,
) -> Service:
    """Create new Service instance with optional configuration and plugins.

    Parameters
    ----------
    config : ServiceConfig or None, optional
        Service configuration.
    plugins : list of tuple[type[ServicePlugin], dict[str, Any]] or None, optional
        List of (plugin_class, config) tuples to register with the service.

    Returns
    -------
    Service
        Configured service instance ready for startup.

    Examples
    --------
    >>> service = create_service_with_plugins()
    >>> isinstance(service, Service)
    True
    """
    service = Service(config)

    if plugins is not None:
        for plugin_class, plugin_config in plugins:
            service.register_plugin(plugin_class, plugin_config)

    return service

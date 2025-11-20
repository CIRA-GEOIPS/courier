"""Service manager.

Manager with Prometheus metrics, RabbitMQ integration, and plugin support.
"""

import logging
import os
import pickle
import signal
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from functools import partial, reduce, wraps
from typing import Any, Protocol, TypeVar

import pika
import prometheus_client
from pika.exceptions import AMQPConnectionError

from geoips_driver import interfaces

# Type variables for generic type hints
T = TypeVar("T")
R = TypeVar("R")


class PluginRunState(Enum):
    """Enumeration of possible plugin states."""

    STOPPED = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    FAILED = auto()
    RESTARTING = auto()


# Functional programming utilities
def compose(*functions: Callable) -> Callable:
    """Compose functions from right to left.

    Examples
    --------
    >>> add_one = lambda x: x + 1
    >>> multiply_two = lambda x: x * 2
    >>> composed = compose(add_one, multiply_two)
    >>> composed(3)  # (3 * 2) + 1
    7
    """
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(*functions: Callable) -> Callable:
    """Pipe functions from left to right.

    Examples
    --------
    >>> add_one = lambda x: x + 1
    >>> multiply_two = lambda x: x * 2
    >>> piped = pipe(add_one, multiply_two)
    >>> piped(3)  # (3 + 1) * 2
    8
    """
    return reduce(lambda f, g: lambda x: g(f(x)), functions, lambda x: x)


def maybe(default: T) -> Callable[[T | None], T]:
    """Return value or default if None.

    Examples
    --------
    >>> maybe_zero = maybe(0)
    >>> maybe_zero(None)
    0
    >>> maybe_zero(5)
    5
    """
    return lambda x: x if x is not None else default


def filter_map(
    predicate: Callable[[T], bool],
    transform: Callable[[T], R],
    items: Iterable[T],
) -> list[R]:
    """Filter and map in a single operation.

    Examples
    --------
    >>> filter_map(lambda x: x % 2 == 0, lambda x: x * 2, [1, 2, 3, 4])
    [4, 8]
    """
    return [transform(item) for item in items if predicate(item)]


# Configuration
@dataclass(frozen=True)
class ServiceConfig:
    """Immutable service configuration with environment variable defaults.

    This configuration class provides default values from environment variables
    for service initialization. All fields are frozen to ensure immutability
    after instantiation.

    Parameters
    ----------
    service_id : str
        Unique identifier for this service instance. Defaults to environment
        variable SERVICE_ID or auto-generated UUID-based identifier.
    database_url : str
        PostgreSQL database connection URL. Defaults to environment variable
        DATABASE_URL or localhost connection.
    prometheus_port : int
        Port number for Prometheus metrics HTTP server. Defaults to environment
        variable PROMETHEUS_PORT or 8000.
    rabbitmq_url : str
        RabbitMQ connection URL. Defaults to environment variable RABBITMQ_URL
        or localhost connection.
    rabbitmq_max_retries : int
        Maximum retry attempts for RabbitMQ operations. Defaults to environment
        variable RABBITMQ_MAX_RETRIES or 5.
    heartbeat_interval : int
        Interval in seconds between heartbeat metric updates.
    plugin_restart_delay : int
        Delay in seconds before attempting to restart a failed plugin.
    plugin_max_restart_attempts : int
        Maximum number of restart attempts for a plugin.
    plugin_health_check_interval : int
        Interval in seconds between plugin health checks.

    Examples
    --------
    >>> config = ServiceConfig()
    >>> isinstance(config.service_id, str)
    True
    >>> config.heartbeat_interval
    10
    >>> config.prometheus_port >= 1024
    True
    """

    service_id: str = field(
        default_factory=lambda: os.environ.get(
            "SERVICE_ID",
            f"watcher-service-{uuid.uuid4().hex[:8]}",
        ),
    )
    service_namespace: str = field(
        default_factory=lambda: os.environ.get(
            "SERVICE_NAMESPACE",
            "default",
        ),
    )
    database_url: str = field(
        default_factory=lambda: os.environ.get(
            "DATABASE_URL",
            "postgresql://admin:admin@localhost:5432/geoips_driver",
        ),
    )
    prometheus_port: int = field(
        default_factory=lambda: int(os.environ.get("PROMETHEUS_PORT", "8000")),
    )
    rabbitmq_url: str = field(
        default_factory=lambda: os.environ.get(
            "RABBITMQ_URL",
            "amqp://admin:admin@localhost:5672/",
        ),
    )
    rabbitmq_max_retries: int = field(
        default_factory=lambda: int(os.environ.get("RABBITMQ_MAX_RETRIES", "5")),
    )
    heartbeat_interval: int = 10
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


def setup_logging(name: str | None = None) -> logging.Logger:
    """Configure logger with standardized formatting and return module logger.

    Sets up basic logging configuration with INFO level and timestamp formatting
    for the entire application, then returns a logger instance.

    Returns
    -------
    logging.Logger
        Configured logger instance.

    Examples
    --------
    >>> logger = setup_logging()
    >>> logger.name == '__main__'
    True
    >>> isinstance(logger, logging.Logger)
    True
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger(name if name else __name__)


logger = setup_logging()


def retry_with_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable:
    """Create retry decorator with exponential backoff for transient failures.

    Returns a decorator that retries function execution on specified exceptions
    with exponentially increasing delay between attempts.

    Parameters
    ----------
    max_retries : int, default 5
        Maximum number of retry attempts before giving up.
    base_delay : float, default 1.0
        Initial delay in seconds, doubled after each failure.
    exceptions : tuple of Exception, default (Exception,)
        Exception types that trigger retry attempts.

    Returns
    -------
    Callable
        Decorator function that wraps target functions with retry logic.

    Examples
    --------
    >>> @retry_with_backoff(max_retries=2, base_delay=0.1)
    ... def unstable_function():
    ...     return "success"
    >>> result = unstable_function()
    >>> result
    'success'
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries - 1:
                        logger.exception(
                            f"Max retries ({max_retries}) reached for {func.__name__}",
                        )
                        raise

                    wait_time = base_delay * (2**attempt)
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {wait_time} seconds...",
                    )
                    time.sleep(wait_time)
            return None

        return wrapper

    return decorator


def log_execution(func: Callable) -> Callable:
    """Create decorator for logging function execution and exceptions.

    Wraps functions to log debug messages on entry/success and exception
    details on failure, then re-raises exceptions for proper error handling.

    Parameters
    ----------
    func : Callable
        Function to be wrapped with execution logging.

    Returns
    -------
    Callable
        Wrapped function with execution logging behavior.

    Examples
    --------
    >>> @log_execution
    ... def sample_function(x):
    ...     return x * 2
    >>> result = sample_function(5)
    >>> result
    10
    """

    @wraps(func)
    def wrapper(*args: list[Any], **kwargs: dict[str, Any]) -> Any:
        logger.debug(f"Executing {func.__name__}")
        try:
            result = func(*args, **kwargs)
            logger.debug(f"Successfully executed {func.__name__}")
            return result
        except Exception:
            logger.exception(f"Error in {func.__name__}")

    return wrapper


class ServicePlugin(Protocol):
    """Protocol defining the interface that all plugins must implement."""

    @property
    def name(self) -> str:
        """Return the plugin name."""
        ...

    @property
    def version(self) -> str:
        """Return the plugin version."""
        ...

    def start(self) -> None:
        """Start the plugin operations."""
        ...

    def stop(self) -> None:
        """Stop the plugin operations."""
        ...

    def is_healthy(self) -> bool:
        """Check if the plugin is healthy."""
        ...

    def get_metrics(self) -> dict[str, Any]:
        """Return plugin-specific metrics."""
        ...


class Service:
    """Service class with plugin support.

    Coordinates startup, health monitoring, heartbeat loop, and graceful shutdown
    of all service components including plugins. Uses dependency injection for
    manager instances and provides centralized service lifecycle management.

    Parameters
    ----------
    config : ServiceConfig, optional
        Service configuration. If None, creates default ServiceConfig instance.

    Examples
    --------
    >>> config = ServiceConfig()
    >>> service = Service(config)
    >>> service._config.heartbeat_interval
    10
    >>> len(service._managers)
    3
    """

    def __init__(self, config: ServiceConfig | None = None):
        self._config = config or ServiceConfig()
        self._signal_handler = SignalHandler()

        self._prometheus_manager = PrometheusManager(self._config)
        self._rabbitmq_manager = RabbitMQManager(self._config)
        self._plugin_manager = PluginManager(self._config, self)

        self._managers: list[ServiceManager] = [
            self._prometheus_manager,
            self._rabbitmq_manager,
            self._plugin_manager,
        ]
        self.namespace = "default"

    @log_execution
    def emit(self, queue: str, message: str) -> None:
        """Publish a message to a message broker queue."""
        self._rabbitmq_manager.add_queue(queue, durable=True, exclusive=False)
        with self._rabbitmq_manager.get_connection_context() as (connection, channel):
            channel.basic_publish(exchange="", routing_key=queue, body=message)

    # @log_execution
    def consume(self, queue: str) -> Generator[bytes, None, None]:
        """Yield messages from a message broker queue.

        This returns a generator that yields messages from the specified queue.
        It handles message acknowledgment on successful processing and
        requeues messages if processing fails or the consumer stops.

        Parameters
        ----------
        queue : str
            The name of the queue to consume messages from. If the queue doesn't
            exist, it will be created.

        Yields
        ------
        bytes
            The message

        Raises
        ------
        GeneratorExit
            Raised when the generator is explicitly closed. Messages are requeued,
            generator is closed and exception is re-raised.
        Exception
            Messages are requeued and exception is re-raised.

        Examples
        --------
        >>> for message in consume('my_queue'):
        ...     data = json.loads(message)
        ...     print(f"Processing: {data}")
        ...     # Message is auto-acked after this

        >>> # Stop consuming after N messages
        >>> consumer_gen = self.consume('my_queue')
        >>> for i, message in enumerate(consumer_gen):
        ...     if i >= 10:
        ...         consumer_gen.close() # MUST manually close or generator will remain open
        ...         break
        ...     do_thing_with_message(message)
        """
        if queue not in self._rabbitmq_manager._queues:
            queue = self._rabbitmq_manager.add_queue(
                queue,
                durable=True,
                exclusive=False,
            )

        with self._rabbitmq_manager.get_connection_context() as (connection, channel):
            for method_frame, properties, body in channel.consume(
                queue,
                auto_ack=False,
            ):
                try:
                    message = pickle.loads(body)
                    yield message
                    # Acknowledge the message after successful processing
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                except GeneratorExit:
                    # Handle generator cleanup (when consumer stops)
                    channel.basic_nack(
                        delivery_tag=method_frame.delivery_tag,
                        requeue=True,
                    )
                    channel.cancel()
                    raise
                except Exception:
                    # If processing fails, reject + requeue message for another day
                    channel.basic_nack(
                        delivery_tag=method_frame.delivery_tag,
                        requeue=True,
                    )
                    raise

    def register_plugin(self, plugin: ServicePlugin, config: dict[str, Any]) -> None:
        """Register a plugin with the service.

        Parameters
        ----------
        plugin : PluginProtocol
            Plugin instance to register.
        config : dict[str, Any]
            Configuration for the plugin.

        Examples
        --------
        >>> from unittest.mock import Mock
        >>> service = Service()
        >>> mock_plugin = Mock(spec=PluginProtocol)
        >>> mock_plugin.name = "test_plugin"
        >>> service.register_plugin(mock_plugin, {})
        """
        self._plugin_manager.register_plugin(plugin, config)

    def _start_managers(self) -> None:
        """Start all service managers in sequence with error handling.

        Iterates through all managers and starts each one. If any manager
        fails to start, logs the exception and re-raises to halt service startup.

        Raises
        ------
        Exception
            If any manager fails to start successfully.
        """

        def start_manager(manager: ServiceManager) -> None:
            try:
                manager.start()
            except Exception:
                logger.exception(f"Failed to start {manager.__class__.__name__}")
                raise

        list(map(start_manager, self._managers))

    def _stop_managers(self) -> None:
        """Stop all managers safely in reverse order.

        Stops managers in reverse order to ensure proper dependency cleanup.
        Logs warnings for stop failures but continues stopping remaining managers.
        """

        def stop_manager(manager: ServiceManager) -> None:
            try:
                manager.stop()
            except Exception as e:
                logger.warning(f"Error stopping {manager.__class__.__name__}: {e}")

        list(map(stop_manager, reversed(self._managers)))

    def _health_check(self) -> bool:
        """Check health status of all service managers.

        Returns
        -------
        bool
            True if all managers report healthy status, False otherwise.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> service = Service(config)
        >>> service._health_check()
        False
        """
        logger.debug(
            "Monitor health checks: "
            + ", ".join(
                f"{manager}: {manager.is_healthy()}" for manager in self._managers
            ),
        )

        return all(manager.is_healthy() for manager in self._managers)

    def _run_heartbeat_loop(self) -> None:
        """Execute main heartbeat loop with interruptable sleep intervals.

        Runs continuous loop sending heartbeat metrics at configured intervals
        while checking for shutdown signals. Uses fractional sleep intervals
        to ensure responsive shutdown handling.
        """
        sleep_time = 1.0
        sleep_iterations = int(self._config.heartbeat_interval / sleep_time)

        while not self._signal_handler.shutdown_requested:
            # Functional approach to sleeping in intervals
            for _ in range(sleep_iterations):
                if self._signal_handler.shutdown_requested:
                    break
                time.sleep(sleep_time)

            if not self._signal_handler.shutdown_requested:
                self._prometheus_manager.send_heartbeat()

                # Log plugin status periodically
                plugin_status = self._plugin_manager.get_plugin_status()
                if plugin_status:
                    logger.debug(f"Plugin status: {plugin_status}")

    @log_execution
    def start(self) -> None:
        """Start service with complete lifecycle management and error handling.

        Orchestrates service startup including manager initialization, health
        verification, heartbeat loop execution, and graceful shutdown handling.
        Ensures proper cleanup regardless of how service terminates.

        Raises
        ------
        RuntimeError
            If service health check fails after startup.
        Exception
            If service startup fails for any other reason.
        """
        logger.info(f"Starting Service {self._config.service_id}")

        try:
            self._start_managers()

            if not self._health_check():
                raise RuntimeError("Service health check failed after startup")

            logger.info(f"Service {self._config.service_id} started successfully")
            self._run_heartbeat_loop()

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception:
            logger.exception("Service startup failed")
            raise
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        """Perform complete resource cleanup for service shutdown.

        Coordinates cleanup of all managers and logs service termination.
        Called automatically during service shutdown regardless of termination cause.
        """
        logger.info("Cleaning up resources...")
        self._stop_managers()
        logger.info(f"Service {self._config.service_id} stopped")


class ServiceManager(ABC):
    """Abstract base class defining interface for service component managers.

    Provides common interface for managing service components with lifecycle
    methods and health checking. All concrete managers must implement these
    methods.

    Examples
    --------
    >>> class TestManager(ServiceManager):
    ...     def __init__(self):
    ...         self.started = False
    ...     def start(self):
    ...         self.started = True
    ...     def stop(self):
    ...         self.started = False
    ...     def is_healthy(self):
    ...         return self.started
    >>> manager = TestManager()
    >>> manager.is_healthy()
    False
    >>> manager.start()
    >>> manager.is_healthy()
    True
    """

    @abstractmethod
    def start(self) -> None:
        """Start the manager and initialize required resources.

        Must be implemented by concrete classes to handle component startup,
        resource allocation, and any necessary initialization logic.
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop the manager and cleanup allocated resources.

        Must be implemented by concrete classes to handle graceful shutdown,
        resource deallocation, and cleanup operations.
        """
        pass

    @abstractmethod
    def is_healthy(self) -> bool:
        """Check current health status of the managed component.

        Must be implemented by concrete classes to return True if the
        component is operational and healthy, False otherwise.

        Returns
        -------
        bool
            True if component is healthy and operational, False otherwise.
        """
        pass


# Plugin management
@dataclass
class PluginStateInfo:
    """Information about a plugin instance."""

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

    Examples
    --------
    >>> config = ServiceConfig()
    >>> manager = PluginManager(config)
    >>> manager.is_healthy()
    True
    """

    def __init__(self, config: ServiceConfig, parent_service: Service) -> None:
        """Initialize plugin manager with configuration and parent service."""
        self._config = config
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

    def register_plugin(self, plugin: ServicePlugin, config: dict[str, Any]) -> None:
        """Register a new plugin with the manager.

        Parameters
        ----------
        plugin : PluginProtocol
            Plugin instance to register.
        config : dict[str, Any]
            Configuration to pass to the plugin.

        Examples
        --------
        >>> from unittest.mock import Mock
        >>> config = ServiceConfig()
        >>> manager = PluginManager(config)
        >>> mock_plugin = Mock(spec=PluginProtocol)
        >>> mock_plugin.name = "test_plugin"
        >>> manager.register_plugin(mock_plugin, {})
        >>> "test_plugin" in manager._plugins
        True
        """
        with self._lock:
            plugin = plugin(self._service)
            if plugin.name in self._plugins:
                raise ValueError(f"Plugin {plugin.name} already registered")

            plugin.initialize(config)
            logger.info(plugin)
            logger.info(plugin.name)
            self._plugins[plugin] = PluginStateInfo(plugin=plugin)
            logger.info(f"Registered plugin: {plugin.name} v{plugin.version}")

    def _start_plugin(self, plugin_info: PluginStateInfo) -> None:
        """Start a plugin in a separate thread."""

        def run_plugin() -> None:
            try:
                plugin_info.state = PluginRunState.STARTING
                logger.info(f"Starting plugin: {plugin_info.plugin.name}")

                plugin_info.plugin.start()
                plugin_info.state = PluginRunState.RUNNING
                plugin_info.error_message = None

                # Update metrics
                self._plugin_state_metric.labels(
                    plugin_name=plugin_info.plugin.name,
                ).set(plugin_info.state.value)

                logger.info(f"Plugin started successfully: {plugin_info.plugin.name}")

                # Keep thread alive while plugin is running
                while self._running and plugin_info.state == PluginRunState.RUNNING:
                    time.sleep(1)

            except Exception as e:
                plugin_info.state = PluginRunState.FAILED
                plugin_info.error_message = str(e)
                logger.exception(f"Plugin {plugin_info.plugin.name} failed")
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
        """Stop a plugin gracefully."""
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

                logger.info(f"Plugin stopped: {plugin_info.plugin.name}")
            except Exception as e:
                logger.warning(f"Error stopping plugin {plugin_info.plugin.name}: {e}")

    def _monitor_plugins(self) -> None:
        """Monitor plugin health and restart failed plugins."""
        while self._running:
            with self._lock:
                for plugin_name, plugin_info in self._plugins.items():
                    try:
                        # Check if plugin needs health check
                        now = datetime.now()
                        if (
                            plugin_info.last_health_check is None
                            or (now - plugin_info.last_health_check).seconds
                            >= self._config.plugin_health_check_interval
                        ):
                            plugin_info.last_health_check = now

                            # Check health
                            if plugin_info.state == PluginRunState.RUNNING:
                                is_healthy = plugin_info.plugin.is_healthy()
                                self._plugin_health_metric.labels(
                                    plugin_name=plugin_name,
                                ).set(1 if is_healthy else 0)

                                if not is_healthy:
                                    logger.warning(f"Plugin {plugin_name} is unhealthy")
                                    plugin_info.state = PluginRunState.FAILED
                                    self._handle_failed_plugin(plugin_info)

                            # Check if thread is alive
                            elif (plugin_info.state == PluginRunState.RUNNING) and (
                                not plugin_info.thread
                                or not plugin_info.thread.is_alive()
                            ):
                                logger.warning(f"Plugin {plugin_name} thread died")
                                plugin_info.state = PluginRunState.FAILED
                                self._handle_failed_plugin(plugin_info)

                    except Exception:
                        logger.exception(f"Error monitoring plugin {plugin_name}")

            time.sleep(1)  # Short sleep to be responsive

    def _handle_failed_plugin(self, plugin_info: PluginStateInfo) -> None:
        """Handle a failed plugin with restart logic."""
        plugin_name = plugin_info.plugin.name
        now = datetime.now()

        # Check if we should attempt restart
        can_restart = (
            plugin_info.restart_count < self._config.plugin_max_restart_attempts
        )

        # Check restart delay
        if plugin_info.last_restart:
            time_since_restart = (now - plugin_info.last_restart).seconds
            if time_since_restart < self._config.plugin_restart_delay:
                can_restart = False

        if can_restart:
            logger.info(
                f"Attempting to restart plugin {plugin_name} "
                f"(attempt {plugin_info.restart_count + 1}/"
                f"{self._config.plugin_max_restart_attempts})",
            )

            plugin_info.state = PluginRunState.RESTARTING
            plugin_info.restart_count += 1
            plugin_info.last_restart = now

            self._plugin_restart_metric.labels(plugin_name=plugin_name).inc()

            # Stop the plugin first
            self._stop_plugin(plugin_info)

            # Wait before restarting
            time.sleep(self._config.plugin_restart_delay)

            # Start the plugin again
            self._start_plugin(plugin_info)
        else:
            logger.error(
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

        # Start monitoring thread
        self._monitor_thread = threading.Thread(
            target=self._monitor_plugins,
            name="PluginMonitor",
            daemon=True,
        )
        self._monitor_thread.start()

        # Start all plugins
        with self._lock:
            for plugin_info in self._plugins.values():
                self._start_plugin(plugin_info)

    def stop(self) -> None:
        """Stop all plugins and the plugin manager."""
        self._running = False

        # Stop all plugins
        with self._lock:
            stop_tasks = [
                partial(self._stop_plugin, plugin_info)
                for plugin_info in self._plugins.values()
            ]

            # Execute all stop tasks
            [f() for f in stop_tasks]

        # Wait for monitor thread
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)

        logger.info("Plugin manager stopped")

    def is_healthy(self) -> bool:
        """Check if plugin manager is healthy.

        Returns True if running and at least one plugin is healthy.
        """
        if not self._running:
            return True  # Not running is a valid state

        with self._lock:
            health = [
                f"{info.plugin.name} is {info.state} and {info.plugin.is_healthy()}"
                for info in self._plugins.values()
            ]
            logger.debug(", ".join(health))
            healthy_plugins = filter_map(
                lambda info: info.state
                in [PluginRunState.RUNNING, PluginRunState.STARTING],
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


# Prometheus Manager
class PrometheusManager(ServiceManager):
    """Manages Prometheus metrics server and heartbeat metric collection.

    Handles Prometheus HTTP server lifecycle and provides heartbeat metric
    functionality for service monitoring. Uses immutable configuration and
    maintains server state.

    Parameters
    ----------
    config : ServiceConfig
        Service configuration containing Prometheus port and other settings.

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

    def __init__(self, config: ServiceConfig):
        self._config = config
        self._heartbeat_metric = self._create_heartbeat_metric()
        self._server_started = False

    def _create_heartbeat_metric(self) -> prometheus_client.Gauge:
        """Create Prometheus gauge metric for heartbeat timestamps.

        Factory method that creates and returns a Prometheus Gauge metric
        for tracking service heartbeat timestamps.

        Returns
        -------
        prometheus_client.Gauge
            Configured gauge metric for heartbeat timestamp tracking.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = PrometheusManager(config)
        >>> metric = manager._create_heartbeat_metric()
        >>> isinstance(metric, prometheus_client.Gauge)
        True
        """
        return prometheus_client.Gauge(
            "app_heartbeat_timestamp_seconds",
            "Last reported service heartbeat timestamp",
        )

    @log_execution
    def start(self) -> None:
        """Start Prometheus HTTP server if not already running.

        Initializes the Prometheus metrics HTTP server on the configured port.
        Server startup is idempotent - subsequent calls have no effect if
        server is already running.
        """
        if not self._server_started:
            logger.info(
                f"Starting Prometheus server on port {self._config.prometheus_port}",
            )
            prometheus_client.start_http_server(self._config.prometheus_port)
            self._server_started = True

    def stop(self) -> None:
        """Stop Prometheus server and log shutdown.

        Logs shutdown message. HTTP server cleanup is handled automatically
        by process termination since prometheus_client doesn't provide
        explicit server stop functionality.
        """
        logger.info("Prometheus manager stopped")

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
        >>> manager.start()
        >>> manager.is_healthy()
        True
        """
        return self._server_started

    def send_heartbeat(self) -> None:
        """Update heartbeat metric with current Unix timestamp.

        Sets the heartbeat gauge metric to the current time as Unix timestamp,
        allowing monitoring systems to track service liveness.

        Examples
        --------
        >>> import time
        >>> config = ServiceConfig()
        >>> manager = PrometheusManager(config)
        >>> before = time.time()
        >>> manager.send_heartbeat()
        >>> after = time.time()
        >>> # Cannot directly test metric value in doctest
        """
        current_time = time.time()
        self._heartbeat_metric.set(current_time)
        logger.debug(f"Heartbeat sent at {current_time}")


# RabbitMQ Manager
class RabbitMQManager(ServiceManager):
    """Manages RabbitMQ connections, channels, and queue configurations.

    Handles RabbitMQ connection lifecycle with retry logic, provides context
    managers for independent connections, and maintains queue configuration
    for connection establishment.

    Parameters
    ----------
    config : ServiceConfig
        Service configuration containing RabbitMQ URL and retry settings.

    Examples
    --------
    >>> config = ServiceConfig()
    >>> manager = RabbitMQManager(config)
    >>> manager.is_healthy()
    False
    >>> manager.add_queue("test_queue", durable=True)
    >>> len(manager._queues)
    1
    """

    def __init__(self, config: ServiceConfig):
        self._config = config
        self._connection: pika.BlockingConnection | None = None
        self._channel: pika.channel.Channel | None = None
        self._queues: dict[str, dict[str, Any]] = {}
        self._namespace = config.service_namespace

    @retry_with_backoff(exceptions=(AMQPConnectionError,))
    def _establish_connection(
        self,
    ) -> tuple[pika.BlockingConnection, pika.channel.Channel]:
        """Establish new RabbitMQ connection and channel with retry logic.

        Creates fresh connection and channel to RabbitMQ using configured URL.
        Includes automatic retry on connection failures with exponential backoff.

        Returns
        -------
        tuple[pika.BlockingConnection, pika.channel.Channel]
            New RabbitMQ connection and channel pair.

        Raises
        ------
        AMQPConnectionError
            If connection fails after all retry attempts.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = RabbitMQManager(config)
        >>> # Note: This would fail in doctest without actual RabbitMQ server
        >>> # connection, channel = manager._establish_connection()
        >>> # isinstance(connection, pika.BlockingConnection)
        >>> # True
        """
        logger.debug(
            f"Attempting to connect to RabbitMQ at url {self._config.rabbitmq_url}",
        )
        parameters = pika.URLParameters(self._config.rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        logger.debug("Successfully connected to RabbitMQ")
        return connection, channel

    @log_execution
    def start(self) -> None:
        """Initialize RabbitMQ connection if not already healthy.

        Establishes RabbitMQ connection and channel if not currently connected.
        Connection establishment is idempotent - no action taken if already
        connected and healthy.
        """
        if not self.is_healthy():
            self._connection, self._channel = self._establish_connection()

    def stop(self) -> None:
        """Close RabbitMQ connection safely and reset connection state.

        Gracefully closes active RabbitMQ connection if present, handles
        connection errors during shutdown, and resets internal state.
        """
        if self._connection and not self._connection.is_closed:
            try:
                self._connection.close()
                logger.info("RabbitMQ connection closed")
            except Exception as e:
                logger.warning(f"Error closing RabbitMQ connection: {e}")

        self._connection = None
        self._channel = None

    def is_healthy(self) -> bool:
        """Check if RabbitMQ connection is active and not closed.

        Returns
        -------
        bool
            True if connection exists and is not closed, False otherwise.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = RabbitMQManager(config)
        >>> manager.is_healthy()
        False
        """
        return self._connection is not None and not self._connection.is_closed

    @contextmanager
    def get_connection_context(
        self,
    ) -> Generator[tuple[pika.BlockingConnection, pika.channel.Channel], None, None]:
        """Provide independent RabbitMQ connection context for isolated operations.

        Creates temporary connection and channel separate from main connection,
        declares configured queues on new connection, and ensures cleanup
        regardless of operation success or failure.

        Yields
        ------
        tuple[pika.BlockingConnection, pika.channel.Channel]
            Independent connection and channel pair for isolated operations.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = RabbitMQManager(config)
        >>> manager.add_queue("temp_queue", durable=False)
        >>> # Note: This would fail in doctest without actual RabbitMQ server
        >>> # with manager.get_connection_context() as (conn, channel):
        >>> #     isinstance(conn, pika.BlockingConnection)
        >>> # True
        """
        connection, channel = None, None
        try:
            connection, channel = self._establish_connection()

            # Declare existing queues on new connection
            for queue_name, config in self._queues.items():
                logger.debug(f"Creating queue {queue_name} with config {config}")
                channel.queue_declare(queue=queue_name, **config)

            yield connection, channel
        finally:
            if connection and not connection.is_closed:
                connection.close()

    def get_queue_name(self, base_name: str) -> str:
        """Generate full queue name with service namespace prefix."""
        return f"{self._namespace}-{base_name}"

    def add_queue(self, queue_name: str, **queue_config: Any) -> str:
        """Register queue configuration for automatic declaration on connections.

        Stores queue configuration that will be applied when establishing
        new connections through get_connection_context().

        Parameters
        ----------
        queue_name : str
            Name of the queue to configure.
        **queue_config
            Keyword arguments for pika queue_declare method (e.g., durable=True).

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = RabbitMQManager(config)
        >>> manager.add_queue("my_queue", durable=True, exclusive=False)
        >>> "my_queue" in manager._queues
        True
        >>> manager._queues["my_queue"]["durable"]
        True
        """
        new_queue_name = self.get_queue_name(queue_name)
        if new_queue_name not in self._queues:
            self._queues[new_queue_name] = queue_config
        else:
            logger.debug(f"Queue {new_queue_name} is already registered")
        return new_queue_name


class SignalHandler:
    """Handles OS signals for graceful service shutdown.

    Manages SIGTERM and SIGINT signals to enable graceful shutdown of the
    service when requested by the operating system or user interruption.

    Examples
    --------
    >>> handler = SignalHandler()
    >>> handler.shutdown_requested
    False
    >>> # Simulating signal reception would require signal.raise_signal()
    >>> # which is not suitable for doctest
    """

    def __init__(self) -> None:
        self._shutdown_requested = False
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        """Configure signal handlers for SIGTERM and SIGINT.

        Registers signal handlers to catch termination and interrupt signals
        for graceful shutdown coordination.
        """
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)

    def _handle_shutdown_signal(self, signal_num: int, frame: Any) -> None:
        """Handle received shutdown signals by setting shutdown flag.

        Parameters
        ----------
        signal_num : int
            Signal number that was received.
        frame : Any
            Current stack frame (unused but required by signal handler interface).
        """
        logger.info(f"Received signal {signal_num}, requesting graceful shutdown...")
        self._shutdown_requested = True

    @property
    def shutdown_requested(self) -> bool:
        """Check if graceful shutdown has been requested via signal.

        Returns
        -------
        bool
            True if shutdown signal was received, False otherwise.

        Examples
        --------
        >>> handler = SignalHandler()
        >>> handler.shutdown_requested
        False
        """
        return self._shutdown_requested


def create_service_with_plugins(
    config: ServiceConfig | None = None,
    plugins: list[tuple[ServicePlugin, dict[str, Any]]] | None = None,
) -> Service:
    """Create new Service instance with optional configuration and plugins.

    Factory function providing clean interface for Service instantiation
    with optional configuration override and plugin registration.

    Parameters
    ----------
    config : ServiceConfig, optional
        Service configuration. If None, Service will create default configuration.
    plugins : list of tuple[PluginProtocol, dict[str, Any]], optional
        List of (plugin, config) tuples to register with the service.

    Returns
    -------
    Service
        Configured service instance ready for startup.

    Examples
    --------
    >>> service = create_service_with_plugins()
    >>> isinstance(service, Service)
    True
    >>> config = ServiceConfig()
    >>> service_with_config = create_service_with_plugins(config)
    >>> service_with_config._config == config
    True
    """
    service = Service(config)

    if plugins:
        register_plugin_partial = partial(
            lambda p_c, s: s.register_plugin(*p_c),
            s=service,
        )
        list(map(register_plugin_partial, plugins))

    return service


def main() -> None:
    """Application entry point with error handling and logging.

    Creates service with default configuration and starts it. Logs any
    application-level failures and re-raises exceptions for proper exit codes.

    Raises
    ------
    Exception
        Re-raises any exception from service creation or startup.

    Examples
    --------
    >>> # main() would start the actual service, not suitable for doctest
    >>> # main()  # This would run the full service
    >>> pass  # Placeholder for doctest
    """
    try:
        ARGS = parse_driver_args()
        plugin_config = interfaces.controller_configs.get_plugin(ARGS.config)
        config = ServiceConfig(
            rabbitmq_url="amqp://admin:admin_test@localhost:5672/",
        )

        from geoips_driver.plugins.modules.job_queuers.dummy_job_queuer import (
            OVERCASTJobQueuer,
        )

        from geoips_driver.plugins.modules.data_monitors.file_system_polling import (
            FileSystemPoller,
        )
        from geoips_driver.plugins.modules.dispatchers.serial import (
            SerialDispatcher,
        )

        plugins = [
            (FileSystemPoller, plugin_config),
            (OVERCASTJobQueuer, plugin_config),
            (SerialDispatcher, plugin_config),
        ]

        service = create_service_with_plugins(config, plugins)
        service.start()
    except Exception:
        logger.exception("Application failed")
        raise


if __name__ == "__main__":
    main()

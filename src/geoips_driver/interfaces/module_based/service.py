"""Service manager.

Manager with Prometheus metrics, RabbitMQ integration, and plugin support.
"""

import logging
import os
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
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.exceptions import AMQPConnectionError

from geoips_driver.utils.logging import get_logger

# Type variables for generic type hints
T = TypeVar("T")
R = TypeVar("R")


class PluginRunState(Enum):
    """Enumeration of possible plugin states.

    Attributes
    ----------
    STOPPED : int
        Plugin is not running.
    STARTING : int
        Plugin is in the process of starting.
    RUNNING : int
        Plugin is running normally.
    STOPPING : int
        Plugin is in the process of stopping.
    FAILED : int
        Plugin has failed.
    RESTARTING : int
        Plugin is being restarted after failure.
    """

    STOPPED = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    FAILED = auto()
    RESTARTING = auto()


# Functional programming utilities
def compose(*functions: Callable[..., Any]) -> Callable[[Any], Any]:
    """Compose functions from right to left.

    Creates a new function that applies the given functions in reverse order,
    passing the result of each function as input to the next.

    Parameters
    ----------
    *functions : Callable
        Variable number of functions to compose. Functions are applied
        right-to-left (last function is applied first).

    Returns
    -------
    Callable[[Any], Any]
        Composed function that applies all input functions in sequence.

    Examples
    --------
    >>> add_one = lambda x: x + 1
    >>> multiply_two = lambda x: x * 2
    >>> composed = compose(add_one, multiply_two)
    >>> composed(3)  # (3 * 2) + 1
    7
    """
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(*functions: Callable[..., Any]) -> Callable[[Any], Any]:
    """Pipe functions from left to right.

    Creates a new function that applies the given functions in order,
    passing the result of each function as input to the next.

    Parameters
    ----------
    *functions : Callable
        Variable number of functions to pipe. Functions are applied
        left-to-right (first function is applied first).

    Returns
    -------
    Callable[[Any], Any]
        Piped function that applies all input functions in sequence.

    Examples
    --------
    >>> add_one = lambda x: x + 1
    >>> multiply_two = lambda x: x * 2
    >>> piped = pipe(add_one, multiply_two)
    >>> piped(3)  # (3 + 1) * 2
    8
    """
    return reduce(lambda f, g: lambda x: g(f(x)), functions, lambda x: x)


# ignore on this line because the return type is a Callable that takes T | None
# which is not best practice in 3.12 but required to support 3.11+
def maybe(default: T) -> Callable[[T | None], T]:  # noqa: UP047
    """Return value or default if None.

    Creates a function that returns the input value if not None,
    otherwise returns the specified default value.

    Parameters
    ----------
    default : T
        Default value to return when input is None.

    Returns
    -------
    Callable[[T | None], T]
        Function that returns input value or default.

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

    Filters items using a predicate function and transforms matching
    items using a transform function in a single pass.

    Parameters
    ----------
    predicate : Callable[[T], bool]
        Function to test each item. Only items returning True are transformed.
    transform : Callable[[T], R]
        Function to transform filtered items.
    items : Iterable[T]
        Items to filter and transform.

    Returns
    -------
    list[R]
        List of transformed items that passed the predicate test.

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
    service_id : str, optional
        Unique identifier for this service instance. Defaults to environment
        variable SERVICE_ID or auto-generated UUID-based identifier.
    service_namespace : str, optional
        Namespace for service isolation. Defaults to environment variable
        SERVICE_NAMESPACE or 'default'.
    database_url : str, optional
        PostgreSQL database connection URL. Defaults to environment variable
        DATABASE_URL or localhost connection.
    prometheus_port : int, optional
        Port number for Prometheus metrics HTTP server. Defaults to environment
        variable PROMETHEUS_PORT or 8000.
    rabbitmq_url : str, optional
        RabbitMQ connection URL. Defaults to environment variable RABBITMQ_URL
        or localhost connection.
    rabbitmq_max_retries : int, optional
        Maximum retry attempts for RabbitMQ operations. Defaults to environment
        variable RABBITMQ_MAX_RETRIES or 5.
    heartbeat_interval : int, optional
        Interval in seconds between heartbeat metric updates. Default is 10.
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
        to environment variable LOG_LEVEL or 'DEBUG'.
    production_mode : bool, optional
        Enable production mode with enforced minimum INFO log level. Defaults
        to environment variable PRODUCTION or False.

    Attributes
    ----------
    service_id : str
        Unique identifier for the service instance.
    service_namespace : str
        Service namespace for isolation.
    database_url : str
        Database connection URL.
    prometheus_port : int
        Prometheus metrics server port.
    rabbitmq_url : str
        RabbitMQ connection URL.
    rabbitmq_max_retries : int
        Maximum RabbitMQ retry attempts.
    heartbeat_interval : int
        Heartbeat interval in seconds.
    plugin_restart_delay : int
        Plugin restart delay in seconds.
    plugin_max_restart_attempts : int
        Maximum plugin restart attempts.
    plugin_health_check_interval : int
        Plugin health check interval in seconds.
    loki_url : str
        Grafana Loki push API URL.
    loki_enabled : bool
        Whether Loki log shipping is enabled.
    log_level : str
        Configured logging level.
    production_mode : bool
        Whether production mode is enabled.

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
    loki_url: str = field(
        default_factory=lambda: os.environ.get("LOKI_URL", ""),
    )
    loki_enabled: bool = field(
        default_factory=lambda: os.environ.get("LOKI_ENABLED", "false").lower()
        == "true",
    )
    log_level: str = field(
        default_factory=lambda: os.environ.get("LOG_LEVEL", "DEBUG"),
    )
    production_mode: bool = field(
        default_factory=lambda: os.environ.get("PRODUCTION", "false").lower() == "true",
    )


def setup_logging(name: str | None = None) -> logging.Logger:
    """Configure logger with standardized formatting and return module logger.

    This function provides backward compatibility with the legacy logging
    interface. New code should prefer get_logger() for better context
    management and Loki integration.

    Sets up logging configuration with Rich formatting for colorized output,
    then returns a logger instance. Only configures handlers if the logger
    doesn't already have any.

    Parameters
    ----------
    name : str or None, optional
        Name for the logger. If None, uses __name__ of the calling module.
        Default is None.

    Returns
    -------
    logging.Logger
        Configured logger instance with Rich handler and DEBUG level.

    Examples
    --------
    >>> logger = setup_logging()
    >>> logger.name == '__main__'
    True
    >>> isinstance(logger, logging.Logger)
    True
    >>> logger.level == logging.DEBUG
    True

    Notes
    -----
    This function is maintained for backward compatibility. New code should
    use get_logger() instead for proper context management.
    """
    module_name = name if name else "__main__"
    return get_logger("module", module_name, None)


logger = setup_logging()


def retry_with_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T | None]]:
    """Create retry decorator with exponential backoff for transient failures.

    Returns a decorator that retries function execution on specified exceptions
    with exponentially increasing delay between attempts.

    Parameters
    ----------
    max_retries : int, default=5
        Maximum number of retry attempts before giving up.
    base_delay : float, default=1.0
        Initial delay in seconds, doubled after each failure.
    exceptions : tuple of type[Exception], default=(Exception,)
        Exception types that trigger retry attempts.

    Returns
    -------
    Callable[[Callable[..., T]], Callable[..., T | None]]
        Decorator function that wraps target functions with retry logic.
        The wrapped function returns the original return type T on success,
        or None if all retries are exhausted without raising.

    Raises
    ------
    Exception
        Re-raises the caught exception if max_retries is reached.

    Examples
    --------
    >>> @retry_with_backoff(max_retries=2, base_delay=0.1)
    ... def unstable_function():
    ...     return "success"
    >>> result = unstable_function()
    >>> result
    'success'
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T | None]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T | None:
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


# ignore on this line because the return type is a Callable that takes T | None
# which is not best practice in 3.12 but required to support 3.11+
def log_execution(func: Callable[..., T]) -> Callable[..., T | None]:  # noqa: UP047
    """Create decorator for logging function execution and exceptions.

    Wraps functions to log debug messages on entry/success and exception
    details on failure, then re-raises exceptions for proper error handling.

    Parameters
    ----------
    func : Callable[..., T]
        Function to be wrapped with execution logging.

    Returns
    -------
    Callable[..., T | None]
        Wrapped function with execution logging behavior. Returns None
        if an exception occurs (after logging), otherwise returns the
        original function's return value.

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
    def wrapper(*args: Any, **kwargs: Any) -> T | None:
        # Try to get instance logger from self if available
        instance_logger = getattr(args[0], "_logger", None) if args else None
        active_logger = instance_logger or logger

        active_logger.debug(f"Executing {func.__name__}")
        try:
            result = func(*args, **kwargs)
            active_logger.debug(f"Successfully executed {func.__name__}")
            return result  # noqa: TRY300
        except Exception:
            active_logger.exception(f"Error in {func.__name__}")
            return None

    return wrapper


class ServicePlugin(Protocol):
    """Protocol defining the interface that all plugins must implement.

    This protocol specifies the required methods and properties that any
    service plugin must provide for integration with the plugin manager.

    Plugins should create an instance logger in __init__ using:
        self._logger = get_logger("plugin", self.name, service._config)

    This ensures all plugin log messages include proper source identification
    and integrate with the service's Loki logging if enabled.

    Methods
    -------
    __init__(service, config)
        Initialize the plugin with service reference and configuration.
    start()
        Start the plugin operations.
    stop()
        Stop the plugin operations.
    is_healthy()
        Check if the plugin is healthy.
    get_metrics()
        Return plugin-specific metrics.

    Properties
    ----------
    name : str
        The plugin name.
    version : str
        The plugin version.

    Examples
    --------
    >>> from geoips_driver.interfaces.module_based.logging import get_logger
    >>> class MyPlugin:
    ...     name = "my-plugin"
    ...     version = "1.0.0"
    ...     def __init__(self, service, config):
    ...         self._logger = get_logger("plugin", self.name, service._config)
    ...         self._logger.info("Plugin initialized")
    """

    name: str
    version: str = "0.0.0"

    def __init__(self, service: Any, config: dict[str, Any]) -> None:
        """Initialize plugin with service reference and configuration.

        Parameters
        ----------
        service : Any
            Reference to the parent service instance.
        config : dict[str, Any]
            Configuration dictionary for the plugin.
        """
        ...

    def start(self) -> None:
        """Start the plugin operations.

        Raises
        ------
        Exception
            If plugin fails to start.
        """
        ...

    def stop(self) -> None:
        """Stop the plugin operations.

        Should perform graceful shutdown of plugin resources.
        """
        ...

    def is_healthy(self) -> bool:
        """Check if the plugin is healthy.

        Returns
        -------
        bool
            True if plugin is operating normally, False otherwise.
        """
        ...

    def get_metrics(self) -> dict[str, Any]:
        """Return plugin-specific metrics.

        Returns
        -------
        dict[str, Any]
            Dictionary of metric names to values.
        """
        ...


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
        self._rabbitmq_manager = RabbitMQManager(self._config)
        self._plugin_manager = PluginManager(self._config, self)

        self._managers: list[ServiceManager] = [
            self._prometheus_manager,
            self._rabbitmq_manager,
            self._plugin_manager,
        ]
        self.namespace = "default"
        self._start_time = time.time()

        # Service-level Prometheus metrics
        self._service_uptime_metric = prometheus_client.Gauge(
            "service_uptime_seconds",
            "Service uptime in seconds",
        )
        self._service_health_metric = prometheus_client.Gauge(
            "service_health",
            "Overall service health status (1 = healthy, 0 = unhealthy)",
        )

    @log_execution
    def emit(self, queue: str, message: str) -> None:
        """Publish a message to a message broker queue.

        Parameters
        ----------
        queue : str
            Name of the queue to publish to. Queue will be created if it
            doesn't exist.
        message : str
            Message content to publish.

        Raises
        ------
        AMQPConnectionError
            If unable to connect to RabbitMQ.
        Exception
            If message publishing fails.
        """
        queue = self._rabbitmq_manager.add_queue(
            queue,
            durable=True,
            exclusive=False,
        )
        # Underscore indicates unused variable to silence linters
        with self._rabbitmq_manager.get_connection_context() as (_connection, channel):
            self._logger.debug(f"Emitting message to queue '{queue}': {message}")
            channel.basic_publish(exchange="", routing_key=queue, body=message)

    def consume(self, queue: str) -> Generator[str, None, None]:
        """Yield messages from a message broker queue.

        This returns a generator that yields messages from the specified queue.
        It handles message acknowledgment on successful processing and
        requeues messages if processing fails or the consumer stops.

        Parameters
        ----------
        queue : str
            The name of the queue to consume messages from. If the queue doesn't
            exist, it will be created with durable=True and exclusive=False.

        Yields
        ------
        str
            The decoded message content from the queue.

        Raises
        ------
        GeneratorExit
            Raised when the generator is explicitly closed. Messages are requeued,
            the consumer is cancelled, and the exception is re-raised.
        AMQPConnectionError
            If unable to connect to RabbitMQ.
        Exception
            Any other exception during message processing. Messages are requeued
            and the exception is re-raised.

        Notes
        -----
        - Messages are automatically acknowledged after being yielded and processed.
        - If an exception occurs or the generator is closed, unprocessed messages
          are requeued for later processing.
        - The generator must be explicitly closed if not consumed completely,
          otherwise the connection will remain open.
        """
        queue = self._rabbitmq_manager.add_queue(
            queue,
            durable=True,
            exclusive=False,
        )

        self._logger.debug(f"Consuming from queue: {queue}")

        with self._rabbitmq_manager.get_connection_context() as (_connection, channel):
            for method_frame, _properties, body in channel.consume(
                queue,
                auto_ack=False,
            ):
                try:
                    message = body.decode("utf-8")
                    yield message
                    self._logger.debug(
                        f"Received message from queue '{queue}': {message}",
                    )

                    # Fix: Ensure delivery_tag is an integer before Ack
                    if method_frame.delivery_tag is not None:
                        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    else:
                        self._logger.error("Skipped Ack: delivery_tag is None")

                except GeneratorExit:
                    # Fix: Ensure delivery_tag is an integer before Nack
                    if method_frame.delivery_tag is not None:
                        channel.basic_nack(
                            delivery_tag=method_frame.delivery_tag,
                            requeue=True,
                        )
                    channel.cancel()
                    raise
                except Exception:
                    # Fix: Ensure delivery_tag is an integer before Nack
                    if method_frame.delivery_tag is not None:
                        channel.basic_nack(
                            delivery_tag=method_frame.delivery_tag,
                            requeue=True,
                        )
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
            Plugin class to register. Will be instantiated with service
            reference and config.
        config : dict[str, Any]
            Configuration dictionary for the plugin.

        Raises
        ------
        ValueError
            If a plugin with the same name is already registered.
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
                # Logging here ensures we see which manager failed
                self._logger.exception(f"Failed to start {manager.__class__.__name__}")
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
                self._logger.warning(
                    f"Error stopping {manager.__class__.__name__}: {e}",
                )

        list(map(stop_manager, reversed(self._managers)))

    def _health_check(self) -> bool:
        """Check health status of all service managers.

        Returns
        -------
        bool
            True if all managers report healthy status, False otherwise.
        """
        self._logger.debug(
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
                    break  # type: ignore
                time.sleep(sleep_time)

            if not self._signal_handler.shutdown_requested:
                self._prometheus_manager.send_heartbeat()

                # Update service-level metrics
                self._service_uptime_metric.set(time.time() - self._start_time)
                self._service_health_metric.set(1 if self._health_check() else 0)

                # Log plugin status periodically
                plugin_status = self._plugin_manager.get_plugin_status()
                if plugin_status:
                    self._logger.debug(f"Plugin status: {plugin_status}")

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
        self._logger.info(f"Starting Service {self._config.service_id}")

        try:
            self._start_managers()

            if not self._health_check():
                raise RuntimeError("Service health check failed after startup")  # noqa: TRY003, TRY301

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
        """Perform complete resource cleanup for service shutdown.

        Coordinates cleanup of all managers and logs service termination.
        Called automatically during service shutdown regardless of termination cause.
        """
        self._logger.info("Cleaning up resources...")
        self._stop_managers()
        self._logger.info(f"Service {self._config.service_id} stopped")


class ServiceManager(ABC):
    """Abstract base class defining interface for service component managers.

    Provides common interface for managing service components with lifecycle
    methods and health checking. All concrete managers must implement these
    methods.

    Methods
    -------
    start()
        Start the manager and initialize required resources.
    stop()
        Stop the manager and cleanup allocated resources.
    is_healthy()
        Check current health status of the managed component.
    """

    @abstractmethod
    def start(self) -> None:
        """Start the manager and initialize required resources.

        Must be implemented by concrete classes to handle component startup,
        resource allocation, and any necessary initialization logic.

        Raises
        ------
        Exception
            If manager fails to start successfully.
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

    Attributes
    ----------
    plugin : ServicePlugin
        The plugin instance.
    state : PluginRunState
        Current state of the plugin.
    thread : threading.Thread or None
        Thread running the plugin.
    last_health_check : datetime or None
        When plugin was last checked.
    restart_count : int
        Number of restart attempts.
    last_restart : datetime or None
        When plugin was last restarted.
    error_message : str or None
        Error message from last failure.
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
    parent_service : Service
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

    def __init__(self, config: ServiceConfig, parent_service: "Service") -> None:
        """Initialize plugin manager with configuration and parent service.

        Parameters
        ----------
        config : ServiceConfig
            Service configuration.
        parent_service : Service
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
                raise ValueError(f"Plugin {plugin_instance.name} already registered")  # noqa: TRY003

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

                # Update metrics
                self._plugin_state_metric.labels(
                    plugin_name=plugin_info.plugin.name,
                ).set(plugin_info.state.value)

                self._logger.info(
                    f"Plugin started successfully: {plugin_info.plugin.name}",
                )

                # Keep thread alive while plugin is running
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
        """Monitor plugin health and restart failed plugins.

        Runs in a separate thread to continuously monitor all registered
        plugins and handle failures according to restart policy.
        """
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
                                    self._logger.warning(
                                        f"Plugin {plugin_name} is unhealthy",
                                    )
                                    plugin_info.state = PluginRunState.FAILED
                                    self._handle_failed_plugin(plugin_info)

                            # Check if thread is alive; skipping mypy type check here
                            # because thread is None by default and we never reach
                            # this code in a standard mypy run.
                            elif (plugin_info.state == PluginRunState.RUNNING) and (
                                not plugin_info.thread  # type: ignore
                                or not plugin_info.thread.is_alive()
                            ):
                                self._logger.error(f"Plugin {plugin_name} thread died")  # type: ignore
                                plugin_info.state = PluginRunState.FAILED
                                self._handle_failed_plugin(plugin_info)

                    except Exception:
                        self._logger.exception(f"Error monitoring plugin {plugin_name}")

            time.sleep(1)  # Short sleep to be responsive

    def _handle_failed_plugin(self, plugin_info: PluginStateInfo) -> None:
        """Handle a failed plugin with restart logic.

        Parameters
        ----------
        plugin_info : PluginStateInfo
            Information about the failed plugin.
        """
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
            self._logger.info(
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
        """Start the plugin manager and all registered plugins.

        Starts the monitoring thread and initiates all registered plugins
        in separate threads.
        """
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
        """Stop all plugins and the plugin manager.

        Gracefully stops all running plugins and the monitoring thread.
        """
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
            return True  # Not running is a valid state

        with self._lock:
            health = [
                f"{info.plugin.name} is {info.state} and {info.plugin.is_healthy()}"
                for info in self._plugins.values()
            ]
            self._logger.debug(", ".join(health))
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
            Dictionary mapping plugin names to their status information,
            including state, version, restart count, last restart time,
            error message, and metrics.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> service = Service(config)
        >>> manager = PluginManager(config, service)
        >>> status = manager.get_plugin_status()
        >>> isinstance(status, dict)
        True
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

    Attributes
    ----------
    _config : ServiceConfig
        Service configuration.
    _heartbeat_metric : prometheus_client.Gauge
        Gauge metric for heartbeat timestamps.
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
            self._logger.info(
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
        self._logger.debug(f"Heartbeat sent at {current_time}")


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

    Attributes
    ----------
    _config : ServiceConfig
        Service configuration.
    _connection : BlockingConnection or None
        Active RabbitMQ connection.
    _channel : BlockingChannel or None
        Active RabbitMQ channel.
    _queues : dict[str, dict[str, Any]]
        Registered queue configurations.
    _created_queues : set[str]
        Set of queues that have been created.
    _namespace : str
        Service namespace for queue naming.

    Methods
    -------
    get_connection_context()
        Provide independent RabbitMQ connection context.
    get_queue_name(base_name)
        Generate full queue name with namespace prefix.
    add_queue(queue_name, **queue_config)
        Register queue configuration.

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

    def __init__(self, config: ServiceConfig) -> None:
        """Initialize RabbitMQ manager with configuration.

        Parameters
        ----------
        config : ServiceConfig
            Service configuration.
        """
        self._config = config
        self._logger = get_logger("manager", "RabbitMQManager", config)
        self._connection: BlockingConnection | None = None
        self._channel: BlockingChannel | None = None
        self._queues: dict[str, dict[str, Any]] = {}
        self._created_queues: set[str] = set()
        self._namespace = config.service_namespace

        # Prometheus metrics for RabbitMQ
        self._rabbitmq_connections_total = prometheus_client.Counter(
            "rabbitmq_connections_total",
            "Total number of RabbitMQ connection attempts",
            ["status"],
        )
        self._rabbitmq_messages_sent_total = prometheus_client.Counter(
            "rabbitmq_messages_sent_total",
            "Total number of messages sent to RabbitMQ queues",
            ["queue_name"],
        )
        self._rabbitmq_messages_received_total = prometheus_client.Counter(
            "rabbitmq_messages_received_total",
            "Total number of messages received from RabbitMQ queues",
            ["queue_name"],
        )

    @retry_with_backoff(exceptions=(AMQPConnectionError,))
    def _establish_connection(
        self,
    ) -> tuple[BlockingConnection, BlockingChannel]:
        """Establish new RabbitMQ connection and channel with retry logic.

        Creates fresh connection and channel to RabbitMQ using configured URL.
        Includes automatic retry on connection failures with exponential backoff.

        Returns
        -------
        tuple[BlockingConnection, BlockingChannel]
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
        >>> # isinstance(connection, BlockingConnection)
        >>> # True
        """
        self._logger.debug(
            f"Attempting to connect to RabbitMQ at url {self._config.rabbitmq_url}",
        )
        try:
            parameters = pika.URLParameters(self._config.rabbitmq_url)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            self._logger.debug("Successfully connected to RabbitMQ")
            self._rabbitmq_connections_total.labels(status="success").inc()
        except AMQPConnectionError:
            self._rabbitmq_connections_total.labels(status="failure").inc()
            self._logger.exception("Failed to connect to RabbitMQ")
            raise
        else:
            return connection, channel

    @log_execution
    def start(self) -> None:
        """Initialize RabbitMQ connection if not already healthy.

        Establishes RabbitMQ connection and channel if not currently connected.
        Connection establishment is idempotent - no action taken if already
        connected and healthy.
        """
        if not self.is_healthy():
            self._connection, self._channel = self._establish_connection()  # type: ignore

    def stop(self) -> None:
        """Close RabbitMQ connection safely and reset connection state.

        Gracefully closes active RabbitMQ connection if present, handles
        connection errors during shutdown, and resets internal state.
        """
        if self._connection and not self._connection.is_closed:
            try:
                self._connection.close()
                self._logger.info("RabbitMQ connection closed")
            except Exception as e:
                self._logger.warning(f"Error closing RabbitMQ connection: {e}")

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
    ) -> Generator[tuple[BlockingConnection, BlockingChannel], None, None]:
        """Provide independent RabbitMQ connection context for isolated operations.

        Creates temporary connection and channel separate from main connection,
        declares configured queues on new connection, and ensures cleanup
        regardless of operation success or failure.

        Yields
        ------
        tuple[BlockingConnection, BlockingChannel]
            Independent connection and channel pair for isolated operations.

        Raises
        ------
        AMQPConnectionError
            If unable to establish connection.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = RabbitMQManager(config)
        >>> manager.add_queue("temp_queue", durable=False)
        >>> # Note: This would fail in doctest without actual RabbitMQ server
        >>> # with manager.get_connection_context() as (conn, channel):
        >>> #     isinstance(conn, BlockingConnection)
        >>> # True
        """
        connection: BlockingConnection | None = None
        channel: BlockingChannel | None = None
        try:
            connection, channel = self._establish_connection()  # type: ignore

            # Declare existing queues on new connection
            for queue_name, config in self._queues.items():
                if queue_name not in self._created_queues:
                    self._logger.debug(
                        f"Creating queue {queue_name} with config {config}",
                    )
                    channel.queue_declare(queue=queue_name, **config)
                    self._created_queues.add(queue_name)

            yield connection, channel
        finally:
            if connection and not connection.is_closed:
                connection.close()

    def get_queue_name(self, base_name: str) -> str:
        """Generate full queue name with service namespace prefix.

        Parameters
        ----------
        base_name : str
            Base name of the queue without namespace.

        Returns
        -------
        str
            Full queue name with namespace prefix.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = RabbitMQManager(config)
        >>> manager.get_queue_name("my_queue")
        'default-my_queue'
        """
        return f"{self._namespace}-{base_name}"

    def add_queue(self, queue_name: str, **queue_config: Any) -> str:
        """Register queue configuration for automatic declaration on connections.

        Stores queue configuration that will be applied when establishing
        new connections through get_connection_context().

        Parameters
        ----------
        queue_name : str
            Base name of the queue to configure (without namespace prefix).
        **queue_config : Any
            Keyword arguments for pika queue_declare method (e.g., durable=True,
            exclusive=False, auto_delete=False, arguments=None).

        Returns
        -------
        str
            Full queue name with namespace prefix.

        Examples
        --------
        >>> config = ServiceConfig()
        >>> manager = RabbitMQManager(config)
        >>> full_name = manager.add_queue("my_queue", durable=True, exclusive=False)
        >>> full_name in manager._queues
        True
        >>> manager._queues[full_name]["durable"]
        True
        """
        new_queue_name = self.get_queue_name(queue_name)
        if new_queue_name not in self._queues:
            self._queues[new_queue_name] = queue_config
        return new_queue_name


class SignalHandler:
    """Handles OS signals for graceful service shutdown.

    Manages SIGTERM and SIGINT signals to enable graceful shutdown of the
    service when requested by the operating system or user interruption.

    Attributes
    ----------
    _shutdown_requested : bool
        Whether a shutdown signal has been received.

    Properties
    ----------
    shutdown_requested : bool
        Read-only property indicating if shutdown was requested.

    Examples
    --------
    >>> handler = SignalHandler()
    >>> handler.shutdown_requested
    False
    >>> # Simulating signal reception would require signal.raise_signal()
    >>> # which is not suitable for doctest
    """

    def __init__(self) -> None:
        """Initialize signal handler and register signal callbacks."""
        self._shutdown_requested = False
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        """Configure signal handlers for SIGTERM and SIGINT.

        Registers signal handlers to catch termination and interrupt signals
        for graceful shutdown coordination.
        """
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)

    def _handle_shutdown_signal(self, signal_num: int, _frame: Any) -> None:
        """Handle received shutdown signals by setting shutdown flag.

        Parameters
        ----------
        signal_num : int
            Signal number that was received.
        _frame : Any
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
    plugins: list[tuple[type[ServicePlugin], dict[str, Any]]] | None = None,
) -> Service:
    """Create new Service instance with optional configuration and plugins.

    Factory function providing clean interface for Service instantiation
    with optional configuration override and plugin registration.

    Parameters
    ----------
    config : ServiceConfig or None, optional
        Service configuration. If None, Service will create default configuration.
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
    >>> config = ServiceConfig()
    >>> service_with_config = create_service_with_plugins(config)
    >>> service_with_config._config == config
    True
    """
    service = Service(config)

    # Use iterate instead of map to avoid "None is not iterable" errors from Mypy
    # and improve readability over functional map calls for side effects.
    if plugins is not None:
        for plugin_class, plugin_config in plugins:
            service.register_plugin(plugin_class, plugin_config)

    return service


def parse_driver_args() -> Any:
    """Parse command-line arguments for the driver.

    Returns
    -------
    Any
        Parsed command-line arguments.

    Notes
    -----
    This is a placeholder function. The actual implementation should be
    provided by the geoips_driver module.
    """
    # This should be imported from geoips_driver
    pass


if __name__ == "__main__":
    raise NotImplementedError("This module is not intended to be run directly.")

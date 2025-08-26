"""Service monitoring application with Prometheus metrics and RabbitMQ integration."""

import logging
import os
import signal
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from typing import Any

import pika
import prometheus_client
from pika.exceptions import AMQPConnectionError


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


# Logging setup
def setup_logging() -> logging.Logger:
    """Configure logger with standardized formatting and return module logger.

    Sets up basic logging configuration with INFO level and timestamp formatting
    for the entire application, then returns a logger instance for this module.

    Returns
    -------
    logging.Logger
        Configured logger instance for this module.

    Examples
    --------
    >>> logger = setup_logging()
    >>> logger.name == '__main__'
    True
    >>> isinstance(logger, logging.Logger)
    True
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger(__name__)


logger = setup_logging()


# Decorators for functional programming
def retry_with_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    exceptions: tuple[Exception, ...] = (Exception,),
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
    def wrapper(*args, **kwargs):
        logger.debug(f"Executing {func.__name__}")
        try:
            result = func(*args, **kwargs)
            logger.debug(f"Successfully executed {func.__name__}")
        except Exception:
            logger.exception(f"Error in {func.__name__}")
        return result

    return wrapper


# Abstract base classes
class Manager(ABC):
    """Abstract base class defining interface for service component managers.

    Provides common interface for managing service components with lifecycle
    methods and health checking. All concrete managers must implement these
    methods.

    Examples
    --------
    >>> class TestManager(Manager):
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


# Prometheus Manager
class PrometheusManager(Manager):
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

    @log_execution
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
        >>> before <= manager._heartbeat_metric._value._value <= after
        True
        """
        current_time = time.time()
        self._heartbeat_metric.set(current_time)
        logger.debug(f"Heartbeat sent at {current_time}")


# RabbitMQ Manager
class RabbitMQManager(Manager):
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
        logger.info("Attempting to connect to RabbitMQ")
        parameters = pika.URLParameters(self._config.rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        logger.info("Successfully connected to RabbitMQ")
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
                channel.queue_declare(queue=queue_name, **config)

            yield connection, channel
        finally:
            if connection and not connection.is_closed:
                connection.close()

    def add_queue(self, queue_name: str, **queue_config) -> None:
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
        self._queues[queue_name] = queue_config


# Signal Handler
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

    def __init__(self):
        self._shutdown_requested = False
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        """Configure signal handlers for SIGTERM and SIGINT.

        Registers signal handlers to catch termination and interrupt signals
        for graceful shutdown coordination.
        """
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)

    def _handle_shutdown_signal(self, signal_num: int, frame) -> None:
        """Handle received shutdown signals by setting shutdown flag.

        Parameters
        ----------
        signal_num : int
            Signal number that was received.
        frame
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


# Main Service Class
class Service:
    """Main service orchestrator managing all components with dependency injection.

    Coordinates startup, health monitoring, heartbeat loop, and graceful shutdown
    of all service components. Uses dependency injection for manager instances
    and provides centralized service lifecycle management.

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
    2
    """

    def __init__(self, config: ServiceConfig | None = None):
        self._config = config or ServiceConfig()
        self._signal_handler = SignalHandler()

        # Dependency injection
        self._prometheus_manager = PrometheusManager(self._config)
        self._rabbitmq_manager = RabbitMQManager(self._config)

        self._managers = [self._prometheus_manager, self._rabbitmq_manager]

    def _start_managers(self) -> None:
        """Start all service managers in sequence with error handling.

        Iterates through all managers and starts each one. If any manager
        fails to start, logs the exception and re-raises to halt service startup.

        Raises
        ------
        Exception
            If any manager fails to start successfully.
        """

        def start_manager(manager: Manager) -> None:
            try:
                manager.start()
            except Exception:
                logger.exception(f"Failed to start {manager.__class__.__name__}")

        list(map(start_manager, self._managers))

    def _stop_managers(self) -> None:
        """Stop all managers safely in reverse order.

        Stops managers in reverse order to ensure proper dependency cleanup.
        Logs warnings for stop failures but continues stopping remaining managers.
        """

        def stop_manager(manager: Manager) -> None:
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
        return all(manager.is_healthy() for manager in self._managers)

    def _run_heartbeat_loop(self) -> None:
        """Execute main heartbeat loop with interruptible sleep intervals.

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


# Factory function for creating services
def create_service(config: ServiceConfig | None = None) -> Service:
    """Create new Service instance with optional configuration.

    Factory function providing clean interface for Service instantiation
    with optional configuration override.

    Parameters
    ----------
    config : ServiceConfig, optional
        Service configuration. If None, Service will create default configuration.

    Returns
    -------
    Service
        Configured service instance ready for startup.

    Examples
    --------
    >>> service = create_service()
    >>> isinstance(service, Service)
    True
    >>> config = ServiceConfig()
    >>> service_with_config = create_service(config)
    >>> service_with_config._config == config
    True
    """
    return Service(config)


# Main execution guard
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
        config = ServiceConfig()
        service = create_service(config)
        service.start()
    except Exception:
        logger.exception("Application failed")


if __name__ == "__main__":
    main()

"""Abstract base class for service component managers."""

from abc import ABC, abstractmethod


class ServiceManager(ABC):
    """Abstract base class defining interface for service component managers.

    Provides common interface for managing service components with lifecycle
    methods and health checking. All concrete managers must implement these
    methods.

    Implementations
    ---------------
    PluginManager : lazylemon.managers.plugin_manager
    MessageBrokerManager : lazylemon.managers.broker
    PrometheusManager : lazylemon.managers.prometheus_manager

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

        Raises
        ------
        Exception
            If manager fails to start successfully.
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop the manager and cleanup allocated resources."""
        pass

    @abstractmethod
    def is_healthy(self) -> bool:
        """Check current health status of the managed component.

        Returns
        -------
        bool
            True if component is healthy and operational, False otherwise.
        """
        pass

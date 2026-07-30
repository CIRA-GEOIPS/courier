"""ServicePlugin protocol — the interface all plugins must implement."""

from typing import Any, ClassVar, Protocol


class ServicePlugin(Protocol):
    """Protocol defining the interface that all plugins must implement.

    This protocol specifies the required methods and properties that any
    service plugin must provide for integration with the plugin manager.

    Plugins should create an instance logger in __init__ using:
        self._logger = get_logger("plugin", self.name, config)

    where ``config`` is the ``ServiceConfig`` passed through the service
    reference.  Do not access ``service._config`` directly; use the public
    ``service.config`` property instead.

    Implementations
    ---------------
    DataMonitorBasePlugin : courier.interfaces.data_monitors
    JobBuilder : courier.interfaces.job_builders
    Dispatcher : courier.interfaces.dispatchers

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
    """

    name: ClassVar[str]
    version: ClassVar[str] = "0.0.0"

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

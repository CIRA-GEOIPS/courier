"""CourierError hierarchy — all package exceptions defined here."""


class CourierError(Exception):
    """Base exception for all courier errors."""


# ---------------------------------------------------------------------------
# Configuration errors
# ---------------------------------------------------------------------------


class ConfigurationError(CourierError):
    """Raised for invalid or missing configuration."""


class InvalidPluginConfigError(ConfigurationError):
    """Raised when a plugin's configuration fails validation."""


class MissingEnvironmentVariableError(ConfigurationError):
    """Raised when a required environment variable is not set."""


class MissingExtraError(ConfigurationError):
    """Raised when a plugin requires an optional extra that is not installed."""


# ---------------------------------------------------------------------------
# Pipeline errors
# ---------------------------------------------------------------------------


class PipelineError(CourierError):
    """Raised for errors during pipeline execution."""


class MetadataConflictError(PipelineError):
    """Raised when metadata values conflict during file enrichment.

    Attributes
    ----------
    field_name : str
        Name of the conflicting field.
    existing_value : object
        The existing value in the File.
    new_value : object
        The new value that conflicts.
    entry_name : str
        Name of the config entry that caused the conflict.
    """

    def __init__(
        self,
        field_name: str,
        existing_value: object,
        new_value: object,
        entry_name: str,
    ) -> None:
        self.field_name = field_name
        self.existing_value = existing_value
        self.new_value = new_value
        self.entry_name = entry_name
        super().__init__(
            f"Metadata conflict for field '{field_name}': "
            f"existing value '{existing_value}' conflicts with "
            f"new value '{new_value}' from entry '{entry_name}'",
        )


class NoMatchError(PipelineError):
    """Raised when no metadata config entry matches a file.

    Attributes
    ----------
    filename : str
        The filename that had no matches.
    configs_checked : list[str]
        Names of configs that were checked.
    """

    def __init__(self, filename: str, configs_checked: list[str]) -> None:
        self.filename = filename
        self.configs_checked = configs_checked
        super().__init__(
            f"No matching config entries found for filename '{filename}'. "
            f"Checked configs: {configs_checked}",
        )


class JobTimeoutError(PipelineError):
    """Raised when a job exceeds its configured timeout."""


# ---------------------------------------------------------------------------
# Broker errors
# ---------------------------------------------------------------------------


class BrokerError(PipelineError):
    """Base class for broker-related errors."""


class BrokerConnectionError(BrokerError):
    """Raised when a broker connection cannot be established."""


class BrokerPublishError(BrokerError):
    """Raised when publishing a message to the broker fails."""


class BrokerConsumeError(BrokerError):
    """Raised when consuming a message from the broker fails."""


class BrokerCapabilityError(BrokerError):
    """Raised when an operation requires a broker capability that is unavailable."""


class TransientBrokerError(BrokerError):
    """Raised for retryable broker failures (connection drops, timeouts)."""


class FatalBrokerError(BrokerError):
    """Raised for non-retryable broker failures (malformed message, permission)."""


# ---------------------------------------------------------------------------
# Routing errors
# ---------------------------------------------------------------------------


class RoutingError(ConfigurationError):
    """Base class for dispatcher-routing configuration errors."""


class InvalidIdentifierError(RoutingError):
    """Raised when a dispatcher identifier violates naming rules.

    Attributes
    ----------
    identifier : str
        The invalid identifier.
    reason : str
        Human-readable reason the identifier was rejected.
    """

    def __init__(self, identifier: str, reason: str) -> None:
        self.identifier = identifier
        self.reason = reason
        super().__init__(
            f"Invalid dispatcher identifier {identifier!r}: {reason}",
        )


class DuplicateTargetError(RoutingError):
    """Raised when a builder config lists the same target more than once.

    Attributes
    ----------
    location : str
        Builder identifier or route name where the duplicate occurred.
    targets : list[str]
        The offending targets list.
    """

    def __init__(self, location: str, targets: list[str]) -> None:
        self.location = location
        self.targets = list(targets)
        super().__init__(
            f"Duplicate targets at {location}: {self.targets}",
        )


class UnknownTargetError(RoutingError):
    """Raised when a builder targets a dispatcher that is not configured.

    Attributes
    ----------
    builder : str
        Builder identifier that declared the unknown target.
    unknown : list[str]
        Targets that did not match any configured dispatcher.
    known : list[str]
        Dispatchers actually present in the config.
    """

    def __init__(
        self,
        builder: str,
        unknown: list[str],
        known: list[str],
    ) -> None:
        self.builder = builder
        self.unknown = list(unknown)
        self.known = list(known)
        super().__init__(
            f"builder {builder!r} targets unknown dispatchers {sorted(self.unknown)}; "
            f"known={sorted(self.known)}",
        )


class AmbiguousImplicitTargetError(RoutingError):
    """Raised when implicit routing is requested but ≠1 dispatcher is defined.

    Attributes
    ----------
    builder : str
        Builder that declared no explicit targets.
    dispatcher_count : int
        Number of configured dispatchers.
    """

    def __init__(self, builder: str, dispatcher_count: int) -> None:
        self.builder = builder
        self.dispatcher_count = dispatcher_count
        super().__init__(
            f"builder {builder!r} has no targets and {dispatcher_count} "
            "dispatchers exist; implicit routing requires exactly one dispatcher",
        )


# ---------------------------------------------------------------------------
# Discovery / plugin errors
# ---------------------------------------------------------------------------


class DiscoveryError(PipelineError):
    """Base class for plugin discovery errors."""


class PluginNotFoundError(DiscoveryError):
    """Raised when a requested plugin cannot be found in any registry."""


class UnknownInterfaceError(DiscoveryError):
    """Raised when a plugin specifies an interface name that is not registered."""


class PluginValidationError(DiscoveryError):
    """Raised when a plugin fails schema or structural validation."""


class RegistryInitError(DiscoveryError):
    """Raised when pluginify registry creation fails at startup."""


class PluginError(PipelineError):
    """Base class for plugin runtime errors."""


class PluginStartupError(PluginError):
    """Raised when a plugin fails to start."""


class PluginHealthCheckError(PluginError):
    """Raised when a plugin's health check reports unhealthy status."""


class PluginMaxRestartsExceededError(PluginError):
    """Raised when a plugin exceeds its maximum restart attempts."""


class InvalidTransitionError(PluginError):
    """Raised when an invalid plugin state transition is attempted."""


# ---------------------------------------------------------------------------
# State sync errors
# ---------------------------------------------------------------------------


class StateSyncError(CourierError):
    """Base class for HA state synchronization errors."""


class StateSyncConnectionError(StateSyncError):
    """Raised when the state-sync Redis connection cannot be established."""

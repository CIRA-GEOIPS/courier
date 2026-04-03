"""GeoIPSDriverError hierarchy — all package exceptions defined here."""


class GeoIPSDriverError(Exception):
    """Base exception for all lazylemon errors."""


# ---------------------------------------------------------------------------
# Configuration errors
# ---------------------------------------------------------------------------


class ConfigurationError(GeoIPSDriverError):
    """Raised for invalid or missing configuration."""


class InvalidPluginConfigError(ConfigurationError):
    """Raised when a plugin's configuration fails validation."""


class MissingEnvironmentVariableError(ConfigurationError):
    """Raised when a required environment variable is not set."""


# ---------------------------------------------------------------------------
# Pipeline errors
# ---------------------------------------------------------------------------


class PipelineError(GeoIPSDriverError):
    """Raised for errors during pipeline execution."""


class MetadataConflictError(PipelineError):
    """Raised when metadata values conflict during file enrichment."""


class NoMatchError(PipelineError):
    """Raised when no metadata config entry matches a file."""


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


class StateSyncError(GeoIPSDriverError):
    """Base class for HA state synchronization errors."""


class StateSyncConnectionError(StateSyncError):
    """Raised when the state-sync Redis connection cannot be established."""

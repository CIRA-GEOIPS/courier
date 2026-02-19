"""Logging infrastructure for GeoIPS Driver with Loki integration.

This module provides comprehensive logging support with optional Grafana Loki
integration, custom TRACE logging level, and contextualized loggers for
services, managers, and plugins.

Functions
---------
get_logger
    Create a contextualized logger with optional Loki integration.
setup_logging
    Backward-compatible wrapper for get_logger (legacy interface).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from rich.logging import RichHandler

if TYPE_CHECKING:
    from geoips_driver.interfaces.module_based.service import ServiceConfig

try:
    import logging_loki  # type: ignore
except ImportError:
    logging_loki = None  # type: ignore[assignment]

# Define TRACE logging level (below DEBUG=10)
TRACE_LEVEL = 5
logging.addLevelName(TRACE_LEVEL, "TRACE")


def trace(self: logging.Logger, message: str, *args: Any, **kwargs: Any) -> None:
    """Log message with TRACE level.

    Parameters
    ----------
    self : logging.Logger
        Logger instance (automatically passed when called as logger.trace()).
    message : str
        Log message format string.
    *args : Any
        Positional arguments for message formatting.
    **kwargs : Any
        Keyword arguments for logging call.

    Examples
    --------
    >>> logger = get_logger("service", "my-service")
    >>> logger.trace("Very detailed trace message")
    """
    if self.isEnabledFor(TRACE_LEVEL):
        self._log(TRACE_LEVEL, message, args, **kwargs)


# Monkey-patch the Logger class to add trace() method
logging.Logger.trace = trace  # type: ignore[attr-defined]


class ContextAdapter(logging.LoggerAdapter):
    """Logger adapter that prepends context to all log messages.

    This adapter automatically prepends source identification to log messages
    in the format: [{source_type}: {source_name}] {message}

    Parameters
    ----------
    logger : logging.Logger
        Base logger to wrap.
    extra : dict[str, Any]
        Extra context including 'source_type' and 'source_name'.

    Examples
    --------
    >>> base_logger = logging.getLogger("my.logger")
    >>> adapter = ContextAdapter(base_logger,
                                {"source_type": "service", "source_name": "my-svc"})
    >>> adapter.info("Starting")  # Logs: [Service: my-svc] Starting
    """

    def process(
        self,
        msg: str,
        kwargs: dict[str, Any] | Any,
    ) -> tuple[str, dict[str, Any]]:
        """Process log message to prepend context.

        Parameters
        ----------
        msg : str
            Original log message.
        kwargs : dict[str, Any] or Any
            Logging keyword arguments.

        Returns
        -------
        tuple[str, dict[str, Any]]
            Modified message with context prepended, and kwargs.
        """
        if self.extra is None:
            return msg, kwargs if isinstance(kwargs, dict) else {}

        source_type = str(self.extra.get("source_type", "unknown"))
        source_name = str(self.extra.get("source_name", "unknown"))
        # Capitalize first letter of source_type for display
        source_type_display = source_type.capitalize()
        if source_type == "" and source_name == "":
            return f"[] {msg}", kwargs if isinstance(
                kwargs,
                dict,
            ) else {}
        else:
            return (
                f"[{source_type_display}: {source_name}] {msg}",
                kwargs
                if isinstance(
                    kwargs,
                    dict,
                )
                else {},
            )


def _create_loki_handler(
    url: str,
    tags: dict[str, str],
    fallback_logger: logging.Logger,
) -> Any:
    """Attempt to create Loki handler with graceful fallback.

    Parameters
    ----------
    url : str
        Loki push API URL.
    tags : dict[str, str]
        Labels to attach to all log entries.
    fallback_logger : logging.Logger
        Logger to use for warning messages if Loki setup fails.

    Returns
    -------
    logging_loki.LokiHandler or None
        Configured Loki handler, or None if setup failed.

    Examples
    --------
    >>> logger = logging.getLogger("fallback")
    >>> handler = _create_loki_handler("http://localhost:3100/loki/api/v1/push",
                                       {"service": "test"}, logger)
    """
    try:
        if not logging_loki or logging_loki is None:
            raise ImportError  # noqa: TRY301
        # Set level tag to 'level' for Grafana
        logging_loki.emitter.LokiEmitter.level_tag = "level"

        handler = logging_loki.LokiHandler(
            url=url,
            version="1",
            tags=tags,
        )
    except ImportError:
        fallback_logger.warning(
            "python-logging-loki not installed. Falling back to console-only logging. "
            "Install with: pip install python-logging-loki",
        )
        return None
    except (ConnectionError, OSError) as e:
        fallback_logger.warning(
            f"Failed to initialize Loki handler: {e}. "
            "Falling back to console-only logging.",
        )
        return None
    except Exception as e:
        fallback_logger.warning(
            f"Unexpected error initializing Loki handler: {e}. "
            "Falling back to console-only logging.",
        )
        return None
    else:
        return handler


def get_logger(
    source_type: str,
    source_name: str,
    config: ServiceConfig | None = None,
) -> logging.Logger:
    """Create a contextualized logger with optional Loki integration.

    This function is the primary logging factory for the GeoIPS Driver system.
    It creates loggers that automatically prepend source identification to
    all messages and optionally ship logs to Grafana Loki.

    Parameters
    ----------
    source_type : str
        Type of source: 'service', 'plugin', 'manager', or 'module'.
    source_name : str
        Identifier for the source (service ID, plugin name, class name, or module name).
    config : ServiceConfig or None, optional
        Service configuration containing Loki settings. If None, uses console-only
        logging with default settings (DEBUG level, no Loki).

    Returns
    -------
    logging.Logger
        Configured logger instance with appropriate handlers and formatting.
        Note: Returns a LoggerAdapter that wraps the base logger to provide
        automatic context prepending.

    Examples
    --------
    >>> from geoips_driver.interfaces.module_based.service import ServiceConfig
    >>> config = ServiceConfig()
    >>> logger = get_logger("service", "my-service-id", config)
    >>> logger.info("Service start")  # Logs: [Service: my-service-id] Service start

    >>> # Without config (console-only, DEBUG level)
    >>> logger = get_logger("module", "my_module")
    >>> logger.debug("Debug message")  # Logs: [Module: my_module] Debug message
    """
    # Create unique logger name based on source type and name
    logger_name = f"geoips_driver.{source_type}.{source_name}"
    base_logger = logging.getLogger(logger_name)

    # Set propagate to False to avoid duplicate messages
    base_logger.propagate = False

    # Only configure if not already configured
    if not base_logger.handlers:
        # Always add RichHandler for console output
        rich_handler = RichHandler(rich_tracebacks=True)
        formatter = logging.Formatter(
            "%(message)s",
            datefmt="[%X]",
        )
        rich_handler.setFormatter(formatter)
        base_logger.addHandler(rich_handler)

        # Determine log level
        if config is not None:
            # Parse log level from config
            log_level_str = config.log_level.upper()
            if log_level_str == "TRACE":
                log_level = TRACE_LEVEL
            else:
                log_level = getattr(logging, log_level_str, logging.DEBUG)

            # Enforce INFO minimum in production mode
            if config.production_mode and log_level < logging.INFO:
                base_logger.warning(
                    f"Production mode enabled: enforcing minimum log level INFO "
                    f"(requested: {config.log_level})",
                )
                log_level = logging.INFO

            base_logger.setLevel(log_level)

            # Add Loki handler if enabled and URL provided
            if config.loki_enabled and config.loki_url:
                # Prepare tags for Loki
                tags: dict[str, str] = {
                    "service": config.service_id,
                    "namespace": config.service_namespace,
                    "source_type": source_type,
                }

                # Add plugin tag if source is a plugin
                if source_type == "plugin":
                    tags["plugin"] = source_name

                # Create a temporary logger for fallback warnings
                fallback_logger = logging.getLogger(f"{logger_name}.fallback")
                if not fallback_logger.handlers:
                    fallback_handler = RichHandler(rich_tracebacks=True)
                    fallback_handler.setFormatter(formatter)
                    fallback_logger.addHandler(fallback_handler)
                    fallback_logger.setLevel(logging.WARNING)

                # Attempt to create Loki handler
                loki_handler = _create_loki_handler(
                    config.loki_url,
                    tags,
                    fallback_logger,
                )

                if loki_handler is not None:
                    base_logger.addHandler(loki_handler)
        else:
            # Default to DEBUG level when no config provided
            base_logger.setLevel(logging.DEBUG)

    # Wrap logger in ContextAdapter to prepend source information
    adapter = ContextAdapter(
        base_logger,
        {"source_type": source_type, "source_name": source_name},
    )

    return adapter  # type: ignore[return-value]


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
    use get_logger() instead:

    >>> # Prefer this
    >>> logger = get_logger("module", __name__)
    >>>
    >>> # Over this
    >>> logger = setup_logging(__name__)
    """
    module_name = name if name else "__main__"
    return get_logger("module", module_name, None)

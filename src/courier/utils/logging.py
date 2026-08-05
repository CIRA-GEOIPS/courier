"""Logging infrastructure for courier with Loki integration.

This module provides comprehensive logging support with Grafana Loki
integration, custom TRACE logging level, and contextualized loggers for
services, managers, and plugins.

``python-logging-loki`` is a required dependency: Loki shipping is a
first-class part of courier's observability story, and an optional import
meant an operator who set ``loki_enabled`` got a warning and console-only
logs instead of the logs they asked for.

Functions
---------
get_logger
    Create a contextualized logger with Loki integration.
"""

from __future__ import annotations

import logging
import sys
import time
from typing import TYPE_CHECKING, Any

import logging_loki  # type: ignore[import-untyped]
from rich.logging import RichHandler

if TYPE_CHECKING:
    from courier.config import ServiceConfig

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

    def trace(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log at the custom TRACE level (below DEBUG).

        ``get_logger`` hands callers this adapter, not a raw ``Logger``, so
        the ``logging.Logger.trace`` monkeypatch alone left ``logger.trace()``
        raising ``AttributeError`` -- despite TRACE being an accepted value
        for ``COURIER_LOG_LEVEL`` and ``--log-level``.
        """
        if self.isEnabledFor(TRACE_LEVEL):
            msg, kwargs = self.process(msg, kwargs)
            self.logger.log(TRACE_LEVEL, msg, *args, **kwargs)

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

    Wraps the raw ``logging_loki.LokiHandler`` in a rate-limited error
    suppressor so that a transient backend outage (Loki 500s) does not
    flood stderr with a traceback per log record.

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
        logging_loki.emitter.LokiEmitter.level_tag = "level"

        raw_handler = logging_loki.LokiHandler(
            url=url,
            version="1",
            tags=tags,
        )

        handler = _ResilientLokiHandler(raw_handler)
    except (ConnectionError, OSError) as e:
        fallback_logger.warning(
            f"Failed to initialize Loki handler: {e}. "
            "Falling back to console-only logging.",
        )
        return None
    except (AttributeError, TypeError, ValueError) as e:
        fallback_logger.warning(
            f"Unexpected error initializing Loki handler: {e}. "
            "Falling back to console-only logging.",
        )
        return None
    else:
        return handler


class _ResilientLokiHandler(logging.Handler):
    """Rate-limited wrapper that suppresses stderr storms on Loki outage.

    When Loki returns 500s, ``logging_loki.LokiHandler.emit()`` raises
    ``ValueError``, which triggers Python's ``Handler.handleError()``
    which prints a full traceback to stderr for *every* log record.
    This wrapper rate-limits the diagnostic to once per 60 seconds.
    """

    _ERROR_SUPPRESS_INTERVAL = 60.0

    def __init__(self, delegate: logging.Handler) -> None:
        level = logging.NOTSET
        try:
            raw = delegate.level
            if isinstance(raw, (int, str)):
                level = int(raw)
        except (ValueError, TypeError):
            pass
        super().__init__(level=level)
        self.delegate = delegate
        self._last_error_log: float = 0.0

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.delegate.emit(record)
        except Exception:
            now = time.monotonic()
            if now - self._last_error_log > self._ERROR_SUPPRESS_INTERVAL:
                self._last_error_log = now
                sys.stderr.write(
                    "courier: Loki handler failed to push log records "
                    "(rate-limited to 1/min); check Loki backend health.\n",
                )
            # Deliberately not delegate.handleError(record): that is exactly
            # the per-record traceback this wrapper exists to suppress, and it
            # was only silenced when production_mode happened to set
            # logging.raiseExceptions = False. The record is dropped; the
            # rate-limited notice above is the diagnostic.

    def handleError(self, record: logging.LogRecord) -> None:  # noqa: N802
        pass  # fully suppressed; diagnostics handled in emit()

    def close(self) -> None:
        self.delegate.close()
        super().close()

    def __repr__(self) -> str:
        return f"ResilientLokiHandler({self.delegate!r})"


def get_logger(
    source_type: str,
    source_name: str,
    config: ServiceConfig | None = None,
) -> ContextAdapter:
    """Create a contextualized logger with optional Loki integration.

    This function is the primary logging factory for the courier system.
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
    >>> from courier.config import ServiceConfig
    >>> config = ServiceConfig()
    >>> logger = get_logger("service", "my-service-id", config)
    >>> logger.info("Service start")  # Logs: [Service: my-service-id] Service start

    >>> # Without config (console-only, DEBUG level)
    >>> logger = get_logger("module", "my_module")
    >>> logger.debug("Debug message")  # Logs: [Module: my_module] Debug message
    """
    # Create unique logger name based on source type and name
    logger_name = f"courier.{source_type}.{source_name}"
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
                # In production, suppress Python's per-record traceback on
                # handler failures (e.g. Loki 500s).  The _ResilientLokiHandler
                # already rate-limits diagnostics to 1/min independently.
                if config.production_mode:
                    logging.raiseExceptions = False
                # Prepare tags for Loki
                tags: dict[str, str] = {
                    "service": config.service_id,
                    "namespace": config.namespace,
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

    return adapter

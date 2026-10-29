"""Logging infrastructure for courier with Loki integration.

This module provides comprehensive logging support with Grafana Loki
integration, custom TRACE logging level, and contextualized loggers for
services, managers, and plugins.

``loki-logger-handler`` is a required dependency: Loki shipping is a
first-class part of courier's observability story, and an optional import
meant an operator who set ``loki_enabled`` got a warning and console-only
logs instead of the logs they asked for.

Functions
---------
get_logger
    Create a contextualized logger with Loki integration.
"""

from __future__ import annotations

import json
import logging
import sys
import threading
import time
from typing import TYPE_CHECKING, Any

from loki_logger_handler.formatters.logger_formatter import (  # type: ignore[import-untyped]
    LoggerFormatter,
)
from loki_logger_handler.loki_logger_handler import (  # type: ignore[import-untyped]
    LokiLoggerHandler,
)
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
            return f"[] {msg}", (
                kwargs
                if isinstance(
                    kwargs,
                    dict,
                )
                else {}
            )
        else:
            return (
                f"[{source_type_display}: {source_name}] {msg}",
                (
                    kwargs
                    if isinstance(
                        kwargs,
                        dict,
                    )
                    else {}
                ),
            )


class _SafeLoggerFormatter(LoggerFormatter):
    """``LoggerFormatter`` that survives a non-JSON-serializable extra.

    ``LokiLoggerHandler`` calls ``json.dumps`` on a whole batch in ``_send``,
    well after the records that make it up have left ``emit()``
    successfully. One record carrying e.g. a ``set`` or a custom object as an
    extra would otherwise raise there and drop every record in that flush,
    not just its own.
    """

    def format(
        self,
        record: logging.LogRecord,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Format *record*, coercing any non-JSON-serializable extra to str.

        Parameters
        ----------
        record : logging.LogRecord
            The log record to format.

        Returns
        -------
        tuple[dict[str, Any], dict[str, Any]]
            The formatted record and its loki_metadata, as returned by
            ``LoggerFormatter.format``, with unserializable values replaced.
        """
        formatted, loki_metadata = super().format(record)
        for key, value in formatted.items():
            try:
                json.dumps(value)
            except (TypeError, ValueError):  # ValueError: circular reference
                formatted[key] = str(value)
        return formatted, loki_metadata


class _CourierLokiHandler(LokiLoggerHandler):
    """``LokiLoggerHandler`` with a rate-limited stderr notice, not tracebacks.

    ``LokiLoggerHandler.emit()`` never raises: formatting and send errors are
    routed to ``handle_unexpected_error``, whose default implementation is
    silent unless ``enable_self_errors`` is set, in which case it prints a
    full traceback for every failed batch. This override stays silent but
    writes a single rate-limited (60s) diagnostic to stderr, so a Loki outage
    is still visible without flooding the console.
    """

    _ERROR_SUPPRESS_INTERVAL = 60.0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._last_error_log: float = 0.0

    def handle_unexpected_error(self, e: Exception) -> None:
        """Mark the handler as errored and rate-limit a stderr notice.

        Parameters
        ----------
        e : Exception
            The exception raised while formatting or sending a batch.

        Returns
        -------
        None
        """
        super().handle_unexpected_error(e)
        now = time.monotonic()
        if now - self._last_error_log > self._ERROR_SUPPRESS_INTERVAL:
            self._last_error_log = now
            sys.stderr.write(
                "courier: Loki handler failed to push log records "
                "(rate-limited to 1/min); check Loki backend health.\n",
            )

    def handleError(self, record: logging.LogRecord) -> None:  # noqa: N802
        pass  # fully suppressed; diagnostics handled in handle_unexpected_error()

    def __repr__(self) -> str:
        return f"CourierLokiHandler({self.labels!r})"


#: Handlers keyed by (url, flush_interval, frozenset(tags.items())). Each
#: ``LokiLoggerHandler`` starts its own daemon flush thread, so repeated calls
#: to ``get_logger`` with the same url/interval/labels must share one handler
#: instead of leaking a thread per call.
_loki_handler_cache: dict[tuple[str, float, frozenset[tuple[str, str]]], Any] = {}
# Plugins call get_logger from their own threads; without this, two racing
# first calls for one label set would each start a flush thread.
_loki_handler_cache_lock = threading.Lock()


def _reset_loki_handler_cache() -> None:
    """Clear the module-level Loki handler cache.

    Not used by production code; exists so tests can isolate themselves
    from handlers (and their flush threads) created by earlier tests.
    """
    _loki_handler_cache.clear()


def _create_loki_handler(
    url: str,
    tags: dict[str, str],
    fallback_logger: logging.Logger,
    flush_interval: float,
) -> Any:
    """Attempt to create (or reuse) a Loki handler with graceful fallback.

    Parameters
    ----------
    url : str
        Loki push API URL.
    tags : dict[str, str]
        Labels to attach to all log entries.
    fallback_logger : logging.Logger
        Logger to use for warning messages if Loki setup fails.
    flush_interval : float
        Seconds between batched pushes to Loki.

    Returns
    -------
    _CourierLokiHandler or None
        Configured Loki handler, or None if setup failed.

    Examples
    --------
    >>> logger = logging.getLogger("fallback")
    >>> handler = _create_loki_handler("http://localhost:3100/loki/api/v1/push",
                                        {"service": "test"}, logger, 2.0)
    """
    cache_key = (url, flush_interval, frozenset(tags.items()))
    with _loki_handler_cache_lock:
        cached = _loki_handler_cache.get(cache_key)
        if cached is not None:
            return cached
        return _build_loki_handler(
            url,
            tags,
            fallback_logger,
            flush_interval,
            cache_key,
        )


def _build_loki_handler(
    url: str,
    tags: dict[str, str],
    fallback_logger: logging.Logger,
    flush_interval: float,
    cache_key: tuple[str, float, frozenset[tuple[str, str]]],
) -> Any:
    """Construct and cache a Loki handler; caller holds the cache lock."""
    try:
        handler = _CourierLokiHandler(
            url=url,
            labels=tags,
            label_keys={"level"},
            timeout=flush_interval,
            compressed=True,
            default_formatter=_SafeLoggerFormatter(),
        )
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
        _loki_handler_cache[cache_key] = handler
        return handler


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
        logging with default settings (INFO level, no Loki).

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

    >>> # Without config (console-only, INFO level)
    >>> logger = get_logger("module", "my_module")
    >>> logger.info("Info message")  # Logs: [Module: my_module] Info message
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
                log_level = getattr(logging, log_level_str, logging.INFO)

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
                # _CourierLokiHandler.emit() never raises -- formatting and
                # send errors are routed to handle_unexpected_error, which
                # already rate-limits its own diagnostic to 1/min -- so this
                # is no longer load-bearing for Loki. It still suppresses
                # Handler.handleError()'s per-record traceback for any other
                # handler attached to this logger (e.g. a broken console
                # write), which is a reasonable production default on its
                # own, so it stays.
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
                    config.loki_flush_interval,
                )

                if loki_handler is not None:
                    base_logger.addHandler(loki_handler)
        else:
            # Default to INFO level when no config provided
            base_logger.setLevel(logging.INFO)

    # Wrap logger in ContextAdapter to prepend source information
    adapter = ContextAdapter(
        base_logger,
        {"source_type": source_type, "source_name": source_name},
    )

    return adapter


def route_external_logger(logger_name: str, target: ContextAdapter) -> None:
    """Adapt stray loggers to a specified logging system.

    Lets external loggers (e.g. OpenTelemetry's error logging) go through
    the same pipeline as courier's logging system instead of falling through to
    the console.

    Parameters
    ----------
    logger_name : str
        Name of the logger as it relates to their module path
    target : ContextAdapter
        The target logging system to adapt to

    Returns
    -------
    None
        Configures the external context in-place

    Examples
    --------
    >>> route_external_logger("opentelemetry", otel_logger)
    >>> route_external_logger("grafana", grafana_logger)
    """
    external = logging.getLogger(logger_name)
    external.propagate = False
    external.setLevel(target.logger.level)
    for handler in target.logger.handlers:
        if handler not in external.handlers:
            external.addHandler(handler)

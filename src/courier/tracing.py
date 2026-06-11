"""OpenTelemetry tracing infrastructure for Courier.

Design notes
------------
Tracing is a global ON/OFF concern: it is controlled by
``ServiceConfig.tracing_enabled`` and the ``OTEL_TRACES_EXPORTER=none``
environment variable.  Both paths converge on a single ``TracerProvider``
(real or NoOp) set at startup via ``opentelemetry.trace.set_tracer_provider``.

**Per-plugin tracing toggle — deferred (Phase 2.4).**
Because the ``TracerProvider`` is installed once at application bootstrap and
every plugin retrieves its tracer from the global provider, selectively
enabling/disabling tracing per plugin is not currently possible without
adding a custom ``SpanProcessor`` that filters spans by plugin name.  This
feature is intentionally deferred to a future release.

**The recommended overhead-control knob is ``tracing_sample_rate``.**
At scale, use ``tracing_sample_rate`` (a float in ``[0.0, 1.0]``) to control
the proportion of requests that produce spans.  This avoids the need for
per-plugin toggles in the common case.
"""

from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from courier.config import ServiceConfig

# ---------------------------------------------------------------------------
# Span attribute constants (Law 5: Intentional Naming)
# ---------------------------------------------------------------------------
ATTR_CORRELATION_ID = "courier.correlation_id"
ATTR_FILE_PATH = "courier.file.path"
ATTR_FILE_HOSTNAME = "courier.file.hostname"
ATTR_FILE_SOURCE = "courier.file.source"
ATTR_FILE_INSTRUMENT = "courier.file.instrument"
ATTR_NUM_MATCHERS = "courier.num_matchers"
ATTR_JOB_ID = "courier.job.id"
ATTR_JOB_NAME = "courier.job.name"
ATTR_JOB_TARGETS = "courier.job.targets"
ATTR_JOB_FILE_COUNT = "courier.job.file_count"
ATTR_JOB_GROUP_NAME = "courier.job_group.name"
ATTR_EXECUTION_RETURN_CODE = "courier.execution_log.return_code"
ATTR_EXECUTION_HOSTNAME = "courier.execution_log.hostname"
ATTR_DISPATCH_LATENCY = "courier.dispatch_latency"
ATTR_TARGET = "courier.target"
ATTR_PLUGIN_NAME = "plugin.name"
ATTR_PLUGIN_VERSION = "plugin.version"
ATTR_PLUGIN_FAMILY = "plugin.family"

_logger = logging.getLogger(__name__)

# Module-level singleton for idempotent init
_tracer_provider: Any = None


def init_tracing(config: ServiceConfig) -> None:
    """Initialize the global OpenTelemetry TracerProvider (idempotent)."""
    global _tracer_provider  # noqa: PLW0603
    import os  # noqa: PLC0415

    # Idempotent: if already initialized, return immediately
    if _tracer_provider is not None:
        return

    # Respect OTEL_TRACES_EXPORTER=none as secondary disable
    if os.environ.get("OTEL_TRACES_EXPORTER", "").lower() == "none":
        config = config.__class__(**{**config.__dict__, "tracing_enabled": False})

    if not config.tracing_enabled:
        from opentelemetry.trace import (  # noqa: PLC0415
            NoOpTracerProvider,
            set_tracer_provider,
        )

        _tracer_provider = NoOpTracerProvider()
        set_tracer_provider(_tracer_provider)
        _logger.info("OpenTelemetry tracing disabled (NoOp provider)")
        return

    # Real OTLP provider
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (  # noqa: PLC0415
        OTLPSpanExporter,
    )
    from opentelemetry.sdk.resources import (  # noqa: PLC0415
        SERVICE_NAME,
        Resource,
    )
    from opentelemetry.sdk.trace import TracerProvider  # noqa: PLC0415
    from opentelemetry.sdk.trace.export import (  # noqa: PLC0415
        BatchSpanProcessor,
    )
    from opentelemetry.sdk.trace.sampling import (  # noqa: PLC0415
        ALWAYS_ON,
        ParentBased,
        TraceIdRatioBased,
    )
    from opentelemetry.trace import set_tracer_provider  # noqa: PLC0415

    service_name = config.tracing_service_name or config.service_id

    # Sampling strategy
    if config.tracing_sample_rate >= 1.0:
        sampler = ParentBased(root=ALWAYS_ON)
    else:
        sampler = ParentBased(
            root=TraceIdRatioBased(config.tracing_sample_rate),
        )

    # Resource
    resource = Resource(attributes={SERVICE_NAME: service_name})

    # Exporter with error callback
    def _on_export_failure(span_data: Any) -> None:
        _logger.warning(
            "Failed to export %d spans to OTLP endpoint %s",
            len(span_data) if hasattr(span_data, "__len__") else 1,
            config.tracing_endpoint,
        )

    exporter = OTLPSpanExporter(endpoint=config.tracing_endpoint)
    # Monkey-patch a warning on export failure by wrapping the export method
    _original_export = exporter.export

    def _export_with_warning(spans: Any) -> Any:
        from opentelemetry.sdk.trace.export import (  # noqa: PLC0415
            SpanExportResult,
        )

        result = _original_export(spans)
        if result == SpanExportResult.FAILURE:
            _on_export_failure(spans)
        return result

    exporter.export = _export_with_warning  # type: ignore[method-assign]

    processor = BatchSpanProcessor(exporter)
    provider = TracerProvider(
        resource=resource,
        sampler=sampler,
        active_span_processor=processor,  # type: ignore[arg-type]
    )
    _tracer_provider = provider
    set_tracer_provider(provider)
    _logger.info(
        "OpenTelemetry tracing initialized: service=%s endpoint=%s sample_rate=%.2f",
        service_name,
        config.tracing_endpoint,
        config.tracing_sample_rate,
    )


def get_tracer(name: str) -> Any:
    """Return a named tracer from the global provider."""
    from opentelemetry.trace import (  # noqa: PLC0415
        get_tracer as _get_tracer,
    )

    return _get_tracer(name)


def shutdown_tracing() -> None:
    """Flush remaining spans and shut down the global TracerProvider."""
    global _tracer_provider  # noqa: PLW0603
    if _tracer_provider is None:
        return
    # NoOpTracerProvider has neither force_flush nor shutdown — clean exit
    from opentelemetry.trace import NoOpTracerProvider  # noqa: PLC0415

    if isinstance(_tracer_provider, NoOpTracerProvider):
        _tracer_provider = None
        return
    try:
        _tracer_provider.force_flush(timeout_millis=5000)
    except Exception:
        _logger.warning("Error during tracer provider force_flush", exc_info=True)
    try:
        _tracer_provider.shutdown()
    except Exception:
        _logger.warning("Error during tracer provider shutdown", exc_info=True)
    finally:
        _tracer_provider = None


def reset_tracing() -> None:
    """Tear down tracing for test isolation. Not for production use."""
    shutdown_tracing()


def trace_plugin_method(
    name: str,
    attributes: dict[str, Any] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Wrap a method in a span for tracing.

    Raises TypeError at decoration time if applied to a generator function
    (Law 4: Fail Loud — generator spans would close on return, not exhaustion).
    """
    from opentelemetry.trace import (  # noqa: PLC0415
        get_tracer as _get_tracer,
    )

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.isgeneratorfunction(func):
            raise TypeError(
                f"@trace_plugin_method cannot be applied to generator function "
                f"{func.__qualname__!r}. Use inline start_as_current_span instead.",
            )

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            tracer = _get_tracer(__name__)
            with tracer.start_as_current_span(
                name,
                attributes=attributes or {},
            ):
                return func(*args, **kwargs)

        wrapper.__name__ = func.__name__
        wrapper.__qualname__ = func.__qualname__
        wrapper.__doc__ = func.__doc__
        wrapper.__wrapped__ = func  # type: ignore[attr-defined]
        return wrapper

    return decorator


def inject_trace_headers() -> dict[str, str]:
    """Inject the current span context into a W3C header dict.

    Returns a dict with 'traceparent' and 'tracestate' keys suitable for
    passing as Kombu message headers.  Called from Service.emit() — the
    single, consolidated injection point.
    """
    from opentelemetry.propagate import inject  # noqa: PLC0415

    carrier: dict[str, str] = {}
    inject(carrier)
    return carrier


def extract_context(headers: dict[str, str]) -> Any:
    """Extract W3C trace context from message headers (fail-safe).

    Returns a parsed OpenTelemetry Context object, or an empty Context()
    on any failure.  Never crashes the pipeline on bad trace headers.
    Implements Parse at boundary (Law 2).
    """
    from opentelemetry import context  # noqa: PLC0415
    from opentelemetry.propagate import extract  # noqa: PLC0415

    try:
        return extract(headers)
    except Exception:
        _logger.debug(
            "Failed to extract trace context from headers %r; "
            "returning empty context",
            headers,
            exc_info=True,
        )
        return context.Context()

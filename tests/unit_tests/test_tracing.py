"""Unit tests for OpenTelemetry tracing infrastructure.

Tests cover the tracing lifecycle (init/shutdown/reset), NoOp mode,
W3C trace context propagation, the trace_plugin_method decorator,
and configuration validation.
"""

from __future__ import annotations

from typing import Any

import pytest

from courier.config import ServiceConfig
from courier.errors import ConfigurationError
from courier.tracing import (
    ATTR_CORRELATION_ID,
    extract_context,
    get_tracer,
    init_tracing,
    inject_trace_headers,
    reset_tracing,
    shutdown_tracing,
    trace_plugin_method,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_list_exporter() -> tuple[Any, list[Any]]:
    """Create a span exporter + span list pair for round-trip verification.

    Returns (exporter_instance, spans_list) where *spans_list* is mutated
    in-place by the exporter's ``export()`` method.
    """
    from opentelemetry.sdk.trace.export import (  # noqa: PLC0415
        SpanExportResult,
        SpanExporter,
    )

    spans: list[Any] = []

    class _ListExporter(SpanExporter):
        def export(self, span_data: Any) -> Any:
            spans.extend(span_data)
            return SpanExportResult.SUCCESS

        def shutdown(self) -> None:
            pass

    return _ListExporter(), spans


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_tracing_after_test() -> Any:
    """Ensure tracing singleton is reset before and after each test."""
    reset_tracing()
    _force_noop_global_provider()
    yield
    reset_tracing()
    _force_noop_global_provider()


def _force_noop_global_provider() -> None:
    """Force the OTel API global tracer provider to NoOp for test isolation.

    The OTel API's ``set_tracer_provider`` can only be called once per
    process lifetime (gated by ``_TRACER_PROVIDER_SET_ONCE``).  After a
    test initializes a real ``TracerProvider`` the gate is permanently
    closed, so subsequent calls are no-ops.  This helper resets the gate
    and installs a fresh ``NoOpTracerProvider`` to guarantee test isolation.
    """
    from opentelemetry.trace import (  # noqa: PLC0415
        NoOpTracerProvider,
        set_tracer_provider,
    )
    from opentelemetry.util._once import Once  # noqa: PLC0415

    import opentelemetry.trace  # noqa: PLC0415

    opentelemetry.trace._TRACER_PROVIDER_SET_ONCE = Once()
    set_tracer_provider(NoOpTracerProvider())


@pytest.fixture
def disabled_config() -> ServiceConfig:
    """ServiceConfig with tracing disabled."""
    return ServiceConfig(tracing_enabled=False)


# ---------------------------------------------------------------------------
# 5.1: init_tracing / shutdown_tracing lifecycle
# ---------------------------------------------------------------------------


class TestInitShutdownLifecycle:
    """Tests for init_tracing() and shutdown_tracing() lifecycle."""

    def test_init_tracing_noop_when_disabled(self, disabled_config: ServiceConfig) -> None:
        """Tracing disabled produces a NoOp TracerProvider with non-recording spans."""
        init_tracing(disabled_config)
        tracer = get_tracer("test")
        span = tracer.start_span("test_span")
        assert span is not None
        assert not span.get_span_context().is_valid
        span.end()

    def test_init_tracing_is_idempotent(self, disabled_config: ServiceConfig) -> None:
        """Calling init_tracing() twice does not re-initialise or crash."""
        init_tracing(disabled_config)
        init_tracing(disabled_config)  # no-op second call
        shutdown_tracing()

    def test_shutdown_clears_singleton(self, disabled_config: ServiceConfig) -> None:
        """After shutdown_tracing(), a fresh init_tracing() works."""
        init_tracing(disabled_config)
        shutdown_tracing()
        init_tracing(disabled_config)  # should succeed
        shutdown_tracing()

    def test_reset_tracing_for_test_isolation(self, disabled_config: ServiceConfig) -> None:
        """reset_tracing() clears module state so a clean init follows."""
        init_tracing(disabled_config)
        reset_tracing()
        init_tracing(disabled_config)
        shutdown_tracing()


# ---------------------------------------------------------------------------
# 5.2: NoOp mode
# ---------------------------------------------------------------------------


class TestNoOpMode:
    """Tests for tracing-disabled paths (NoOp TracerProvider)."""

    def test_tracing_enabled_false(self) -> None:
        """Explicit tracing_enabled=False initialises a NoOp provider."""
        config = ServiceConfig(tracing_enabled=False)
        init_tracing(config)
        tracer = get_tracer("test-noop")
        span = tracer.start_span("should_be_noop")
        assert not span.get_span_context().is_valid
        span.end()
        shutdown_tracing()

    def test_otel_traces_exporter_none_overrides(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """OTEL_TRACES_EXPORTER=none forces NoOp even when tracing_enabled=True."""
        monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")
        config = ServiceConfig(tracing_enabled=True)
        init_tracing(config)
        tracer = get_tracer("test-exporter-none")
        span = tracer.start_span("disabled")
        assert not span.get_span_context().is_valid
        span.end()
        shutdown_tracing()


# ---------------------------------------------------------------------------
# 5.3: W3C trace context inject/extract round-trip
# ---------------------------------------------------------------------------


class TestW3CPropagation:
    """Tests for W3C trace context injection and extraction."""

    def test_inject_extract_round_trip(self) -> None:
        """Inject headers from a span, extract them back, verify parent-child."""
        from opentelemetry.sdk.trace import (  # noqa: PLC0415
            TracerProvider,
        )
        from opentelemetry.sdk.trace.export import (  # noqa: PLC0415
            SimpleSpanProcessor,
        )

        exporter, spans = _make_list_exporter()
        provider = TracerProvider(
            active_span_processor=SimpleSpanProcessor(exporter),  # type: ignore[arg-type]
        )
        tracer = provider.get_tracer("test-roundtrip")

        with tracer.start_as_current_span("producer") as span:
            span.set_attribute(ATTR_CORRELATION_ID, "test-correlation-123")
            # Injection via the single consolidated injection point
            headers = inject_trace_headers()
            assert "traceparent" in headers

            # Extraction at the boundary
            ctx = extract_context(headers)
            assert ctx is not None

            # Create a child span using the extracted context
            with tracer.start_as_current_span(
                "consumer", context=ctx
            ) as child:
                child.set_attribute(ATTR_CORRELATION_ID, "test-correlation-123")

        # Spans are finished inner-first: consumer, then producer.
        assert len(spans) == 2

        # Identify spans by name (order-independent)
        consumer = next(s for s in spans if s.name == "consumer")
        producer = next(s for s in spans if s.name == "producer")
        assert consumer.parent is not None
        assert consumer.parent.span_id == producer.context.span_id

    # -----------------------------------------------------------------------
    # fail-safe extraction
    # -----------------------------------------------------------------------

    def test_extract_empty_headers_returns_empty_context(self) -> None:
        """Empty headers produce an empty Context, not a crash."""
        ctx = extract_context({})
        assert ctx is not None

    def test_extract_missing_traceparent(self) -> None:
        """Headers with no traceparent yield empty Context."""
        ctx = extract_context({"tracestate": "foo=bar"})
        assert ctx is not None

    def test_extract_garbage_traceparent(self) -> None:
        """Garbage traceparent value does not crash (fail-safe)."""
        ctx = extract_context({"traceparent": "not-a-valid-traceparent"})
        assert ctx is not None

    def test_extract_truncated_traceparent(self) -> None:
        """Too-short traceparent value is handled gracefully."""
        ctx = extract_context({"traceparent": "00-abc-def-01"})
        assert ctx is not None

    def test_extract_valid_looking_traceparent(self) -> None:
        """Well-formed traceparent extracts a span context."""
        ctx = extract_context(
            {
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
            }
        )
        assert ctx is not None


# ---------------------------------------------------------------------------
# trace_plugin_method decorator tests
# ---------------------------------------------------------------------------


class TestTracePluginMethod:
    """Tests for the @trace_plugin_method decorator."""

    def test_decorator_wraps_method_and_returns_value(self) -> None:
        """Decorated method returns the original function's result."""

        class TestPlugin:
            @trace_plugin_method("test.method", attributes={"key": "val"})
            def my_method(self) -> int:
                return 42

        plugin = TestPlugin()
        result = plugin.my_method()
        assert result == 42

    def test_decorator_preserves_function_metadata(self) -> None:
        """Decorator preserves __name__ and __doc__ on the wrapper."""

        class TestPlugin:
            @trace_plugin_method("test.metadata")
            def documented_method(self) -> str:
                """Original docstring."""
                return "ok"

        plugin = TestPlugin()
        assert plugin.documented_method.__name__ == "documented_method"
        assert plugin.documented_method.__doc__ == "Original docstring."
        assert plugin.documented_method() == "ok"

    def test_decorator_raises_on_generator(self) -> None:
        """Generator functions are rejected at decoration time with TypeError."""

        with pytest.raises(TypeError, match="generator"):

            class BadPlugin:  # noqa: PT005
                @trace_plugin_method("test.generator")
                def my_generator(self) -> Any:  # noqa: PT022
                    yield 1


# ---------------------------------------------------------------------------
# Configuration validation
# ---------------------------------------------------------------------------


class TestConfigValidation:
    """Tests for tracing-related ServiceConfig validation."""

    @pytest.mark.parametrize("rate", [0.0, 0.5, 1.0])
    def test_sample_rate_in_range(self, rate: float) -> None:
        """Valid sample rates create a ServiceConfig without error."""
        config = ServiceConfig(tracing_sample_rate=rate)
        assert config.tracing_sample_rate == rate

    def test_sample_rate_below_zero_raises(self) -> None:
        """Negative sample rate raises ConfigurationError."""
        with pytest.raises(ConfigurationError):
            ServiceConfig(tracing_sample_rate=-0.1)

    def test_sample_rate_above_one_raises(self) -> None:
        """Sample rate > 1.0 raises ConfigurationError."""
        with pytest.raises(ConfigurationError):
            ServiceConfig(tracing_sample_rate=1.5)

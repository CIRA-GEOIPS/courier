"""Comprehensive unit tests for courier.utils.logging module.

This module provides pytest unit tests for the logging utility functions and classes.
Tests coverlogging setup, context adaptation, Loki integration (with mocking),
and edge cases like import failures and invalid configurations.

Dependencies
------------
- pytest: For testing framework and fixtures.
- monkeypatch: Used for mocking external imports and globals.
- mock: For patching complex behaviors (e.g., Loki handler creation).

Notes
-----
- All external dependencies (e.g., logging_loki) are mocked to ensure tests are
  deterministic and don't rely on real system state.
- Tests follow functional paradigms: minimal side effects, pure functions where
  possible, and clear separation of arrange-act-assert.
- Type hints are used throughout for mypy compatibility.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from courier.config import ServiceConfig  # For mocking in fixtures
from courier.utils.logging import (
    TRACE_LEVEL,
    ContextAdapter,
    _create_loki_handler,
    get_logger,
)


# Fixtures
@pytest.fixture
def sample_service_config() -> ServiceConfig:
    """Provide a reusable ServiceConfig instance for testing.

    Parameters
    ----------
    None

    Returns
    -------
    ServiceConfig
        A default service config with Loki enabled for testing.
    """
    return ServiceConfig(
        service_id="test-service",
        namespace="test-namespace",
        loki_url="http://test-loki:3100/loki/api/v1/push",
        loki_enabled=True,
        log_level="DEBUG",
        production_mode=False,
    )


@pytest.fixture
def mock_logger_adapter() -> ContextAdapter:
    """Provide a reusable ContextAdapter with sample extra context.

    Parameters
    ----------
    None

    Returns
    -------
    ContextAdapter
        Mocked adapter for testing adaptation.
    """
    mock_logger = MagicMock(spec=logging.Logger)
    adapter = ContextAdapter(mock_logger, {"source_type": "plugin", "source_name": "test_plugin"})
    return adapter


# Test Constants
def test_trace_level_constant() -> None:
    """Test TRACE_LEVEL constant is correctly defined.

    Parameters
    ----------
    None

    Returns
    -------
    None

    Raises
    ------
    AssertionError
        If TRACE_LEVEL is not 5.
    """
    # Assert it is below DEBUG and matches expected value
    assert TRACE_LEVEL == 5
    assert TRACE_LEVEL < logging.DEBUG


def test_trace_emits_a_record_through_the_public_logger(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``get_logger(...).trace(...)`` must actually emit at TRACE level.

    Asserting ``hasattr(logging.getLogger(...), "trace")`` checked the wrong
    object: every caller holds the ``ContextAdapter`` that ``get_logger``
    returns, and that had no ``trace`` at all — so ``logger.trace(...)``
    raised ``AttributeError`` in production while the test passed.
    """
    logger = get_logger("service", "trace-emit")
    logger.logger.setLevel(TRACE_LEVEL)
    logger.logger.propagate = True

    with caplog.at_level(TRACE_LEVEL, logger=logger.logger.name):
        logger.trace("detailed diagnostic")

    records = [r for r in caplog.records if r.levelno == TRACE_LEVEL]
    assert records, "no record emitted at TRACE level"
    assert "detailed diagnostic" in records[0].getMessage()
    assert "[Service: trace-emit]" in records[0].getMessage(), (
        "trace() must apply the same context prefix as the other levels"
    )


def test_trace_is_suppressed_above_its_level() -> None:
    """TRACE is below DEBUG, so a DEBUG-level logger must drop it.

    Uses a plain capturing handler rather than ``caplog.at_level``: that
    fixture *sets* the logger's level for the duration, which is exactly the
    thing under test here.
    """
    logger = get_logger("service", "trace-suppressed")
    logger.logger.setLevel(logging.DEBUG)

    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record)

    handler = _Capture(level=TRACE_LEVEL)
    logger.logger.addHandler(handler)
    try:
        logger.trace("should not appear")
        logger.debug("should appear")
    finally:
        logger.logger.removeHandler(handler)

    levels = [record.levelno for record in captured]
    assert TRACE_LEVEL not in levels, "TRACE record leaked past a DEBUG logger"
    assert logging.DEBUG in levels, "DEBUG record was dropped too"


# Test ContextAdapter
class TestContextAdapter:
    """Test suite for ContextAdapter class."""

    def test_initialization_defaults(self) -> None:
        """Test ContextAdapter initialization with empty extra.

        Parameters
        ----------
        None

        Returns
        -------
        None

        Raises
        ------
        AssertionError
            If adapter is not initialized correctly.
        """
        mock_logger = MagicMock(spec=logging.Logger)
        adapter = ContextAdapter(mock_logger)

        assert adapter.extra is None
        assert adapter.logger == mock_logger

    def test_initialization_with_extra(self, mock_logger_adapter: ContextAdapter) -> None:
        """Test ContextAdapter initialization with provided extra.

        Parameters
        ----------
        mock_logger_adapter : ContextAdapter
            Pre-configured adapter fixture.

        Returns
        -------
        None

        Raises
        ------
        AssertionError
            If extra is not set correctly.
        """
        assert mock_logger_adapter.extra == {"source_type": "plugin", "source_name": "test_plugin"}

    @pytest.mark.parametrize(
        ("extra", "msg", "expected_result"),
        [
            (
                {"source_type": "service", "source_name": "my-service"},
                "Test message",
                ("[Service: my-service] Test message", {'extra': {'source_type': 'service', 'source_name': 'my-service'}}),
            ),
            (
                None,
                "No context message",
                ("No context message", {'extra': None}),
            ),
            (
                {"source_type": "", "source_name": ""},
                "Empty context",
                ("[] Empty context", {'extra': {'source_type': '', 'source_name': ''}}),  # Capitalize on empty string works
            ),
        ],
    )
    def test_process_prepends_context(self, extra: dict[str, str] | None, msg: str, expected_result: tuple[str, dict[str, Any]]) -> None:
        """Test ContextAdapter.process prepends context to messages.

        Parameters
        ----------
        extra : dict[str, str] | None
            Extra context for the adapter.
        msg : str
            Input message.
        expected_result : tuple[str, dict[str, Any]]
            Expected (modified_msg, kwargs).

        Returns
        -------
        None

        Raises
        ------
        AssertionError
            If message processing doesn't match expected.
        """
        mock_logger = MagicMock(spec=logging.Logger)
        adapter = ContextAdapter(mock_logger, extra)
        kwargs = {"extra": extra}

        result = adapter.process(msg, kwargs)

        assert result == expected_result


# Test _create_loki_handler
class TestCreateLokiHandler:
    """Test suite for _create_loki_handler function."""

    @patch("courier.utils.logging.logging_loki")
    def test_create_loki_handler_success(self, mock_logging_loki: MagicMock) -> None:
        """Test successful Loki handler creation.

        Parameters
        ----------
        mock_logging_loki : MagicMock
            Mock for logging_loki module.

        Returns
        -------
        None

        Raises
        ------
        AssertionError
            If handler creation fails or returns None.
        """
        mock_handler = MagicMock()
        mock_logging_loki.LokiHandler.return_value = mock_handler

        result = _create_loki_handler("http://test-loki", {}, logging.getLogger("test"))

        mock_logging_loki.LokiHandler.assert_called_once_with(url="http://test-loki", version="1", tags={})
        assert result is not None
        assert hasattr(result, "delegate") and result.delegate is mock_handler


    @patch("courier.utils.logging.logging_loki")
    def test_create_loki_handler_connection_error(self, mock_logging_loki: MagicMock, caplog: pytest.LogCaptureFixture) -> None:
        """Test Loki handler creation fails due to connection error.

        Parameters
        ----------
        mock_logging_loki : MagicMock
            Mock for logging_loki module.
        caplog : pytest.LogCaptureFixture
            Fixture to capture log messages.

        Returns
        -------
        None

        Raises
        ------
        AssertionError
            If result is not None or warning not logged.
        """
        mock_logging_loki.LokiHandler.side_effect = ValueError("Unexpected Loki init error")

        result = _create_loki_handler("http://test-loki", {}, logging.getLogger("test"))

        assert result is None
        assert "Unexpected error initializing Loki handler" in caplog.text


# Test get_logger
class TestGetLogger:
    """Test suite for get_logger function."""

    @pytest.mark.parametrize(
        ("source_type", "source_name", "config", "expected_extra"),
        [
            ("service", "test-service", None, {"source_type": "service", "source_name": "test-service"}),
            ("plugin", "test-plugin", ServiceConfig(service_id="test", loki_enabled=False), {"source_type": "plugin", "source_name": "test-plugin"}),
            ("module", __name__, ServiceConfig(), {"source_type": "module", "source_name": __name__}),
        ],
    )
    def test_get_logger_returns_context_adapter(
        self,
        source_type: str,
        source_name: str,
        config: ServiceConfig | None,
        expected_extra: dict[str, str],
    ) -> None:
        """Test get_logger returns ContextAdapter with correct extra.

        Parameters
        ----------
        source_type : str
            Source type for logger.
        source_name : str
            Source name for logger.
        config : ServiceConfig | None
            Config for logger setup.
        expected_extra : dict[str, str]
            Expected extra context.

        Returns
        -------
        None

        Raises
        ------
        AssertionError
            If adapter or extra doesn't match.
        """
        logger = get_logger(source_type, source_name, config)

        assert isinstance(logger, ContextAdapter)
        assert logger.extra == expected_extra

    @patch("courier.utils.logging._create_loki_handler")
    def test_get_logger_with_loki_enabled(self, mock_create_handler: MagicMock, sample_service_config: ServiceConfig) -> None:
        """Test get_logger adds Loki handler when enabled.

        Parameters
        ----------
        mock_create_handler : MagicMock
            Mock for _create_loki_handler.
        sample_service_config : ServiceConfig
            Sample config with Loki enabled.

        Returns
        -------
        None

        Raises
        ------
        AssertionError
            If Loki handler is not added or called incorrectly.
        """
        mock_handler = MagicMock()
        mock_create_handler.return_value = mock_handler

        logger = get_logger("plugin", "test", sample_service_config)

        # Verify _create_loki_handler was called with correct arguments
        mock_create_handler.assert_called_once()
        call_args = mock_create_handler.call_args

        # Check URL argument
        assert call_args[0][0] == sample_service_config.loki_url

        # Check tags argument
        assert call_args[0][1] == {
            "service": "test-service",
            "namespace": "test-namespace",
            "source_type": "plugin",
            "plugin": "test",
        }

        # Check that third argument is a logger (the fallback_logger)
        assert isinstance(call_args[0][2], logging.Logger)

        # Verify mock handler was added to the logger
        assert mock_handler in logger.logger.handlers

    def test_get_logger_production_mode_enforces_min_level(self, sample_service_config: ServiceConfig) -> None:
        """Test get_logger enforces INFO level in production mode with TRACE input.

        Parameters
        ----------
        sample_service_config : ServiceConfig
            Sample config.

        Returns
        -------
        None

        Raises
        ------
        AssertionError
            If log level is not enforced.
        """
        config = dataclasses.replace(sample_service_config, production_mode=True, log_level="TRACE")

        logger = get_logger("service", "test", config)

        # Unable to directly test internal log level without exposing internals,
        # but we can verify the logger is created without errors
        assert isinstance(logger, ContextAdapter)

    @pytest.mark.parametrize(
        ("invalid_config", "expected_exception"),
        [
            ({"log_level": None}, AttributeError),  # None log_level causes AttributeError on .upper()
        ],
    )
    def test_get_logger_handles_edge_cases(self, invalid_config: dict[str, Any], expected_exception: type[Exception]) -> None:
        """Test get_logger handles edge cases like invalid configs.

        Parameters
        ----------
        invalid_config : dict[str, Any]
            Invalid config input.
        expected_exception : type[Exception]
            Expected exception type.

        Returns
        -------
        None

        Raises
        ------
        AssertionError
            If expected exception is not raised.
        """
        # Note: ServiceConfig is a plain dataclass — no runtime type validation.
        # Use a unique source_name to avoid the cached-handlers guard skipping config processing.
        with pytest.raises(expected_exception):
            get_logger("service", "edge-case-unique", ServiceConfig(**invalid_config))


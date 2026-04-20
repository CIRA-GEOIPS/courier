"""Shared fixtures for routing unit tests.

Routing tests instantiate plugin classes that register Prometheus
metrics at import time. Two consecutive tests constructing the same
plugin reuse the existing collectors (metrics are class-level), but
the ``_reset_prometheus_registry`` fixture guards against per-instance
collectors added in future plugin variants.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY


@pytest.fixture(autouse=True)
def _reset_prometheus_registry():
    pre = list(REGISTRY._collector_to_names.keys())  # noqa: SLF001
    yield
    for collector in list(REGISTRY._collector_to_names.keys()):  # noqa: SLF001
        if collector not in pre:
            try:
                REGISTRY.unregister(collector)
            except KeyError:
                pass


@pytest.fixture
def mock_service() -> MagicMock:
    """Minimal service stub used by routing plugin tests."""
    service = MagicMock()
    service._config = MagicMock()
    service._config.log_level = "DEBUG"
    service._config.loki_enabled = False
    service._config.namespace = "test-ns"
    service.config = service._config
    return service
